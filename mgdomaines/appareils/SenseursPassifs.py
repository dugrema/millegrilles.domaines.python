# Module avec les classes de donnees, processus et gestionnaire de sous domaine mgdomaines.appareils.SenseursPassifs
from millegrilles import Constantes
from millegrilles.Domaines import GestionnaireDomaine
from millegrilles.processus.MGProcessus import MGProcessus, MGProcessusTransaction
from millegrilles.dao.MessageDAO import BaseCallback
from bson.objectid import ObjectId
import datetime


# Constantes pour SenseursPassifs
class SenseursPassifsConstantes:

    COLLECTION_NOM = 'mgdomaines_appareils_SenseursPassifs'

    LIBELLE_DOCUMENT_SENSEUR = 'senseur.individuel'
    LIBELLE_DOCUMENT_NOEUD = 'noeud.individuel'
    LIBELLE_DOCUMENT_GROUPE = 'groupe.senseurs'

    TRANSACTION_NOEUD = 'noeud'
    TRANSACTION_ID_SENSEUR = 'senseur'
    TRANSACTION_DATE_LECTURE = 'temps_lecture'
    TRANSACTION_LOCATION = 'location'
    TRANSACTION_VALEUR_DOMAINE = 'mgdomaines.appareils.SenseursPassifs.lecture'

    EVENEMENT_MAJ_HORAIRE = 'miseajour.horaire'
    EVENEMENT_MAJ_QUOTIDIENNE = 'miseajour.quotidienne'

# Gestionnaire pour le domaine mgdomaines.appareils.SenseursPassifs.
class GestionnaireSenseursPassifs(GestionnaireDomaine):

    def __init__(self):
        super().__init__()
        self._traitement_lecture = None
        self.traiter_transaction = None   # Override de la methode super().traiter_transaction

    def configurer(self):
        super().configurer()

        self._traitement_lecture = TraitementMessageLecture(self)
        self.traiter_transaction = self._traitement_lecture.callbackAvecAck   # Transfert methode

        nom_millegrille = self.configuration.nom_millegrille
        nom_queue_senseurspassifs = self.get_nom_queue()

        # Configurer la Queue pour SenseursPassifs sur RabbitMQ
        self.message_dao.channel.queue_declare(
            queue=nom_queue_senseurspassifs,
            durable=True)

        self.message_dao.channel.queue_bind(
            exchange=self.configuration.exchange_evenements,
            queue=nom_queue_senseurspassifs,
            routing_key='%s.destinataire.domaine.mgdomaines.appareils.SenseursPassifs.#' % nom_millegrille
        )

    def traiter_backlog(self):
        pass

    def get_nom_queue(self):
        nom_millegrille = self.configuration.nom_millegrille
        nom_queue_senseurspassifs = 'mg.%s.mgdomaines.appareils.SenseursPassifs' % nom_millegrille
        return nom_queue_senseurspassifs


class TraitementMessageLecture(BaseCallback):

    def __init__(self, gestionnaire):
        super().__init__(gestionnaire.configuration)
        self._gestionnaire = gestionnaire

    def traiter_message(self, ch, method, properties, body):
        message_dict = self.json_helper.bin_utf8_json_vers_dict(body)
        evenement = message_dict.get("evenements")

        if evenement == Constantes.EVENEMENT_TRANSACTION_PERSISTEE:
            processus = "mgdomaines_appareils_SenseursPassifs:ProcessusTransactionSenseursPassifsLecture"
            self._gestionnaire.demarrer_processus(processus, message_dict)
        elif evenement == SenseursPassifsConstantes.EVENEMENT_MAJ_HORAIRE:
            processus = "mgdomaines_appareils_SenseursPassifs:ProcessusTransactionSenseursPassifsMAJHoraire"
            self._gestionnaire.demarrer_processus(processus, message_dict)
        elif evenement == SenseursPassifsConstantes.EVENEMENT_MAJ_QUOTIDIENNE:
            processus = "mgdomaines_appareils_SenseursPassifs:ProcessusTransactionSenseursPassifsMAJQuotidienne"
            self._gestionnaire.demarrer_processus(processus, message_dict)
        else:
            # Type d'evenement inconnu, on lance une exception
            raise ValueError("Type d'evenement inconnu: %s" % evenement)


# Classe qui produit et maintient un document de metadonnees et de lectures pour un SenseurPassif.
class ProducteurDocumentSenseurPassif:

    def __init__(self, message_dao, document_dao):
        self._message_dao = message_dao
        self._document_dao = document_dao

    ''' 
    Extrait l'information d'une lecture de senseur passif pour creer ou mettre a jour le document du senseur.
    
    :param transaction: Document de la transaction.
    :return: L'identificateur mongo _id du document de senseur qui a ete cree/modifie.
    '''
    def maj_document_senseur(self, transaction):

        # Verifier si toutes les cles sont presentes
        contenu_transaction = transaction[Constantes.TRANSACTION_MESSAGE_LIBELLE_CHARGE_UTILE].copy()
        noeud = contenu_transaction[SenseursPassifsConstantes.TRANSACTION_NOEUD]
        id_appareil = contenu_transaction[SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR]
        date_lecture_epoch = contenu_transaction[SenseursPassifsConstantes.TRANSACTION_DATE_LECTURE]

        # Transformer les donnees en format natif (plus facile a utiliser plus tard)
        date_lecture = datetime.datetime.fromtimestamp(date_lecture_epoch)   # Mettre en format date standard
        contenu_transaction[SenseursPassifsConstantes.TRANSACTION_DATE_LECTURE] = date_lecture

        # Preparer le critere de selection de la lecture. Utilise pour trouver le document courant et pour l'historique
        selection = {
            Constantes.DOCUMENT_INFODOC_LIBELLE: SenseursPassifsConstantes.LIBELLE_DOCUMENT_SENSEUR,
            SenseursPassifsConstantes.TRANSACTION_NOEUD: noeud,
            SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR: id_appareil,
            SenseursPassifsConstantes.TRANSACTION_DATE_LECTURE: {'$lt': date_lecture}
        }

        # Effectuer une maj sur la date de derniere modification.
        # Inserer les champs par defaut lors de la creation du document.
        operation = {
            '$currentDate': {Constantes.DOCUMENT_INFODOC_DERNIERE_MODIFICATION: True},
            '$setOnInsert': {Constantes.DOCUMENT_INFODOC_LIBELLE: SenseursPassifsConstantes.LIBELLE_DOCUMENT_SENSEUR}
        }

        # Si location existe, s'assurer de l'ajouter uniquement lors de l'insertion (possible de changer manuellement)
        if contenu_transaction.get(SenseursPassifsConstantes.TRANSACTION_LOCATION) is not None:
            operation['$setOnInsert'][SenseursPassifsConstantes.TRANSACTION_LOCATION] = \
                contenu_transaction.get(SenseursPassifsConstantes.TRANSACTION_LOCATION)
            del contenu_transaction[SenseursPassifsConstantes.TRANSACTION_LOCATION]

        # Mettre a jour les informations du document en copiant ceux de la transaction
        operation['$set'] = contenu_transaction

        print("Donnees senseur passif: selection=%s, operation=%s" % (str(selection), str(operation)))

        collection = self._document_dao.get_collection(SenseursPassifsConstantes.COLLECTION_NOM)
        document_senseur = collection.find_one_and_update(filter=selection, update=operation, upsert=False, fields="_id:1")

        # Verifier si un document a ete modifie.
        if document_senseur is None:
            # Aucun document n'a ete modifie. Verifier si c'est parce qu'il n'existe pas. Sinon, le match a echoue
            # parce qu'une lecture plus recente a deja ete enregistree (c'est OK)
            selection_sansdate = selection.copy()
            del selection_sansdate[SenseursPassifsConstantes.TRANSACTION_DATE_LECTURE]
            document_senseur = collection.find_one(filter=selection_sansdate)

            if document_senseur is None:
                # Executer la meme operation avec upsert=True pour inserer un nouveau document
                resultat_update = collection.update_one(filter=selection, update=operation, upsert=True)
                document_senseur = {'_id': resultat_update.upserted_id}
                print("_id du nouveau document: %s" % str(resultat_update.upserted_id))
            else:
                print("Document existant non MAJ: %s" % str(document_senseur))
                document_senseur = None
        else:
            print("MAJ update: %s" % str(document_senseur))

        return document_senseur

    ''' 
    Calcule les moyennes/min/max de la derniere journee pour un senseur avec donnees numeriques. 
    
    :param id_document_senseur: _id de base de donnees Mongo pour le senseur a mettre a jour.
    '''
    def calculer_aggregation_journee(self, id_document_senseur):

        senseur_objectid_key = {"_id": ObjectId(id_document_senseur)}

        # Charger l'information du senseur
        collection_senseurs = self._document_dao.get_collection(SenseursPassifsConstantes.COLLECTION_NOM)
        document_senseur = collection_senseurs.find_one(senseur_objectid_key)

        print("Document charge: %s" % str(document_senseur))

        noeud = document_senseur[SenseursPassifsConstantes.TRANSACTION_NOEUD]
        no_senseur = document_senseur[SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR]

        regroupement_champs = {
            'temperature-moyenne': {'$avg': '$charge-utile.temperature'},
            'humidite-moyenne': {'$avg': '$charge-utile.humidite'},
            'pression-moyenne': {'$avg': '$charge-utile.pression'}
        }

        # Creer l'intervalle pour les donnees. Utiliser timezone pour s'assurer de remonter un nombre d'heures correct
        time_range_from, time_range_to = ProducteurDocumentSenseurPassif.calculer_daterange(hours=25)

        # Transformer en epoch (format de la transaction)
        time_range_to = int(time_range_to.timestamp())
        time_range_from = int(time_range_from.timestamp())

        selection = {
            'info-transaction.domaine': SenseursPassifsConstantes.TRANSACTION_VALEUR_DOMAINE,
            'charge-utile.temps_lecture': {'$gte': time_range_from, '$lt': time_range_to},
            'charge-utile.senseur': no_senseur,
            'charge-utile.noeud': noeud
        }

        # Noter l'absence de timezone - ce n'est pas important pour le regroupement par heure.
        regroupement_periode = {
            'year': {'$year': {'$arrayElemAt': ['$evenements.transaction_nouvelle', 0]}},
            'month': {'$month': {'$arrayElemAt': ['$evenements.transaction_nouvelle', 0]}},
            'day': {'$dayOfMonth': {'$arrayElemAt': ['$evenements.transaction_nouvelle', 0]}},
            'hour': {'$hour': {'$arrayElemAt': ['$evenements.transaction_nouvelle', 0]}},
        }

        regroupement = {
            '_id': {
                'noeud': '$charge-utile.noeud',
                'senseur': '$charge-utile.senseur',
                'periode': {
                    '$dateFromParts': regroupement_periode
                }
            }
        }
        regroupement.update(regroupement_champs)

        operation = [
            {'$match': selection},
            {'$group': regroupement},
        ]

        print("Operation aggregation: %s" % str(operation))

        collection_transactions = self._document_dao.get_collection(Constantes.DOCUMENT_COLLECTION_TRANSACTIONS)
        resultat_curseur = collection_transactions.aggregate(operation)

        resultat = []
        for res in resultat_curseur:
            # Extraire la date, retirer le reste de la cle (redondant, ca va deja etre dans le document du senseur)
            res['periode'] = res['_id']['periode']
            del res['_id']
            resultat.append(res)

        # Trier les resultats en ordre decroissant de date
        resultat.sort(key=lambda res2: res2['periode'], reverse=True)
        for res in resultat:
            print("Resultat: %s" % res)

        # Sauvegarde de l'information dans le document du senseur
        operation_update = {
            '$set': {'moyennes_dernier_jour': resultat},
            '$currentDate': {Constantes.DOCUMENT_INFODOC_DERNIERE_MODIFICATION: True}
        }
        collection_senseurs.update_one(filter=senseur_objectid_key, update=operation_update, upsert=False)

    '''
    Calcule les moyennes/min/max du dernier mois pour un senseur avec donnees numeriques.
    
    :param id_document_senseur: _id de base de donnees Mongo pour le senseur a mettre a jour.
    '''
    def calculer_aggregation_mois(self, id_document_senseur):
        senseur_objectid_key = {"_id": ObjectId(id_document_senseur)}

        # Charger l'information du senseur
        collection_senseurs = self._document_dao.get_collection(SenseursPassifsConstantes.COLLECTION_NOM)
        document_senseur = collection_senseurs.find_one(senseur_objectid_key)

        print("Document charge: %s" % str(document_senseur))

        noeud = document_senseur[SenseursPassifsConstantes.TRANSACTION_NOEUD]
        no_senseur = document_senseur[SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR]

        regroupement_champs = {
            'temperature-maximum': {'$max': '$charge-utile.temperature'},
            'temperature-minimum': {'$min': '$charge-utile.temperature'},
            'humidite-maximum': {'$max': '$charge-utile.humidite'},
            'humidite-minimum': {'$min': '$charge-utile.humidite'},
            'pression-maximum': {'$max': '$charge-utile.pression'},
            'pression-minimum': {'$min': '$charge-utile.pression'}
        }

        # Creer l'intervalle pour les donnees
        time_range_from, time_range_to = ProducteurDocumentSenseurPassif.calculer_daterange(days=31)

        # Transformer en epoch (format de la transaction)
        time_range_to = int(time_range_to.timestamp())
        time_range_from = int(time_range_from.timestamp())

        selection = {
            'info-transaction.domaine': SenseursPassifsConstantes.TRANSACTION_VALEUR_DOMAINE,
            'charge-utile.temps_lecture': {'$gte': time_range_from, '$lt': time_range_to},
            'charge-utile.senseur': no_senseur,
            'charge-utile.noeud': noeud
        }

        # Noter l'utilisation de la timezone pour le regroupement. Important pour faire la separation des donnees
        # correctement.
        regroupement_periode = {
            'year': {'$year':  {'date': '$info-transaction.estampille', 'timezone': 'America/Montreal'}},
            'month': {'$month':  {'date': '$info-transaction.estampille', 'timezone': 'America/Montreal'}},
            'day': {'$dayOfMonth':  {'date': '$info-transaction.estampille', 'timezone': 'America/Montreal'}}
        }

        regroupement = {
            '_id': {
                'noeud': '$charge-utile.noeud',
                'senseur': '$charge-utile.senseur',
                'periode': {
                    '$dateFromParts': regroupement_periode
                }
            }
        }
        regroupement.update(regroupement_champs)

        operation = [
            {'$match': selection},
            {'$group': regroupement},
        ]

        print("Operation aggregation: %s" % str(operation))

        collection_transactions = self._document_dao.get_collection(Constantes.DOCUMENT_COLLECTION_TRANSACTIONS)
        resultat_curseur = collection_transactions.aggregate(operation)

        resultat = []
        for res in resultat_curseur:
            # Extraire la date, retirer le reste de la cle (redondant, ca va deja etre dans le document du senseur)
            res['periode'] = res['_id']['periode']
            del res['_id']
            resultat.append(res)

        # Trier les resultats en ordre decroissant de date
        resultat.sort(key=lambda res2: res2['periode'], reverse=True)
        for res in resultat:
            print("Resultat: %s" % res)

        # Sauvegarde de l'information dans le document du senseur
        operation_update = {
            '$set': {'extremes_dernier_mois': resultat},
            '$currentDate': {Constantes.DOCUMENT_INFODOC_DERNIERE_MODIFICATION: True}
        }
        collection_senseurs.update_one(filter=senseur_objectid_key, update=operation_update, upsert=False)

    ''' 
    Methode qui calcule un date range a partir de maintenant
    
    :param days: Nombre de jour a remonter (passe) 
    :param hours: Nombre de jour a remonter (passe)
    :return: Format datetime, from, to 
    '''
    @staticmethod
    def calculer_daterange(days=0, hours=0):
        date_reference = datetime.datetime.utcnow()
        time_range_to = datetime.datetime(date_reference.year, date_reference.month,
                                          date_reference.day,
                                          date_reference.hour)
        time_range_from = time_range_to - datetime.timedelta(days=days, hours=hours)
        time_range_from = time_range_from.replace(minute=0, second=0, microsecond=0)
        if days > 0 and hours == 0:  # Ajuster pour avoir la journee au complet
            time_range_from = time_range_from.replace(hour=0)

        return time_range_from, time_range_to

    '''
    Retourne les _id de tous les documents de senseurs. 
    '''
    def trouver_id_documents_senseurs(self):
        collection_senseurs = self._document_dao.get_collection(SenseursPassifsConstantes.COLLECTION_NOM)

        selection = {
            Constantes.DOCUMENT_INFODOC_LIBELLE: SenseursPassifsConstantes.LIBELLE_DOCUMENT_SENSEUR,
        }
        documents = collection_senseurs.find(filter=selection, projection={'_id': 1})

        # Extraire les documents du curseur, change de ObjectId vers un string
        document_ids = []
        for doc in documents:
            document_ids.append(str(doc['_id']))

        return document_ids

# Classe qui gere le document pour un noeud. Supporte une mise a jour incrementale des donnees.
class ProducteurDocumentNoeud:

    def __init__(self, message_dao, document_dao):
        self._message_dao = message_dao
        self._document_dao = document_dao

    '''
    Mise a jour du document de noeud par une transaction senseur passif
    
    :param id_document_senseur: _id du document du senseur.
    '''
    def maj_document_noeud_senseurpassif(self, id_document_senseur):

        collection_senseurs = self._document_dao.get_collection(SenseursPassifsConstantes.COLLECTION_NOM)
        document_senseur = collection_senseurs.find_one(ObjectId(id_document_senseur))

        noeud = document_senseur['noeud']
        no_senseur = document_senseur[SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR]

        champs_a_copier = ['bat_mv', 'humidite', 'temperature', 'pression', 'temps_lecture', 'location']
        valeurs = {}
        for champ in champs_a_copier:
            valeur = document_senseur.get(champ)
            if valeur is not None:
                valeurs[champ] = valeur

        donnees_senseur = {
            'dict_senseurs.%s' % str(no_senseur): valeurs
        }

        filtre = {
            Constantes.DOCUMENT_INFODOC_LIBELLE: SenseursPassifsConstantes.LIBELLE_DOCUMENT_NOEUD,
            'noeud': noeud
        }

        update = {
            '$currentDate': {Constantes.DOCUMENT_INFODOC_DERNIERE_MODIFICATION: True},
            '$setOnInsert': filtre,
            '$set': donnees_senseur
        }

        collection_senseurs.update_one(filter=filtre, update=update, upsert=True)


# Processus pour enregistrer une transaction d'un senseur passif
class ProcessusTransactionSenseursPassifsLecture(MGProcessusTransaction):

    def __init__(self, controleur, evenement):
        super().__init__(controleur, evenement)

    ''' Enregistrer l'information de la transaction dans le document du senseur '''
    def initiale(self):
        doc_transaction = self.charger_transaction()
        print("Document processus: %s" % self._document_processus)
        print("Document transaction: %s" % doc_transaction)

        producteur_document = ProducteurDocumentSenseurPassif(self.message_dao(), self.document_dao())
        document_senseur = producteur_document.maj_document_senseur(doc_transaction)

        parametres = None
        if document_senseur and document_senseur.get("_id") is not None:
            # Preparer la prochaine etape - mettre a jour le noeud
            parametres = {"id_document_senseur": document_senseur.get("_id")}
            self.set_etape_suivante(ProcessusTransactionSenseursPassifsLecture.maj_noeud.__name__)
        else:
            # Le document de senseur n'a pas ete modifie, probablement parce que les donnees n'etaient pas
            # les plus recentes. Il n'y a plus rien d'autre a faire.
            self.set_etape_suivante()   # Etape finale par defaut

        return parametres

    ''' Mettre a jour l'information du noeud pour ce senseur '''
    def maj_noeud(self):

        id_document_senseur = self._document_processus['parametres']['id_document_senseur']

        producteur_document = ProducteurDocumentNoeud(self.message_dao(), self.document_dao())
        producteur_document.maj_document_noeud_senseurpassif(id_document_senseur)

        self.set_etape_suivante()  # Etape finale


class ProcessusTransactionSenseursPassifsMAJHoraire(MGProcessus):

    def __init__(self, controleur, evenement):
        super().__init__(controleur, evenement)

    def initiale(self):
        # Faire liste des documents a mettre a jour
        producteur = ProducteurDocumentSenseurPassif(self.message_dao(), self.document_dao())
        liste_documents = producteur.trouver_id_documents_senseurs()

        parametres = {}
        if len(liste_documents) > 0:
            parametres['documents_senseurs'] = liste_documents
            self.set_etape_suivante(ProcessusTransactionSenseursPassifsMAJHoraire.calculer_moyennes.__name__)
        else:
            self.set_etape_suivante()   # Rien a faire, etape finale

        return parametres

    def calculer_moyennes(self):
        producteur = ProducteurDocumentSenseurPassif(self.message_dao(), self.document_dao())

        liste_documents = self._document_processus['parametres']['documents_senseurs']
        for doc_senseur in liste_documents:
            producteur.calculer_aggregation_journee(doc_senseur)

        self.set_etape_suivante()  # Rien a faire, etape finale


class ProcessusTransactionSenseursPassifsMAJQuotidienne(MGProcessus):

    def __init__(self, controleur, evenement):
        super().__init__(controleur, evenement)

    def initiale(self):
        # Faire liste des documents a mettre a jour
        producteur = ProducteurDocumentSenseurPassif(self.message_dao(), self.document_dao())
        liste_documents = producteur.trouver_id_documents_senseurs()

        parametres = {}
        if len(liste_documents) > 0:
            parametres['documents_senseurs'] = liste_documents
            self.set_etape_suivante(
                ProcessusTransactionSenseursPassifsMAJQuotidienne.calculer_valeurs_quotidiennes.__name__)
        else:
            self.set_etape_suivante()   # Rien a faire, etape finale

        return parametres

    def calculer_valeurs_quotidiennes(self):
        producteur = ProducteurDocumentSenseurPassif(self.message_dao(), self.document_dao())

        liste_documents = self._document_processus['parametres']['documents_senseurs']
        for doc_senseur in liste_documents:
            producteur.calculer_aggregation_mois(doc_senseur)

        self.set_etape_suivante()  # Rien a faire, etape finale

# Processus pour mettre a jour un document de noeud suite a une transaction de senseur passif
class ProcessusMAJNoeudsSenseurPassif(MGProcessus):

    def __init__(self, controleur, evenement):
        super().__init__(controleur, evenement)
