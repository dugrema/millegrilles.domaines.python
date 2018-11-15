# Module avec les classes de donnees, processus et gestionnaire de sous domaine mgdomaines.appareils.SenseursPassifs
from millegrilles import Constantes
from millegrilles.Domaines import GestionnaireDomaine
from bson.objectid import ObjectId
import datetime

# Constantes pour SenseursPassifs
class SenseursPassifsConstantes:

    COLLECTION_NOM = 'mgdomaines_appareils_SenseursPassifs'

    LIBELLE_DOCUMENT_SENSEUR = 'senseur.individuel'
    LIBELLE_DOCUMENT_NOEUD = 'senseur.noeud'
    LIBELLE_DOCUMENT_GROUPE = 'groupe.senseurs'

    TRANSACTION_NOEUD = 'noeud'
    TRANSACTION_ID_SENSEUR = 'senseur'
    TRANSACTION_DATE_LECTURE = 'temps_lecture'
    TRANSACTION_LOCATION = 'location'
    TRANSACTION_VALEUR_DOMAINE = 'mgdomaines.appareils.senseur'


# Gestionnaire pour le domaine mgdomaines.appareils.SenseursPassifs.
class GestionnaireSenseursPassifs(GestionnaireDomaine):

    def __init__(self):
        super().__init__()

    def configurer(self):
        pass

    def traiter_backlog(self):
        pass

    def traiter_transaction(self):
        pass


# Classe qui s'occupe de l'interaction avec la collection SenseursPassifs dans Mongo
class SenseursPassifsCollectionDao:
    pass


# Classe qui produit et maintient un document de metadonnees et de lectures pour un SenseurPassif.
class ProducteurDocumentSenseurPassif:

    def __init__(self, message_dao, document_dao):
        self._message_dao = message_dao
        self._document_dao = document_dao

    ''' Extrait l'information d'une lecture de senseur passif pour creer ou mettre a jour le document du senseur.  '''
    def maj_document_senseur(self, transaction):

        # Verifier si toutes les cles sont presentes
        contenu_transaction = transaction[Constantes.TRANSACTION_MESSAGE_LIBELLE_CHARGE_UTILE].copy()
        noeud = contenu_transaction[SenseursPassifsConstantes.TRANSACTION_NOEUD]
        id_appareil = contenu_transaction[SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR]
        date_lecture_epoch = contenu_transaction[SenseursPassifsConstantes.TRANSACTION_DATE_LECTURE]

        # Transformer les donnees en format natif (plus facile a utiliser plus tard)
        date_lecture = datetime.datetime.fromtimestamp(date_lecture_epoch) # Mettre en format date standard
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
        resultat_update = collection.update_one(filter=selection, update=operation, upsert=False)

        # Verifier si un document a ete modifie.
        if resultat_update.matched_count == 0:
            # Aucun document n'a ete modifie. Verifier si c'est parce qu'il n'existe pas. Sinon, le match a echoue
            # parce qu'une lecture plus recente a deja ete enregistree (c'est OK)
            selection_sansdate = selection.copy()
            del selection_sansdate[SenseursPassifsConstantes.TRANSACTION_DATE_LECTURE]
            document = collection.find_one(selection_sansdate)

            if document is None:
                # Executer la meme operation avec upsert=True pour inserer un nouveau document
                resultat_update = collection.update_one(filter=selection, update=operation, upsert=True)
                print("_id du nouveau document: %s" % str(resultat_update.upserted_id))

    ''' 
    Calcule les moyennes/min/max de la derniere journee pour un senseur avec donnees numeriques. 
    
    :param id_document_senseur: _id de base de donnees Mongo pour le senseur a mettre a jour.
    '''
    def calculer_aggregation_journee(self, id_document_senseur):

        senseur_objectId_key = {"_id": ObjectId(id_document_senseur)}

        # Charger l'information du senseur
        collection_senseurs = self._document_dao.get_collection(SenseursPassifsConstantes.COLLECTION_NOM)
        document_senseur = collection_senseurs.find_one(senseur_objectId_key)

        print("Document charge: %s" % str(document_senseur))

        noeud = document_senseur[SenseursPassifsConstantes.TRANSACTION_NOEUD]
        no_senseur = document_senseur[SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR]

        regroupement_champs = {
            'temperature-moyenne': {'$avg': '$charge-utile.temperature'},
            'humidite-moyenne': {'$avg': '$charge-utile.humidite'},
            'pression-moyenne': {'$avg': '$charge-utile.pression'}
        }

        # Creer l'intervalle pour les donnees
        #date_reference = datetime.datetime.utcnow()
        date_reference = datetime.datetime(2018, 11, 10, 0)
        time_range_to = datetime.datetime(date_reference.year, date_reference.month,
                                          date_reference.day,
                                          date_reference.hour)
        time_range_from = time_range_to - datetime.timedelta(days=1)

        # Transformer en epoch (format de la transaction)
        time_range_to = int(time_range_to.strftime('%s'))
        time_range_from = int(time_range_from.strftime('%s'))

        selection = {
            'info-transaction.domaine': SenseursPassifsConstantes.TRANSACTION_VALEUR_DOMAINE,
            'charge-utile.temps_lecture': {'$gte': time_range_from, '$lt': time_range_to},
            'charge-utile.senseur': no_senseur,
            'charge-utile.noeud': noeud
        }

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
        operation_update={'$set': {'lectures_dernier_jour': resultat}}
        collection_senseurs.update_one(filter=senseur_objectId_key, update=operation_update, upsert=False)

    '''
    Calcule les moyennes/min/max du dernier mois pour un senseur avec donnees numeriques.
    
    :param id_document_senseur: _id de base de donnees Mongo pour le senseur a mettre a jour.
    '''
    def calculer_aggregation_mois(self, id_document_senseur):
        pass


# Classe qui gere le document pour un noeud. Supporte une mise a jour incrementale des donnees.
class ProducteurDocumentSenseurPassifNoeud:

    def __init__(self):
        pass
