# Module avec les classes de donnees, processus et gestionnaire de sous domaine mgdomaines.appareils.SenseursPassifs
from millegrilles.Domaines import GestionnaireDomaine
import millegrilles.Constantes as Constantes
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

            # Executer la meme operation avec upsert=True pour inserer un nouveau document
            resultat_update = collection.update_one(filter=selection, update=operation, upsert=True)

            print("_id du nouveau document: %s" % str(resultat_update.upserted_id))

    ''' 
    Calcule les moyennes/min/max de la derniere journee pour un senseur avec donnees numeriques. 
    
    :param id_document_senseur: _id de base de donnees Mongo pour le senseur a mettre a jour.
    '''
    def calculer_aggregation_journee(self, id_document_senseur):
        pass

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
