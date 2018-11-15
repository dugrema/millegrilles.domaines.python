# Module avec les classes de donnees, processus et gestionnaire de sous domaine mgdomaines.appareils.SenseursPassifs
from millegrilles.Domaines import GestionnaireDomaine
import millegrilles.Constantes as Constantes
import datetime

# Constantes pour SenseursPassifs
class SenseursPassifsConstantes:

    LIBELLE_DOCUMENT_SENSEUR = 'senseur.individuel'
    LIBELLE_DOCUMENT_NOEUD = 'senseur.noeud'
    LIBELLE_DOCUMENT_GROUPE = 'groupe.senseurs'

    TRANSACTION_NOEUD = 'noeud'
    TRANSACTION_ID_SENSEUR = 'senseur'
    TRANSACTION_DATE_LECTURE = 'temps_lecture'


# Gestionnaire pour le domaine mgdomaines.appareils.SenseursPassifs.
class GestionnaireSenseursPassifs(GestionnaireDomaine):

    def __init__(self):
        super().__init__()

    def configurer(self):
        pass

    def traiter_backlog(self):
        pass


# Classe qui produit et maintient un document de metadonnees et de lectures pour un SenseurPassif.
class ProducteurDocumentSenseurPassif:

    def __init__(self, message_dao, document_dao):
        self._message_dao = message_dao
        self._document_dao = document_dao

    ''' Extrait l'information d'une lecture de senseur passif pour creer ou mettre a jour le document du senseur.  '''
    def maj_document_senseur(self, transaction):

        # Verifier si toutes les cles sont presentes
        contenu_transaction = transaction[Constantes.TRANSACTION_MESSAGE_LIBELLE_CHARGE_UTILE]
        noeud = contenu_transaction.get(SenseursPassifsConstantes.TRANSACTION_NOEUD)
        id_appareil = contenu_transaction.get(SenseursPassifsConstantes.TRANSACTION_ID_SENSEUR)
        date_lecture_epoch = contenu_transaction.get(SenseursPassifsConstantes.TRANSACTION_DATE_LECTURE)

        if noeud is None or id_appareil is None or date_lecture_epoch is None:
            raise ValueError(
                "Identificateurs incomplets: noeud:%s, id_appareil:%s, date_lecture:%s" %
                (noeud, id_appareil, str(date_lecture_epoch)))

        # Transformer les donnees
        date_lecture = datetime.datetime.fromtimestamp(date_lecture_epoch) # Mettre en format date standard

        # Mettre a jour les informations du document en copiant ceux de la transaction


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
