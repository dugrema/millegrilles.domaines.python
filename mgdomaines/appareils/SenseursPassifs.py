# Module avec les classes de donnees, processus et gestionnaire de sous domaine mgdomaines.appareils.SenseursPassifs
from millegrilles.Domaines import GestionnaireDomaine


# Gestionnaire pour le deomaine mgdomaines.appareils.SenseursPassifs.
class GestionnaireSenseursPassifs(GestionnaireDomaine):

    def __init__(self):
        super().__init__()

    def configurer(self):
        pass

    def traiter_backlog(self):
        pass


# Classe qui produit et maintient un document de metadonnees et de lectures pour un SenseurPassif.
class ProducteurDocumentSenseurPassif:

    def __init__(self):
        pass

    ''' Extrait l'information d'une lecture de senseur passif pour creer ou mettre a jour le document du senseur.  '''
    def maj_document_senseur(self, transaction):

        # Trouver le document de senseur passif dans MongoDB
        noeud = None
        id_appareil = None
        document = None

        if document is None:
            # Creer un nouveau document pour le senseur
            pass

        derniere_maj = None

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
