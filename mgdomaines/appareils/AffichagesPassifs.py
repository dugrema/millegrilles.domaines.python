# Affichages sans interaction/boutons qui sont controles via documents ou timers.


# Affichage qui se connecte a un ou plusieurs documents et recoit les changements live
class AfficheurDocumentMAJDirecte:

    def __init__(self):
        self._configuration = None
        self.document_dao = None
        self.collection = None
        self.documents = dict()

    def initialiser(self, configuration, document_dao):
        self._configuration = configuration
        self.document_dao = document_dao
        self.initialiser_documents()

    def get_collection(self):
        raise NotImplemented('Doit etre implementee dans la sous-classe')

    def get_filtre(self):
        raise NotImplemented('Doit etre implementee dans la sous-classe')

    def initialiser_documents(self):
        self.collection = self.get_collection()
        filtre = self.get_filtre()
        curseur_documents = self.collection.find(filtre)

        # Activer watch par _id pour les documents

        # Sauvegarder la version la plus recente de chaque document
        for document in curseur_documents:
            # Sauvegarder le document le plus recent
            self.documents['_id'] = document.get('_id')
            print("Document##: %s" % str(document))



