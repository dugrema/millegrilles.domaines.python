from mgdomaines.appareils.AffichagesPassifs import AfficheurDocumentMAJDirecte
from millegrilles.dao.Configuration import TransactionConfiguration
from millegrilles.dao.DocumentDAO import MongoDAO
from bson import ObjectId

class AfficheurDocumentMAJDirecteTest(AfficheurDocumentMAJDirecte):

    def __init__(self):
        super().__init__()

    def get_collection(self):
        return self.document_dao.get_collection('mathieu')

    def get_filtre(self):
        return {"_id": ObjectId("5bf80ce3e597dd0008fe557b")}

    def test(self):
        for document in self.documents:
            print("Document charge: %s" % str(document))


# Demarrer test
configuration = TransactionConfiguration()
configuration.loadEnvironment()
document_dao = MongoDAO(configuration)
document_dao.connecter()

test = AfficheurDocumentMAJDirecteTest()
test.initialiser(configuration, document_dao)

test.test()

document_dao.deconnecter()
