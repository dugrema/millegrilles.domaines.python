from mgdomaines.appareils.SenseursPassifs import ProducteurDocumentSenseurPassif
from millegrilles.dao.Configuration import TransactionConfiguration
from millegrilles.dao.MessageDAO import PikaDAO
from millegrilles.dao.DocumentDAO import MongoDAO


class CreationDocumentTest:

    def __init__(self):
        self.configuration = TransactionConfiguration()
        self.configuration.loadEnvironment()
        print("Connecter Pika")
        self.messageDao = PikaDAO(self.configuration)
        self.messageDao.connecter()
        print("Connection MongDB")
        self.documentDao = MongoDAO(self.configuration)
        self.documentDao.connecter()

        self.producteur = ProducteurDocumentSenseurPassif(self.messageDao, self.documentDao)

    def run(self):

        transaction = self.documentDao.charger_transaction_par_id("5bee12bbe09409b7881c0d72")
        self.producteur.maj_document_senseur(transaction)

### MAIN ###

test = CreationDocumentTest()
test.run()