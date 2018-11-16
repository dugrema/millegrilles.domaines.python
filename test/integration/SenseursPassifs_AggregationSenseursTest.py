from mgdomaines.appareils.SenseursPassifs import ProducteurDocumentSenseurPassif
from millegrilles.dao.Configuration import TransactionConfiguration
from millegrilles.dao.MessageDAO import PikaDAO
from millegrilles.dao.DocumentDAO import MongoDAO


class AggregationSenseursDocumentTest:

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

        self.producteur.calculer_aggregation_journee("5bedd619ed0c9b4b9c06971a")
        #self.producteur.calculer_aggregation_mois("5bedd619ed0c9b4b9c06971a")

### MAIN ###

test = AggregationSenseursDocumentTest()
test.run()