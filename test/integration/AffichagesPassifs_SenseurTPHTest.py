from mgdomaines.appareils.AffichagesPassifs import AfficheurSenseurPassifTemperatureHumiditePression
from millegrilles.dao.Configuration import TransactionConfiguration
from millegrilles.dao.DocumentDAO import MongoDAO
from bson import ObjectId
import time
import traceback


class AfficheurSenseurPassifTemperatureHumiditePressionTest(AfficheurSenseurPassifTemperatureHumiditePression):

    def __init__(self):
        configuration = TransactionConfiguration()
        configuration.loadEnvironment()
        document_dao = MongoDAO(configuration)
        document_dao.connecter()

        document_ids = ['5bef321b82cc2cb5ab0e33c2', '5bef323482cc2cb5ab0e995d']

        super().__init__(configuration, document_dao, document_ids=document_ids, intervalle_secs=5)

    def test(self):
        for document_id in self.get_documents():
            print("Document charge: %s" % str(self._documents[document_id]))


# Demarrer test

test = AfficheurSenseurPassifTemperatureHumiditePressionTest()
try:
    print("Test debut")
    test.start()

    test.test()
    time.sleep(16)

    print("Test termine")
except Exception as e:
    print("Erreur main: %s" % e)
    traceback.print_exc()
finally:
    test.fermer()
    test.document_dao.deconnecter()
