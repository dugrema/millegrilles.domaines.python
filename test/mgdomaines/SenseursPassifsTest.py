import unittest
import datetime
from bson.objectid import ObjectId
from mgdomaines.appareils.SenseursPassifs import ProducteurDocumentSenseurPassif

class SenseursPassifsTest(unittest.TestCase):

    def setUp(self):
        self._document_dao = DocumentDaoStub()
        self._message_dao = MessageDaoStub()

        self.producteur_document_senseur_passif = ProducteurDocumentSenseurPassif(self._message_dao, self._document_dao)

        self.transaction_sample1 = {
            "_id": ObjectId("5bec7292e09409320fc8896f"),
            "charge-utile": {
                "temps_lecture": 1541826019,
                "bat_mv": 3369,
                "pression": 100.6,
                "humidite": 0,
                "noeud": "cuisine.maple.mdugre.info",
                "senseur": 16,
                "temperature": 18.4,
                "location": "Bureau"
            },
            "info-transaction": {
                "source-systeme": "cuisine@cuisine.maple.mdugre.info",
                "signature_contenu": "",
                "uuid-transaction": "9beb5cea-e840-11e8-b40b-00155d011f00",
                "domaine": "mgdomaines.appareils.senseur",
                "estampille": 1541826019
            },
            "evenements": {
                "transaction_nouvelle": [
                    datetime.datetime.strptime("2018-11-10T00:00:19.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
                ],
                "document_persiste": [
                    datetime.datetime.strptime("2018-11-14T09:08:02.000Z", "%Y-%m-%dT%H:%M:%S.%fZ")
                ]
            }
        }

    def test_init(self):
        self.assertTrue(True)


class MessageDaoStub:
    pass


class DocumentDaoStub:
    pass
