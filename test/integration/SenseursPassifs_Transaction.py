from mgdomaines.appareils.SenseursPassifs import ProducteurTransactionSenseursPassifs
import datetime


class TransactionSenseursPassifsTest:

    def __init__(self):
        self._producteur = ProducteurTransactionSenseursPassifs()

    def test1(self):
        message_dict = dict()
        message_dict['senseur'] = 1
        message_dict['temps_lecture'] = int(datetime.datetime.now().timestamp())
        message_dict['temperature'] = 22.5
        message_dict['humidite'] = 67.2
        message_dict['hachi-parmentier'] = 'Nah nah nah, nah!'
        uuid_transaction = self._producteur.transmettre_lecture_senseur(message_dict)
        print("Sent: UUID:%s = %s" % (uuid_transaction, message_dict))


test = TransactionSenseursPassifsTest()

# TEST
print("Envoyer message")
test._producteur.connecter()

test.test1()

test._producteur.deconnecter()

# FIN TEST

