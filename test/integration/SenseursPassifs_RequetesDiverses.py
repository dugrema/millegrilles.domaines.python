# Classe utilisee pour faire diverses requetes sur les senseurs passifs (test).
from millegrilles.dao.Configuration import TransactionConfiguration
from millegrilles.dao.DocumentDAO import MongoDAO
from mgdomaines.appareils.SenseursPassifs import SenseursPassifsConstantes
from millegrilles import Constantes
from mgdomaines.appareils.SenseursPassifs import TraitementBacklogLecturesSenseursPassifs
import datetime

class SenseursPassifsRequetesTest:

    def __init__(self):
        self.configuration = TransactionConfiguration()
        self.configuration.loadEnvironment()
        print("Connection MongDB")
        self.document_dao = MongoDAO(self.configuration)
        self.document_dao.connecter()

    def run_requete_plusrecentetransactionlecture_parsenseur(self):
        filtre = {
            'info-transaction.domaine': SenseursPassifsConstantes.TRANSACTION_VALEUR_DOMAINE,
            'evenements.transaction_traitee': {'$exists': False}
        }

        regroupement = {
            '_id': {
                'noeud': '$charge-utile.noeud',
                'senseur': '$charge-utile.senseur'
            },
            'temps_lecture': {'$max': '$charge-utile.temps_lecture'}
        }

        operation = [
            {'$match': filtre},
            {'$group': regroupement}
        ]

        print("Operation aggregation: %s" % str(operation))

        collection_transactions = self.document_dao.get_collection(Constantes.DOCUMENT_COLLECTION_TRANSACTIONS)
        resultat_curseur = collection_transactions.aggregate(operation)

        liste_transaction_senseurs = []
        for res in resultat_curseur:
            transaction = {
                'noeud': res['_id']['noeud'],
                'senseur': res['_id']['senseur'],
                'temps_lecture': res['temps_lecture']
            }
            liste_transaction_senseurs.append(transaction)
            print("Resultat: %s" % str(transaction))

        return liste_transaction_senseurs

    def run_requete_genererdeclencheur_parsenseur(self, liste_senseurs):

        collection_transactions = self.document_dao.get_collection(Constantes.DOCUMENT_COLLECTION_TRANSACTIONS)

        for transaction_senseur in liste_senseurs:
            filtre = {
                'charge-utile.noeud': transaction_senseur['noeud'],
                'charge-utile.senseur': transaction_senseur['senseur'],
                'charge-utile.temps_lecture': transaction_senseur['temps_lecture']
            }
            projection = {
                '_id': 1
            }
            resultat_curseur = collection_transactions.find(filter=filtre, projection=projection)

            for res in resultat_curseur:
                # Preparer un message pour declencher la transaction
                print("Transaction a declencher: _id = %s" % str(res['_id']))

                # Marquer toutes les transaction anterieures comme traitees
                filtre['evenements.transaction_traitee'] = {'$exists': False}
                filtre['charge-utile.temps_lecture'] = {'$lt': transaction_senseur['temps_lecture']}

                operation = {
                    '$push': {'evenements.transaction_traitee': datetime.datetime.utcnow()}
                }

                collection_transactions.update_many(filter=filtre, update=operation)

### MAIN ###

test = SenseursPassifsRequetesTest()
liste_transactions = test.run_requete_plusrecentetransactionlecture_parsenseur()
test.run_requete_genererdeclencheur_parsenseur(liste_transactions)