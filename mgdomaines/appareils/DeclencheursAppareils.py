from millegrilles.Declencheur import Declencheur
import sys, datetime

class DeclencheurSenseursPassif(Declencheur):

    def __init__(self):
        super().__init__()

    def maj_horaire(self):
        domaine = 'mgdomaines.appareils.SenseursPassifs.MAJHoraire'
        dict_message = {
            'evenements': 'miseajour.horaire',
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        self.transmettre_declencheur_domaine(domaine, dict_message)

if len(sys.argv) == 2:

    declencheur = DeclencheurSenseursPassif()
    methode_declencheur = getattr(declencheur, sys.argv[1])

    # Executer la methode
    methode_declencheur()

else:
    print("Il faut fournir le nom de la methode a executer")