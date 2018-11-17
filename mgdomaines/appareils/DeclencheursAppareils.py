from millegrilles.Declencheur import Declencheur
import sys
import datetime


class DeclencheurSenseursPassif(Declencheur):

    NOM_DOMAINE = 'mgdomaines.appareils.SenseursPassifs'

    def __init__(self):
        super().__init__()

    def maj_horaire(self):
        domaine = '%s.%s' % (self.NOM_DOMAINE, 'MAJHoraire')
        dict_message = {
            'evenements': 'miseajour.horaire',
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        self.transmettre_declencheur_domaine(domaine, dict_message)

    def maj_quotidienne(self):
        domaine = '%s.%s' % (self.NOM_DOMAINE, 'MAJQuotidienne')
        dict_message = {
            'evenements': 'miseajour.quotidienne',
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        self.transmettre_declencheur_domaine(domaine, dict_message)

# **** MAIN ****

if len(sys.argv) == 2:

    declencheur = DeclencheurSenseursPassif()
    methode_declencheur = getattr(declencheur, sys.argv[1])

    # Executer la methode
    methode_declencheur()

else:
    print("Il faut fournir le nom de la methode a executer. Arguments fournis: %s" % sys.argv)
