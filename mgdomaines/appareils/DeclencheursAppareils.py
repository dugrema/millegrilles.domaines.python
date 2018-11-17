from millegrilles.Declencheur import Declencheur
from mgdomaines.appareils.SenseursPassifs import SenseursPassifsConstantes
import sys
import datetime


class DeclencheurAppareils(Declencheur):

    NOM_DOMAINE = 'mgdomaines.appareils'

    def __init__(self):
        super().__init__()

    def senseurspassifs_maj_horaire(self):
        domaine = '%s.%s' % (self.NOM_DOMAINE, 'SenseursPassifs.MAJHoraire')
        dict_message = {
            'evenements': SenseursPassifsConstantes.EVENEMENT_MAJ_HORAIRE,
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        self.transmettre_declencheur_domaine(domaine, dict_message)

    def senseurspassifs_maj_quotidienne(self):
        domaine = '%s.%s' % (self.NOM_DOMAINE, 'SenseursPassifs.MAJQuotidienne')
        dict_message = {
            'evenements': SenseursPassifsConstantes.EVENEMENT_MAJ_QUOTIDIENNE,
            'timestamp': datetime.datetime.utcnow().isoformat()
        }
        self.transmettre_declencheur_domaine(domaine, dict_message)

# **** MAIN ****

if len(sys.argv) == 2:

    declencheur = DeclencheurAppareils()
    methode_declencheur = getattr(declencheur, sys.argv[1])

    # Executer la methode
    methode_declencheur()

else:
    print("Il faut fournir le nom de la methode a executer. Arguments fournis: %s" % sys.argv)
