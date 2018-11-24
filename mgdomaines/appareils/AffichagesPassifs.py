# Affichages sans interaction/boutons qui sont controles via documents ou timers.
import traceback
import datetime
from threading import Thread, Event

from pymongo.errors import OperationFailure
from bson import ObjectId

from mgdomaines.appareils.SenseursPassifs import SenseursPassifsConstantes


# Affichage qui se connecte a un ou plusieurs documents et recoit les changements live
class AfficheurDocumentMAJDirecte:

    # :params intervalle_secs: Intervalle (secondes) entre rafraichissements si watch ne fonctionne pas.
    def __init__(self, configuration, document_dao, intervalle_secs=30):
        self._configuration = configuration
        self._document_dao = document_dao
        self._documents = dict()
        self._intervalle_secs = intervalle_secs
        self._stop_event = Event()  # Evenement qui indique qu'on arrete la thread

        self._collection = None
        self._curseur_changements = None  # Si None, on fonctionne par timer
        self._thread_maj_document = None

    def start(self):
        self.initialiser_documents()

        # Thread.start
        self._thread_maj_document = Thread(target=self.run_maj_document)
        self._thread_maj_document.start()
        print("AfficheurDocumentMAJDirecte: thread demarree")

    def fermer(self):
        self._stop_event.set()

    def get_collection(self):
        raise NotImplemented('Doit etre implementee dans la sous-classe')

    def get_filtre(self):
        raise NotImplemented('Doit etre implementee dans la sous-classe')

    def initialiser_documents(self):
        self._collection = self.get_collection()
        filtre = self.get_filtre()

        filtre_watch = dict()
        # Rebatir le filtre avec fullDocument
        for cle in filtre:
            cle_watch = 'fullDocument.%s' % cle
            filtre_watch[cle_watch] = filtre[cle]

        # Tenter d'activer watch par _id pour les documents
        try:
            match = {
                '$match': filtre_watch
             }
            # match = {"$match": {"fullDocument": {"_id": "5bf954458343c70008dafd87"}}}
            pipeline = [match]
            print("Pipeline watch: %s" % str(pipeline))
            self._curseur_changements = self._collection.watch(pipeline)
        except OperationFailure as opf:
            print("Erreur activation watch, on fonctionne par timer: %s" % str(opf))

        self.charger_documents()  # Charger une version initiale des documents

    def charger_documents(self):
        # Sauvegarder la version la plus recente de chaque document
        filtre = self.get_filtre()
        curseur_documents = self._collection.find(filtre)
        for document in curseur_documents:
            # Sauvegarder le document le plus recent
            self._documents[document.get('_id')] = document
            print("#### Document rafraichi: %s" % str(document))

    def get_documents(self):
        return self._documents

    def run_maj_document(self):

        while not self._stop_event.is_set():
            try:
                # Executer la boucle de rafraichissement. Il y a deux comportements:
                # Si on a un _curseur_changements, on fait juste ecouter les changements du replice sets
                # Si on n'a pas de curseur, on utiliser un timer (_intervalle_sec) pour recharger avec Mongo
                if self._curseur_changements is not None:
                    print("Attente changement")
                    valeur = next(self._curseur_changements)
                    doc_update = valeur['fullDocument']
                    print("Valeur changee: %s" % str(doc_update))
                    self._documents[doc_update['_id']] = doc_update
                else:
                    self.charger_documents()
                    self._stop_event.wait(self._intervalle_secs)
            except Exception as e:
                print("AfficheurDocumentMAJDirecte: Erreur %s" % str(e))
                traceback.print_exc()
                self._stop_event.wait(self._intervalle_secs)


# Classe qui charge des senseurs pour afficher temperature, humidite, pression/tendance
# pour quelques senseurs passifs.
class AfficheurSenseurPassifTemperatureHumiditePression(AfficheurDocumentMAJDirecte):

    def __init__(self, configuration, document_dao, document_ids, intervalle_secs=30):
        super().__init__(configuration, document_dao, intervalle_secs)
        self._document_ids = document_ids
        self._thread_affichage = None
        self._thread_horloge = None
        self._horloge_event = Event()  # Evenement pour synchroniser l'heure
        self._lignes_ecran = None

    def get_collection(self):
        return self._document_dao.get_collection(SenseursPassifsConstantes.COLLECTION_NOM)

    def get_filtre(self):
        document_object_ids = []
        for doc_id in self._document_ids:
            document_object_ids.append(ObjectId(doc_id))

        filtre = {"_id": {'$in': document_object_ids}}
        return filtre

    def start(self):
        super().start()  # Demarre thread de lecture de documents
        self._thread_horloge = Thread(target=self.set_horloge_event)
        self._thread_horloge.start()

        # Thread.start
        self._thread_affichage = Thread(target=self.run_affichage)
        self._thread_affichage.start()
        print("AfficheurDocumentMAJDirecte: thread demarree")

    def set_horloge_event(self):
        while not self._stop_event.is_set():
            # print("Tick")
            self._horloge_event.set()
            self._stop_event.wait(1)

    def run_affichage(self):

        while not self._stop_event.is_set():  # Utilise _stop_event de la superclasse pour synchroniser l'arret

            try:
                self.afficher_tph()

                # Afficher heure et date pendant 5 secondes
                self.afficher_heure()

            except Exception as e:
                print("Erreur durant affichage: %s" % str(e))
                traceback.print_exc()
                self._stop_event.wait(10)  # Attendre 10 secondes et ressayer du debut

    def maj_affichage(self, lignes_affichage):
        self._lignes_ecran = lignes_affichage

    def afficher_tph(self):
        if not self._stop_event.is_set():
            lignes = self.generer_lignes()
            lignes.reverse()  # On utilise pop(), premieres lectures vont a la fin

            while len(lignes) > 0:
                lignes_affichage = [lignes.pop()]
                if len(lignes) > 0:
                    lignes_affichage.append(lignes.pop())

                # Remplacer contenu ecran
                self.maj_affichage(lignes_affichage)

                # print("Affichage: %s" % lignes_affichage)
                self._stop_event.wait(5)

    def afficher_heure(self):
        nb_secs = 5
        self._horloge_event.clear()
        while not self._stop_event.is_set() and nb_secs > 0:
            self._horloge_event.wait(1)
            nb_secs -= 1

            # Prendre heure courante, formatter
            now = datetime.datetime.now()
            datestring = now.strftime('%Y-%m-%d')
            timestring = now.strftime('%H:%M:%S')

            lignes_affichage = [datestring, timestring]
            # print("Horloge: %s" % str(lignes_affichage))
            self.maj_affichage(lignes_affichage)

            # Attendre 1 seconde
            self._horloge_event.clear()
            self._horloge_event.wait(1)

    def generer_lignes(self):
        lignes = []
        pression = None
        for senseur_id in self._documents:
            senseur = self._documents[senseur_id]
            info_loc_temp_hum = "{location} {temperature:2.1f}C/{humidite:2.0f}%".format(**senseur)
            lignes.append(info_loc_temp_hum)

            if pression is None and senseur.get('pression') is not None:
                pression = senseur.get('pression')

        if pression is not None:
            lecture = {'pression': pression, 'tendance': '?'}
            contenu = "Press: {pression:3.1f}kPa{tendance}".format(**lecture)
            lignes.append(contenu)

        return lignes
