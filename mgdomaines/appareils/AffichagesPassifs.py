# Affichages sans interaction/boutons qui sont controles via documents ou timers.
import traceback
from threading import Thread, Event

from pymongo.errors import OperationFailure


# Affichage qui se connecte a un ou plusieurs documents et recoit les changements live
class AfficheurDocumentMAJDirecte:

    # :params intervalle_secs: Intervalle (secondes) entre rafraichissements si watch ne fonctionne pas.
    def __init__(self, configuration, document_dao, intervalle_secs=30):
        self._configuration = configuration
        self._document_dao = document_dao
        self._collection = None
        self.documents = dict()
        self._curseur_changements = None  # Si None, on fonctionne par timer
        self._intervalle_secs = intervalle_secs
        self._thread = None
        self._stop_event = None

    def start(self):
        self.initialiser_documents()

        # Thread.start
        self._thread = Thread(target=self.run)
        self._stop_event = Event()  # Evenement qui indique qu'on arrete la thread
        self._thread.start()
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

        # Tenter d'activer watch par _id pour les documents
        try:
            match = {"$match": filtre}
            pipeline = [match]
            self._curseur_changements = self._collection.watch(pipeline=pipeline)
        except OperationFailure as opf:
            print("Erreur activation watch, on fonctionne par timer: %s" % str(opf))

        self.charger_documents()  # Charger une version initiale des documents

    def charger_documents(self):
        # Sauvegarder la version la plus recente de chaque document
        filtre = self.get_filtre()
        curseur_documents = self._collection.find(filtre)
        for document in curseur_documents:
            # Sauvegarder le document le plus recent
            self.documents[document.get('_id')] = document
            print("#### Document rafraichi: %s" % str(document))

    def run(self):

        while not self._stop_event.is_set():
            try:
                # Executer la boucle de rafraichissement. Il y a deux comportements:
                # Si on a un _curseur_changements, on fait juste ecouter les changements du replice sets
                # Si on n'a pas de curseur, on utiliser un timer (_intervalle_sec) pour recharger avec Mongo
                if self._curseur_changements is not None:
                    pass
                else:
                    self.charger_documents()
                    self._stop_event.wait(self._intervalle_secs)
            except Exception as e:
                print("AfficheurDocumentMAJDirecte: Erreur %s" % str(e))
                traceback.print_exc()
                self._stop_event.wait(self._intervalle_secs)
