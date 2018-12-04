# Module qui permet d'extraire des pages web ou feed RSS et de les sauvegarder comme "transaction:

import urllib.request
import certifi
import logging

from millegrilles.transaction.GenerateurTransaction import GenerateurTransaction

logger = logging.getLogger(__name__)


# Classe qui permet de telecharger une page web et la transmettre comme nouvelle transaction.
class WebPageDownload:

    TRANSACTION_VALEUR_DOMAINE = 'mgdomaines.appareils.WebPoll.WebPageDownload'

    def __init__(self, configuration, message_dao, limit_bytes=50*1024):
        self._generateur_transaction = GenerateurTransaction(configuration, message_dao)
        # self._configuration = configuration
        # self._message_dao = message_dao
        self._limit_bytes = limit_bytes  # Taille limite du download

        self.url = None
        self.contenu = None
        self.domaine = None

    def produire_transaction(self, url, domaine=None):
        self.url = url
        self.domaine = domaine
        self.contenu = WebPageDownload.telecharger(url)

        contenu_dict = self.traiter_contenu(self.contenu)
        self._generateur_transaction.soumettre_transaction(contenu_dict, domaine)

    @staticmethod
    def telecharger(url):
        logger.debug("certifi: Certificats utilises pour telecharger: %s" % certifi.where())
        with urllib.request.urlopen(url, cafile=certifi.where()) as response:
            contenu = response.read()
        return contenu

    def traiter_contenu(self, contenu):
        contenu_dict = {
            "url": self.url,
            "contenuPage": str(contenu)
        }
        return contenu_dict
