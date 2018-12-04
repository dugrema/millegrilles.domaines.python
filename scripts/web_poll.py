# Script pour demarrer un web poll

import traceback
import logging

from millegrilles.util.UtilScriptLigneCommande import ModeleAvecMessageDAO
from mgdomaines.web.WebPoll import WebPageDownload

logger = logging.getLogger(__name__)


class CLWebPoll(ModeleAvecMessageDAO):

    def __init__(self):
        super().__init__()

    def configurer_parser(self):
        super().configurer_parser()

        self.parser.add_argument(
            'url', type=str, help="URL a telecharger"
        )

        self.parser.add_argument(
            '--domaine',
            type=str,
            required=False,
            default=WebPageDownload.TRANSACTION_VALEUR_DOMAINE,
            help="Domaine de la transaction"
        )

    def executer(self):
        downloader = WebPageDownload(self.configuration, self.message_dao)
        url = self.args.url
        domaine = self.args.domaine
        downloader.produire_transaction(url, domaine)


# **** MAIN ****
def main():
    try:
        demarreur.parse()

        logging.basicConfig(level=logging.WARNING)
#        logging.getLogger().setLevel(logging.DEBUG)
        logging.getLogger(__name__).setLevel(logging.DEBUG)
        logging.getLogger("mgdomaines").setLevel(logging.DEBUG)

        demarreur.connecter()
        demarreur.executer()
    except Exception as e:
        print("MAIN: Erreur fatale, voir log. Erreur %s" % str(e))
        logger.exception("MAIN: Erreur")
        demarreur.print_help()
    finally:
        demarreur.deconnecter()


if __name__ == "__main__":
    demarreur = CLWebPoll()
    main()
