from mgdomaines.appareils.SenseursPassifs import GestionnaireSenseursPassifs

gestionnaire = GestionnaireSenseursPassifs()

gestionnaire.initialiser()
gestionnaire.configurer()
gestionnaire.demarrer_traitement_messages_blocking(gestionnaire.get_nom_queue())