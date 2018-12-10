# Image pour scripts python MilleGrilles domaines
# Note: les fichiers doivent avoir ete copies dans le repertoire courant sous src/

FROM registry.maple.mdugre.info:5000/millegrilles_consignation_python:v0.8.9

# Indiquer ou va se trouver le fichier de configurations pour tous les gestionnaires de domaines
ENV MG_DOMAINES_JSON=$BUNDLE_FOLDER/domaines.json

COPY scripts/ $BUILD_FOLDER/scripts
COPY ./ $SRC_FOLDER/MilleGrilles.domaines.python/

RUN $BUILD_FOLDER/scripts/setup.sh

CMD ["demarrer_domaine.py"]
