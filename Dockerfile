# Image pour scripts python MilleGrilles domaines
# Note: les fichiers doivent avoir ete copies dans le repertoire courant sous src/

FROM registry.maple.mdugre.info:5000/millegrilles_consignation_python.x86_64:v0.7.0

COPY scripts/ $BUILD_FOLDER/scripts
COPY ./ $SRC_FOLDER/MilleGrilles.domaines.python/

RUN $BUILD_FOLDER/scripts/setup.sh

CMD ["demarrer_domaine.py", "-m", "mgdomaines.appareils.SenseursPassifs", "-c", "GestionnaireSenseursPassifs"]
