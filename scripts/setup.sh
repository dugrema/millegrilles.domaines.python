#!/bin/bash
GIT_NAME=MilleGrilles.domaines.python
GIT_FOLDER=$SRC_FOLDER/$GIT_NAME

REQ_FILE=$GIT_FOLDER/requirements.txt

if [ ! -f $REQ_FILE ]; then
    echo Fichier $REQ_FILE introuvable

    echo Repertoire git: $GIT_FOLDER
    echo Repertoire src: $SRC_FOLDER

    ls -la $SRC_FOLDER
    ls -la $GIT_FOLDER
    exit 1
fi

echo "Installer dependances Python avec pip: fichier $GIT_FOLDER/requirements.txt"
pip install --no-cache-dir -r $REQ_FILE

echo Installer package MilleGrilles.domaines
cd $GIT_FOLDER
python3 setup.py install

#echo "Copier script demarrer dans $BUNDLE_FOLDER"
#mkdir -p $BUNDLE_FOLDER
cp $GIT_FOLDER/scripts/demarrer*.py $BUNDLE_FOLDER
# Copier fichier de reference pour la configuration de tous les domaines
cp $GIT_FOLDER/scripts/domaines.json $BUNDLE_FOLDER

cd $BUNDLE_FOLDER
rm -rf $BUILD_FOLDER
