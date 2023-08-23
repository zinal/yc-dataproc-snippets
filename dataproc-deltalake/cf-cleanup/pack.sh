#! /bin/sh

FILE=deltalake-cleanup.zip
rm -f ${FILE}
zip -9 ${FILE} requirements.txt cfunc.py
