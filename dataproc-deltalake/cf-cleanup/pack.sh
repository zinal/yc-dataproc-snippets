#! /bin/sh

FILE=cf-delta-cleanup.zip
rm -f ${FILE}
zip -9 ${FILE} requirements.txt cfunc.py
