.PHONY: all

all: dp-compute-extract.zip

dp-compute-extract.zip: requirements.txt cfunc.py
	rm -f $@
	zip -9 $@ $^
