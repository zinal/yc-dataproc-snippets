.PHONY: all

all: dp-compute-scan.zip

dp-compute-scan.zip: requirements.txt cfunc.py
	rm -f $@
	zip -9 $@ $^
