.PHONY: all

all: dp-compute-colorizer.zip

dp-compute-colorizer.zip: requirements.txt cfunc.py
	rm -f $@
	zip -9 $@ $^
