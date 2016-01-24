export SHELL:=/usr/local/bin/bash -O extglob -c
export GO15VENDOREXPERIMENT:=1

build:
	go build .

run: build
	./kt
