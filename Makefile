export SHELL:=/usr/local/bin/bash -O extglob -c
export GO15VENDOREXPERIMENT:=1

build: GOOS ?= darwin
build: GOARCH ?= amd64
build:
	rm -f kt
	GOOS=${GOOS} GOARCH=${GOARCH} go build .

release-linux:
	GOOS=linux $(MAKE) build
	file kt
	tar Jcf kt-`git describe --abbrev=0 --tags`-linux-amd64.xz kt

release-darwin:
	GOOS=darwin $(MAKE) build
	file kt
	tar Jcf kt-`git describe --abbrev=0 --tags`-darwin-amd64.xz kt

release: clean release-linux release-darwin

clean:
	rm -f kt
	rm -f kt-*.xz

run: build
	./kt
