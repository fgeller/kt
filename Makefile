export SHELL:=/usr/bin/env bash -O extglob -c
export GO111MODULE:=on
export OS=$(shell uname | tr '[:upper:]' '[:lower:]')

build: GOOS ?= ${OS}
build: GOARCH ?= amd64
build:
	rm -f kt
	GOOS=${GOOS} GOARCH=${GOARCH} go build -ldflags "-X main.buildTime=`date --iso-8601=s` -X main.buildVersion=`git rev-parse HEAD | cut -c-7`" .

release-linux: testing
	GOOS=linux $(MAKE) build
	tar Jcf kt-`git describe --abbrev=0 --tags`-linux-amd64.txz kt

release-darwin:
	GOOS=darwin $(MAKE) build
	tar Jcf kt-`git describe --abbrev=0 --tags`-darwin-amd64.txz kt

release: testing clean release-linux release-darwin

dep-up:
	docker-compose -f ./test-dependencies.yml up -d --remove-orphan 
	sleep 4

dep-down:
	docker-compose -f ./test-dependencies.yml down

testing: dep-up test dep-down

test: clean
	go test -v -vet=all -failfast

clean:
	rm -f kt
	rm -f kt-*.txz

run: build
	./kt
