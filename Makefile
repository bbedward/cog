SHELL := /bin/bash

DESTDIR ?=
PREFIX = /usr/local
BINDIR = $(PREFIX)/bin

INSTALL := install -m 0755
INSTALL_PROGRAM := $(INSTALL)

GO := go
GOOS := $(shell $(GO) env GOOS)
GOARCH := $(shell $(GO) env GOARCH)

PYTHON := python
PYTEST := pytest
MYPY := mypy

default: all

.PHONY: all
all: cog

pkg/dockerfile/embed/cog.whl: python/* python/cog/* python/cog/server/* python/cog/command/*
	@echo "Building Python library"
	rm -rf python/dist
	pip install build && $(PYTHON) -m build python/ --wheel
	mkdir -p pkg/dockerfile/embed
	cp python/dist/*.whl $@

cog: pkg/dockerfile/embed/cog.whl
	$(eval COG_VERSION ?= $(shell git describe --tags --match 'v*' --abbrev=0)+dev)
	CGO_ENABLED=0 $(GO) build -o $@ \
		-ldflags "-X github.com/replicate/cog/pkg/global.Version=$(COG_VERSION) -X github.com/replicate/cog/pkg/global.BuildTime=$(shell date +%Y-%m-%dT%H:%M:%S%z) -w" \
		cmd/cog/cog.go

.PHONY: install
install: cog
	$(INSTALL_PROGRAM) -d $(DESTDIR)$(BINDIR)
	$(INSTALL_PROGRAM) cog $(DESTDIR)$(BINDIR)/cog

.PHONY: uninstall
uninstall:
	rm -f $(DESTDIR)$(BINDIR)/cog

.PHONY: clean
clean:
	$(GO) clean
	rm -rf python/build python/dist
	rm -f cog
	rm -f pkg/dockerfile/embed/cog.whl

.PHONY: test-go
test-go: pkg/dockerfile/embed/cog.whl | check-fmt vet lint-go
	$(GO) get gotest.tools/gotestsum
	$(GO) run gotest.tools/gotestsum -- -timeout 1200s -parallel 5 ./... $(ARGS)

.PHONY: test-integration
test-integration: cog
	cd test-integration/ && $(MAKE) PATH="$(PWD):$(PATH)" test

.PHONY: test-python
test-python:
	cd python/ && $(PYTEST) -n auto -vv

.PHONY: test
test: test-go test-python test-integration


.PHONY: fmt
fmt:
	$(GO) run golang.org/x/tools/cmd/goimports -w -d .

.PHONY: generate
generate:
	$(GO) generate ./...


.PHONY: vet
vet:
	$(GO) vet ./...


.PHONY: check-fmt
check-fmt:
	$(GO) run golang.org/x/tools/cmd/goimports -d .
	@test -z $$($(GO) run golang.org/x/tools/cmd/goimports -l .)

.PHONY: lint-go
lint-go:
	$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint run ./...

.PHONY: lint-python
lint-python:
	$(MYPY) python/cog

.PHONY: lint
lint: lint-go lint-python

.PHONY: mod-tidy
mod-tidy:
	$(GO) mod tidy
