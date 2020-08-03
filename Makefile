.PHONY: all
all:
	dune build @install

.PHONY: install
install: all
	dune install

.PHONY: uninstall
uninstall:
	dune uninstall

.PHONY: test
test: integration

.PHONY: clean
clean:
	dune clean

# Travis sets PACKAGE to determine which subpackages to build
# `kafka` is always included and if unset all packages will be tested
ifdef PACKAGE
ONLY_PACKAGES:=kafka,$(PACKAGE)
else
ONLY_PACKAGES:=kafka,kafka_async,kafka_lwt
endif

.PHONY: integration
integration: ## Run integration tests, requires Kafka
	dune build --only-packages $(ONLY_PACKAGES) @integration --force
