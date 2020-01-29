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
test:
	dune runtest --force

.PHONY: clean
clean:
	dune clean

.PHONY: integration
integration: ## Run integration tests, requires Kafka
	dune build @integration --force
