TARGETS = okafka.cma okafka.cmxa okafka.cmxs
LIB = $(addprefix _build/, $(TARGETS))

all:
	ocamlbuild $(TARGETS)

tests: tests.ml
	ocamlbuild -libs okafka tests.native
	_build/tests.native

kafkatail.native: kafkatail.ml
	ocamlbuild -libs okafka kafkatail.native

clean:
	ocamlbuild -clean

.PHONY: all clean tests
