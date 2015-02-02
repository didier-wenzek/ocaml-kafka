TARGETS = okafka.cma okafka.cmxa okafka.cmxs dllocamlkafka.so kafka.cmi kafka.cma kafka.cmx
LIB = $(addprefix _build/, $(TARGETS))

all:
	ocamlbuild $(TARGETS)

install:
	ocamlfind install okafka META $(LIB)

uninstall:
	ocamlfind remove okafka

tests: tests.ml
	ocamlbuild -libs okafka tests.native
	_build/tests.native

kafkatail.native: kafkatail.ml
	ocamlbuild -libs okafka kafkatail.native

clean:
	ocamlbuild -clean

.PHONY: all clean tests install
