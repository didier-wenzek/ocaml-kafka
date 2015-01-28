TARGETS = okafka.cma okafka.cmxa okafka.cmxs
LIB = $(addprefix _build/, $(TARGETS))

all:
	ocamlbuild $(TARGETS)

tests:
	ocamlbuild -libs okafka -lflags -ccopt,-lrdkafka tests.native --
	_build/tests.native

clean:
	ocamlbuild -clean

.PHONY: all clean tests
