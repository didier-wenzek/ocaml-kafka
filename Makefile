TARGETS = okafka.cma okafka.cmxa okafka.cmxs
LIB = $(addprefix _build/, $(TARGETS))

all:
	ocamlbuild $(TARGETS)

foo:
	ocamlbuild ocaml_kafka.o
	ocamlbuild -lflags -ccopt,ocaml_kafka.o,-ccopt,-lrdkafka kafka.native

clean:
	ocamlbuild -clean

.PHONY: all clean tests
