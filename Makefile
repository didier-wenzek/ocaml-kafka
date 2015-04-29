MODULES = kafka.cmi kafkaConsumer.cmi kafkaProducer.cmi
TARGETS = okafka.cma okafka.cmxa okafka.cmxs okafka.a dllocamlkafka.so libocamlkafka.a $(MODULES)
LIB = $(addprefix _build/, $(TARGETS))
BIN = kafkatail.native create_topic.native tests.native
TOOLS = tail_kafka_topic.native sendto_kafka_topic.native

all:
	rm -f libocamlkafka.clib okafka.mllib META
	ln -s libocamlkafka.clib.sync libocamlkafka.clib
	ln -s okafka.mllib.sync okafka.mllib
	ln -s META.sync META
	ocamlbuild $(TARGETS)
	ocamlbuild -libs okafka,unix $(BIN)

LWT_TARGETS = $(TARGETS) kafka_lwt.cmi
LWT_LIB = $(addprefix _build/, $(LWT_TARGETS))
LWT_OPT = -cflags -ccopt,-I,-ccopt,$(shell ocamlfind query lwt.unix)
lwt:
	rm -f libocamlkafka.clib okafka.mllib META
	ln -s libocamlkafka.clib.lwt libocamlkafka.clib
	ln -s okafka.mllib.lwt okafka.mllib
	ln -s META.lwt META
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix $(LWT_TARGETS)
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix -libs okafka $(BIN)

install:
	ocamlfind install okafka META $(LIB) _build/kafka*.cmi

uninstall:
	ocamlfind remove okafka

tools: lwt
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix,cmdliner -libs okafka $(TOOLS)

tests:
	_build/tests.native

clean:
	ocamlbuild -clean

.PHONY: all clean tests install
