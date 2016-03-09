TARGETS = okafka.cma okafka.cmxa okafka.cmxs okafka.a dllocamlkafka.so libocamlkafka.a
LIB = $(addprefix _build/, $(TARGETS))
BIN = create_topic.native tests.native
TOOLS = tail_kafka_topic.native sendto_kafka_topic.native

CFLAGS = -cflag -safe-string -cflag -bin-annot

all:
	rm -f libocamlkafka.clib okafka.mllib META
	ln -s libocamlkafka.clib.sync libocamlkafka.clib
	ln -s okafka.mllib.sync okafka.mllib
	ln -s META.sync META
	ocamlbuild $(CFLAGS) $(TARGETS)
	ocamlbuild -libs okafka,unix $(CFLAGS) $(BIN)

LWT_OPT = -cflags -ccopt,-I,-ccopt,$(shell ocamlfind query lwt.unix)
lwt:
	rm -f libocamlkafka.clib okafka.mllib META
	ln -s libocamlkafka.clib.lwt libocamlkafka.clib
	ln -s okafka.mllib.lwt okafka.mllib
	ln -s META.lwt META
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix $(CFLAGS) $(TARGETS)
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix -libs okafka $(CFLAGS) $(BIN)

install:
	ocamlfind install okafka META $(LIB) _build/kafka*.cmi _build/kafka*.mli _build/kafka*.cmt*

uninstall:
	ocamlfind remove okafka

tools: lwt
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix,cmdliner -libs okafka $(CFLAGS) $(TOOLS)

issues: lwt
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix,cmdliner -libs okafka $(CFLAGS) issue3.native

tests:
	_build/tests.native

clean:
	ocamlbuild -clean

.PHONY: all clean tests install
