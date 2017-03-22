TARGETS = okafka.cma okafka.cmxa okafka.cmxs okafka.a dllocamlkafka.so libocamlkafka.a
LIB = $(addprefix _build/, $(TARGETS))
TOOLS = tail_kafka_topic.native sendto_kafka_topic.native

CFLAGS = -cflag -safe-string -cflag -bin-annot

all: librdkafka-version
	rm -f libocamlkafka.clib okafka.mllib META
	ln -s libocamlkafka.clib.sync libocamlkafka.clib
	ln -s okafka.mllib.sync okafka.mllib
	ln -s META.sync META
	ocamlbuild -use-ocamlfind $(CFLAGS) kafka.ml $(TARGETS)

LWT_OPT = -cflags -ccopt,-I,-ccopt,$(shell ocamlfind query lwt.unix)
lwt: librdkafka-version
	rm -f libocamlkafka.clib okafka.mllib META
	ln -s libocamlkafka.clib.lwt libocamlkafka.clib
	ln -s okafka.mllib.lwt okafka.mllib
	ln -s META.lwt META
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix kafka.ml $(CFLAGS) $(TARGETS)

librdkafka-version: librdkafka-version.c
	$(CC) -o librdkafka-version librdkafka-version.c -lrdkafka

_build/tests.native:
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix -libs okafka $(CFLAGS) tests.native

install:
	ocamlfind install okafka META $(LIB) _build/kafka*.cmi _build/kafka*.mli _build/kafka*.cmt* _build/kafka*cmx

uninstall:
	ocamlfind remove okafka

tools: lwt
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix,cmdliner -libs okafka $(CFLAGS) $(TOOLS)

issues: lwt
	ocamlbuild -use-ocamlfind $(LWT_OPT) -pkgs lwt,lwt.unix,cmdliner -libs okafka $(CFLAGS) issue3.native

tests: _build/tests.native
	_build/tests.native

clean:
	ocamlbuild -clean

.PHONY: all clean tests install
