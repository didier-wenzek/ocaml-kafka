all:
	dune build @install

install: all
	dune install

uninstall:
	dune uninstall

test:
	dune runtest

clean:
	dune clean

.PHONY: all clean test install uninstall
