all:
	jbuilder build @install

install: all
	jbuilder install

uninstall:
	jbuilder uninstall

test:
	jbuilder runtest

clean:
	jbuilder clean

.PHONY: all clean test install uninstall
