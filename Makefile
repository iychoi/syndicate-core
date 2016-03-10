include buildconf.mk

all: syndicate

syndicate: protobufs libsyndicate libsyndicate-ug ms syndicate-python

.PHONY: protobufs
protobufs:
	$(MAKE) -C protobufs

.PHONY: libsyndicate
libsyndicate: protobufs
	$(MAKE) -C libsyndicate

.PHONY: libsyndicate-ug
libsyndicate-ug: libsyndicate protobufs
	$(MAKE) -C libsyndicate-ug

.PHONY: ms
ms: protobufs 
	$(MAKE) -C ms

.PHONY: syndicate-python
syndicate-python: protobufs ms libsyndicate-ug libsyndicate
	$(MAKE) -C python

.PHONY: clean
clean:
	$(MAKE) -C libsyndicate clean
	$(MAKE) -C protobufs clean
	$(MAKE) -C libsyndicate-ug clean
	$(MAKE) -C ms clean
	$(MAKE) -C python clean

