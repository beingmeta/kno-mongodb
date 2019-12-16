prefix		::= $(shell knoconfig prefix)
libsuffix	::= $(shell knoconfig libsuffix)
KNO_CFLAGS	::= -I. -fPIC $(shell knoconfig cflags)
KNO_LDFLAGS	::= -fPIC $(shell knoconfig ldflags)
MCD		::= ./mongo-c-driver/src
MCB		::= ./mongo-c-driver/cmake-build/src
MONGODB_CFLAGS  ::= -I${MCB}/libbson/src/bson -I${MCB}/libmongoc/src/mongoc \
		    -I${MCB}/libbson/src -I${MCB}/libmongoc/src \
		    -I${MCD}/libbson/src/bson -I${MCD}/libmongoc/src/mongoc \
		    -I${MCD}/libbson/src -I${MCD}/libmongoc/src
MONGODB_LDFLAGS ::= -L${MCB}/libbson -L${MCB}/libmongoc
CFLAGS		::= ${KNO_CFLAGS} ${MONGODB_CFLAGS}
CMODULES	::= $(DESTDIR)$(shell knoconfig cmodules)
LDFLAGS		::= ${KNO_LDFLAGS} ${MONGODB_LDFLAGS}
LIBS		::= $(shell knoconfig libs)
LIB		::= $(shell knoconfig lib)
INCLUDE		::= $(shell knoconfig include)
KNO_VERSION	::= $(shell knoconfig version)
KNO_MAJOR	::= $(shell knoconfig major)
KNO_MINOR	::= $(shell knoconfig minor)
MKSO		::= $(CC) -shared $(LDFLAGS) $(LIBS)
MSG		::= echo
SYSINSTALL      ::= /usr/bin/install -c
GPGID           ::= FE1BC737F9F323D732AA26330620266BE5AFF294
SUDO            ::=

CLIBS=${MCB}/src/libmongoc/libmongoc-static-1.0.a ${MCB}/src/libbson/libbson-static-1.0.a

default: mongodb.${libsuffix}

mongo-c-driver/.git:
	git submodule init
	git submodule update
mongo-c-driver/cmake-build/Makefile: mongo-c-driver/.git
	if ! -d mongo-c-driver/cmake-build;  mkdir mongo-c-driver/cmake-build; fi && \
	cd mongo-c-driver/cmake-build && cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF ..

install:
	install mongodb.${libsuffix} ${CMODULES}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR}
	ln -sf ${DESTDIR}${cmodules}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR} ${DESTDIR}${cmodules}/mongodb.so.${KNO_MAJOR}
	ln -sf ${DESTDIR}${cmodules}/mongodb.so.${KNO_MINOR} ${DESTDIR}${cmodules}/mongodb.so.${KNO_MAJOR}

mongodb.o: mongodb.c mongodb.h makefile ${CLIBS}
	$(CC) $(CFLAGS) $(LDFLAGS)  -o $@ -c $< -lbson-static-1.0 -lmongoc-static-1.0
	@$(MSG) CC "(MONGODB)" $@
mongodb.so: mongodb.o mongodb.h
	@$(MKSO) -L./ -L./lib@ -o $@ $<
	@ln -sf $(@F) $(@D)/$(@F).4
	@$(MSG) MKSO "(MONGODB)" $@
mongodb.dylib: mongodb.o mongodb.h
	@$(MACLIBTOOL) -install_name \
		`basename $(@F) .dylib`.${KNO_MAJOR}.dylib \
		$(DYLIB_FLAGS) \
		-o $@ mongodb.o 
	@$(MSG) MACLIBTOOL "(MONGODB)" $@

${CLIBS}: mongo-c-driver/cmake-build/Makefile
	make -C mongo-c-driver/cmake-build

clibs: ${CLIBS}

clean:
	rm -f *.o *.${libsuffix}
deep-clean: clean
	if -f mongo-c-driver/Makefile; then cd mongo-c-driver; make clean; fi;

debian.built: mongodb.c mongodb.h makefile debian/rules debian/control
	dpkg-buildpackage -sa -us -uc -b -rfakeroot && \
	touch $@

debian.signed: debian.built
	debsign --re-sign -k${GPGID} ../kno-mongo_*.changes && \
	touch $@

debian.updated: debian.signed
	dupload -c ./debian/dupload.conf --nomail --to bionic ../kno-mongo_*.changes && touch $@

update-apt: debian.updated

debclean:
	rm ../kno-mongo_* ../kno-mongo-*

debfresh:
	make debclean
	make debian.built
