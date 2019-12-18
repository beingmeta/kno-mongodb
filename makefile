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
#MONGOLIBS	::= -lrt -lresolv -licuuc -licudata 
MONGOLIBS	::= -lrt -licuuc
LIB		::= $(shell knoconfig lib)
INCLUDE		::= $(shell knoconfig include)
KNO_VERSION	::= $(shell knoconfig version)
KNO_MAJOR	::= $(shell knoconfig major)
KNO_MINOR	::= $(shell knoconfig minor)
PKG_RELEASE	::= $(cat ./etc/release)
DPKG_NAME	::= $(shell ./etc/dpkgname)
MKSO		::= $(CC) -shared $(LDFLAGS) $(LIBS)
MSG		::= echo
SYSINSTALL      ::= /usr/bin/install -c
MOD_RELEASE     ::= $(shell cat etc/release)
MOD_VERSION	::= ${KNO_MAJOR}.${KNO_MINOR}.${MOD_RELEASE}

GPGID           ::= FE1BC737F9F323D732AA26330620266BE5AFF294
SUDO            ::= $(shell which sudo)

CLIBS=${MCB}/libbson/libbson-static-1.0.a ${MCB}/libmongoc/libmongoc-static-1.0.a
#CLIBS=${MCB}/libmongoc/libmongoc-static-1.0.a

default: mongodb.${libsuffix}

mongo-c-driver/.git:
	git submodule init
	git submodule update
mongo-c-driver/cmake-build/Makefile: mongo-c-driver/.git
	if test ! -d mongo-c-driver/cmake-build; then mkdir mongo-c-driver/cmake-build; fi && \
	cd mongo-c-driver/cmake-build && cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF -DCMAKE_POSITION_INDEPENDENT_CODE=ON ..
# set(CMAKE_POSITION_INDEPENDENT_CODE ON)

install:
	@${SUDO} ${SYSINSTALL} mongodb.${libsuffix} ${CMODULES}/mongodb.so.${MOD_VERSION}
	@echo === Installed ${CMODULES}/mongodb.so.${MOD_VERSION}
	@${SUDO} ln -sf mongodb.so.${MOD_VERSION} ${DESTDIR}${CMODULES}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR}
	@echo === Linked ${CMODULES}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR} to mongodb.so.${MOD_VERSION}
	@${SUDO} ln -sf mongodb.so.${MOD_VERSION} ${DESTDIR}${CMODULES}/mongodb.so.${KNO_MAJOR}
	@echo === Linked ${CMODULES}/mongodb.so.${KNO_MAJOR} to mongodb.so.${MOD_VERSION}
	@${SUDO} ln -sf mongodb.so.${MOD_VERSION} ${DESTDIR}${CMODULES}/mongodb.so
	@echo === Linked ${CMODULES}/mongodb.so to mongodb.so.${MOD_VERSION}

mongodb.o: mongodb.c mongodb.h makefile ${CLIBS}
	$(CC) $(CFLAGS) $(LDFLAGS) -lrt -o $@ -c $<
	@$(MSG) CC "(MONGODB)" $@
mongodb.so: mongodb.o mongodb.h makefile
	 $(MKSO) -o $@ mongodb.o \
		 -Wl,-soname=$(@F).${MOD_VERSION} \
		 -Wl,--allow-multiple-definition \
	         -Wl,--whole-archive ${CLIBS} -Wl,--no-whole-archive \
		${MONGOLIBS} ${LIBS}
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
	if test -f mongo-c-driver/Makefile; then cd mongo-c-driver; make clean; fi;
	rm -rf mongo-c-driver/cmake-build

debian/changelog: mongodb.c mongodb.h makefile debian/rules debian/control debian/changelog.base
	cat debian/changelog.base | etc/gitchangelog kno-mongo > $@

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
	rm debian/changelog

debfresh:
	make debclean
	make debian.built
