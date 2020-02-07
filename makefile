KNOCONFIG         = knoconfig
KNOBUILD          = knobuild
prefix		::= $(shell ${KNOCONFIG} prefix)
libsuffix	::= $(shell ${KNOCONFIG} libsuffix)
KNO_CFLAGS	::= -I. -fPIC $(shell ${KNOCONFIG} cflags)
KNO_LDFLAGS	::= -fPIC $(shell ${KNOCONFIG} ldflags)
CMODULES	::= $(DESTDIR)$(shell ${KNOCONFIG} cmodules)
INSTALLMODS	::= $(DESTDIR)$(shell ${KNOCONFIG} installmods)
LIBS		::= $(shell ${KNOCONFIG} libs)
LIB		::= $(shell ${KNOCONFIG} lib)
INCLUDE		::= $(shell ${KNOCONFIG} include)
KNO_VERSION	::= $(shell ${KNOCONFIG} version)
KNO_MAJOR	::= $(shell ${KNOCONFIG} major)
KNO_MINOR	::= $(shell ${KNOCONFIG} minor)
PKG_RELEASE	::= $(cat ./etc/release)
DPKG_NAME	::= $(shell ./etc/dpkgname)
MKSO		::= $(CC) -shared $(LDFLAGS) $(LIBS)
MSG		::= echo
SYSINSTALL      ::= /usr/bin/install -c
DIRINSTALL      ::= /usr/bin/install -d
SUDO            ::= $(shell which sudo)
MODINSTALL      ::= /usr/bin/install -m 0664

PKG_NAME	  = mongodb
GPGID             = FE1BC737F9F323D732AA26330620266BE5AFF294
PKG_RELEASE     ::= $(shell cat etc/release)
PKG_VERSION	::= ${KNO_MAJOR}.${KNO_MINOR}.${PKG_RELEASE}
CODENAME	::= $(shell ${KNOCONFIG} codename)
RELSTATUS	::= $(shell ${KNOBUILD} BUILDSTATUS stable)
DEFAULT_ARCH    ::= $(shell /bin/arch)
ARCH            ::= $(shell ${KNOBUILD} ARCH ${DEFAULT_ARCH})
APKREPO         ::= $(shell ${KNOBUILD} getbuildopt APKREPO /srv/repo/kno/apk)
APK_ARCH_DIR      = ${APKREPO}/staging/${ARCH}

INIT_CFLAGS     ::= ${CFLAGS}
INIT_LDFAGS     ::= ${LDFLAGS}
BSON_CFLAGS       = $(shell etc/pkc --cflags libbson-static-1.0)
BSON_LDFLAGS      = $(shell etc/pkc --libs libbson-static-1.0)
MONGODB_CFLAGS    = $(shell etc/pkc --cflags libmongoc-static-1.0)
MONGODB_LDFLAGS   = $(shell etc/pkc --libs libmongoc-static-1.0)
CFLAGS		  = ${INIT_CFLAGS} ${KNO_CFLAGS} ${BSON_CFLAGS} ${MONGODB_CFLAGS}
LDFLAGS		  = ${INIT_LDFLAGS} ${KNO_LDFLAGS} ${BSON_LDFLAGS} ${MONGODB_LDFLAGS}


default build: mongodb.${libsuffix}

mongo-c-driver/.git:
	git submodule init
	git submodule update
mongo-c-driver/cmake-build/Makefile: mongo-c-driver/.git
	if test ! -d mongo-c-driver/cmake-build; then mkdir mongo-c-driver/cmake-build; fi && \
	cd mongo-c-driver/cmake-build && \
	cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
	      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
	      -DCMAKE_INSTALL_PREFIX=../../installed \
	      ${CMAKE_FLAGS} \
	      ..

installed:
	if ! -d installed; then mkdir installed; fi

STATICLIBS=installed/lib/libbson-static-1.0.a installed/lib/libmongoc-static-1.0.a

mongodb.o: mongodb.c mongodb.h makefile ${STATICLIBS}
	@$(CC) $(CFLAGS) -o $@ -c $<
	@$(MSG) CC "(MONGODB)" $@
mongodb.so: mongodb.o mongodb.h makefile
	@$(MKSO) -o $@ mongodb.o -Wl,-soname=$(@F).${PKG_VERSION} \
	          -Wl,--allow-multiple-definition \
	          -Wl,--whole-archive ${STATICLIBS} -Wl,--no-whole-archive \
		 $(LDFLAGS)
	@if test ! -z "${COPY_CMODS}"; then cp $@ ${COPY_CMODS}; fi;
	@$(MSG) MKSO "(MONGODB)" $@

mongodb.dylib: mongodb.o mongodb.h
	@$(MACLIBTOOL) -install_name \
		`basename $(@F) .dylib`.${KNO_MAJOR}.dylib \
		$(DYLIB_FLAGS) $(BSON_LDFLAGS) $(MONGODB_LDFLAGS) \
		-o $@ mongodb.o 
	@if test ! -z "${COPY_CMODS}"; then cp $@ ${COPY_CMODS}; fi;
	@$(MSG) MACLIBTOOL "(MONGODB)" $@

${STATICLIBS}: # mongo-c-driver/cmake-build/Makefile
	make mongo-c-driver/cmake-build/Makefile
	make -C mongo-c-driver/cmake-build install
	if test -d installed/lib; then \
	  echo > /dev/null; \
	elif test -d installed/lib64; then \
	  ln -sf lib64 installed/lib; \
	else echo "No install libdir"; \
	fi
staticlibs: ${STATICLIBS}
mongodb.dylib mongodb.so: staticlibs

install: install-cmodule install-scheme
suinstall doinstall:
	sudo make install

${CMODULES}:
	@${DIRINSTALL} ${CMODULES}

install-cmodule: build ${CMODULES}
	@${SUDO} ${SYSINSTALL} mongodb.${libsuffix} ${CMODULES}/mongodb.so.${PKG_VERSION}
	@echo === Installed ${CMODULES}/mongodb.so.${PKG_VERSION}
	@${SUDO} ln -sf mongodb.so.${PKG_VERSION} ${CMODULES}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR}
	@echo === Linked ${CMODULES}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR} to mongodb.so.${PKG_VERSION}
	@${SUDO} ln -sf mongodb.so.${PKG_VERSION} ${CMODULES}/mongodb.so.${KNO_MAJOR}
	@echo === Linked ${CMODULES}/mongodb.so.${KNO_MAJOR} to mongodb.so.${PKG_VERSION}
	@${SUDO} ln -sf mongodb.so.${PKG_VERSION} ${CMODULES}/mongodb.so
	@echo === Linked ${CMODULES}/mongodb.so to mongodb.so.${PKG_VERSION}

${INSTALLMODS}/mongodb:
	${SUDO} ${DIRINSTALL} $@

install-scheme: ${INSTALLMODS}/mongodb
	${SUDO} ${MODINSTALL} scheme/mongodb/*.scm ${INSTALLMODS}/mongodb

clean:
	rm -f *.o *.${libsuffix} *.${libsuffix}*
deep-clean: clean
	if test -f mongo-c-driver/Makefile; then cd mongo-c-driver; make clean; fi;
	rm -rf mongo-c-driver/cmake-build installed
fresh: clean
	make

gitup gitup-trunk:
	git checkout trunk && git pull

# Debian packaging

debian: mongodb.c mongodb.h makefile \
		dist/debian/rules dist/debian/control \
		dist/debian/changelog.base
	rm -rf debian
	cp -r dist/debian debian
	cat debian/changelog.base | \
		knomod debchangelog kno-${PKG_NAME} ${CODENAME} ${RELSTATUS} > $@.tmp

debian/changelog: debian mongodb.c mongodb.h makefile
	cat debian/changelog.base | \
		knomod debchangelog kno-${PKG_NAME} ${CODENAME} ${RELSTATUS} > $@.tmp
	if test ! -f debian/changelog; then \
	  mv debian/changelog.tmp debian/changelog; \
	elif diff debian/changelog debian/changelog.tmp 2>&1 > /dev/null; then \
	  mv debian/changelog.tmp debian/changelog; \
	else rm debian/changelog.tmp; fi

dist/debian.built: mongodb.c mongodb.h makefile debian debian/changelog
	dpkg-buildpackage -sa -us -uc -b -rfakeroot && \
	touch $@

dist/debian.signed: dist/debian.built
	debsign --re-sign -k${GPGID} ../kno-mongodb_*.changes && \
	touch $@

deb debs dpkg dpkgs: dist/debian.signed

dist/debian.updated: dist/debian.signed
	dupload -c ./dist/dupload.conf --nomail --to bionic ../kno-mongodb_*.changes && touch $@

update-apt: dist/debian.updated

debinstall: dist/debian.signed
	${SUDO} dpkg -i ../kno-mongo*.deb

debclean: clean
	rm -rf ../kno-mongodb_* ../kno-mongodb-* debian dist/debian.*

debfresh:
	make debclean
	make dist/debian.signed

# Alpine packaging

staging/alpine:
	@install -d $@

staging/alpine/APKBUILD: dist/alpine/APKBUILD staging/alpine
	cp dist/alpine/APKBUILD staging/alpine

staging/alpine/kno-${PKG_NAME}.tar: staging/alpine
	git archive --prefix=kno-${PKG_NAME}/ -o staging/alpine/kno-${PKG_NAME}.tar HEAD

dist/alpine.done: staging/alpine/APKBUILD makefile ${STATICLIBS} \
	staging/alpine/kno-${PKG_NAME}.tar
	if [ ! -d ${APK_ARCH_DIR} ]; then mkdir -p ${APK_ARCH_DIR}; fi;
	cd staging/alpine; \
		abuild -P ${APKREPO} clean cleancache cleanpkg && \
		abuild checksum && \
		abuild -P ${APKREPO} && \
		touch ../../$@

alpine: dist/alpine.done

.PHONY: alpine

