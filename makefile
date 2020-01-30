KNOCONFIG       ::= knoconfig
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
MODINSTALL      ::= /usr/bin/install -C --mode=0664
MOD_RELEASE     ::= $(shell cat etc/release)
MOD_VERSION	::= ${KNO_MAJOR}.${KNO_MINOR}.${MOD_RELEASE}
APKREPO         ::= $(shell ${KNOCONFIG} apkrepo)

GPGID           ::= FE1BC737F9F323D732AA26330620266BE5AFF294
SUDO            ::= $(shell which sudo)
CMAKE_FLAGS       =

INIT_CFLAGS     ::= ${CFLAGS}
INIT_LDFAGS     ::= ${LDFLAGS}
BSON_CFLAGS       = $(shell etc/pkc --cflags libbson-static-1.0)
BSON_LDFLAGS      = $(shell etc/pkc --libs libbson-static-1.0)
MONGO_CFLAGS      = $(shell etc/pkc --cflags libmongoc-static-1.0)
MONGO_LDFLAGS     = $(shell etc/pkc --libs libmongoc-static-1.0)
CFLAGS		  = ${INIT_CFLAGS} ${KNO_CFLAGS} ${BSON_CFLAGS} ${MONGO_CFLAGS}
LDFLAGS		  = ${INIT_LDFLAGS} ${KNO_LDFLAGS} ${BSON_LDFLAGS} ${MONGO_LDFLAGS}


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
	      ..

STATICLIBS=installed/lib/libbson-static-1.0.a installed/lib/libmongoc-static-1.0.a

mongodb.o: mongodb.c mongodb.h makefile ${STATICLIBS}
	@$(CC) $(CFLAGS) -o $@ -c $<
	@$(MSG) CC "(MONGODB)" $@
mongodb.so: mongodb.o mongodb.h makefile
	@$(MKSO) -o $@ mongodb.o -Wl,-soname=$(@F).${MOD_VERSION} \
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

${STATICLIBS}: mongo-c-driver/cmake-build/Makefile
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

install-cmodule: build
	@${SUDO} ${SYSINSTALL} mongodb.${libsuffix} ${CMODULES}/mongodb.so.${MOD_VERSION}
	@echo === Installed ${CMODULES}/mongodb.so.${MOD_VERSION}
	@${SUDO} ln -sf mongodb.so.${MOD_VERSION} ${CMODULES}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR}
	@echo === Linked ${CMODULES}/mongodb.so.${KNO_MAJOR}.${KNO_MINOR} to mongodb.so.${MOD_VERSION}
	@${SUDO} ln -sf mongodb.so.${MOD_VERSION} ${CMODULES}/mongodb.so.${KNO_MAJOR}
	@echo === Linked ${CMODULES}/mongodb.so.${KNO_MAJOR} to mongodb.so.${MOD_VERSION}
	@${SUDO} ln -sf mongodb.so.${MOD_VERSION} ${CMODULES}/mongodb.so
	@echo === Linked ${CMODULES}/mongodb.so to mongodb.so.${MOD_VERSION}

${INSTALLMODS}/mongodb:
	${SUDO} ${DIRINSTALL} $@

install-scheme: ${INSTALLMODS}/mongodb
	${SUDO} ${MODINSTALL} scheme/mongodb/*.scm ${INSTALLMODS}/mongodb

clean:
	rm -f *.o *.${libsuffix} *.${libsuffix}*
deep-clean: clean
	if test -f mongo-c-driver/Makefile; then cd mongo-c-driver; make clean; fi;
	rm -rf mongo-c-driver/cmake-build installed

debian: mongodb.c mongodb.h makefile \
		dist/debian/rules dist/debian/control \
		dist/debian/changelog.base
	rm -rf debian
	cp -r dist/debian debian
	cat debian/changelog.base | etc/gitchangelog kno-mongo > debian/changelog

debian/changelog: debian mongodb.c mongodb.h makefile
	cat debian/changelog.base | etc/gitchangelog kno-mongo > $@.tmp
	@if test ! -f debian/changelog; then \
	  mv debian/changelog.tmp debian/changelog; \
	 elif diff debian/changelog debian/changelog.tmp 2>&1 > /dev/null; then \
	  mv debian/changelog.tmp debian/changelog; \
	 else rm debian/changelog.tmp; fi

dist/debian.built: mongodb.c mongodb.h makefile debian debian/changelog
	dpkg-buildpackage -sa -us -uc -b -rfakeroot && \
	touch $@

dist/debian.signed: dist/debian.built
	debsign --re-sign -k${GPGID} ../kno-mongo_*.changes && \
	touch $@

deb debs dpkg dpkgs: dist/debian.signed

dist/debian.updated: dist/debian.signed
	dupload -c ./dist/dupload.conf --nomail --to bionic ../kno-mongo_*.changes && touch $@

update-apt: dist/debian.updated

debinstall: dist/debian.signed
	${SUDO} dpkg -i ../kno-mongo*.deb

debclean: clean
	rm -rf ../kno-mongo_* ../kno-mongo-* debian dist/debian.*

debfresh:
	make debclean
	make dist/debian.built

# Alpine packaging

staging/alpine/APKBUILD: dist/alpine/APKBUILD
	if test ! -d staging; then mkdir staging; fi
	if test ! -d staging/alpine; then mkdir staging/alpine; fi
	cp dist/alpine/APKBUILD staging/alpine/APKBUILD

dist/alpine.done: staging/alpine/APKBUILD
	cd dist/alpine; \
		abuild -P ${APKREPO} clean cleancache cleanpkg
	cd staging/alpine; \
		abuild -P ${APKREPO} checksum && \
		abuild -P ${APKREPO} && \
		cd ../..; touch $@

alpine: dist/alpine.done

.PHONY: alpine
