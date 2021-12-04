PKGNAME		  = mongodb
LIBNAME		  = mongodb
KNOCONFIG         = knoconfig
KNOBUILD          = knobuild
MONGOCINSTALL     = mongoc-install

prefix		::= $(shell ${KNOCONFIG} prefix)
libsuffix	::= $(shell ${KNOCONFIG} libsuffix)
KNO_CFLAGS	::= -I. -fPIC $(shell ${KNOCONFIG} cflags)
KNO_LDFLAGS	::= -fPIC $(shell ${KNOCONFIG} ldflags)
KNO_LIBS	::= $(shell ${KNOCONFIG} libs)
CMODULES	::= $(DESTDIR)$(shell ${KNOCONFIG} cmodules)
INSTALLMODS	::= $(DESTDIR)$(shell ${KNOCONFIG} installmods)
LIBS		::= $(shell ${KNOCONFIG} libs)
LIB		::= $(shell ${KNOCONFIG} lib)
INCLUDE		::= $(shell ${KNOCONFIG} include)
KNO_VERSION	::= $(shell ${KNOCONFIG} version)
KNO_MAJOR	::= $(shell ${KNOCONFIG} major)
KNO_MINOR	::= $(shell ${KNOCONFIG} minor)
PKG_VERSION     ::= $(shell u8_gitversion ./etc/knomod_version)
PKG_MAJOR       ::= $(shell cat ./etc/knomod_version | cut -d. -f1)
FULL_VERSION    ::= ${KNO_MAJOR}.${KNO_MINOR}.${PKG_VERSION}
PATCHLEVEL      ::= $(shell u8_gitpatchcount ./etc/knomod_version)
PATCH_VERSION   ::= ${FULL_VERSION}-${PATCHLEVEL}

SUDO            ::= $(shell which sudo)
INIT_CFLAGS     ::= ${CFLAGS}
INIT_LDFAGS     ::= ${LDFLAGS}
BSON_CFLAGS     ::= $(shell INSTALLROOT=mongoc-install etc/pkc --static --cflags libbson-static-1.0)
BSON_LDFLAGS    ::= $(shell INSTALLROOT=mongoc-install etc/pkc --static --libs libbson-static-1.0)
MONGODB_CFLAGS  ::= $(shell INSTALLROOT=mongoc-install etc/pkc --static --cflags libmongoc-static-1.0)
MONGODB_LDFLAGS ::= $(shell INSTALLROOT=mongoc-install etc/pkc --static --libs libmongoc-static-1.0)
XCFLAGS	  	  = ${INIT_CFLAGS} ${MONGODB_CFLAGS} ${KNO_CFLAGS} ${BSON_CFLAGS} ${MONGODB_CFLAGS}
XLDFLAGS	  = ${INIT_LDFLAGS} ${KNO_LDFLAGS} ${BSON_LDFLAGS} ${MONGODB_LDFLAGS}

MKSO		  = $(CC) -shared $(LDFLAGS) $(LIBS)
SYSINSTALL        = /usr/bin/install -c
DIRINSTALL        = /usr/bin/install -d
MODINSTALL        = /usr/bin/install -m 0664
USEDIR        	  = /usr/bin/install -d
MSG		  = echo
MACLIBTOOL	  = $(CC) -dynamiclib -single_module -undefined dynamic_lookup \
			$(LDFLAGS)

GPGID           ::= ${OVERRIDE_GPGID:-FE1BC737F9F323D732AA26330620266BE5AFF294}
CODENAME	::= $(shell ${KNOCONFIG} codename)
REL_BRANCH	::= $(shell ${KNOBUILD} getbuildopt REL_BRANCH current)
REL_STATUS	::= $(shell ${KNOBUILD} getbuildopt REL_STATUS stable)
REL_PRIORITY	::= $(shell ${KNOBUILD} getbuildopt REL_PRIORITY medium)
ARCH            ::= $(shell ${KNOBUILD} getbuildopt BUILD_ARCH || uname -m)
APKREPO         ::= $(shell ${KNOBUILD} getbuildopt APKREPO /srv/repo/kno/apk)
APK_ARCH_DIR      = ${APKREPO}/staging/${ARCH}
RPMDIR		  = dist

STATICLIBS=mongoc-install/lib/libbson-static-1.0.a \
	mongoc-install/lib/libmongoc-static-1.0.a

default:
	@make ${STATICLIBS}
	@make mongodb.${libsuffix}

mongo-c-driver/CMakeLists.txt:
	@git submodule init
	@git submodule update

mongoc-build/Makefile: mongo-c-driver/CMakeLists.txt
	${USEDIR} mongoc-build
	cd mongoc-build; cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF \
	      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
	      -DBUILD_SHARED_LIBS=off 		\
	      -DCMAKE_INSTALL_PREFIX=../mongoc-install \
	      ${CMAKE_FLAGS} \
	      ../mongo-c-driver

mongodb.o: mongodb.c mongodb.h makefile ${STATICLIBS}
	@echo XCFLAGS=${XCFLAGS}
	@$(CC) --save-temps $(XCFLAGS) -D_FILEINFO="\"$(shell u8_fileinfo ./$< $(dirname $(pwd))/)\"" -o $@ -c $<
	@$(MSG) CC "(MONGODB)" $@
mongodb.so: mongodb.o mongodb.h makefile
	@$(MKSO) -o $@ mongodb.o -Wl,-soname=$(@F).${FULL_VERSION} \
	          -Wl,--allow-multiple-definition \
	          -Wl,--whole-archive ${STATICLIBS} -Wl,--no-whole-archive \
		 $(XLDFLAGS)
	@$(MSG) MKSO "(MONGODB)" $@

mongodb.dylib: mongodb.o mongodb.h
	@$(MACLIBTOOL) -install_name \
		`basename $(@F) .dylib`.${KNO_MAJOR}.dylib \
		$(DYLIB_FLAGS) $(BSON_LDFLAGS) $(MONGODB_LDFLAGS) \
		-o $@ mongodb.o 
	@$(MSG) MACLIBTOOL "(MONGODB)" $@

mongoc-install/lib/libbson-static-1.0.a mongoc-install/lib/libmongoc-static-1.0.a: mongoc-build/Makefile
	${USEDIR} mongoc-install
	make -C mongoc-build install
	if test -d mongoc-install/lib; then \
	  echo > /dev/null; \
	elif test -d mongoc-install/lib64; then \
	  ln -sf lib64 mongoc-install/lib; \
	else echo "No install libdir"; \
	fi
	touch ${STATICLIBS}
staticlibs: ${STATICLIBS}
mongodb.dylib mongodb.so: staticlibs

scheme/mongodb.zip: scheme/mongodb/*.scm
	cd scheme; zip mongodb.zip mongodb -x "*~" -x "#*" -x "*.attic/*" -x ".git*"

install: install-cmodule install-scheme
suinstall doinstall:
	sudo make install

${CMODULES}:
	@${DIRINSTALL} ${CMODULES}

install-cmodule: ${CMODULES}
	${SUDO} u8_install_shared ${LIBNAME}.${libsuffix} ${CMODULES} ${FULL_VERSION} "${SYSINSTALL}"

${INSTALLMODS}/mongodb:
	${SUDO} ${DIRINSTALL} $@

install-scheme-zip: ${INSTALLMODS}/mongodb.zip
	${SUDO} ${MODINSTALL} scheme/mongodb/*.scm ${INSTALLMODS}/mongodb

install-scheme: ${INSTALLMODS}/mongodb
	${SUDO} ${MODINSTALL} scheme/mongodb/*.scm ${INSTALLMODS}/mongodb

clean:
	rm -f *.o *.${libsuffix} *.${libsuffix}*
deep-clean: clean
	if test -f mongo-c-driver/Makefile; then cd mongo-c-driver; make clean; fi;
	rm -rf mongoc-build install
fresh: clean
	make
deep-fresh: deep-clean
	make

gitup gitup-trunk:
	git checkout trunk && git pull

# Alpine packaging

staging/alpine:
	@install -d $@

staging/alpine/APKBUILD: dist/alpine/APKBUILD staging/alpine
	cp dist/alpine/APKBUILD staging/alpine

staging/alpine/kno-${PKG_NAME}.tar: staging/alpine
	git archive --prefix=kno-${PKG_NAME}/ -o staging/alpine/kno-${PKG_NAME}.tar HEAD

dist/alpine.setup: staging/alpine/APKBUILD makefile ${STATICLIBS} \
	staging/alpine/kno-${PKG_NAME}.tar
	if [ ! -d ${APK_ARCH_DIR} ]; then mkdir -p ${APK_ARCH_DIR}; fi && \
	( cd staging/alpine; \
		abuild -P ${APKREPO} clean cleancache cleanpkg && \
		abuild checksum ) && \
	touch $@

dist/alpine.done: dist/alpine.setup
	( cd staging/alpine; abuild -P ${APKREPO} ) && touch $@
dist/alpine.installed: dist/alpine.setup
	( cd staging/alpine; abuild -i -P ${APKREPO} ) && touch dist/alpine.done && touch $@

TAGS: mongodb.c mongodb.h scheme/mongodb/*.scm
	etags -o $@ $^

alpine: dist/alpine.done
install-alpine: dist/alpine.done

.PHONY: alpine

