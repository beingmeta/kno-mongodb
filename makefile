prefix		::= $(shell knoconfig prefix)
libsuffix	::= $(shell knoconfig libsuffix)
KNO_CFLAGS	::= -I. -fPIC $(shell knoconfig cflags)
KNO_LDFLAGS	::= -fPIC $(shell knoconfig ldflags)
MCD		::= ./mongo-c-driver/src
MONGODB_CFLAGS  ::= -I${MCD}/libbson/src -I${MCD}/libbson/src/bson \
		    -I${MCD}/libmongoc/src -I${MCD}/libmongoc/src/mongoc
MONGODB_LDFLAGS ::= -L${MCD}/libbson -L${MCD}/libmongoc
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

CLIBS=${MCD}/libmongoc/libmongoc-static-1.0.a ${MCD}/libbson/libbson-static-1.0.a

default: mongodb.${libsuffix}

mongo-c-driver/build: mongo-c-driver/.git
mongo-c-driver/Makefile: mongo-c-driver/build
	cd mongo-c-driver/build; cmake -DENABLE_AUTOMATIC_INIT_AND_CLEANUP=OFF .. && touch ../Makefile

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

${CLIBS}: mongo-c-driver/Makefile
	make -C mongo-c-driver && touch ${CLIBS}
clibs: ${CLIBS}

clean:
	rm -f *.o *.${libsuffix}
deep-clean: clean
	if -f mongo-c-driver/Makefile; then cd mongo-c-driver; make clean; fi;
