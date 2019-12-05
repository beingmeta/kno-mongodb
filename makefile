prefix		::= $(shell knoconfig prefix)
libsuffix	::= $(shell knoconfig libsuffix)
KNO_CFLAGS	::= -I. -fPIC $(shell knoconfig cflags)
KNO_LDFLAGS	::= -fPIC $(shell knoconfig ldflags)
MCD		::= ./mongo-c-driver/src
MONGODB_CFLAGS  ::= -I${MCD}/libbson/src -I${MCD}/libbson/src/bson \
		    -I${MCD}/libmongoc/src -I${MCD}/libmongoc/src/mongoc
MONGODB_LDFLAGS ::= -L${MCD}/libbson -L${MCD}/libmongoc
CFLAGS		::= ${KNO_CFLAGS} ${MONGODB_CFLAGS}
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

mongodb.o: mongodb.c mongodb.h makefile
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

${CLIBS}:
	cd mongo-c-driver; cmake .; make;

