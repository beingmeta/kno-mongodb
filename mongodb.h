#define MONGOC_ENABLE_SSL 1

#include <bson.h>
#include <mongoc.h>

#include <kno/storage.h>

/*
  BSON -> LISP mapping

  strings, packets, ints, doubles, true, false, symbols,
  timestamp, uuid (direct),
  slotmaps are objects (unparse-arg/parse-arg),
  BSON arrays are LISP choices, BSON_NULL is empty choice,
  LISP vectors are converted into arrays of arrays,
  other types are also objects, with a _kind attribute,
  including bignums, rational and complex numbers, quoted
  choices, etc.

*/

#define KNO_MONGODB_SLOTIFY       0x00001
#define KNO_MONGODB_COLONIZE      0x00002
#define KNO_MONGODB_PREFCHOICES   0x00004
#define KNO_MONGODB_CHOICESLOT    0x00008
#define KNO_MONGODB_SYMSLOT       0x00010
#define KNO_MONGODB_RAWSLOT       0x00020
#define KNO_MONGODB_NOBLOCK       0x10000
#define KNO_MONGODB_LOGOPS	  0x20000

#define KNO_MONGODB_DEFAULTS (KNO_MONGODB_COLONIZE    | KNO_MONGODB_SLOTIFY)

#ifndef CHOICE_TAGSTRING_TEXT
#define CHOICE_TAGSTRING_TEXT "%%ChOiCe%%"
#endif

KNO_EXPORT u8_condition kno_MongoDB_Error, kno_MongoDB_Warning;
KNO_EXPORT kno_lisp_type kno_mongoc_server, kno_mongoc_collection, kno_mongoc_cursor;

typedef struct KNO_BSON_OUTPUT {
  bson_t *bson_doc;
  lispval bson_opts, bson_fieldmap;
  int bson_flags;} KNO_BSON_OUTPUT;
typedef struct KNO_BSON_INPUT {
  bson_iter_t *bson_iter;
  lispval bson_opts, bson_fieldmap;
  int bson_flags;} KNO_BSON_INPUT;
typedef struct KNO_BSON_INPUT *kno_bson_input;

typedef struct KNO_MONGODB_DATABASE {
  KNO_CONS_HEADER;
  u8_string dburi, dbname, dbspec;
  lispval dbopts;
  int dbflags;
  mongoc_client_pool_t *dbclients;
  mongoc_uri_t *dburi_info;} KNO_MONGODB_DATABASE;
typedef struct KNO_MONGODB_DATABASE *kno_mongodb_database;

typedef struct KNO_MONGODB_COLLECTION {
  KNO_CONS_HEADER;
  u8_string collection_name;
  lispval collection_db;
  lispval collection_opts;
  lispval collection_oidslot;
  u8_string collection_oidkey;
  int collection_flags;}
  KNO_MONGODB_COLLECTION;
typedef struct KNO_MONGODB_COLLECTION *kno_mongodb_collection;

typedef struct KNO_MONGODB_CURSOR {
  KNO_CONS_HEADER;
  lispval cursor_db, cursor_coll, cursor_query;
  lispval cursor_opts;
  int cursor_flags, cursor_done;
  ssize_t cursor_skipped;
  ssize_t cursor_read;
  long long cursor_threadid;
  mongoc_client_t *cursor_connection;
  mongoc_collection_t *cursor_collection;
  bson_t *cursor_query_bson;
  bson_t *cursor_opts_bson;
  const bson_t *cursor_value_bson;
  mongoc_read_prefs_t *cursor_readprefs;
  mongoc_cursor_t *mongoc_cursor;}
  KNO_MONGODB_CURSOR;
typedef struct KNO_MONGODB_CURSOR *kno_mongodb_cursor;

KNO_EXPORT lispval kno_bson_write(bson_t *out,int flags,lispval in);
KNO_EXPORT bson_t *kno_lisp2bson(lispval,int,lispval);
KNO_EXPORT lispval kno_bson2lisp(bson_t *,int,lispval);
KNO_EXPORT lispval kno_bson_output(struct KNO_BSON_OUTPUT,lispval);
KNO_EXPORT int kno_init_mongodb(void);

#define COLLECTION2DB(dom) \
  ((struct KNO_MONGODB_DATABASE *) ((dom)->collection_db))
#define COLL2DB(dom) \
  ((struct KNO_MONGODB_DATABASE *) ((dom)->collection_db))
#define CURSOR2COLL(cursor) \
  ((struct KNO_MONGODB_COLLECTION *) ((cursor)->cursor_coll))

/* Compatability stuff */

#ifndef KNO_AGGREGATE
#ifdef KNO_NDCALL
#define KNO_AGGREGATE KNO_NDCALL
#endif
#endif
