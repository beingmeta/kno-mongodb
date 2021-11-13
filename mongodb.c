/* -*- Mode: C; Character-encoding: utf-8; -*- */

/* mongodb.c
   This implements Kno bindings to mongodb.
   Copyright (C) 2007-2019 beingmeta, inc.
   Copyright (C) 2020-2021 beingmeta, LLC
*/

#ifndef _FILEINFO
#define _FILEINFO __FILE__
#endif

static int mongodb_loglevel;
#define U8_LOGLEVEL mongodb_loglevel

#define U8_INLINE_IO 1

#include "kno/knosource.h"
#include "kno/lisp.h"
#include "kno/compounds.h"
#include "kno/numbers.h"
#include "kno/eval.h"
#include "kno/sequences.h"
#include "kno/texttools.h"
#include "kno/bigints.h"
#include "kno/cprims.h"
#include "kno/knoregex.h"
#include "mongodb.h"

#include <libu8/libu8.h>
#include <libu8/u8printf.h>
#include <libu8/u8crypto.h>
#include <libu8/u8pathfns.h>

#include <math.h>

/* Initialization */

u8_condition kno_MongoDB_Error=_("MongoDB error");
u8_condition kno_BSON_Error=_("BSON conversion error");
u8_condition kno_MongoDB_Warning=_("MongoDB warning");
u8_condition kno_BSON_Input_Error=_("BSON input error");
u8_condition kno_BSON_Compound_Overflow=_("BSON/Kno compound overflow");

/* These are the new cons types introducted for mongodb */
kno_lisp_type kno_mongoc_server, kno_mongoc_collection, kno_mongoc_cursor;
#define KNO_MONGOC_SERVER     0xEF5970L
#define KNO_MONGOC_COLLECTION 0xEF5971L
#define KNO_MONGOC_CURSOR     0xEF5972L
#define kno_mongoc_server_type kno_mongoc_server
#define kno_mongoc_collection_type kno_mongoc_collection
#define kno_mongoc_cursor_type kno_mongoc_cursor

#define KNO_FIND_MATCHES  1
#define KNO_COUNT_MATCHES 0

static lispval sslsym, smoketest_sym;
static int mongodb_loglevel = LOG_NOTICE;
static int logops = 0, logcmds = 0;

static int default_ssl = 0;
static u8_string default_cafile = NULL;
static u8_string default_cadir = NULL;
static u8_string default_certfile = NULL;

static lispval dbname_symbol, username_symbol, auth_symbol, knotag_symbol;
static lispval hosts_symbol, connections_symbol, fieldmap_symbol, logopsym;
static lispval knoparse_symbol, dotcar_symbol, dotcdr_symbol;
static lispval certfile, certpass, cafilesym, cadirsym, crlsym;
static lispval symslots_symbol, choiceslots_symbol, rawslots_symbol;
static lispval mongo_timestamp_tag;

static lispval choice_tagstring, choice_tagsym;

/* The mongo_opmap translates symbols for mongodb operators (like
   $addToSet) into the correctly capitalized strings to use in
   operations. */
static struct KNO_KEYVAL *mongo_opmap = NULL;
static int mongo_opmap_size = 0, mongo_opmap_space = 0;
#ifndef MONGO_OPMAP_MAX
#define MONGO_OPMAP_MAX 8000
#endif

#define MONGO_MULTISLOTS_MAX 2032
static lispval multislots[MONGO_MULTISLOTS_MAX];
static int n_multislots = 0;
static u8_mutex multislots_lock;

static bool bson_append_keyval(struct KNO_BSON_OUTPUT,lispval,lispval);
static bool bson_append_lisp(struct KNO_BSON_OUTPUT,const char *,int,
			      lispval,int);
static lispval idsym, maxkey, minkey;
static lispval oidtag, mongofun, mongouser, mongomd5;
static lispval bsonflags, raw, slotify, stringkeys, softfailsym;
static lispval colonize, rawstrings, choices, nochoices;
static lispval skipsym, limitsym, batchsym, writesym, readsym;;
static lispval fieldssym, upsertsym, newsym, removesym, singlesym, wtimeoutsym;
static lispval returnsym, originalsym;
static lispval primarysym, primarypsym, secondarysym, secondarypsym;
static lispval nearestsym, poolmaxsym;
static lispval mongovec_symbol;


static int default_socket_timeout = -1;
static int default_connect_timeout = -1;
static int default_server_select_timeout = -1;
static int default_maxpools_limit = -1;

DEFC_PRIM("mongodb/oid",mongodb_oidref,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"oid",kno_any_type,KNO_VOID})
static lispval mongodb_oidref(lispval oid)
{
  if (KNO_OIDP(oid)) {
    KNO_OID addr = KNO_OID_ADDR(oid);
    unsigned int hi = KNO_OID_HI(addr), lo = KNO_OID_LO(addr);
    u8_string rep = u8_mkstring("ObjectId(\"00000000%08x%08x\")",hi,lo);
    return kno_init_string(NULL,-1,rep);}
  else return kno_err("NotAnOID","mongodb_oidref",NULL,oid);
}

static void grab_mongodb_error(bson_error_t *error,u8_string caller)
{
  u8_seterr(kno_MongoDB_Error,caller,u8_strdup(error->message));
}

/*#define HAVE_MONGOC_OPTS_FUNCTIONS (MONGOC_CHECK_VERSION(1,7,0)) */
#define HAVE_MONGOC_OPTS_FUNCTIONS (MONGOC_CHECK_VERSION(1,5,0))
#define HAVE_MONGOC_COUNT_DOCUMENTS (MONGOC_CHECK_VERSION(1,12,0))
#define HAVE_MONGOC_COUNT_WITH_OPTS (MONGOC_CHECK_VERSION(1,6,0))
#define HAVE_MONGOC_BULK_OPERATION_WITH_OPTS (MONGOC_CHECK_VERSION(1,9,0))
#define HAVE_MONGOC_URI_SET_DATABASE (MONGOC_CHECK_VERSION(1,4,0))

#define MONGODB_CLIENT_BLOCK 1
#define MONGODB_CLIENT_NOBLOCK 0

#define IGNORE_ERRNO(iftrue) if (iftrue) errno=0; else {};

static mongoc_client_t *get_client(KNO_MONGODB_DATABASE *server,int block)
{
  mongoc_client_t *client;
  if (block) {
    u8_logf(LOG_DEBUG,_("MongoDB/get_client"),
	    "Getting client from server %llx (%s)",server->dbclients,server->dbspec);
    client = mongoc_client_pool_pop(server->dbclients);}
  else client = mongoc_client_pool_try_pop(server->dbclients);
  IGNORE_ERRNO(client);
  u8_logf(LOG_DEBUG,_("MongoDB/got_client"),
	  "Got client %llx from server %llx (%s)",
	  client,server->dbclients,server->dbspec);
  return client;
}

static void release_client(KNO_MONGODB_DATABASE *server,mongoc_client_t *client)
{
  u8_logf(LOG_DEBUG,_("MongoDB/release_client"),
	  "Releasing client %llx to server %llx (%s)",
	  client,server->dbclients,server->dbspec);
  mongoc_client_pool_push(server->dbclients,client);
}

static int boolopt(lispval opts,lispval key,int dflt)
{
  if (KNO_TABLEP(opts)) {
    lispval v = kno_get(opts,key,KNO_VOID);
    if (KNO_VOIDP(v))
      return dflt;
    else if (KNO_FALSEP(v))
      return 0;
    else {
      kno_decref(v);
      return 1;}}
  else return dflt;
}

static u8_string fileopt(lispval opts,lispval key,u8_string dflt)
{
  if (KNO_TABLEP(opts)) {
    lispval v = kno_getopt(opts,key,KNO_VOID);
    if ((KNO_VOIDP(v))||(KNO_FALSEP(v))) {
      if (dflt == NULL) return dflt;
      else return u8_realpath(dflt,NULL);}
    else if (KNO_STRINGP(v))
      return u8_realpath(KNO_CSTRING(v),NULL);
    else if (KNO_TYPEP(v,kno_secret_type))
      return u8_realpath(KNO_CSTRING(v),NULL);
    else {
      u8_logf(LOG_ERR,"Invalid string option","%q=%q",key,v);
      kno_decref(v);
      return NULL;}}
  else if (dflt == NULL) return dflt;
  else return u8_realpath(dflt,NULL);
}

U8_MAYBE_UNUSED static bson_t *get_projection(lispval opts,int flags)
{
  lispval projection = kno_getopt(opts,returnsym,KNO_VOID);
  if (!(KNO_CONSP(projection)))
    return NULL;
  else if ( (KNO_SLOTMAPP(projection)) || (KNO_SCHEMAPP(projection)) ) {
    bson_t *fields = kno_lisp2bson(projection,flags,opts);
    kno_decref(projection);
    return fields;}
  else if ( (KNO_SYMBOLP(projection)) ||
	    (KNO_STRINGP(projection)) ) {
    struct KNO_KEYVAL kv[1];
    kv[0].kv_key = projection;
    kv[0].kv_val = KNO_INT(1);
    kno_incref(projection);
    lispval map = kno_make_slotmap(1,1,kv);
    bson_t *fields = kno_lisp2bson(map,flags,opts);
    kno_decref(map);
    return fields;}
  else if (KNO_CHOICEP(projection)) {
    int i = 0, len = KNO_CHOICE_SIZE(projection);
    struct KNO_KEYVAL kv[len];
    KNO_DO_CHOICES(field,projection) {
      if ( (KNO_STRINGP(field)) )
	kno_incref(field);
      else if (KNO_SYMBOLP(field)) {}
      else field=VOID;
      if (!(KNO_VOIDP(field))) {
	kv[i].kv_key = field;
	kv[i].kv_val = KNO_INT(1);
	i++;}}
    lispval map = kno_make_slotmap(i,i,kv);
    bson_t *fields = kno_lisp2bson(map,flags,opts);
    kno_decref(map);
    kno_decref(projection);
    return fields;}
  else return NULL;
}

static int grow_lisp_vec(lispval **vecp,size_t n,size_t *vlenp)
{
  lispval *vec = *vecp; size_t vlen = *vlenp;
  if (n<vlen) return 1;
  else if (vec == NULL) {
    vec = u8_alloc_n(64,lispval);
    if (vec) {
      *vlenp = 64; *vecp = vec;
      return 1;}}
  else {
    size_t new_len = ((vlen<8192)?(vlen*2):(vlen+8192));
    lispval *new_vec = u8_realloc_n(vec,new_len,lispval);
    if (new_vec) {
      *vecp = new_vec; *vlenp = new_len;
      return 1;}}
  if (vec) {
    int i = 0; while (i<n) { kno_decref(vec[i++]); }
    u8_free(vec);}
  return 0;
}

static void free_lisp_vec(lispval *vec,int n)
{
  if (vec == NULL) return;
  else {
    int i = 0; while (i<n) { kno_decref(vec[i++]); }
    u8_free(vec);}
}

/* Handling options and flags */

int mongodb_defaults = KNO_MONGODB_DEFAULTS;

DEF_KNOSYM(prefchoices); DEF_KNOSYM(prefvecs); DEF_KNOSYM(noblock);

static int getflags(lispval opts,int dflt)
{
  if ((KNO_VOIDP(opts)||(KNO_FALSEP(opts))||(KNO_DEFAULTP(opts))))
    if (dflt<0) return mongodb_defaults;
    else return dflt;
  else if (KNO_UINTP(opts)) return KNO_FIX2INT(opts);
  else if ((KNO_CHOICEP(opts))||(KNO_SYMBOLP(opts))) {
    int flags = KNO_MONGODB_DEFAULTS;
    if (kno_overlapp(opts,raw)) flags = 0;
    if (kno_overlapp(opts,slotify))
      flags |= KNO_MONGODB_SLOTIFY;
    else if (kno_overlapp(opts,stringkeys))
      flags &= (~KNO_MONGODB_SLOTIFY);
    else NO_ELSE;
    if (kno_overlapp(opts,colonize))
      flags |= KNO_MONGODB_COLONIZE;
    else if (kno_overlapp(opts,rawstrings))
      flags &= (~KNO_MONGODB_COLONIZE);
    else NO_ELSE;
    if (kno_overlapp(opts,KNOSYM(prefchoices)))
      flags |= KNO_MONGODB_PREFCHOICES;
    if (kno_overlapp(opts,KNOSYM(prefvecs)))
      flags &= (~KNO_MONGODB_PREFCHOICES);
    if (kno_overlapp(opts,logopsym))
      flags |= KNO_MONGODB_LOGOPS;
    if (kno_overlapp(opts,KNOSYM(noblock)))
      flags |= KNO_MONGODB_NOBLOCK;
    return flags;}
  else if (KNO_TABLEP(opts)) {
    lispval flagsv = kno_getopt(opts,bsonflags,KNO_VOID);
    int flags = (!(VOIDP(flagsv))) ? (getflags(flagsv,dflt)) :
      (dflt<0) ? (KNO_MONGODB_DEFAULTS) : (dflt);
    if (kno_testopt(opts,raw,KNO_VOID)) flags = 0;
    if (kno_testopt(opts,slotify,KNO_FALSE))
      flags &= (~(KNO_MONGODB_SLOTIFY));
    else if (kno_testopt(opts,slotify,KNO_VOID))
      flags |= KNO_MONGODB_SLOTIFY;
    else NO_ELSE;
    if (kno_testopt(opts,colonize,KNO_FALSE))
      flags &= (~(KNO_MONGODB_COLONIZE));
    else if (kno_testopt(opts,colonize,KNO_VOID))
      flags |= KNO_MONGODB_COLONIZE;
    else NO_ELSE;
    if (kno_testopt(opts,KNOSYM(prefchoices),KNO_VOID))
      flags |= KNO_MONGODB_PREFCHOICES;
    if (kno_testopt(opts,KNOSYM(prefvecs),KNO_VOID))
      flags &= (~KNO_MONGODB_PREFCHOICES);

    if (kno_testopt(opts,KNOSYM(noblock),KNO_VOID))
      flags |= KNO_MONGODB_NOBLOCK;
    if (kno_testopt(opts,logopsym,KNO_VOID))
      flags |= KNO_MONGODB_LOGOPS;
    return flags;}
  else if (dflt<0) return KNO_MONGODB_DEFAULTS;
  else return dflt;
}

static int get_write_flags(lispval val)
{
  if (KNO_VOIDP(val))
    return MONGOC_WRITE_CONCERN_W_DEFAULT;
  else if (KNO_FALSEP(val))
    return MONGOC_WRITE_CONCERN_W_UNACKNOWLEDGED;
  else if (KNO_TRUEP(val))
    return MONGOC_WRITE_CONCERN_W_MAJORITY;
  else if ((KNO_FIXNUMP(val))&&(KNO_FIX2INT(val)<0))
    return MONGOC_WRITE_CONCERN_W_ERRORS_IGNORED;
  else if ((KNO_UINTP(val))&&(KNO_FIX2INT(val)>0))
    return KNO_FIX2INT(val);
  else {
    u8_logf(LOG_ERR,"mongodb/get_write_flags","Bad MongoDB write concern %q",val);
    return MONGOC_WRITE_CONCERN_W_DEFAULT;}
}

static mongoc_write_concern_t *get_write_concern(lispval opts)
{
  lispval val = kno_getopt(opts,writesym,KNO_VOID);
  lispval wait = kno_getopt(opts,wtimeoutsym,KNO_VOID);
  if ((KNO_VOIDP(val))&&(KNO_VOIDP(wait))) return NULL;
  else {
    mongoc_write_concern_t *wc = mongoc_write_concern_new();
    if (!(KNO_VOIDP(val))) {
      int w = get_write_flags(val);
      mongoc_write_concern_set_w(wc,w);}
    if (KNO_UINTP(wait)) {
      int msecs = KNO_FIX2INT(wait);
      mongoc_write_concern_set_wtimeout(wc,msecs);}
    kno_decref(wait);
    kno_decref(val);
    return wc;}
}

static int getreadmode(lispval val)
{
  if (KNO_EQ(val,primarysym))
    return MONGOC_READ_PRIMARY;
  else if (KNO_EQ(val,primarypsym))
    return MONGOC_READ_PRIMARY_PREFERRED;
  else if (KNO_EQ(val,secondarysym))
    return MONGOC_READ_SECONDARY;
  else if (KNO_EQ(val,secondarypsym))
    return MONGOC_READ_SECONDARY_PREFERRED;
  else if (KNO_EQ(val,nearestsym))
    return MONGOC_READ_NEAREST;
  else {
    u8_logf(LOG_ERR,"mongodb/getreadmode",
	    "Bad MongoDB read mode %q",val);
    return MONGOC_READ_PRIMARY;}
}

static mongoc_read_prefs_t *get_read_prefs(lispval opts)
{
  lispval spec = kno_getopt(opts,readsym,KNO_VOID);
  if (KNO_VOIDP(spec)) return NULL;
  else {
    mongoc_read_prefs_t *rp = mongoc_read_prefs_new(MONGOC_READ_PRIMARY);
    int flags = getflags(opts,mongodb_defaults);
    KNO_DO_CHOICES(s,spec) {
      if (KNO_SYMBOLP(s)) {
	int p = getreadmode(s);
	mongoc_read_prefs_set_mode(rp,p);}
      else if (KNO_TABLEP(s)) {
	const bson_t *bson = kno_lisp2bson(s,flags,opts);
	mongoc_read_prefs_add_tag(rp,bson);}
      else {
	u8_logf(LOG_ERR,"mongodb/get_read_prefs",
		"Bad MongoDB read preference %q",s);}}
    kno_decref(spec);
    return rp;}
}

static lispval combine_opts(lispval opts,lispval clopts)
{
  if (opts == clopts) {
    kno_incref(opts); return opts;}
  else if (KNO_PAIRP(opts)) {
    kno_incref(opts); return opts;}
  else if ((KNO_TABLEP(opts))&&(KNO_TABLEP(clopts))) {
    return kno_make_pair(opts,clopts);}
  else if (KNO_VOIDP(opts)) {
    kno_incref(clopts); return clopts;}
  else {
    kno_incref(opts);
    return opts;}
}

int allow_disk_use = -1;
int record_id = -1; /* showRecordId */
int max_runtime_ms = -1; /* maxTimeMS  */
int max_wait_ms = -1; /* maxAwaitTimeMS  */
int batch_size = -1; /* batchSize, singleBatch */

DEF_KNOSYM(usedisk); DEF_KNOSYM(withid);
DEF_KNOSYM(maxruntime); DEF_KNOSYM(maxwait);
DEF_KNOSYM(notimeout);

#define UNSPECIFIEDP(x) \
  ( (KNO_VOIDP(x)) || (KNO_EMPTYP(x)) || (KNO_DEFAULTP(x)) )

static U8_MAYBE_UNUSED bson_t *get_search_opts(lispval opts,int flags,int for_find)
{
  struct KNO_BSON_OUTPUT out;
  bson_t *doc = bson_new();
  out.bson_doc = doc;
  out.bson_opts = opts;
  out.bson_flags = flags;
  out.bson_fieldmap = kno_getopt(opts,fieldmap_symbol,KNO_VOID);

  lispval max_runtime = kno_getopt(opts,KNOSYM(maxruntime),KNO_VOID);
  lispval notimeout = kno_getopt(opts,KNOSYM(notimeout),KNO_VOID);

  if (UNSPECIFIEDP(max_runtime)) {
    if (max_runtime_ms>0)
      bson_append_lisp(out,"maxTimeMS",9,KNO_INT(max_runtime_ms),0);
    else NO_ELSE;}
  else if (KNO_FIXNUMP(max_runtime)) {
    long long maxtime = KNO_FIX2INT(max_runtime);
    if ( (maxtime<0) || (maxtime >= UINT_MAX) )
      u8_log(LOGERR,"MongoErr/maxTimeMS","Invalid value %q",max_runtime);
    else bson_append_lisp(out,"maxTimeMS",9,max_runtime,0);}
  else kno_decref(max_runtime);

  if ( (!(UNSPECIFIEDP(notimeout))) && (!(FALSEP(notimeout))) )
    bson_append_lisp(out,"noTimeout",8,KNO_TRUE,0);

  if (!(for_find)) return out.bson_doc;

  lispval skip_arg = (for_find) ? (kno_getopt(opts,skipsym,KNO_FIXZERO)) :
    (KNO_VOID);
  lispval limit_arg = kno_getopt(opts,limitsym,KNO_VOID);
  lispval sort_arg   = kno_getopt(opts,KNOSYM_SORTED,KNO_VOID);
  lispval batch_arg = (for_find) ? (kno_getopt(opts,batchsym,KNO_VOID)) : (KNO_VOID);
  lispval projection = (for_find) ? (kno_getopt(opts,returnsym,KNO_VOID)) : (KNO_VOID);
  lispval use_disk = (for_find) ? (kno_getopt(opts,KNOSYM(usedisk),KNO_VOID)) : (KNO_VOID);
  lispval with_id = kno_getopt(opts,KNOSYM(withid),KNO_VOID);
  lispval max_wait = kno_getopt(opts,KNOSYM(maxwait),KNO_VOID);

  if (KNO_FIXNUMP(skip_arg))
    bson_append_lisp(out,"skip",4,skip_arg,0);
  else kno_decref(skip_arg);
  if (KNO_FIXNUMP(limit_arg))
    bson_append_lisp(out,"limit",5,limit_arg,0);
  else kno_decref(limit_arg);

  int tailable = 0;
  if ( (KNO_VOIDP(max_wait)) || (KNO_EMPTYP(max_wait)) ||
       (KNO_DEFAULTP(max_wait)) )
    tailable = (max_wait_ms>0);
  else if ( (KNO_FIXNUMP(max_wait)) || (KNO_TRUEP(max_wait)) )
    tailable=1;
  else NO_ELSE;
  if (tailable) {
    bson_append_lisp(out,"tailable",8,KNO_TRUE,0);
    bson_append_lisp(out,"awaitData",9,KNO_TRUE,0);}
  kno_decref(max_wait);

  if (for_find) {
    if (UNSPECIFIEDP(batch_arg)) {
      if (batch_size>0)
	bson_append_lisp(out,"batchSize",9,KNO_INT(batch_size),0);
      else if (batch_size==0)
	bson_append_lisp(out,"singleBatch",11,KNO_TRUE,0);
      else NO_ELSE;
      kno_decref(batch_arg);}
    else if (KNO_FIXNUMP(batch_arg)) {
      long long batch_size = KNO_FIX2INT(batch_arg);
      if ( (batch_size>0) && (batch_size < UINT_MAX) )
	bson_append_lisp(out,"batchSize",9,batch_arg,0);}
    else if (KNO_FALSEP(batch_arg))
      bson_append_lisp(out,"singleBatch",11,KNO_TRUE,0);
    else {
      kno_decref(batch_arg);}}

  if ( (KNO_VOIDP(use_disk)) || (KNO_EMPTYP(use_disk)) ||
       (KNO_DEFAULTP(use_disk)) ) {
    if (allow_disk_use>0)
      bson_append_lisp(out,"diskUse",7,KNO_TRUE,0);
    else if (allow_disk_use==0)
      bson_append_lisp(out,"diskUse",7,KNO_FALSE,0);
    else NO_ELSE;}
  else if (KNO_FALSEP(use_disk))
    bson_append_lisp(out,"diskUse",7,use_disk,0);
  else {
    bson_append_lisp(out,"diskUse",7,KNO_TRUE,0);
    kno_decref(use_disk);}

  if  (KNO_TABLEP(sort_arg))
    bson_append_lisp(out,"sort",4,sort_arg,0);

  if ( (KNO_SYMBOLP(projection)) || (KNO_CONSP(projection)) ) {
    struct KNO_BSON_OUTPUT fields; bson_t proj;
    int ok = bson_append_document_begin(doc,"projection",10,&proj);
    if (ok) {
      fields.bson_doc = &proj;
      fields.bson_flags = ((flags<0)?(getflags(opts,KNO_MONGODB_DEFAULTS)):(flags));
      fields.bson_opts = opts;
      fields.bson_fieldmap = out.bson_fieldmap;
      if ( (KNO_SLOTMAPP(projection)) || (KNO_SCHEMAPP(projection)) )
	kno_bson_output(fields,projection);
      else if (KNO_SYMBOLP(projection))
	bson_append_keyval(fields,projection,KNO_INT(1));
      else if (KNO_STRINGP(projection))
	bson_append_keyval(fields,projection,KNO_INT(1));
      else if (KNO_CHOICEP(projection)) {
	KNO_DO_CHOICES(key,projection) {
	  if ( (KNO_SYMBOLP(key)) || (KNO_STRINGP(key)) )
	    bson_append_keyval(fields,key,KNO_INT(1));}}
      else {}
      bson_append_document_end(doc,&proj);}
    else {
      kno_seterr(kno_BSON_Error,"get_search_opts(mongodb)",NULL,opts);
      kno_decref(out.bson_fieldmap);
      return NULL;}}

  kno_decref(sort_arg);

  return out.bson_doc;
}

static U8_MAYBE_UNUSED bson_t *getbulkopts(lispval opts,int flags)
{
  struct KNO_BSON_OUTPUT out;
  bson_t *doc = bson_new();
  out.bson_doc = doc;
  out.bson_opts = opts;
  out.bson_flags = flags;
  out.bson_fieldmap = kno_getopt(opts,fieldmap_symbol,KNO_VOID);
  lispval ordered_arg = kno_getopt(opts,KNOSYM_SORTED,KNO_FALSE);
  if (!(KNO_FALSEP(ordered_arg))) {
    bson_append_lisp(out,"ordered",4,ordered_arg,0);}

  lispval wcval = kno_getopt(opts,writesym,KNO_VOID);
  lispval wcwait = kno_getopt(opts,wtimeoutsym,KNO_VOID);

  if (!(KNO_VOIDP(wcval))) {
    bson_append_lisp(out,"writeConcern",4,wcval,0);}
  if (!(KNO_VOIDP(wcwait))) {
    bson_append_lisp(out,"wtimeout",4,wcwait,0);}

  kno_decref(ordered_arg);
  kno_decref(wcval);
  kno_decref(wcwait);

  return out.bson_doc;
}

static int mongodb_getflags(lispval mongodb);

/* MongoDB multislots */

/* These are slots which should always have vector (array) values, so
   if their value is a choice, it is rendered as an array, but if it's
   a singleton, it's rendered as an array of one value. */

static int get_choiceslot(lispval slot)
{
  int i = 0, n = n_multislots;
  while (i<n)
    if (multislots[i] == slot)
      return i;
    else i++;
  return -1;
}

static int add_choiceslot(lispval slot)
{
  int off = get_choiceslot(slot);
  if (off>=0) return off;
  else {
    u8_lock_mutex(&multislots_lock);
    int i = 0, n = n_multislots;
    while (i<n)
      if (multislots[i] == slot) {
	u8_unlock_mutex(&multislots_lock);
	return i;}
      else i++;
    if (i >= MONGO_MULTISLOTS_MAX) {
      u8_unlock_mutex(&multislots_lock);
      return -1;}
    multislots[i] = slot;
    n_multislots++;
    u8_unlock_mutex(&multislots_lock);
    return i;}
}

static lispval multislots_config_get(lispval var,void *data)
{
  lispval result = KNO_EMPTY;
  int i = 0, n = n_multislots;
  while (i < n) {
    lispval slot = multislots[i++];
    KNO_ADD_TO_CHOICE(result,slot);}
  return result;
}

static int multislots_config_add(lispval var,lispval val,void *data)
{
  lispval sym = KNO_VOID;
  if (KNO_SYMBOLP(val))
    sym = val;
  else if (KNO_STRINGP(val))
    sym = kno_intern(CSTRING(val));
  else {
    kno_seterr("Not symbolic","mongodb/multislots_config_add",
	       NULL,val);
    return -1;}
  int rv = add_choiceslot(sym);
  if (rv < 0) {
    char buf[64];
    kno_seterr("Too many multislots declared","mongodb/multislots_config_add",
	       u8_sprintf(buf,sizeof(buf),"%d",MONGO_MULTISLOTS_MAX),
	       val);
    return rv;}
  else return rv;
}

/* Consing MongoDB clients, collections, and cursors */

static u8_string get_connection_spec(mongoc_uri_t *info);
static int setup_tls(mongoc_ssl_opt_t *,mongoc_uri_t *,lispval);

#if MONGOC_CHECK_VERSION(1,6,0)
static int set_uri_opt(mongoc_uri_t *uri,const char *option,lispval val)
{
  if (KNO_VOIDP(val)) return 0;
  else if (KNO_FALSEP(val))
    return mongoc_uri_set_option_as_bool(uri,option,0);
  else if (KNO_TRUEP(val))
    return mongoc_uri_set_option_as_bool(uri,option,1);
  else if (KNO_STRINGP(val))
    return mongoc_uri_set_option_as_utf8(uri,option,KNO_CSTRING(val));
  else if (KNO_SYMBOLP(val)) {
    lispval config_val = kno_config_get(KNO_SYMBOL_NAME(val));
    int rv = -1;
    if (KNO_VOIDP(config_val)) {}
    else if (KNO_STRINGP(config_val))
      rv = mongoc_uri_set_option_as_utf8(uri,option,KNO_SYMBOL_NAME(val));
    else if (KNO_PACKETP(config_val))
      rv = mongoc_uri_set_option_as_utf8(uri,option,KNO_PACKET_DATA(val));
    else if (KNO_PACKETP(config_val)) {
      ssize_t len = KNO_PACKET_LENGTH(config_val);
      ssize_t valid_len = u8_validate(KNO_PACKET_DATA(config_val),len);
      if (valid_len==len) {
	u8_byte copy[len+1];
	memcpy(copy,KNO_PACKET_DATA(config_val),len);
	copy[len]='\0';
	mongoc_uri_set_option_as_utf8(uri,option,copy);}
      else u8_log(LOGERR,"BadMongoConfig",
		  "Configured value for %s (%q) isn't valid UTF-8: %q",
		  option,val,config_val);}
    else u8_log(LOGWARN,"BadMongoDBConfig",option,val);
    kno_decref(config_val);
    if (rv<0)
      return mongoc_uri_set_option_as_utf8(uri,option,KNO_SYMBOL_NAME(val));
    U8_CLEAR_ERRNO();
    return rv;}
  else if (KNO_UINTP(val))
    return mongoc_uri_set_option_as_int32(uri,option,KNO_FIX2INT(val));
  else if (KNO_FIXNUMP(val)) /* Assume negative means use default */
    return 0;
  else if (KNO_FLONUMP(val)) {
    long long msecs = (int) floor(KNO_FLONUM(val)*1000.0);
    if (msecs > 0) {
      if (msecs > INT_MAX) msecs=INT_MAX;
      return mongoc_uri_set_option_as_int32(uri,option,msecs);}}
  else NO_ELSE;
  kno_seterr("BadOptionValue","mongodb/set_uri_opt",option,val);
  return -1;
}
#else
static void escape_uri(u8_output out,u8_string s,int len);
static u8_string add_to_query(u8_string qstring,const char *option,lispval val)
{
  if (KNO_VOIDP(val)) return qstring;
  u8_string result = qstring;
  size_t option_len = strlen(option);
  char optbuf[option_len+2];
  strcpy(optbuf,option); strcat(optbuf,"=");
  u8_string poss_start = strstr(qstring,optbuf);
  if ( (poss_start) && (poss_start>qstring) &&
       ( (poss_start[-1] == '?') || (poss_start[-1] == '&') ) )
    return qstring;
  u8_string tail = qstring+strlen(qstring)-1;
  u8_string sep = (*tail == '&') ? ("") :
    (*tail == '?') ? ("") : (strchr(qstring,'?')) ? ("&") : ("?");
  if (KNO_FALSEP(val))
    result = u8_mkstring("%s%s%s=false&",qstring,sep,option);
  else if (KNO_TRUEP(val))
    result = u8_mkstring("%s%s%s=true&",qstring,sep,option);
  else if (KNO_STRINGP(val)) {
    struct U8_OUTPUT out; unsigned char buf[200];
    U8_INIT_OUTPUT_BUF(&out,200,buf);
    escape_uri(&out,KNO_CSTRING(val),KNO_STRLEN(val));
    result = u8_mkstring("%s%s%s=%s&",qstring,sep,out.u8_outbuf);
    u8_close_output(&out);}
  else if (KNO_SYMBOLP(val)) {
    struct U8_OUTPUT out; unsigned char buf[200];
    u8_string pname = KNO_SYMBOL_NAME(val);
    U8_INIT_OUTPUT_BUF(&out,200,buf);
    escape_uri(&out,pname,strlen(pname));
    result = u8_mkstring("%s%s%s=%s&",qstring,sep,out.u8_outbuf);
    u8_close_output(&out);}
  else if (KNO_UINTP(val))
    result = u8_mkstring("%s%s%s=%d&",qstring,sep,KNO_FIX2INT(val));
  else if (KNO_FLONUMP(val)) {
    long long msecs = (int) floor(KNO_FLONUM(val)*1000.0);
    if (msecs > 0) {
      if (msecs > INT_MAX) msecs=INT_MAX;
      result  = u8_mkstring("%s%s%s=%lld&",qstring,sep,msecs);}}
  else NO_ELSE;
  if (result != qstring) u8_free(qstring);
  return result;
}
static void escape_uri(u8_output out,u8_string s,int len)
{
  u8_string lim = ((len<0)?(NULL):(s+len));
  while ((lim)?(s<lim):(*s))
    if (((*s)>=0x80)||(isspace(*s))||
	(*s=='+')||(*s=='%')||(*s=='=')||
	(*s=='&')||(*s=='#')||(*s==';')||
	(!((isalnum(*s))||(strchr("-_.~",*s)!=NULL)))) {
      char buf[8];
      sprintf(buf,"%%%02x",*s);
      u8_puts(out,buf); s++;}
    else {u8_putc(out,*s); s++;}
}
#endif

static u8_string get_config_string(lispval opts,u8_string prop)
{
  int from_opts = 0;
  u8_byte buf[100];
  lispval val = kno_getopt(opts,kno_getsym(prop),KNO_VOID);
  if (KNO_VOIDP(val)) {
    u8_string conf_name = u8_bprintf(buf,"mongodb:%s",prop);
    val = kno_config_get(conf_name);}
  else from_opts=1;
  if (KNO_VOIDP(val)) return NULL;
  if (KNO_STRINGP(val))
    return u8_strdup(KNO_CSTRING(val));
  else if (KNO_PACKETP(val)) {
    ssize_t len = KNO_PACKET_LENGTH(val);
    ssize_t valid_len = u8_validate(KNO_PACKET_DATA(val),len);
    if (valid_len==len) {
      u8_byte *copy = u8_malloc(len+1);
      memcpy(copy,KNO_PACKET_DATA(val),len);
      copy[len]='\0';
      return copy;}
    else return NULL;}
  else {
    u8_log(LOGERR,"BadMongoConfig",
	   "Configuration for '%s' (%s) was %q",prop,
	   (from_opts)?("from options"):("from config"),
	   val);
    return NULL;}
}

static mongoc_uri_t *setup_mongoc_uri(mongoc_uri_t *info,lispval opts)
{
  if (info == NULL) return info;
  u8_string dbname = mongoc_uri_get_database(info);
  lispval dbarg   = kno_getopt(opts,kno_intern("dbname"),KNO_VOID);
  if ( (dbname) &&
       ((KNO_VOIDP(dbarg)) || (KNO_FALSEP(dbarg)) || (KNO_DEFAULTP(dbarg)) ) ) {}
  else if ((KNO_VOIDP(dbarg)) || (KNO_FALSEP(dbarg)) || (KNO_DEFAULTP(dbarg)) ) {
    kno_seterr("NoDBName","setup_mongoc_uri",
	       mongoc_uri_get_string(info),KNO_VOID);
    kno_decref(dbarg);
    return NULL;}
  /* TODO: check version */
#if HAVE_MONGOC_URI_SET_DATABASE
  else if (!(KNO_STRINGP(dbarg))) {
    kno_seterr("Invalid MongoDBName","setup_mongoc_uri",
	       mongoc_uri_get_string(info),
	       dbarg);
    kno_decref(dbarg);
    return NULL;}
  else if ( (dbname) && (strcmp(dbname,KNO_CSTRING(dbarg))) ) {}
  else {
    mongoc_uri_set_database(info,KNO_CSTRING(dbarg));}
#else
  else if (dbname == NULL) {
    kno_seterr("NoDBName","setup_mongoc_uri",
	       mongoc_uri_get_string(info),KNO_VOID);
    kno_decref(dbarg);
    return NULL;}
  else if ( (KNO_STRINGP(dbarg)) && (strcmp(dbname,KNO_CSTRING(dbarg))) == 0) {}
  else {
    kno_seterr("CantSetDB","setup_mongoc_uri",
	       "Can't set/modify database in this version of MongoDB",
	       KNO_VOID);
    kno_decref(dbarg);
    return NULL;}
#endif

  lispval timeout =
    kno_getopt(opts,kno_intern("timeout"),KNO_INT(default_socket_timeout));
  lispval ctimeout =
    kno_getopt(opts,kno_intern("ctimeout"),KNO_INT(default_connect_timeout));
  lispval stimeout =
    kno_getopt(opts,kno_intern("stimeout"),KNO_INT(default_server_select_timeout));
  lispval maxpool =
    kno_getopt(opts,kno_intern("maxpool"),KNO_INT(default_maxpools_limit));
  u8_string appname = get_config_string(opts,"appname");
  u8_string username = get_config_string(opts,"username");
  u8_string password = get_config_string(opts,"password");
  u8_string auth_source = get_config_string(opts,"authsource");
#if MONGOC_CHECK_VERSION(1,6,0)
  set_uri_opt(info,MONGOC_URI_SOCKETTIMEOUTMS,timeout);
  set_uri_opt(info,MONGOC_URI_CONNECTTIMEOUTMS,ctimeout);
  set_uri_opt(info,MONGOC_URI_SERVERSELECTIONTIMEOUTMS,stimeout);
  set_uri_opt(info,MONGOC_URI_MAXPOOLSIZE,maxpool);
  if (appname)
    mongoc_uri_set_option_as_utf8(info,MONGOC_URI_APPNAME,appname);
  else mongoc_uri_set_option_as_utf8(info,MONGOC_URI_APPNAME,u8_appid());
  if ( (boolopt(opts,sslsym,default_ssl)) ||
       (boolopt(opts,cafilesym,(default_cafile!=NULL))) ) {
    mongoc_uri_set_option_as_bool(info,MONGOC_URI_SSL,1);}
  if (username) mongoc_uri_set_username(info,username);
  if (password) mongoc_uri_set_password(info,password);
  if (auth_source) mongoc_uri_set_auth_source(info,auth_source);
#else
  u8_string uri = mongoc_uri_get_string(info);
  u8_string qmark = strchr(uri,'?');
  u8_string qstring = (qmark) ? (u8_strdup(qmark)) : (u8_strdup("?"));
  qstring = add_to_query(qstring,"socketTimeoutMS",timeout);
  qstring = add_to_query(qstring,"connectTimeoutMS",ctimeout);
  qstring = add_to_query(qstring,"serverSelectionTimeoutMS",stimeout);
  qstring = add_to_query(qstring,"maxPoolSize",maxpool);
  if (boolopt(opts,sslsym,default_ssl)) {
    qstring = add_to_query(qstring,"ssl",KNO_TRUE);}
  size_t base_len = (qmark==NULL) ? (strlen(uri)) : (qmark-uri);
  unsigned char newbuf[base_len+strlen(qstring)+1];
  strcpy(newbuf,uri);
  strcpy(newbuf+base_len,qstring);
  mongoc_uri_t *new_info = mongoc_uri_new(newbuf);
  mongoc_uri_destroy(info);
  info = new_info;
#endif
  if (appname) u8_free(appname);
  if (username) u8_free(username);
  if (password) u8_free(password);
  if (auth_source) u8_free(auth_source);
  kno_decref(timeout);
  kno_decref(ctimeout);
  kno_decref(stimeout);
  kno_decref(maxpool);
  return info;
}

static u8_string mongodb_check(mongoc_client_pool_t *client_pool)
{
  mongoc_client_t *probe = mongoc_client_pool_pop(client_pool);
  if (probe) {
    bson_t *command = BCON_NEW("ping",BCON_INT32(1)), reply;
    bson_error_t error;
    bool rv = mongoc_client_command_simple(probe,"admin",command,NULL,&reply,&error);
    mongoc_client_pool_push(client_pool,probe);
    bson_destroy(command);
    if (! rv )
      return u8_strdup(error.message);
    else {
      bson_destroy(&reply);
      return NULL;}}
  else return u8_strdup("mongoc_client_pool_pop failed");
}

#if MONGOC_CHECK_VERSION(1,7,0)
#define mongoc_uri_info(s,e) mongoc_uri_new_with_error((s),e)
#else
#define mongoc_uri_info(s,e) mongoc_uri_new((s))
#endif

/* This returns a MongoDB server object which wraps a MongoDB client
   pool. */

DEFC_PRIM("mongodb/open",mongodb_open,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID},
	  {"opts",kno_any_type,KNO_VOID})
static lispval mongodb_open(lispval arg,lispval opts)
{
  mongoc_client_pool_t *client_pool;
  mongoc_uri_t *info;
  mongoc_ssl_opt_t ssl_opts={ 0 };
  bson_error_t error = { 0 };
  int smoke_test = boolopt(opts,smoketest_sym,1);
  int flags = getflags(opts,mongodb_defaults);;

  if ( (KNO_STRINGP(arg)) || (KNO_TYPEP(arg,kno_secret_type)) )
    info = mongoc_uri_info(KNO_CSTRING(arg),&error);
  else if (KNO_SYMBOLP(arg)) {
    lispval conf_val = kno_config_get(KNO_SYMBOL_NAME(arg));
    if (KNO_VOIDP(conf_val))
      return kno_type_error("MongoDB URI config","mongodb_open",arg);
    else if ((KNO_STRINGP(conf_val))||
	     (KNO_TYPEP(conf_val,kno_secret_type))) {
      info = mongoc_uri_info(KNO_CSTRING(conf_val),&error);
      kno_decref(conf_val);}
    else return kno_type_error("MongoDB URI config val",
			       KNO_SYMBOL_NAME(arg),conf_val);}
  else return kno_type_error("MongoDB URI","mongodb_open",arg);

  if (info == NULL)
    return kno_err("MongoDB URI spec","mongodb_open",NULL,arg);
  else info = setup_mongoc_uri(info,opts);

  if (!(info))
    return kno_err(kno_MongoDB_Error,"mongodb_open",error.message,arg);
  else client_pool = mongoc_client_pool_new(info);

  if (client_pool == NULL) {
    mongoc_uri_destroy(info);
    return kno_type_error("MongoDB client URI","mongodb_open",arg);}
  else if ( (setup_tls(&ssl_opts,info,opts)) ) {
    mongoc_client_pool_set_ssl_opts(client_pool,&ssl_opts);
    u8_free(ssl_opts.pem_file);
    u8_free(ssl_opts.pem_pwd);
    u8_free(ssl_opts.ca_file);
    u8_free(ssl_opts.ca_dir);
    u8_free(ssl_opts.crl_file);}
  else NO_ELSE;

  if (smoke_test) {
    u8_string errmsg = mongodb_check(client_pool);
    if (errmsg) {
      lispval uri_string = knostring(mongoc_uri_get_string(info));
      kno_seterr("MongoDB/ConnectFailed","mongodb_open",errmsg,uri_string);
      kno_decref(uri_string);
      mongoc_client_pool_destroy(client_pool);
      mongoc_uri_destroy(info);
      u8_free(errmsg);
      return KNO_ERROR;}}

  u8_string uri = u8_strdup(mongoc_uri_get_string(info));
  struct KNO_MONGODB_DATABASE *srv = u8_alloc(struct KNO_MONGODB_DATABASE);
  u8_string dbname = mongoc_uri_get_database(info);
  lispval poolmax = kno_getopt(opts,poolmaxsym,KNO_VOID);
  if (KNO_UINTP(poolmax)) {
    int pmax = KNO_FIX2INT(poolmax);
    mongoc_client_pool_max_size(client_pool,pmax);}
  kno_decref(poolmax);
  KNO_INIT_CONS(srv,kno_mongoc_server);
  srv->dburi = uri;
  if (dbname == NULL)
    srv->dbname = NULL;
  else srv->dbname = u8_strdup(dbname);
  srv->dbspec = get_connection_spec(info);
  srv->dburi_info = info;
  srv->dbclients = client_pool;
  srv->dbopts = kno_incref(opts);
  srv->dbflags = flags;
  if ((logops)||(flags&KNO_MONGODB_LOGOPS))
    u8_logf(LOG_INFO,"MongoDB/open","Opened %s with %s",dbname,srv->dbspec);
  return (lispval)srv;
}
static void recycle_server(struct KNO_RAW_CONS *c)
{
  struct KNO_MONGODB_DATABASE *s = (struct KNO_MONGODB_DATABASE *)c;
  mongoc_uri_destroy(s->dburi_info);
  mongoc_client_pool_destroy(s->dbclients);
  kno_decref(s->dbopts);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}
static int unparse_server(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_MONGODB_DATABASE *srv = (struct KNO_MONGODB_DATABASE *)x;
  u8_printf(out,"#<MongoDB/Server %s/%s>",srv->dbspec,srv->dbname);
  return 1;
}

static u8_string get_connection_spec(mongoc_uri_t *info)
{
  const mongoc_host_list_t *hosts = mongoc_uri_get_hosts(info);
  u8_string username = mongoc_uri_get_username(info), result = NULL;
  u8_string server_name = "unknown";
  if (hosts) server_name = hosts->host_and_port;
  else {
#if MONGOC_CHECK_VERSION(1,9,0)
    server_name = mongoc_uri_get_service(info);
#endif
  }
  if (server_name == NULL) server_name="unknown";
  if (username) {
    struct U8_OUTPUT out;
    U8_INIT_OUTPUT(&out,256);
    u8_printf(&out,"%s@%s",username,server_name);
    result = out.u8_outbuf;}
  else result = u8_strdup(server_name);
  return result;
}

static int setup_tls(mongoc_ssl_opt_t *ssl_opts,
		     mongoc_uri_t *info,
		     lispval opts)
{
  if ( (mongoc_uri_get_tls(info)) ||
       (boolopt(opts,sslsym,default_ssl)) ||
       ( (kno_testopt(opts,cafilesym,KNO_VOID)) &&
	 (!(kno_testopt(opts,cafilesym,KNO_FALSE)))) ) {
    const mongoc_ssl_opt_t *default_opts = mongoc_ssl_opt_get_default();
    memcpy(ssl_opts,default_opts,sizeof(mongoc_ssl_opt_t));
    ssl_opts->pem_file = fileopt(opts,certfile,default_certfile);
    ssl_opts->pem_pwd = fileopt(opts,certpass,NULL);
    ssl_opts->ca_file = fileopt(opts,cafilesym,default_cafile);
    ssl_opts->ca_dir = fileopt(opts,cadirsym,default_cadir);
    ssl_opts->crl_file = fileopt(opts,crlsym,NULL);
    return (!((ssl_opts->pem_file == NULL)&&
	      (ssl_opts->pem_pwd == NULL)&&
	      (ssl_opts->ca_file == NULL)&&
	      (ssl_opts->ca_dir == NULL)&&
	      (ssl_opts->crl_file == NULL)));}
  else return 0;
}

/* Creating collections */

DEF_KNOSYM(oidslot);

/* Collection creation is actually deferred until the collection is
   used because we want the collection to be "thread safe" which means
   that it won't correspond to a single mongoc_collection_t object,
   but that each use of the collection will pop a client from the pool
   and create a collection with that client. */

DEFC_PRIM("collection/open",mongodb_collection,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"server",kno_any_type,KNO_VOID},
	  {"name_arg",kno_string_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval mongodb_collection(lispval server,lispval name_arg,
				  lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *result;
  struct KNO_MONGODB_DATABASE *srv;
  u8_string name = KNO_CSTRING(name_arg), collection_name = NULL;
  lispval opts; int flags;
  if (KNO_TYPEP(server,kno_mongoc_server)) {
    srv = (struct KNO_MONGODB_DATABASE *)server;
    flags = getflags(opts_arg,srv->dbflags);
    opts = combine_opts(opts_arg,srv->dbopts);
    kno_incref(server);}
  else if ((KNO_STRINGP(server))||
	   (KNO_SYMBOLP(server))||
	   (KNO_TYPEP(server,kno_secret_type))) {
    lispval consed = mongodb_open(server,opts_arg);
    if (KNO_ABORTP(consed)) return consed;
    server = consed; srv = (struct KNO_MONGODB_DATABASE *)consed;
    flags = getflags(opts_arg,srv->dbflags);
    opts = combine_opts(opts_arg,srv->dbopts);}
  else return kno_type_error("MongoDB client","mongodb_collection",server);
  if (strchr(name,'/')) {
    char *slash = strchr(name,'/');
    collection_name = slash+1;}
  else if (srv->dbname == NULL) {
    return kno_err(_("MissingDBName"),"mongodb_open",NULL,server);}
  else {
    collection_name = u8_strdup(name);}
  lispval oidslot = kno_getopt(opts,KNOSYM(oidslot),KNO_VOID);
  if (KNO_VOIDP(oidslot)) {}
  else if (!(KNO_SYMBOLP(oidslot))) {
    u8_log(LOGERR,"mongodb_collection","Invalid `oidslot` value: %q",oidslot);
    kno_decref(oidslot);
    oidslot=KNO_VOID;}
  else NO_ELSE;
  result = u8_alloc(struct KNO_MONGODB_COLLECTION);
  KNO_INIT_CONS(result,kno_mongoc_collection);
  result->collection_db = server;
  result->collection_opts = opts;
  result->collection_flags = flags;
  result->collection_oidslot = (KNO_SYMBOLP(oidslot)) ? (oidslot) : (idsym);
  result->collection_oidkey = (KNO_SYMBOLP(oidslot)) ?
    (KNO_SYMBOL_NAME(oidslot)) : (U8S("_id"));
  result->collection_name = collection_name;
  return (lispval) result;
}
static void recycle_collection(struct KNO_RAW_CONS *c)
{
  struct KNO_MONGODB_COLLECTION *collection = (struct KNO_MONGODB_COLLECTION *)c;
  kno_decref(collection->collection_db);
  kno_decref(collection->collection_opts);
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}
static int unparse_collection(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_MONGODB_COLLECTION *collection = (struct KNO_MONGODB_COLLECTION *)x;
  struct KNO_MONGODB_DATABASE *db=
    (struct KNO_MONGODB_DATABASE *) (collection->collection_db);
  u8_printf(out,"#<MongoDB/Collection %s/%s/%s>",
	    db->dbspec,db->dbname,collection->collection_name);
  return 1;
}

/* Using collections */

/*  This is where the collection actually gets created based on
    a client popped from the client pool for the server. */
mongoc_collection_t *open_collection(struct KNO_MONGODB_COLLECTION *coll,
				     mongoc_client_t **clientp,
				     int flags)
{
  struct KNO_MONGODB_DATABASE *server=
    (struct KNO_MONGODB_DATABASE *)(coll->collection_db);
  u8_string dbname = server->dbname;
  u8_string collection_name = coll->collection_name;
  mongoc_client_t *client = get_client(server,(!(flags&KNO_MONGODB_NOBLOCK)));
  if (client) {
    mongoc_collection_t *collection=
      mongoc_client_get_collection(client,dbname,collection_name);
    if (collection) {
      *clientp = client;
      return collection;}
    else {
      release_client(server,client);
      return NULL;}}
  else return NULL;
}

/* This returns a client to a client pool.  The argument can be either
   a server or a collection (which is followed to its server). */
static void client_done(lispval arg,mongoc_client_t *client)
{
  if (KNO_TYPEP(arg,kno_mongoc_server)) {
    struct KNO_MONGODB_DATABASE *server = (struct KNO_MONGODB_DATABASE *)arg;
    release_client(server,client);}
  else if (KNO_TYPEP(arg,kno_mongoc_collection)) {
    struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
    struct KNO_MONGODB_DATABASE *server=
      (struct KNO_MONGODB_DATABASE *)(coll->collection_db);
    release_client(server,client);}
  else {
    u8_logf(LOG_ERR,"BAD client_done call","Wrong type for %q",arg);}
}

/* This destroys the collection returns a client to a client pool. */
static void collection_done(mongoc_collection_t *collection,
			    mongoc_client_t *client,
			    struct KNO_MONGODB_COLLECTION *coll)
{
  struct KNO_MONGODB_DATABASE *server=
    (kno_mongodb_database)coll->collection_db;
  mongoc_collection_destroy(collection);
  release_client(server,client);
}

mongoc_cursor_t *open_cursor(mongoc_collection_t *collection,
			     bson_t *q,bson_t *findopts,
			     mongoc_read_prefs_t *rp,
			     lispval opts)
{
  lispval max_wait = kno_getopt(opts,KNOSYM(maxwait),KNO_VOID);
  mongoc_cursor_t *cursor =
    mongoc_collection_find_with_opts(collection,q,findopts,rp);
  if ( (cursor) && (KNO_FIXNUMP(max_wait)) )
    mongoc_cursor_set_max_await_time_ms(cursor,KNO_FIX2INT(max_wait));
  return cursor;
}

/* Basic operations on collections */

#if HAVE_MONGOC_OPTS_FUNCTIONS

DEFC_PRIM("collection/insert!",collection_insert,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "(COLLECTION/INSERT! *collection* *objects* *opts*) "
	  "**undocumented**",
	  {"collection",kno_any_type,KNO_VOID},
	  {"objects",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_FALSE})
static lispval collection_insert(lispval arg,lispval objects,lispval opts_arg)
{
  if (KNO_EMPTY_CHOICEP(arg))
    return KNO_EMPTY_CHOICE;
  else if (KNO_EMPTY_CHOICEP(objects))
    return KNO_EMPTY_CHOICE;
  else if (KNO_CHOICEP(arg)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(collection,arg) {
      if (KNO_TYPEP(collection,kno_mongoc_collection_type)) {
	lispval rv = collection_insert(collection,objects,opts_arg);
	if (KNO_ABORTP(rv)) {
	  kno_decref(results);
	  KNO_STOP_DO_CHOICES;
	  return rv;}
	else {
	  KNO_ADD_TO_CHOICE(results,rv);}}
      else {
	kno_type_error(_("MongoDB collection"),"collection_insert",collection);
	kno_decref(results);
	return KNO_ERROR;}}
    return results;}
  else if (!(KNO_TYPEP(arg,kno_mongoc_collection)))
    return kno_type_error(_("MongoDB collection"),"collection_insert",arg);
  else NO_ELSE;
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  struct KNO_MONGODB_DATABASE *db=
    (struct KNO_MONGODB_DATABASE *) (coll->collection_db);
  lispval result;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,db->dbopts);
  bson_t *bulkopts = getbulkopts(opts,flags);
  mongoc_client_t *client = NULL; bool retval;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    bson_t reply;
    bson_error_t error = { 0 };
    if ((logops)||(flags&KNO_MONGODB_LOGOPS))
      u8_logf(LOG_NOTICE,"collection_insert",
	      "Inserting %d items into %q",KNO_CHOICE_SIZE(objects),arg);
    if (KNO_CHOICEP(objects)) {
      mongoc_bulk_operation_t *bulk=
	mongoc_collection_create_bulk_operation_with_opts
	(collection,bulkopts);
      KNO_DO_CHOICES(elt,objects) {
	bson_t *doc = kno_lisp2bson(elt,flags,opts);
	if (doc) {
	  mongoc_bulk_operation_insert(bulk,doc);
	  bson_destroy(doc);}}
      retval = mongoc_bulk_operation_execute(bulk,&reply,&error);
      mongoc_bulk_operation_destroy(bulk);
      if (retval) {
	result = kno_bson2lisp(&reply,flags,opts);}
      else {
	u8_byte buf[1000];
	if (errno) u8_graberrno("collection_insert",NULL);
	kno_seterr(kno_MongoDB_Error,"collection_insert",
		   u8_sprintf(buf,1000,"%s (%s>%s)",
			      error.message,db->dburi,coll->collection_name),
		   kno_incref(objects));
	result = KNO_ERROR_VALUE;}
      bson_destroy(&reply);}
    else {
      bson_t *doc = kno_lisp2bson(objects,flags,opts);
      mongoc_write_concern_t *wc = get_write_concern(opts);

      retval = (doc==NULL) ? (0) :
	(mongoc_collection_insert
	 (collection,MONGOC_INSERT_NONE,doc,wc,&error));
      if (retval) {
	U8_CLEAR_ERRNO();
	result = KNO_TRUE;}
      else {
	u8_byte buf[1000];
	if (doc) bson_destroy(doc);
	if (errno) u8_graberrno("collection_insert",NULL);
	kno_seterr(kno_MongoDB_Error,"collection_insert",
		   u8_sprintf(buf,1000,"%s (%s>%s)",
			      error.message,db->dburi,coll->collection_name),
		   kno_incref(objects));
	result = KNO_ERROR_VALUE;}
      if (wc) mongoc_write_concern_destroy(wc);}
    collection_done(collection,client,coll);}
  else result = KNO_ERROR_VALUE;
  kno_decref(opts);
  U8_CLEAR_ERRNO();
  return result;
}
#else

DEFC_PRIM("collection/insert!",collection_insert,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2)|KNO_NDCALL,
	  "(COLLECTION/INSERT! *collection* *objects* *opts*) "
	  "**undocumented**",
	  {"collection",kno_any_type,KNO_VOID},
	  {"objects",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_FALSE})
static lispval collection_insert(lispval collection,lispval objects,
				 lispval opts_arg)
{
  if (KNO_EMPTY_CHOICEP(collection))
    return KNO_EMPTY_CHOICE;
  else if (KNO_EMPTY_CHOICEP(objects))
    return KNO_EMPTY_CHOICE;
  else if (KNO_CHOICEP(collection)) {
    lispval results = KNO_EMPTY;
    KNO_DO_CHOICES(c,collection) {
      if (KNO_TYPEP(c,kno_mongoc_collection_type)) {
      lispval rv = collection_insert(c,objects,opts_arg);
      if (KNO_ABORTP(rv)) {
	kno_decref(results);
	KNO_STOP_DO_CHOICES;
	return rv;}
      else {
	KNO_ADD_TO_CHOICE(results,rv);}}
    return results;}
  else if (!(KNO_TYPEP(collection,kno_mongoc_collection)))
    return kno_type_error(_("MongoDB collection"),"collection_insert",arg);
  else NO_ELSE;
  struct KNO_MONGODB_COLLECTION *coll =
    (struct KNO_MONGODB_COLLECTION *)collection;
  struct KNO_MONGODB_DATABASE *db =
    (struct KNO_MONGODB_DATABASE *) (coll->collection_db);
  lispval result;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,db->dbopts);
  mongoc_client_t *client = NULL; bool retval;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    bson_t reply;
    bson_error_t error;
    mongoc_write_concern_t *wc = get_write_concern(opts);
    if ((logops)||(flags&KNO_MONGODB_LOGOPS))
      u8_logf(LOG_NOTICE,"collection_insert",
	      "Inserting %d items into %q",KNO_CHOICE_SIZE(objects),arg);
    if (KNO_CHOICEP(objects)) {
      mongoc_bulk_operation_t *bulk=
	mongoc_collection_create_bulk_operation(collection,true,wc);
      KNO_DO_CHOICES(elt,objects) {
	bson_t *doc = kno_lisp2bson(elt,flags,opts);
	if (doc) {
	  mongoc_bulk_operation_insert(bulk,doc);
	  bson_destroy(doc);}}
      retval = mongoc_bulk_operation_execute(bulk,&reply,&error);
      mongoc_bulk_operation_destroy(bulk);
      if (retval) {
	U8_CLEAR_ERRNO();
	result = kno_bson2lisp(&reply,flags,opts);}
      else {
	u8_byte buf[1000];
	if (errno) u8_graberrno("collection_insert",NULL);
	kno_seterr(kno_MongoDB_Error,"collection_insert",
		   u8_sprintf(buf,1000,"%s (%s>%s)",
			      error.message,db->dburi,coll->collection_name),
		   kno_incref(objects));
	result = KNO_ERROR_VALUE;}
      bson_destroy(&reply);}
    else {
      bson_t *doc = kno_lisp2bson(objects,flags,opts);
      retval = (doc==NULL) ? (0) :
	(mongoc_collection_insert(collection,MONGOC_INSERT_NONE,doc,wc,&error));
      if (retval) {
	U8_CLEAR_ERRNO();
	result = KNO_TRUE;}
      else {
	u8_byte buf[1000];
	if (doc) bson_destroy(doc);
	if (errno) u8_graberrno("collection_insert",NULL);
	kno_seterr(kno_MongoDB_Error,"collection_insert",
		   u8_sprintf(buf,1000,"%s (%s>%s)",
			      error.message,db->dburi,coll->collection_name),
		   kno_incref(objects));
	result = KNO_ERROR_VALUE;}}
    if (wc) mongoc_write_concern_destroy(wc);
    collection_done(collection,client,coll);}
  else result = KNO_ERROR_VALUE;
  kno_decref(opts);
  U8_CLEAR_ERRNO();
  return result;
}
#endif


DEFC_PRIM("collection/remove!",collection_remove,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"coll",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"obj",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_remove(lispval coll_arg,lispval obj,lispval opts_arg)
{
  lispval result = KNO_VOID;
  struct KNO_MONGODB_COLLECTION *coll=(struct KNO_MONGODB_COLLECTION *)coll;
  struct KNO_MONGODB_DATABASE *db = COLL2DB(coll);
  lispval opts = combine_opts(opts_arg,db->dbopts);
  int flags = getflags(opts_arg,coll->collection_flags), hasid = 1;
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  u8_string oidkey = coll->collection_oidkey;
  lispval oidslot = coll->collection_oidslot;
  if (collection) {
    struct KNO_BSON_OUTPUT q; bson_error_t error;
    mongoc_write_concern_t *wc = get_write_concern(opts);
    q.bson_doc = bson_new();
    q.bson_opts = opts;
    q.bson_flags = flags;
    q.bson_fieldmap = KNO_VOID;
    if (KNO_OIDP(obj)) {
      bson_append_lisp(q,oidkey,3,obj,-1);}
    else if (KNO_TABLEP(obj)) {
      lispval id = kno_get(obj,oidslot,KNO_VOID);
      /* If the selector has an _id field, we always want to remove a single
	 record (there should be only one); otherwise we may remove multiple.
	 Passing MONGOC_REMOVE_SINGLE_REMOVE allows the search for matches to
	 stop sooner on the MongoDB side. */
      if (KNO_VOIDP(id)) {
	id = kno_get(obj,idsym,KNO_VOID);
	if (KNO_VOIDP(id)) {
	  q.bson_fieldmap = kno_getopt(opts,fieldmap_symbol,KNO_VOID);
	  kno_bson_output(q,obj);
	  hasid = 0;}
	else {
	  bson_append_lisp(q,"_id",3,id,-1);
	  kno_decref(id);}}
      else {
	bson_append_lisp(q,oidkey,3,id,-1);
	kno_decref(id);}}
    else bson_append_lisp(q,oidkey,3,obj,-1);
    if ((logops)||(flags&KNO_MONGODB_LOGOPS))
      u8_logf(LOG_NOTICE,"mongodb_remove","Removing %q items from %q",obj,coll);
    if (mongoc_collection_remove(collection,
				 ((hasid)?
				  (MONGOC_REMOVE_SINGLE_REMOVE):
				  (MONGOC_REMOVE_NONE)),
				 q.bson_doc,wc,&error)) {
      U8_CLEAR_ERRNO();
      result = KNO_TRUE;}
    else {
      u8_byte buf[1000];
      kno_seterr(kno_MongoDB_Error,"mongodb_remove",
		 u8_sprintf(buf,1000,"%s (%s>%s)",
			    error.message,db->dburi,coll->collection_name),
		 kno_incref(obj));
      result = KNO_ERROR_VALUE;}
    collection_done(collection,client,coll);
    if (wc) mongoc_write_concern_destroy(wc);
    kno_decref(q.bson_fieldmap);
    if (q.bson_doc) bson_destroy(q.bson_doc);}
  else result = KNO_ERROR_VALUE;
  kno_decref(opts);
  return result;
}

static lispval mongodb_updater(lispval arg,lispval query,lispval update,
			       mongoc_update_flags_t add_update_flags,
			       lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  struct KNO_MONGODB_DATABASE *db = COLL2DB(coll);
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    bson_t *q = kno_lisp2bson(query,flags,opts);
    bson_t *u = kno_lisp2bson(update,flags,opts);
    mongoc_write_concern_t *wc = get_write_concern(opts);
    mongoc_update_flags_t update_flags=
      (MONGOC_UPDATE_NONE) | (add_update_flags) |
      ((boolopt(opts,upsertsym,0))?(MONGOC_UPDATE_UPSERT):(0)) |
      ((boolopt(opts,singlesym,0))?(0):(MONGOC_UPDATE_MULTI_UPDATE));
    bson_error_t error;
    int success = 0, no_error = boolopt(opts,softfailsym,0);
    if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
      char *qstring = bson_as_json(q,NULL);
      char *ustring = bson_as_json(u,NULL);
      u8_logf(LOG_NOTICE,"mongodb_updater",
	      "Updating matches in %q to\n%Q\n%s\n+%Q\n+%s",
	      arg,query,qstring,update,ustring);
      bson_free(qstring);
      bson_free(ustring);}
    if ((q)&&(u))
      success = mongoc_collection_update(collection,update_flags,q,u,wc,&error);
    IGNORE_ERRNO(success);
    collection_done(collection,client,coll);
    if (q) bson_destroy(q);
    if (u) bson_destroy(u);
    if (wc) mongoc_write_concern_destroy(wc);
    kno_decref(opts);
    if (success) return KNO_TRUE;
    else if (no_error) {
      if ((q)&&(u))
	u8_logf(LOG_ERR,"mongodb_update",
		"Error on %s>%s: %s\n\twith query\nquery =  %q\nupdate =  %q\nflags = %q",
		db->dburi,coll->collection_name,error.message,query,update,opts);
      else u8_logf(LOG_ERR,"mongodb_update",
		   "Error on %s>%s:  %s\n\twith query\nquery =  %q\nupdate =  %q\nflags = %q",
		   db->dburi,coll->collection_name,error.message,query,update,opts);
      return KNO_FALSE;}
    else if ((q)&&(u)) {
      u8_byte buf[1000];
      kno_seterr(kno_MongoDB_Error,"mongodb_update/call",
		 u8_sprintf(buf,1000,"%s (%s>%s)",
			    error.message,db->dburi,coll->collection_name),
		 kno_make_pair(query,update));}
    else {
      u8_byte buf[1000];
      kno_seterr(kno_BSON_Error,"mongodb_update/prep",
		 u8_sprintf(buf,1000,"%s (%s>%s)",
			    error.message,db->dburi,coll->collection_name),
		 kno_make_pair(query,update));}
    return KNO_ERROR_VALUE;}
  else {
    kno_decref(opts);
    return KNO_ERROR_VALUE;}
}


DEFC_PRIM("collection/update!",collection_update,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	  "(COLLECTION/UPDATE! *collection* *object* *opts*) "
	  "**undocumented**",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"update",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_update(lispval arg,lispval query,lispval update,
				 lispval opts_arg)
{
  return mongodb_updater(arg,query,update,0,opts_arg);
}

DEFC_PRIM("collection/upsert!",collection_upsert,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(2),
	  "(COLLECTION/UPSERT! *collection* *object* *opts*) "
	  "**undocumented**",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"update",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_upsert(lispval collection,lispval query,
				 lispval update,lispval opts_arg)
{
  return mongodb_updater(collection,query,update,(MONGOC_UPDATE_UPSERT),opts_arg);
}

#if HAVE_MONGOC_OPTS_FUNCTIONS

DEFC_PRIM("collection/find",collection_find,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_find(lispval arg,lispval query,lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    lispval results = KNO_EMPTY_CHOICE;
    mongoc_cursor_t *cursor = NULL;
    const bson_t *doc;
    bson_t *q = kno_lisp2bson(query,flags,opts);
    bson_t *findopts = get_search_opts(opts,flags,KNO_FIND_MATCHES);
    mongoc_read_prefs_t *rp = get_read_prefs(opts);
    lispval *vec = NULL; size_t n = 0, max = 0;
    int sort_results = kno_testopt(opts,KNOSYM_SORTED,KNO_VOID);
    if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
      char *qstring = bson_as_json(q,NULL);
      u8_logf(LOG_NOTICE,"mongodb_find","Matches in %q to\n%Q\n%s",
	      arg,query,qstring);
      bson_free(qstring);}
    if (q) cursor = open_cursor(collection,q,findopts,rp,opts);
    if (cursor) {
      U8_CLEAR_ERRNO();
      while (mongoc_cursor_next(cursor,&doc)) {
	/* u8_string json = bson_as_json(doc,NULL); */
	lispval r = kno_bson2lisp((bson_t *)doc,flags,opts);
	if (KNO_ABORTP(r)) {
	  kno_decref(results);
	  free_lisp_vec(vec,n);
	  results = KNO_ERROR_VALUE;
	  sort_results = 0;
	  break;}
	else if (sort_results) {
	  if (n>=max) {
	    if (!(grow_lisp_vec(&vec,n,&max))) {
	      free_lisp_vec(vec,n);
	      results = KNO_ERROR_VALUE;
	      sort_results = 0;
	      break;}}
	  vec[n++]=r;}
	else {
	  KNO_ADD_TO_CHOICE(results,r);}}
      bson_error_t err;
      bool trouble = (KNO_ABORTP(results)) ? (1) :
	(mongoc_cursor_error(cursor,&err));
      if (trouble) {
	free_lisp_vec(vec,n);
	if (! (KNO_ABORTP(results)) )
	  grab_mongodb_error(&err,"mongodb_find");
	mongoc_cursor_destroy(cursor);
	kno_decref(opts);
	return KNO_ERROR;}
      else mongoc_cursor_destroy(cursor);}
    else {
      u8_byte buf[1000];
      kno_seterr(kno_MongoDB_Error,"mongodb_find",
		 u8_sprintf(buf,1000,
			    "couldn't get query cursor over %q with options:\n%Q",
			    arg,opts),
		 kno_incref(query));
      results = KNO_ERROR_VALUE;}
    if (rp) mongoc_read_prefs_destroy(rp);
    if (q) bson_destroy(q);
    if (findopts) bson_destroy(findopts);
    collection_done(collection,client,coll);
    kno_decref(opts);
    U8_CLEAR_ERRNO();
    if (KNO_ABORTED(results)) {}
    else if (sort_results) {
      if ((vec == NULL)||(n==0)) return kno_make_vector(0,NULL);
      else results = kno_make_vector(n,vec);
      if (vec) u8_free(vec);}
    return results;}
  else {
    kno_decref(opts);
    U8_CLEAR_ERRNO();
    return KNO_ERROR_VALUE;}
}
#else

DEFC_PRIM("collection/find",collection_find,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_find(lispval collection,lispval query,lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll =
    (struct KNO_MONGODB_COLLECTION *)collection;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    lispval results = KNO_EMPTY_CHOICE;
    mongoc_cursor_t *cursor = NULL;
    const bson_t *doc;
    lispval skip_arg = kno_getopt(opts,skipsym,KNO_FIXZERO);
    lispval limit_arg = kno_getopt(opts,limitsym,KNO_FIXZERO);
    lispval batch_arg = kno_getopt(opts,batchsym,KNO_FIXZERO);
    int sort_results = kno_testopt(opts,KNOSYM_SORTED,KNO_VOID);
    lispval *vec = NULL; size_t n = 0, max = 0;
    if ((KNO_UINTP(skip_arg))&&(KNO_UINTP(limit_arg))&&(KNO_UINTP(batch_arg))) {
      bson_t *q = kno_lisp2bson(query,flags,opts);
      bson_t *fields = get_projection(opts,flags);
      mongoc_read_prefs_t *rp = get_read_prefs(opts);
      if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
	unsigned char *qstring = bson_as_json(q,NULL);
	u8_logf(LOG_NOTICE,"mongodb_find",
		"Matches in %q to\n%Q\n%s",arg,query,qstring);
	bson_free(qstring);}
      if (q) cursor = mongoc_collection_find
	       (collection,MONGOC_QUERY_NONE,
		KNO_FIX2INT(skip_arg),
		KNO_FIX2INT(limit_arg),
		KNO_FIX2INT(batch_arg),
		q,
		fields,
		rp);
      if (cursor) {
	while (mongoc_cursor_next(cursor,&doc)) {
	  /* u8_string json = bson_as_json(doc,NULL); */
	  U8_CLEAR_ERRNO();
	  lispval r = kno_bson2lisp((bson_t *)doc,flags,opts);
	  if (KNO_ABORTP(r)) {
	    kno_decref(results);
	    free_lisp_vec(vec,n);
	    results = KNO_ERROR_VALUE;
	    sort_results = 0;}
	  else if (sort_results) {
	    if (n>=max) {
	      if (!(grow_lisp_vec(&vec,n,&max))) {
		free_lisp_vec(vec,n);
		results = KNO_ERROR_VALUE;
		sort_results = 0;
		break;}}
	    vec[n++]=r;}
	  else {
	    KNO_ADD_TO_CHOICE(results,r);}}
	mongoc_cursor_destroy(cursor);}
      else results = kno_err(kno_MongoDB_Error,"mongodb_find","couldn't get cursor",opts);
      if (rp) mongoc_read_prefs_destroy(rp);
      if (q) bson_destroy(q);
      if (fields) bson_destroy(fields);}
    else {
      results = kno_err(kno_TypeError,"mongodb_find","bad skip/limit/batch",opts);
      sort_results = 0;}
    collection_done(collection,client,coll);
    U8_CLEAR_ERRNO();
    kno_decref(opts);
    kno_decref(skip_arg);
    kno_decref(limit_arg);
    kno_decref(batch_arg);
    if (sort_results) {
      if ((vec == NULL)||(n==0)) return kno_make_vector(0,NULL);
      else results = kno_make_vector(n,vec);
      if (vec) u8_free(vec);}
    return results;}
  else {
    kno_decref(opts);
    U8_CLEAR_ERRNO();
    return KNO_ERROR_VALUE;}
}
#endif


static int query_check(lispval query)
{
  if ( (KNO_FALSEP(query)) || (KNO_EMPTYP(query)) )
    return 0;
  else if (KNO_SLOTMAPP(query))
    return KNO_SLOTMAP_SIZE(query);
  else if (KNO_SCHEMAPP(query))
    return KNO_SCHEMAP_SIZE(query);
  else  return -1;
}

DEFC_PRIM("collection/count",collection_count,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_count(lispval arg,lispval query,lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,coll->collection_flags);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    lispval result = KNO_VOID;
    long n_documents = -1;
    bson_error_t error = { 0 };
    lispval opts = combine_opts(opts_arg,coll->collection_opts);
    bson_t *findopts = get_search_opts(opts,flags,KNO_COUNT_MATCHES);
    mongoc_read_prefs_t *rp = get_read_prefs(opts);
    int n_keys = query_check(query);
    if (RARELY(n_keys<0)) {
      kno_seterr("BadMongoQuery","collection_count",coll->collection_name,
		 query);
      return KNO_ERROR;}
    else if (n_keys == 0)
      n_documents = mongoc_collection_estimated_document_count
	(collection,findopts,rp,NULL,&error);
    else {
      bson_t *q = kno_lisp2bson(query,flags,opts);
      if (q == NULL) {
	n_documents = -1;
	result = KNO_ERROR_VALUE;}
      else {
	if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
	  unsigned char *qstring = bson_as_json(q,NULL);
	  u8_logf(LOG_NOTICE,"mongodb_count",
		  "Counting matches in %q to\n%Q\n%s",
		  arg,query,qstring);
	  bson_free(qstring);}
	n_documents = mongoc_collection_count_documents
	  (collection,q,findopts,rp,NULL,&error);
	bson_destroy(q);}}
    if (n_documents>=0) {
      U8_CLEAR_ERRNO();
      result = KNO_INT(n_documents);}
    else if (KNO_VOIDP(result)) {
      u8_byte buf[1000];
      kno_seterr(kno_MongoDB_Error,"mongodb_count",
		 u8_sprintf(buf,1000,
			    "(%s) couldn't count documents in %s matching\n"
			    "%Q\n given options:\n%Q",
			    error.message,coll->collection_name,
			    query,opts),
		 kno_incref(query));
      result = KNO_ERROR_VALUE;}
    else NO_ELSE;
    if (rp) mongoc_read_prefs_destroy(rp);
    if (findopts) bson_destroy(findopts);
    collection_done(collection,client,coll);
    kno_decref(opts);
    U8_CLEAR_ERRNO();
    return result;}
  else {
    U8_CLEAR_ERRNO();
    return KNO_ERROR_VALUE;}
}


DEFC_PRIM("collection/get",collection_get,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_get(lispval arg,lispval query,lispval opts_arg)
{
  lispval result = KNO_EMPTY_CHOICE;
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  mongoc_client_t *client = NULL;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    mongoc_cursor_t *cursor;
    const bson_t *doc;
    bson_t *q, *findopts = get_search_opts(opts,flags,KNO_FIND_MATCHES);
    mongoc_read_prefs_t *rp = get_read_prefs(opts);
    if (OIDP(query)) {
      struct KNO_BSON_OUTPUT out;
      out.bson_doc = bson_new();
      out.bson_flags = ((flags<0)?(getflags(opts,KNO_MONGODB_DEFAULTS)):(flags));
      out.bson_opts = opts;
      bson_append_lisp(out,coll->collection_oidkey,3,query,-1);
      q = out.bson_doc;}
    else if (KNO_TABLEP(query))
      q = kno_lisp2bson(query,flags,opts);
    else {
      struct KNO_BSON_OUTPUT out;
      out.bson_doc = bson_new();
      out.bson_flags = ((flags<0)?(getflags(opts,KNO_MONGODB_DEFAULTS)):(flags));
      out.bson_opts = opts;
      bson_append_lisp(out,"_id",3,query,-1);
      q = out.bson_doc;}
    if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
      unsigned char *qstring = bson_as_json(q,NULL);
      u8_logf(LOG_NOTICE,"collection_get",
	      "Matches in %q to \n%Q\n%s",arg,query,qstring);
      bson_free(qstring);}
    if (q) cursor = open_cursor(collection,q,findopts,rp,opts);
    else cursor=NULL;
    if ((cursor)&&(mongoc_cursor_next(cursor,&doc))) {
      U8_CLEAR_ERRNO();
      result = kno_bson2lisp((bson_t *)doc,flags,opts);}
    if (cursor) mongoc_cursor_destroy(cursor);
    if (rp) mongoc_read_prefs_destroy(rp);
    if (findopts) bson_destroy(findopts);
    if (q) bson_destroy(q);
    kno_decref(opts);
    collection_done(collection,client,coll);
    U8_CLEAR_ERRNO();
    return result;}
  else {
    kno_decref(opts);
    U8_CLEAR_ERRNO();
    return KNO_ERROR_VALUE;}
}

DEFC_PRIM("collection/oidslot",collection_oidslot,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the OID slot associated with *collection*",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID})
static lispval collection_oidslot(lispval arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  return coll->collection_oidslot;
}

/* Find and Modify */

static int getnewopt(lispval opts,int dflt);


DEFC_PRIM("collection/modify!",collection_modify,
	  KNO_MAX_ARGS(4)|KNO_MIN_ARGS(3),
	  "**undocumented**",
	  {"collection",KNO_MONGOC_COLLECTION,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"update",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval collection_modify(lispval arg,lispval query,
				 lispval update,
				 lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  struct KNO_MONGODB_DATABASE *db = COLL2DB(coll);
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  mongoc_client_t *client;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {
    lispval result = KNO_VOID;
    lispval sort = kno_getopt(opts,KNOSYM_SORT,KNO_VOID);
    lispval fields = kno_getopt(opts,fieldssym,KNO_VOID);
    lispval upsert = kno_getopt(opts,upsertsym,KNO_FALSE);
    lispval remove = kno_getopt(opts,removesym,KNO_FALSE);
    int return_new = getnewopt(opts,1);
    bson_t *q = kno_lisp2bson(query,flags,opts);
    bson_t *u = kno_lisp2bson(update,flags,opts);
    if ((q == NULL)||(u == NULL)) {
      U8_CLEAR_ERRNO();
      return KNO_ERROR_VALUE;}
    if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
      unsigned char *qstring = bson_as_json(q,NULL);
      unsigned char *ustring = bson_as_json(u,NULL);
      u8_logf(LOG_NOTICE,"mongodb_modify",
	      "Updating in %q to\n%Q\n%s\n+%Q\n+%s",
	      arg,query,qstring,update,ustring);
      bson_free(qstring);
      bson_free(ustring);}
    bson_t reply; bson_error_t error = { 0 };
    if (mongoc_collection_find_and_modify
	(collection,
	 q,kno_lisp2bson(sort,flags,opts),
	 u,kno_lisp2bson(fields,flags,opts),
	 ((KNO_FALSEP(remove))?(false):(true)),
	 ((KNO_FALSEP(upsert))?(false):(true)),
	 ((return_new)?(true):(false)),
	 &reply,&error)) {
      U8_CLEAR_ERRNO();
      result = kno_bson2lisp(&reply,flags,opts);}
    else {
      u8_byte buf[1000];
      kno_seterr(kno_MongoDB_Error,"mongodb_modify",
		 u8_sprintf(buf,1000,"%s (%s>%s)",
			    error.message,db->dburi,coll->collection_name),
		 kno_make_pair(query,update));
      result = KNO_ERROR_VALUE;}
    collection_done(collection,client,coll);
    if (q) bson_destroy(q);
    if (u) bson_destroy(u);
    bson_destroy(&reply);
    kno_decref(opts);
    kno_decref(fields);
    kno_decref(upsert);
    kno_decref(remove);
    U8_CLEAR_ERRNO();
    return result;}
  else {
    kno_decref(opts);
    U8_CLEAR_ERRNO();
    return KNO_ERROR_VALUE;}
}

static int getnewopt(lispval opts,int dflt)
{
  lispval v = kno_getopt(opts,newsym,KNO_VOID);
  if (KNO_VOIDP(v)) {
    v = kno_getopt(opts,originalsym,KNO_VOID);
    if (KNO_VOIDP(v))
      return dflt;
    else if (KNO_FALSEP(v))
      return 1;
    else {
      kno_decref(v);
      return 0;}}
  else if (KNO_FALSEP(v))
    return 0;
  else {
    kno_decref(v);
    return 1;}
}

/* Command execution */

DEFC_PRIM("->mongovec",make_mongovec,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"vec",kno_vector_type,KNO_VOID})
static lispval make_mongovec(lispval vec)
{
  lispval *elts = KNO_VECTOR_ELTS(vec);
  int n = KNO_VECTOR_LENGTH(vec);
  return kno_init_compound_from_elts
    (NULL,mongovec_symbol,KNO_COMPOUND_INCREF|KNO_COMPOUND_SEQUENCE,n,elts);
}

static lispval make_command(int n,kno_argvec values)
{
  if ((n%2)==1)
    return kno_err(kno_SyntaxError,"make_command","Odd number of arguments",KNO_VOID);
  else {
    lispval result = kno_make_slotmap(n/2,n/2,NULL);
    struct KNO_KEYVAL *keyvals = KNO_SLOTMAP_KEYVALS(result);
    int n_slots=n/2;
    int i = 0; while (i<n_slots) {
      lispval key = values[i*2];
      lispval value = values[i*2+1];
      keyvals[i].kv_key=kno_incref(key);
      if (KNO_VECTORP(value))
	keyvals[i].kv_val=make_mongovec(value);
      else keyvals[i].kv_val=kno_incref(value);
      i++;}
    return result;}
}

static lispval collection_command(lispval arg,lispval command,lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  lispval fields = kno_get(opts,fieldssym,KNO_VOID);
  mongoc_client_t *client;
  mongoc_collection_t *collection = open_collection(coll,&client,flags);
  if (collection) {U8_CLEAR_ERRNO();}
  if (collection) {
    lispval results = KNO_EMPTY_CHOICE;
    bson_t *cmd = kno_lisp2bson(command,flags,opts);
    bson_t *flds = kno_lisp2bson(fields,flags,opts);
    if (cmd) {
      const bson_t *doc;
      lispval skip_arg = kno_getopt(opts,skipsym,KNO_FIXZERO);
      lispval limit_arg = kno_getopt(opts,limitsym,KNO_FIXZERO);
      lispval batch_arg = kno_getopt(opts,batchsym,KNO_FIXZERO);
      if ((KNO_UINTP(skip_arg))&&
	  (KNO_UINTP(limit_arg))&&
	  (KNO_UINTP(batch_arg))) {
	if (logcmds) {
	  u8_log(LOG_INFO,"MongoDBCommand",
		 "For %q:\n  COMMAND: %Q",arg,command);}
	mongoc_cursor_t *cursor = mongoc_collection_command
	  (collection,MONGOC_QUERY_EXHAUST,
	   (KNO_FIX2INT(skip_arg)),
	   (KNO_FIX2INT(limit_arg)),
	   (KNO_FIX2INT(batch_arg)),
	   cmd,flds,NULL);
	if (cursor) {
	  U8_CLEAR_ERRNO();
	  while ((cursor) && (mongoc_cursor_next(cursor,&doc))) {
	    lispval r = kno_bson2lisp((bson_t *)doc,flags,opts);
	    KNO_ADD_TO_CHOICE(results,r);}
	  mongoc_cursor_destroy(cursor);}
	else results=KNO_ERROR_VALUE;}
      else results = kno_err(kno_TypeError,"collection_command",
			     "bad skip/limit/batch",opts);
      mongoc_collection_destroy(collection);
      client_done(arg,client);
      if (cmd) bson_destroy(cmd);
      if (flds) bson_destroy(flds);
      kno_decref(opts);
      kno_decref(fields);
      U8_CLEAR_ERRNO();
      return results;}
    else {
      return KNO_ERROR_VALUE;}}
  else return KNO_ERROR_VALUE;
}

static lispval db_command(lispval arg,lispval command,
			  lispval opts_arg)
{
  struct KNO_MONGODB_DATABASE *srv = (struct KNO_MONGODB_DATABASE *)arg;
  int flags = getflags(opts_arg,srv->dbflags);
  lispval opts = combine_opts(opts_arg,srv->dbopts);
  lispval fields = kno_getopt(opts,fieldssym,KNO_VOID);
  mongoc_client_t *client = get_client(srv,MONGODB_CLIENT_BLOCK);
  if (client) {U8_CLEAR_ERRNO();}
  if (client) {
    lispval results = KNO_EMPTY_CHOICE;
    bson_t *cmd = kno_lisp2bson(command,flags,opts);
    bson_t *flds = kno_lisp2bson(fields,flags,opts);
    if (cmd) {
      const bson_t *doc;
      lispval skip_arg = kno_getopt(opts,skipsym,KNO_FIXZERO);
      lispval limit_arg = kno_getopt(opts,limitsym,KNO_FIXZERO);
      lispval batch_arg = kno_getopt(opts,batchsym,KNO_FIXZERO);
      if ((KNO_UINTP(skip_arg))&&
	  (KNO_UINTP(limit_arg))&&
	  (KNO_UINTP(batch_arg))) {
	if (logcmds) {
	  unsigned char *cmdstring = bson_as_json(cmd,NULL);
	  u8_log(LOG_INFO,"MongoDBCommand",
		 "For %q:\n  COMMAND: %Q\n  JSON:%s",
		 arg,command,cmdstring);
	  bson_free(cmdstring);}
	mongoc_cursor_t *cursor = mongoc_client_command
	  (client,srv->dbname,MONGOC_QUERY_EXHAUST,
	   (KNO_FIX2INT(skip_arg)),
	   (KNO_FIX2INT(limit_arg)),
	   (KNO_FIX2INT(batch_arg)),
	   cmd,flds,NULL);
	if (cursor) {
	  U8_CLEAR_ERRNO();
	  while (mongoc_cursor_next(cursor,&doc)) {
	    lispval r = kno_bson2lisp((bson_t *)doc,flags,opts);
	    KNO_ADD_TO_CHOICE(results,r);}
	  mongoc_cursor_destroy(cursor);}
	else {
	  kno_decref(results);
	  results=KNO_ERROR_VALUE;}}
      else results = kno_err(kno_TypeError,"collection_command",
			     "bad skip/limit/batch",opts);
      kno_decref(skip_arg);
      kno_decref(limit_arg);
      kno_decref(batch_arg);
      client_done(arg,client);
      if (cmd) bson_destroy(cmd);
      if (flds) bson_destroy(flds);
      kno_decref(opts);
      kno_decref(fields);
      return results;}
    else {
      return KNO_ERROR_VALUE;}}
  else return KNO_ERROR_VALUE;
}


DEFC_PRIMN("mongodb/results",mongodb_command,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval mongodb_command(int n,kno_argvec args)
{
  lispval arg = args[0], opts = KNO_VOID, command = KNO_VOID, result = KNO_VOID;
  int flags = mongodb_getflags(arg);
  if (flags<0)
    return kno_type_error(_("MongoDB"),"mongodb_command",arg);
  else if (n==2) {
    command = args[1]; kno_incref(command); opts = KNO_VOID;}
  else if ((n==3)&&(KNO_TABLEP(args[1]))) {
    command = args[1]; kno_incref(command); opts = args[2];}
  else if (n%2) {
    command = make_command(n-1,args+1);}
  else {
    command = make_command(n-2,args+2);
    opts = args[1];}
  if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
    u8_logf(LOG_DEBUG,"mongodb_command","At %q: %q",arg,command);}
  if (KNO_TYPEP(arg,kno_mongoc_server))
    result = db_command(arg,command,opts);
  else if (KNO_TYPEP(arg,kno_mongoc_collection))
    result = collection_command(arg,command,opts);
  else {}
  kno_decref(command);
  return result;
}

static lispval collection_simple_command(lispval arg,lispval command,
					 lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  bson_t *cmd = kno_lisp2bson(command,flags,opts);
  if (cmd) {
    mongoc_client_t *client;
    mongoc_collection_t *collection = open_collection(coll,&client,flags);
    if (collection) {U8_CLEAR_ERRNO();}
    if (collection) {
      bson_t response; bson_error_t error;
      if (logcmds) {
	unsigned char *cmd_string = bson_as_json(cmd,NULL);
	u8_log(LOG_INFO,"MongoDBCollectionSimpleCommand",
	       "For %q:\n  COMMAND: %Q\n JSON=%s",
	       arg,command,cmd_string);
	bson_free(cmd_string);}
      if (mongoc_collection_command_simple
	  (collection,cmd,NULL,&response,&error)) {
	U8_CLEAR_ERRNO();
	lispval result = kno_bson2lisp(&response,flags,opts);
	collection_done(collection,client,coll);
	bson_destroy(cmd);
	kno_decref(opts);
	return result;}
      else {
	grab_mongodb_error(&error,"collection_simple_command");
	collection_done(collection,client,coll);
	bson_destroy(cmd);
	kno_decref(opts);
	return KNO_ERROR_VALUE;}}
    else {
      bson_destroy(cmd);
      kno_decref(opts);
      return KNO_ERROR_VALUE;}}
  else {
    kno_decref(opts);
    return KNO_ERROR_VALUE;}
}

static lispval db_simple_command(lispval arg,lispval command,
				 lispval opts_arg)
{
  struct KNO_MONGODB_DATABASE *srv = (struct KNO_MONGODB_DATABASE *)arg;
  int flags = getflags(opts_arg,srv->dbflags);
  lispval opts = combine_opts(opts_arg,srv->dbopts);
  mongoc_client_t *client = get_client(srv,MONGODB_CLIENT_BLOCK);
  if (client) {
    bson_t response; bson_error_t error;
    bson_t *cmd = kno_lisp2bson(command,flags,opts);
    if (cmd) {
      if (logcmds) {
	unsigned char *cmd_string = bson_as_json(cmd,NULL);
	u8_log(LOG_INFO,"MongoDBSimpleCommand",
	       "For %q:\n  COMMAND: %Q\n  JSON: %s",
	       arg,command,cmd_string);
	bson_free(cmd_string);}
      if (mongoc_client_command_simple
	  (client,srv->dbname,cmd,NULL,&response,&error)) {
	U8_CLEAR_ERRNO();
	lispval result = kno_bson2lisp(&response,flags,opts);
	client_done(arg,client);
	bson_destroy(cmd);
	kno_decref(opts);
	return result;}
      else {
	grab_mongodb_error(&error,"db_simple_command");
	client_done(arg,client);
	bson_destroy(cmd);
	kno_decref(opts);
	return KNO_ERROR_VALUE;}}
    else {
      return KNO_ERROR_VALUE;}}
  else return KNO_ERROR_VALUE;
}


DEFC_PRIMN("mongodb/cmd",mongodb_simple_command,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(2),
	   "**undocumented**")
static lispval mongodb_simple_command(int n,kno_argvec args)
{
  lispval arg = args[0], opts = KNO_VOID, command = KNO_VOID, result = KNO_VOID;
  int flags = mongodb_getflags(arg);
  if (flags<0) return kno_type_error(_("MongoDB"),"mongodb_command",arg);
  else if (n==2) {
    command = args[1]; kno_incref(command); opts = KNO_VOID;}
  else if ((n==3)&&(KNO_TABLEP(args[1]))) {
    command = args[1]; kno_incref(command); opts = args[2];}
  else if (n%2) {
    command = make_command(n-1,args+1);}
  else {
    command = make_command(n-2,args+2);
    opts = args[1];}
  if ((logops)||(flags&KNO_MONGODB_LOGOPS)) {
    u8_logf(LOG_DEBUG,"mongodb_simple_command","At %q: %q",arg,command);}
  if (KNO_TYPEP(arg,kno_mongoc_server))
    result = db_simple_command(arg,command,opts);
  else if (KNO_TYPEP(arg,kno_mongoc_collection))
    result = collection_simple_command(arg,command,opts);
  else {}
  kno_decref(command);
  return result;
}

/* Cursor creation */

static int reckless_threading = 0;

DEF_KNOSYM(skip);

#if HAVE_MONGOC_OPTS_FUNCTIONS

DEFC_PRIM("cursor/open",mongodb_cursor,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval mongodb_cursor(lispval arg,lispval query,lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  mongoc_client_t *connection;
  mongoc_cursor_t *cursor = NULL;
  mongoc_collection_t *collection = open_collection(coll,&connection,flags);
  bson_t *bq = kno_lisp2bson(query,flags,opts);
  bson_t *findopts = get_search_opts(opts,flags,KNO_FIND_MATCHES);
  mongoc_read_prefs_t *rp = get_read_prefs(opts);
  if (collection) cursor = open_cursor(collection,bq,findopts,rp,opts);
  if (cursor) {
    U8_CLEAR_ERRNO();
    lispval wait_ms = kno_getopt(opts,KNOSYM(maxwait),KNO_VOID);
    lispval skip_arg = kno_getopt(opts,KNOSYM(skip),KNO_VOID);
    struct KNO_MONGODB_CURSOR *consed = u8_alloc(struct KNO_MONGODB_CURSOR);
    KNO_INIT_CONS(consed,kno_mongoc_cursor);
    consed->cursor_coll = arg; kno_incref(arg);
    consed->cursor_db = coll->collection_db;
    kno_incref(coll->collection_db);
    consed->cursor_threadid = u8_threadid();
    consed->cursor_skipped = (KNO_UINTP(skip_arg)) ? (KNO_INT(skip_arg)): (0);
    consed->cursor_read = 0;
    consed->cursor_query = query; kno_incref(query);
    consed->cursor_query_bson = bq;
    consed->cursor_value_bson = NULL;
    consed->cursor_readprefs = rp;
    consed->cursor_flags = flags;
    consed->cursor_done  = 0;
    consed->cursor_opts = opts;
    consed->cursor_connection = connection;
    consed->cursor_collection = collection;
    consed->mongoc_cursor = cursor;
    if ( (KNO_FIXNUMP(wait_ms)) && ((KNO_FIX2INT(wait_ms))>=0) &&
	 ((KNO_FIX2INT(wait_ms)) < UINT_MAX) ) {
      unsigned int milliseconds = KNO_INT(wait_ms);
      mongoc_cursor_set_max_await_time_ms(cursor,milliseconds);}
    kno_decref(wait_ms);
    kno_decref(skip_arg);
    return (lispval) consed;}
  else {
    kno_decref(opts);
    if (rp) mongoc_read_prefs_destroy(rp);
    if (findopts) bson_destroy(findopts);
    if (bq) bson_destroy(bq);
    if (collection) collection_done(collection,connection,coll);
    return KNO_ERROR_VALUE;}
}
#else

DEFC_PRIM("cursor/open",mongodb_cursor,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(2),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID},
	  {"query",kno_any_type,KNO_VOID},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval mongodb_cursor(lispval arg,lispval query,lispval opts_arg)
{
  struct KNO_MONGODB_COLLECTION *coll = (struct KNO_MONGODB_COLLECTION *)arg;
  int flags = getflags(opts_arg,coll->collection_flags);
  lispval opts = combine_opts(opts_arg,coll->collection_opts);
  mongoc_client_t *connection;
  mongoc_cursor_t *cursor = NULL;
  mongoc_collection_t *collection = open_collection(coll,&connection,flags);
  lispval skip_arg = kno_getopt(opts,skipsym,KNO_FIXZERO);
  lispval limit_arg = kno_getopt(opts,limitsym,KNO_FIXZERO);
  lispval batch_arg = kno_getopt(opts,batchsym,KNO_FIXZERO);
  bson_t *bq = kno_lisp2bson(query,flags,opts);
  bson_t *fields = get_projection(opts,flags);
  mongoc_read_prefs_t *rp = get_read_prefs(opts);
  if ((collection)&&
      (KNO_UINTP(skip_arg))&&
      (KNO_UINTP(limit_arg))&&
      (KNO_UINTP(batch_arg))) {
    cursor = mongoc_collection_find
      (collection,MONGOC_QUERY_NONE,
       KNO_FIX2INT(skip_arg),
       KNO_FIX2INT(limit_arg),
       KNO_FIX2INT(batch_arg),
       bq,fields,rp);}
  if (cursor) {
    struct KNO_MONGODB_CURSOR *consed = u8_alloc(struct KNO_MONGODB_CURSOR);
    KNO_INIT_CONS(consed,kno_mongoc_cursor);
    consed->cursor_coll = arg; kno_incref(arg);
    consed->cursor_db = coll->collection_db;
    kno_incref(coll->collection_db);
    consed->cursor_threadid = u8_threadid();
    consed->cursor_skipped = (KNO_UINTP(skip_arg)) ? (KNO_INT(skip_arg)): (0);
    consed->cursor_read = 0;
    consed->cursor_query = query; kno_incref(query);
    consed->cursor_query_bson = bq;
    consed->cursor_value_bson = NULL;
    consed->cursor_opts_bson = fields;
    consed->cursor_readprefs = rp;
    consed->cursor_connection = connection;
    consed->cursor_collection = collection;
    consed->mongoc_cursor = cursor;
    return (lispval) consed;}
  else {
    kno_decref(skip_arg); kno_decref(limit_arg); kno_decref(batch_arg);
    kno_decref(opts);
    if (bq) bson_destroy(bq);
    if (fields) bson_destroy(fields);
    if (collection) collection_done(collection,connection,coll);
    return KNO_ERROR_VALUE;}
}
#endif

DEFC_PRIM("cursor/close!",cursor_close,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Closes a MongoDB cursor",
	  {"cursor",KNO_MONGOC_CURSOR,KNO_VOID})
static lispval cursor_close(lispval cursor_val)
{
  struct KNO_MONGODB_CURSOR *cursor = (struct KNO_MONGODB_CURSOR *)cursor_val;
  if (cursor->mongoc_cursor==NULL) return KNO_VOID;
  struct KNO_MONGODB_COLLECTION *coll = CURSOR2COLL(cursor);
  struct KNO_MONGODB_DATABASE *s = COLL2DB(coll);
  mongoc_cursor_t *mc = cursor->mongoc_cursor;
  cursor->mongoc_cursor=NULL;
  mongoc_cursor_destroy(mc);
  mongoc_collection_destroy(cursor->cursor_collection);
  cursor->cursor_collection=NULL;
  release_client(s,cursor->cursor_connection);
  cursor->cursor_connection=NULL;
  if (cursor->cursor_query_bson) {
    bson_destroy(cursor->cursor_query_bson);
    cursor->cursor_query_bson=NULL;}
  if (cursor->cursor_opts_bson) {
    bson_destroy(cursor->cursor_opts_bson);
    cursor->cursor_opts_bson=NULL;}
  if (cursor->cursor_readprefs) {
    mongoc_read_prefs_destroy(cursor->cursor_readprefs);
    cursor->cursor_readprefs=NULL;}
  cursor->cursor_value_bson = NULL;
  U8_CLEAR_ERRNO();
  return KNO_FALSE;
}

static void recycle_cursor(struct KNO_RAW_CONS *c)
{
  struct KNO_MONGODB_CURSOR *cursor = (struct KNO_MONGODB_CURSOR *)c;
  struct KNO_MONGODB_COLLECTION *coll = CURSOR2COLL(cursor);
  struct KNO_MONGODB_DATABASE *s = COLL2DB(coll);
  if (cursor->mongoc_cursor) mongoc_cursor_destroy(cursor->mongoc_cursor);
  if (cursor->cursor_collection) mongoc_collection_destroy(cursor->cursor_collection);
  if (cursor->cursor_connection) release_client(s,cursor->cursor_connection);
  kno_decref(cursor->cursor_coll);
  kno_decref(cursor->cursor_query);
  kno_decref(cursor->cursor_opts);
  if (cursor->cursor_query_bson)
    bson_destroy(cursor->cursor_query_bson);
  if (cursor->cursor_opts_bson)
    bson_destroy(cursor->cursor_opts_bson);
  if (cursor->cursor_readprefs)
    mongoc_read_prefs_destroy(cursor->cursor_readprefs);
  cursor->cursor_value_bson = NULL;
  if (!(KNO_STATIC_CONSP(c))) u8_free(c);
}
static int unparse_cursor(struct U8_OUTPUT *out,lispval x)
{
  struct KNO_MONGODB_CURSOR *cursor = (struct KNO_MONGODB_CURSOR *)x;
  struct KNO_MONGODB_COLLECTION *coll = CURSOR2COLL(cursor);
  struct KNO_MONGODB_DATABASE *db = COLL2DB(coll);
  u8_printf(out,"#<MongoDB/Cursor '%s/%s' %q>",
	    db->dbname,coll->collection_name,cursor->cursor_query);
  return 1;
}

/* Operations on cursors */

static int cursor_advance(struct KNO_MONGODB_CURSOR *c,u8_context caller)
{
  if (c->cursor_done) return 0;
  if (c->mongoc_cursor == NULL) {
    kno_seterr("MongoCursorClosed","mongodb_cursor_reader",NULL,(lispval)c);
    return -1;}
  if ( (c->cursor_threadid>0) && ( c->cursor_threadid != u8_threadid() ) ) {
    u8_byte msg[100];
    kno_seterr("CursorThreadConflict",caller,
	       u8_bprintf(msg,"Opened in thread %llx, using in thread %llx",
			  c->cursor_threadid,u8_threadid()),
	       (lispval)c);
    return -1;}
  bool ok = mongoc_cursor_next(c->mongoc_cursor,&(c->cursor_value_bson));
  if (ok) {
    U8_CLEAR_ERRNO();
    return 1;}
  bson_error_t err;
  ok = mongoc_cursor_error(c->mongoc_cursor,&err);
  if (ok) {
    grab_mongodb_error(&err,caller);
    return -1;}
  else {
    c->cursor_done = 1;
    return 0;}
}


DEFC_PRIM("cursor/done?",cursor_donep,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns true if *cursor* is exhausted",
	  {"cursor",KNO_MONGOC_CURSOR,KNO_VOID})
static lispval cursor_donep(lispval cursor)
{
  struct KNO_MONGODB_CURSOR *c = (struct KNO_MONGODB_CURSOR *)cursor;
  if (c->cursor_value_bson)
    return KNO_TRUE;
  else {
    int rv = cursor_advance(c,"cursor_donep");
    if (rv == 0)
      return KNO_TRUE;
    else if (rv == 1)
      return KNO_FALSE;
    else return KNO_ERROR;}
}

DEFC_PRIM("cursor/skipcount",cursor_skipcount,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the number of items skipped to date for *cursor*",
	  {"cursor",KNO_MONGOC_CURSOR,KNO_VOID})
static lispval cursor_skipcount(lispval cursor)
{
  struct KNO_MONGODB_CURSOR *c = (struct KNO_MONGODB_CURSOR *)cursor;
  return KNO_INT(c->cursor_skipped);
}

DEFC_PRIM("cursor/readcount",cursor_readcount,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "Returns the number of items read (to date) from *cursor*",
	  {"cursor",KNO_MONGOC_CURSOR,KNO_VOID})
static lispval cursor_readcount(lispval cursor)
{
  struct KNO_MONGODB_CURSOR *c = (struct KNO_MONGODB_CURSOR *)cursor;
  return KNO_INT(c->cursor_read);
}

DEFC_PRIM("cursor/skip",cursor_skip,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "Skips ahead *howmany* records on *cursor*. Returns #f when "
	  "no items are retrieved or the number of items otherwise.",
	  {"cursor",KNO_MONGOC_CURSOR,KNO_VOID},
	  {"howmany",kno_fixnum_type,KNO_INT(1)})
static lispval cursor_skip(lispval cursor,lispval howmany)
{
  struct KNO_MONGODB_CURSOR *c = (struct KNO_MONGODB_CURSOR *)cursor;
  if (!(KNO_UINTP(howmany)))
    return kno_type_error("uint","mongodb_skip",howmany);
  int n = KNO_FIX2INT(howmany), i = 0, rv = 0;
  while  ((i<n) && ((rv=cursor_advance(c,"mongodb_skip")) > 0)) i++;
  if (i>0) c->cursor_skipped += i;
  if (i<0) return KNO_ERROR;
  else if (i == 0) return KNO_FALSE;
  else return KNO_INT(i);
}

static lispval cursor_reader(lispval cursor,lispval howmany,
			     lispval opts_arg,int sorted)
{
  struct KNO_MONGODB_CURSOR *c = (struct KNO_MONGODB_CURSOR *)cursor;
  if (c->mongoc_cursor == NULL)
    return kno_err("MongoCursorClosed","mongodb_cursor_reader",NULL,cursor);
  else if (c->cursor_done)
    return KNO_EMPTY;
  else if ( (c->cursor_threadid > 0) && ( c->cursor_threadid != u8_threadid() ) ) {
    u8_byte msg[100];
    kno_seterr("CursorThreadConflict","mongodb_cursor_reader",
	       u8_bprintf(msg,"Opened in thread %llx, using in thread %llx",
			  c->cursor_threadid,u8_threadid()),
	       (lispval)c);
    return KNO_ERROR;}
  else if (!(KNO_UINTP(howmany)))
    return kno_type_error("uint","mongodb_cursor_read",howmany);
  else NO_ELSE;
  int n = KNO_FIX2INT(howmany);
  int flags = getflags(opts_arg,c->cursor_flags);
  if (sorted < 0) sorted = kno_testopt(opts_arg,KNOSYM_SORTED,KNO_VOID);
  if (n == 0) {
    if (sorted)
      return kno_make_vector(0,NULL);
    else return KNO_EMPTY_CHOICE;}
  else NO_ELSE;
  lispval opts = combine_opts(opts_arg,c->cursor_opts);
  if ( (n == 1) && (c->cursor_value_bson != NULL) ) {
    lispval r = kno_bson2lisp((bson_t *)c->cursor_value_bson,flags,opts);
    kno_decref(opts);
    c->cursor_value_bson = NULL;
    c->cursor_read++;
    if (sorted)
      return kno_make_vector(1,&r);
    else return r;}
  else {
    lispval vec[n];
    mongoc_cursor_t *scan = c->mongoc_cursor;
    const bson_t *doc;
    int i = 0, ok = 0;
    if (c->cursor_value_bson != NULL) {
      lispval v = kno_bson2lisp(((bson_t *)c->cursor_value_bson),flags,opts);
      c->cursor_value_bson=NULL;
      vec[i++] = v;}
    while ( (i < n) && (ok=mongoc_cursor_next(scan,&doc)) ) {
      lispval r = kno_bson2lisp((bson_t *)doc,flags,opts);
      if (KNO_ABORTP(r)) {
	kno_decref_elts(vec,i);
	kno_decref(opts);
	return KNO_ERROR;}
      else vec[i++] = r;}
    if (!(ok)) {
      bson_error_t err;
      int cursor_err = mongoc_cursor_error(scan,&err);
      if (cursor_err) {
	grab_mongodb_error(&err,"mongodb");
	kno_decref_elts(vec,i);
	kno_decref(opts);
	return KNO_ERROR;}
      else c->cursor_done=1;}
    c->cursor_read += i;
    if (sorted) {
      if (i == 0)
	return kno_make_vector(0,NULL);
      else return kno_make_vector(i,vec);}
    else if (i == 0)
      return KNO_EMPTY_CHOICE;
    else if (i == 1)
      return vec[0];
    else return kno_init_choice
	   (NULL,i,vec,KNO_CHOICE_DOSORT|KNO_CHOICE_COMPRESS);}
}

DEFC_PRIM("cursor/read",cursor_read,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"cursor",KNO_MONGOC_CURSOR,KNO_VOID},
	  {"howmany",kno_fixnum_type,KNO_INT(1)},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval cursor_read(lispval cursor,lispval howmany,lispval opts)
{
  return cursor_reader(cursor,howmany,opts,-1);
}

DEFC_PRIM("cursor/readvec",cursor_readvec,
	  KNO_MAX_ARGS(3)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"cursor",KNO_MONGOC_CURSOR,KNO_VOID},
	  {"howmany",kno_fixnum_type,KNO_INT(1)},
	  {"opts_arg",kno_any_type,KNO_VOID})
static lispval cursor_readvec(lispval cursor,lispval howmany,lispval opts)
{
  return cursor_reader(cursor,howmany,opts,1);
}

/* BSON output functions */

static bool bson_append_lisp(struct KNO_BSON_OUTPUT b,
			     const char *key,int keylen,
			     lispval val,
			     int flags)
{
  bool ok = true;
  int colonize = flags & KNO_MONGODB_COLONIZE;
  bson_t *out = b.bson_doc;
  if (flags <= 0) flags = b.bson_flags;
  if ( ((flags)&(KNO_MONGODB_CHOICESLOT)) && (!(KNO_CHOICEP(val))) ) {
    /* Choiceslots are always rendered as arrays */
    struct KNO_BSON_OUTPUT wrapper_out = { 0 };
    bson_t values;
    ok = bson_append_array_begin(out,key,keylen,&values);
    wrapper_out.bson_doc = &values;
    wrapper_out.bson_flags = b.bson_flags;
    wrapper_out.bson_opts = b.bson_opts;
    wrapper_out.bson_fieldmap = b.bson_fieldmap;
    flags &= (~KNO_MONGODB_CHOICESLOT);
    if (ok) ok = bson_append_lisp(wrapper_out,"0",1,val,flags);
    if (ok) ok = bson_append_document_end(out,&values);
    return ok;}
  else if (KNO_CONSP(val)) {
    kno_lisp_type ctype = KNO_TYPEOF(val);
    switch (ctype) {
    case kno_string_type: {
      u8_string str = KNO_CSTRING(val);
      int len = KNO_STRLEN(val);
      int needs_escape = ((colonize) && (str[0]==':') );
      if (needs_escape) {
	/* If colonizing, escape leading colons */
	unsigned char buf[len+2];
	buf[0]='\\'; strncpy(buf+1,str,len); buf[len+1]='\0';
	ok = bson_append_utf8(out,key,keylen,buf,len+1);}
      else ok = bson_append_utf8(out,key,keylen,str,len);
      break;}
    case kno_packet_type:
      ok = bson_append_binary(out,key,keylen,BSON_SUBTYPE_BINARY,
			      KNO_PACKET_DATA(val),KNO_PACKET_LENGTH(val));
      break;
    case kno_flonum_type: {
      double d = KNO_FLONUM(val);
      ok = bson_append_double(out,key,keylen,d);
      break;}
    case kno_bigint_type: {
      kno_bigint b = kno_consptr(kno_bigint,val,kno_bigint_type);
      if (kno_bigint_fits_in_word_p(b,32,1)) {
	long int b32 = kno_bigint_to_long(b);
	ok = bson_append_int32(out,key,keylen,b32);}
      else if (kno_bigint_fits_in_word_p(b,65,1)) {
	long long int b64 = kno_bigint_to_long_long(b);
	ok = bson_append_int64(out,key,keylen,b64);}
      else {
	u8_logf(LOG_CRIT,kno_MongoDB_Warning,
		"Can't save bigint value %q",val);
	ok = bson_append_int32(out,key,keylen,0);}
      break;}
    case kno_timestamp_type: {
      struct KNO_TIMESTAMP *knot=
	kno_consptr(struct KNO_TIMESTAMP* ,val,kno_timestamp_type);
      unsigned long long millis = (knot->u8xtimeval.u8_tick*1000)+
	((knot->u8xtimeval.u8_prec>u8_second)?
	 (knot->u8xtimeval.u8_nsecs/1000000):
	 (0));
      ok = bson_append_date_time(out,key,keylen,millis);
      break;}
    case kno_uuid_type: {
      struct KNO_UUID *uuid = kno_consptr(struct KNO_UUID *,val,kno_uuid_type);
      ok = bson_append_binary(out,key,keylen,BSON_SUBTYPE_UUID,uuid->uuid16,16);
      break;}
    case kno_choice_type: case kno_prechoice_type: {
      int prechoicep = KNO_PRECHOICEP(val);
      lispval choice = (prechoicep) ? (kno_make_simple_choice(val)) : (val);
      struct KNO_BSON_OUTPUT inner = { 0 };
      bson_t arr; char numbuf[16];
      ok = bson_append_array_begin(out,key,keylen,&arr);
      if (ok) {
	int i = 0;
	inner.bson_doc = &arr; inner.bson_flags = b.bson_flags;
	inner.bson_opts = b.bson_opts; inner.bson_fieldmap = b.bson_fieldmap;
	int add_choice_tag =
	  (! ( ( (flags) & (KNO_MONGODB_CHOICESLOT) ) ||
	       ( (flags) & (KNO_MONGODB_PREFCHOICES) ) ) );
	if  (add_choice_tag) {
	  ok = bson_append_lisp(inner,"0",1,choice_tagstring,b.bson_flags);
	  i++;}
	KNO_ITER_CHOICES(scan,limit,choice);
	while (scan<limit) {
	  lispval elt = *scan++;
	  sprintf(numbuf,"%d",i++);
	  ok = bson_append_lisp(inner,numbuf,strlen(numbuf),elt,b.bson_flags);
	  if (!(ok)) break;}
	bson_append_array_end(out,&arr);}
      if (prechoicep) kno_decref(choice);
      break;}
    case kno_vector_type: {
      struct KNO_BSON_OUTPUT rout;
      bson_t arr, ch; char buf[16];
      struct KNO_VECTOR *vec = (struct KNO_VECTOR *)val;
      int i = 0, lim = vec->vec_length, doc_started = 0;
      lispval *data = vec->vec_elts;
      int wrap_vector = ((flags&KNO_MONGODB_PREFCHOICES)&&(key[0]!='$'));
      ok = bson_append_array_begin(out,key,keylen,&arr);
      if (ok) {
	doc_started = 1;
	memset(&rout,0,sizeof(struct KNO_BSON_OUTPUT));
	rout.bson_doc = &arr; rout.bson_flags = b.bson_flags;
	rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
	while (i<lim) {
	  lispval v = data[i]; sprintf(buf,"%d",i++);
	  ok = bson_append_lisp(rout,buf,strlen(buf),v,-1);
	  if (!(ok)) break;}}
      else break;
      if (!(doc_started)) break;
      else bson_append_array_end(out,&arr);
      break;}
    case kno_slotmap_type: case kno_hashtable_type: case kno_schemap_type: {
      struct KNO_BSON_OUTPUT rout;
      bson_t doc;
      lispval keys = kno_getkeys(val);
      ok = bson_append_document_begin(out,key,keylen,&doc);
      int doc_started = ok;
      if (ok) {
	memset(&rout,0,sizeof(struct KNO_BSON_OUTPUT));
	rout.bson_doc = &doc; rout.bson_flags = b.bson_flags;
	rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
	KNO_DO_CHOICES(key,keys) {
	  lispval value = kno_get(val,key,KNO_VOID);
	  if (!(KNO_VOIDP(value))) {
	    ok = bson_append_keyval(rout,key,value);
	    kno_decref(value);
	    if (!(ok)) KNO_STOP_DO_CHOICES;}}}
      kno_decref(keys);
      if (doc_started) bson_append_document_end(out,&doc);
      break;}
    case kno_regex_type: {
      struct KNO_REGEX *rx = (struct KNO_REGEX *)val;
      char opts[8], *write = opts; int flags = rx->rxflags;
      if (flags&REG_EXTENDED) *write++='x';
      if (flags&REG_ICASE) *write++='i';
      if (flags&REG_NEWLINE) *write++='m';
      *write++='\0';
      bson_append_regex(out,key,keylen,rx->rxsrc,opts);
      break;}
    case kno_pair_type: {
      struct KNO_PAIR *pair = (kno_pair) val;
      struct KNO_BSON_OUTPUT rout = { 0 };
      bson_t doc;
      ok = bson_append_document_begin(out,key,keylen,&doc);
      if (ok) {
	rout.bson_doc = &doc; rout.bson_flags = b.bson_flags;
	rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
	ok = bson_append_lisp(rout,":|>car>|",8,pair->car,-1);
	if (ok) {
	  ok = bson_append_lisp(rout,":|>cdr>|",8,pair->cdr,-1);
	  if (ok) bson_append_document_end(out,&doc);}}
      break;}
    case kno_compound_type: {
      struct KNO_COMPOUND *compound = KNO_XCOMPOUND(val);
      if (compound == NULL) return KNO_ERROR_VALUE;
      lispval tag = compound->typetag, *elts = KNO_COMPOUND_ELTS(val);
      int len = KNO_COMPOUND_LENGTH(val);
      struct KNO_BSON_OUTPUT rout;
      bson_t doc;
      if (tag == oidtag) {
	lispval packet = elts[0];
	bson_oid_t oid;
	bson_oid_init_from_data(&oid,KNO_PACKET_DATA(packet));
	ok = bson_append_oid(out,key,keylen,&oid);
	break;}
      else if (tag == mongovec_symbol)
	ok = bson_append_array_begin(out,key,keylen,&doc);
      else if ( (tag == mongo_timestamp_tag) && (len == 2) &&
		(KNO_INTEGERP(KNO_COMPOUND_REF(val,1))) &&
		(KNO_TYPEP(KNO_COMPOUND_REF(val,0),kno_timestamp_type)) ) {
	struct KNO_TIMESTAMP *ts = (kno_timestamp)KNO_COMPOUND_REF(val,0);
	ok = bson_append_timestamp(b.bson_doc,key,keylen,
				   ts->u8xtimeval.u8_tick,
				   kno_getint(KNO_COMPOUND_REF(val,1)));
	break;}
      else ok = bson_append_document_begin(out,key,keylen,&doc);
      /* Initialize the substream */
      memset(&rout,0,sizeof(struct KNO_BSON_OUTPUT));
      rout.bson_doc = &doc; rout.bson_flags = b.bson_flags;
      rout.bson_opts = b.bson_opts; rout.bson_fieldmap = b.bson_fieldmap;
      if (tag == mongovec_symbol) {
	/* Output a vector */
	lispval *scan = elts, *limit = scan+len; int i = 0;
	while (scan<limit) {
	  u8_byte buf[16]; sprintf(buf,"%d",i);
	  ok = bson_append_lisp(rout,buf,strlen(buf),*scan,-1);
	  scan++; i++;
	  if (!(ok)) break;}}
      else {
	/* Or a tagged object */
	int i = 0;
	ok = bson_append_lisp(rout,"%knotag",6,tag,-1);
	if (ok) while (i<len) {
	    char buf[16]; sprintf(buf,"%d",i);
	    ok = bson_append_lisp(rout,buf,strlen(buf),elts[i++],-1);
	    if (!(ok)) break;}}
      if (tag == mongovec_symbol)
	bson_append_array_end(out,&doc);
      else bson_append_document_end(out,&doc);
      break;}
    default: {
      struct U8_OUTPUT vout; unsigned char buf[128];
      U8_INIT_OUTPUT_BUF(&vout,128,buf);
      vout.u8_streaminfo |= U8_STREAM_VERBOSE;
      if (flags&KNO_MONGODB_COLONIZE)
	u8_printf(&vout,":%q",val);
      else kno_unparse(&vout,val);
      ok = bson_append_utf8(out,key,keylen,vout.u8_outbuf,
			    vout.u8_write-vout.u8_outbuf);
      u8_close((u8_stream)&vout);}}
    return ok;}
  else if (KNO_INTP(val))
    return bson_append_int32(out,key,keylen,((int)(KNO_FIX2INT(val))));
  else if (KNO_FIXNUMP(val))
    return bson_append_int64(out,key,keylen,KNO_FIX2INT(val));
  else if (KNO_OIDP(val)) {
    unsigned char bytes[12];
    KNO_OID addr = KNO_OID_ADDR(val); bson_oid_t oid;
    unsigned int hi = KNO_OID_HI(addr), lo = KNO_OID_LO(addr);
    bytes[0]=bytes[1]=bytes[2]=bytes[3]=0;
    bytes[4]=((hi>>24)&0xFF); bytes[5]=((hi>>16)&0xFF);
    bytes[6]=((hi>>8)&0xFF); bytes[7]=(hi&0xFF);
    bytes[8]=((lo>>24)&0xFF); bytes[9]=((lo>>16)&0xFF);
    bytes[10]=((lo>>8)&0xFF); bytes[11]=(lo&0xFF);
    bson_oid_init_from_data(&oid,bytes);
    return bson_append_oid(out,key,keylen,&oid);}
  else if (KNO_SYMBOLP(val)) {
    if ((flags)&(KNO_MONGODB_SYMSLOT)) {
      u8_string pname = KNO_SYMBOL_NAME(val), scan = pname;
      size_t pname_len = strlen(pname);
      U8_STATIC_OUTPUT(keyout,pname_len*2);
      int c = u8_sgetc(&scan); while (c>=0) {
	c = u8_tolower(c);
	u8_putc(&keyout,c);
	c = u8_sgetc(&scan);}
      return bson_append_utf8
	(out,key,keylen,keyout.u8_outbuf,u8_outlen(&keyout));}
    else if (flags&KNO_MONGODB_COLONIZE) {
      u8_string pname = KNO_SYMBOL_NAME(val);
      u8_byte _buf[512], *buf=_buf;
      size_t len = strlen(pname);
      if (len>510) buf = u8_malloc(len+2);
      buf[0]=':'; strcpy(buf+1,pname);
      if (buf!=_buf) {
	bool rval = bson_append_utf8(out,key,keylen,buf,len+1);
	u8_free(buf);
	return rval;}
      else return bson_append_utf8(out,key,keylen,buf,len+1);}
    else return bson_append_utf8(out,key,keylen,KNO_SYMBOL_NAME(val),-1);}
  else if (KNO_CHARACTERP(val)) {
    int code = KNO_CHARCODE(val);
    if (code<128) {
      char c = code;
      return bson_append_utf8(out,key,keylen,&c,1);}
    else {
      struct U8_OUTPUT vout; unsigned char buf[16];
      U8_INIT_OUTPUT_BUF(&vout,16,buf);
      u8_putc(&vout,code);
      return bson_append_utf8(out,key,keylen,vout.u8_outbuf,
			      vout.u8_write-vout.u8_outbuf);}}
  else switch (val) {
    case KNO_TRUE: case KNO_FALSE:
      return bson_append_bool(out,key,keylen,(val == KNO_TRUE));
    default: {
      struct U8_OUTPUT vout; unsigned char buf[128]; bool rv;
      U8_INIT_OUTPUT_BUF(&vout,128,buf);
      vout.u8_streaminfo |= U8_STREAM_VERBOSE;
      if (flags&KNO_MONGODB_COLONIZE)
	u8_printf(&vout,":%q",val);
      else kno_unparse(&vout,val);
      rv = bson_append_utf8(out,key,keylen,vout.u8_outbuf,
			    vout.u8_write-vout.u8_outbuf);
      u8_close((u8_stream)&vout);
      return rv;}}
}

#define lookup_op(key) (kno_sortvec_get((key),mongo_opmap,mongo_opmap_size))

static lispval lookup_mapfn(lispval fieldmap,u8_string keystring,int keylen)
{
  struct KNO_STRING _probe;
  lispval probe = kno_init_string(&_probe,keylen,keystring);
  KNO_SET_REFCOUNT(&_probe,0); /* Make it static */
  return kno_get(fieldmap,probe,KNO_VOID);
}

static bool bson_append_keyval(KNO_BSON_OUTPUT b,lispval key,lispval val)
{
  if (KNO_VOIDP(val)) return 0;
  int flags = b.bson_flags;
  lispval fieldmap = b.bson_fieldmap, mapfn = KNO_VOID;
  struct U8_OUTPUT keyout; unsigned char buf[100];
  U8_INIT_OUTPUT_BUF(&keyout,1000,buf);
  const char *keystring = NULL; int keylen; bool ok = true;
  if ( (STRINGP(key)) || (kno_testopt(fieldmap,rawslots_symbol,key)) ) {
    flags = flags | KNO_MONGODB_RAWSLOT;
    flags = flags & (~KNO_MONGODB_PREFCHOICES);
    flags = flags & (~KNO_MONGODB_CHOICESLOT);
    flags = flags & (~KNO_MONGODB_COLONIZE);
    flags = flags & (~KNO_MONGODB_SYMSLOT);}
  else {
    /* Get other flags for the value */
    if ( (get_choiceslot(key) >= 0) ||
	 (kno_testopt(fieldmap,choiceslots_symbol,key)) )
      flags = flags | KNO_MONGODB_CHOICESLOT;
    if (kno_testopt(fieldmap,symslots_symbol,key))
      flags = flags | KNO_MONGODB_SYMSLOT;}
  if (KNO_SYMBOLP(key)) {
    struct KNO_KEYVAL *opmap = lookup_op(key);
    if (KNO_RARELY(opmap!=NULL))  {
      if (KNO_STRINGP(opmap->kv_val)) {
	lispval mapped = opmap->kv_val;
	keystring = KNO_CSTRING(mapped);
	keylen = KNO_STRLEN(mapped);}}
    else if (flags&KNO_MONGODB_SLOTIFY) {
      keystring = KNO_SYMBOL_NAME(key);
      keylen = strlen(keystring);}
    else {
      u8_printf(&keyout,":%s",KNO_SYMBOL_NAME(key));
      keystring = keyout.u8_outbuf;
      keylen = keyout.u8_write-keyout.u8_outbuf;}
    if (!(KNO_VOIDP(fieldmap)))
      mapfn = kno_get(fieldmap,key,KNO_VOID);}
  else if (KNO_OIDP(key)) {
    KNO_OID addr = KNO_OID_ADDR(key);
    u8_printf(&keyout,"@%x/%x",KNO_OID_HI(addr),KNO_OID_LO(addr));
    keystring = keyout.u8_outbuf;
    keylen = keyout.u8_write-keyout.u8_outbuf;}
  else if (KNO_STRINGP(key)) {
    keystring = KNO_CSTRING(key);
    keylen = KNO_STRLEN(key);}
  else {
    keyout.u8_write = keyout.u8_outbuf;
    if (flags&KNO_MONGODB_SLOTIFY)
      u8_putc(&keyout,':');
    kno_unparse(&keyout,key);
    keystring = keyout.u8_outbuf;
    keylen = keyout.u8_write-keyout.u8_outbuf;}
  lispval store_value = val; int decref_stored = 0;
  if (!(KNO_VOIDP(mapfn))) {
    decref_stored=1;
    if (KNO_APPLICABLEP(mapfn))
      store_value = kno_apply(mapfn,1,&val);
    else if (KNO_TABLEP(mapfn))
      store_value = kno_get(mapfn,val,KNO_VOID);
    else if (KNO_TRUEP(mapfn)) {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
      out.u8_streaminfo |= U8_STREAM_VERBOSE;
      kno_unparse(&out,val);
      store_value = kno_stream2string(&out);}
    else if ( (KNO_FALSEP(mapfn)) || (KNO_EMPTYP(mapfn)) ||
	      (KNO_DEFAULTP(mapfn)) )
      decref_stored = 0;
    else {
      struct U8_OUTPUT out; U8_INIT_OUTPUT(&out,256);
      out.u8_streaminfo |= U8_STREAM_VERBOSE;
      kno_unparse(&out,val);
      store_value = kno_stream2string(&out);}
    if (KNO_ABORTED(store_value)) {
      u8_close((u8_stream)&keyout);
      return 0;}}
  /* Keys can't have periods in them, so we need to replace the
     periods on write and replace them back on read. */
  size_t  max_len = keylen+1;
  u8_byte newbuf[max_len], *write = newbuf;
  u8_string dotted = strchr(keystring,'.');
  if ( (dotted) || (keystring[0] == '+') || (keystring[0] == '/') )  {
    const u8_byte *scan = keystring, *limit = scan+keylen;
    while (scan < limit) {
      unsigned char c = *scan++;
      if (c == '.') *write++ = 0x02;
      else *write++ = c;}
    keystring = newbuf;}
  ok = bson_append_lisp(b,keystring,keylen,store_value,flags);
  if (decref_stored) kno_decref(store_value);
  u8_close((u8_stream)&keyout);
  return ok;
}

KNO_EXPORT lispval kno_bson_output(struct KNO_BSON_OUTPUT out,lispval obj)
{
  int ok = 1;
  if (KNO_VECTORP(obj)) {
    int i = 0, len = KNO_VECTOR_LENGTH(obj);
    lispval *elts = KNO_VECTOR_ELTS(obj);
    while (i<len) {
      lispval elt = elts[i]; u8_byte buf[16];
      sprintf(buf,"%d",i++);
      if (ok)
	ok = bson_append_lisp(out,buf,strlen(buf),elt,-1);
      else break;}}
  else if (KNO_CHOICEP(obj)) {
    int i = 0; KNO_DO_CHOICES(elt,obj) {
      u8_byte buf[16]; sprintf(buf,"%d",i++);
      if (ok) ok = bson_append_lisp(out,buf,strlen(buf),elt,-1);
      else {
	KNO_STOP_DO_CHOICES;
	break;}}}
  else if (KNO_SLOTMAPP(obj)) {
    struct KNO_SLOTMAP *smap = (kno_slotmap) obj;
    int i = 0, n = smap->n_slots;
    struct KNO_KEYVAL *keyvals = smap->sm_keyvals;
    while (i < n) {
      lispval key = keyvals[i].kv_key;
      lispval val = keyvals[i].kv_val;
      ok = bson_append_keyval(out,key,val);
      if (!(ok)) break;
      i++;}}
  else if (KNO_SCHEMAPP(obj)) {
    struct KNO_SCHEMAP *smap = (kno_schemap) obj;
    lispval *schema = smap->table_schema;
#if KNO_MAJOR_VERSION >= 2004
    lispval *values = smap->table_values;
#else
    lispval *values = smap->schema_values;
#endif
    int i = 0, n = smap->schema_length;
    while (i < n) {
      lispval key = schema[i];
      lispval val = values[i];
      ok = bson_append_keyval(out,key,val);
      if (!(ok)) break;
      i++;}}
  else if (KNO_TABLEP(obj)) {
    lispval keys = kno_getkeys(obj);
    {KNO_DO_CHOICES(key,keys) {
	lispval val = kno_get(obj,key,KNO_VOID);
	if (ok) ok = bson_append_keyval(out,key,val);
	kno_decref(val);
	if (!(ok)) break;}}
    kno_decref(keys);}
  else if (KNO_COMPOUNDP(obj)) {
    struct KNO_COMPOUND *compound = KNO_XCOMPOUND(obj);
    if (compound == NULL) return KNO_ERROR_VALUE;
    lispval tag = compound->typetag, *elts = KNO_COMPOUND_ELTS(obj);
    int len = KNO_COMPOUND_LENGTH(obj);
    if (tag == mongovec_symbol) {
      lispval *scan = elts, *limit = scan+len; int i = 0;
      while (scan<limit) {
	u8_byte buf[16]; sprintf(buf,"%d",i);
	ok = bson_append_lisp(out,buf,strlen(buf),*scan,-1);
	i++; scan++;
	if (!(ok)) break;}}
    else {
      int i = 0;
      ok = bson_append_lisp(out,"%knotag",6,tag,0);
      if (ok) while (i<len) {
	  char buf[16]; sprintf(buf,"%d",i);
	  ok = bson_append_lisp(out,buf,strlen(buf),elts[i++],-1);
	  if (!(ok)) break;}}}
  if (!(ok)) {
    kno_seterr("BSONError","kno_bson_output",NULL,obj);
    return KNO_ERROR_VALUE;}
  else return KNO_VOID;
}

KNO_EXPORT bson_t *kno_lisp2bson(lispval obj,int flags,lispval opts)
{
  if (KNO_VOIDP(obj)) return NULL;
  else if ( (KNO_STRINGP(obj)) && (strchr(KNO_CSTRING(obj),'{')) ) {
    u8_string json = KNO_CSTRING(obj); int free_it = 0;
    bson_error_t error; bson_t *result;
    if (strchr(json,'"')<0) {
      json = u8_string_subst(json,"'","\""); free_it = 1;}
    result = bson_new_from_json(json,KNO_STRLEN(obj),&error);
    if (free_it) u8_free(json);
    if (result) return result;
    kno_seterr("Bad JSON","kno_lisp2bson/json",error.message,kno_incref(obj));
    return NULL;}
  else {
    struct KNO_BSON_OUTPUT out;
    out.bson_doc = bson_new();
    out.bson_flags = ((flags<0)?(getflags(opts,KNO_MONGODB_DEFAULTS)):(flags));
    out.bson_opts = opts;
    out.bson_fieldmap = kno_getopt(opts,fieldmap_symbol,KNO_VOID);
    lispval result = kno_bson_output(out,obj);
    kno_decref(out.bson_fieldmap);
    if (KNO_ABORTP(result)) {
      bson_destroy(out.bson_doc);
      return NULL;}
    else return out.bson_doc;}
}

/* BSON input functions */

#define slotify_char(c)                                                 \
  ((c=='_')||(c=='-')||(c=='%')||(c=='.')||(c=='/')||(c=='$')||(u8_isalnum(c)))

/* -1 means don't slotify at all,
   0 means use getsym (downcases),
   1 means intern */
static int slotifyp(u8_string s)
{
  const u8_byte *scan = s; int c, i = 0, hasupper = 0;
  while ((c = u8_sgetc(&scan))>=0) {
    if (i>32)
      /* Don't slotify long strings */
      return -1;
    else if (!(slotify_char(c)))
      /* Don't slotify strings with weird characters */
      return -1;
    else i++;
    /* Track whether the string will need to be dowcnased */
    if (u8_isupper(c)) hasupper = 1;}
  return hasupper;
}

static lispval bson_read_vector(KNO_BSON_INPUT b,int flags);
static lispval bson_read_choice(KNO_BSON_INPUT b,int flags);
static lispval bson_read_generic(KNO_BSON_INPUT b,int flags);

static int oidstringp(u8_string string)
{
  u8_string scan = string; int len = 0;
  if (*scan!='@') return 0; else scan++;
  if (!(isxdigit(*scan))) return 0;
  while ((isxdigit(*scan))) {scan++; len++;}
  if (*scan=='\0') return (len<=16);
  else if (*scan=='/') {
    if (len>16) return 0; else len=0;

    if (!(isxdigit(*scan))) return 0;
    while ((isxdigit(*scan))) {
      scan++; len++;}
    if ( (*scan=='\0') && (len<=8) ) return 1;
    else return 0;}
  else return 0;
}

static void bson_read_step(KNO_BSON_INPUT b,int flags,
			   lispval into,lispval *loc)
{
  int symslot = 0, choiceslot = 0, choicevals = 0;
  bson_iter_t *in = b.bson_iter;
  const unsigned char *field = bson_iter_key(in);
  const size_t field_len = strlen(field);
  unsigned char tmpbuf[field_len+1];
  if (flags < 0) flags = b.bson_flags;
  if (strchr(field,0x02)) {
    const unsigned char *read = field, *limit = read+field_len;
    unsigned char *write = tmpbuf;
    while ( read < limit ) {
      unsigned char c = *read++;
      if (c == 0x02) *write++='.'; else *write++=c;}
    *write++='\0';
    field = tmpbuf;}
  bson_type_t bt = bson_iter_type(in);
  lispval slotid, value;
  if ( (flags&KNO_MONGODB_SLOTIFY) &&
       (flags&KNO_MONGODB_COLONIZE) &&
       (field[0]==':') &&
       (field[1]) )
    slotid = kno_parse_arg((u8_string)field);
  else if ( (flags&KNO_MONGODB_SLOTIFY) && (field[0]=='@') &&
	    (oidstringp(field)) )
    slotid = kno_parse_arg((u8_string)field);
  else if ( (flags&KNO_MONGODB_SLOTIFY) && (field[0]==':') &&
	    (field[1]=='@') && (oidstringp(field+1)) )
    slotid = kno_parse_arg((u8_string)field);
  else if (flags&KNO_MONGODB_SLOTIFY) {
    int slotify = slotifyp((u8_string)field);
    if (slotify<0)
      slotid = kno_make_string(NULL,-1,(unsigned char *)field);
    else if (slotify==0) {
      slotid = kno_getsym((unsigned char *)field);
      symslot = 1;}
    else slotid = kno_intern((unsigned char *)field);}
  else slotid = kno_make_string(NULL,-1,(unsigned char *)field);
  if (KNO_ABORTED(slotid)) {
    kno_clear_errors(1);
    slotid=kno_make_string(NULL,-1,(unsigned char *)field);}
  lispval fieldmap = b.bson_fieldmap;
  if ( (KNO_STRINGP(slotid)) ||
       (kno_testopt(fieldmap,rawslots_symbol,slotid)) ) {
    flags = flags | KNO_MONGODB_RAWSLOT;
    flags = flags & (~KNO_MONGODB_PREFCHOICES);
    flags = flags & (~KNO_MONGODB_COLONIZE);
    flags = flags & (~KNO_MONGODB_CHOICESLOT);
    flags = flags & (~KNO_MONGODB_SYMSLOT);}
  else if ( (KNO_OIDP(slotid)) || (KNO_SYMBOLP(slotid)) ) {
    if (kno_testopt(fieldmap,symslots_symbol,slotid))
      flags = flags | KNO_MONGODB_SYMSLOT;
    if (kno_testopt(fieldmap,choiceslots_symbol,slotid))
      choiceslot=1;}
  else NO_ELSE;
  switch (bt) {
  case BSON_TYPE_DOUBLE:
    value = kno_make_double(bson_iter_double(in)); break;
  case BSON_TYPE_BOOL:
    value = (bson_iter_bool(in))?(KNO_TRUE):(KNO_FALSE); break;
  case BSON_TYPE_REGEX: {
    const char *props, *src = bson_iter_regex(in,&props);
    int flags = 0;
    if (strchr(props,'x')>=0) flags |= REG_EXTENDED;
    if (strchr(props,'i')>=0) flags |= REG_ICASE;
    if (strchr(props,'m')>=0) flags |= REG_NEWLINE;
    value = kno_make_regex((u8_string)src,flags);
    break;}
  case BSON_TYPE_UTF8: {
    int len = -1;
    const unsigned char *bytes = bson_iter_utf8(in,&len);
    if ( (flags&KNO_MONGODB_COLONIZE) && (bytes[0] == ':') )
      value = kno_parse_arg((u8_string)(bytes));
    else if ( (flags&KNO_MONGODB_COLONIZE) && (bytes[0]=='\\'))
      value = kno_make_string(NULL,((len>0)?(len-1):(-1)),
			      (unsigned char *)bytes+1);
    else if (flags&KNO_MONGODB_SYMSLOT) {
      if ( (bytes[0] == ':') || (bytes[0] == '@') ||
	   (bytes[0] == '#') )
	value = kno_parse_arg((u8_string)(bytes));
      else value = kno_make_string(NULL,((len>0)?(len):(-1)),
				   (unsigned char *)bytes);}
    else value = kno_make_string(NULL,((len>0)?(len):(-1)),
				 (unsigned char *)bytes);
    if (KNO_ABORTED(value)) {
      u8_exception ex = u8_current_exception;
      u8_log(LOGWARN,"MongoDBParseError","%s<%s> (%s): %s",
	     ex->u8x_cond,ex->u8x_context,
	     ex->u8x_details,bytes);
      kno_clear_errors(1);
      value = kno_make_string(NULL,((len>0)?(len):(-1)),
			      (unsigned char *)bytes);}
    break;}
  case BSON_TYPE_BINARY: {
    int len; bson_subtype_t st; const unsigned char *data;
    bson_iter_binary(in,&st,&len,&data);
    if (st == BSON_SUBTYPE_UUID) {
      struct KNO_UUID *uuid = u8_alloc(struct KNO_UUID);
      KNO_INIT_CONS(uuid,kno_uuid_type);
      memcpy(uuid->uuid16,data,len);
      value = (lispval) uuid;}
    else {
      lispval packet = kno_make_packet(NULL,len,(unsigned char *)data);
      if (st == BSON_SUBTYPE_BINARY)
	value = packet;
      else if (st == BSON_SUBTYPE_USER)
	value = kno_init_compound(NULL,mongouser,0,1,packet);
      else if (st == BSON_SUBTYPE_MD5)
	value = kno_init_compound(NULL,mongomd5,0,1,packet);
      else if (st == BSON_SUBTYPE_FUNCTION)
	value = kno_init_compound(NULL,mongofun,0,1,packet);
      else value = packet;}
    break;}
  case BSON_TYPE_INT32:
    value = KNO_INT(bson_iter_int32(in));
    break;
  case BSON_TYPE_INT64:
    value = KNO_INT(bson_iter_int64(in));
    break;
  case BSON_TYPE_OID: {
    const bson_oid_t *oidval = bson_iter_oid(in);
    const unsigned char *bytes = oidval->bytes;
    if ((bytes[0]==0)&&(bytes[1]==0)&&(bytes[2]==0)&&(bytes[3]==0)) {
      KNO_OID dtoid;
      unsigned int hi=
	(((((bytes[4]<<8)|(bytes[5]))<<8)|(bytes[6]))<<8)|(bytes[7]);
      unsigned int lo=
	(((((bytes[8]<<8)|(bytes[9]))<<8)|(bytes[10]))<<8)|(bytes[11]);
      memset(&dtoid,0,sizeof(dtoid));
      KNO_SET_OID_HI(dtoid,hi); KNO_SET_OID_LO(dtoid,lo);
      value = kno_make_oid(dtoid);}
    else {
      lispval packet = kno_make_packet(NULL,12,(unsigned char *)bytes);
      value = kno_init_compound(NULL,oidtag,0,1,packet);}
    break;}
  case BSON_TYPE_UNDEFINED:
    value = KNO_VOID; break;
  case BSON_TYPE_NULL:
    value = KNO_EMPTY_CHOICE; break;
  case BSON_TYPE_DATE_TIME: {
    unsigned long long millis = bson_iter_date_time(in);
    struct KNO_TIMESTAMP *ts = u8_alloc(struct KNO_TIMESTAMP);
    KNO_INIT_CONS(ts,kno_timestamp_type);
    u8_init_xtime(&(ts->u8xtimeval),millis/1000,u8_millisecond,
		  ((millis%1000)*1000000),0,0);
    value = (lispval)ts;
    break;}
  case BSON_TYPE_TIMESTAMP: {
    uint32_t ts, inc; bson_iter_timestamp(in,&ts,&inc);
    time_t base_time = ts;
    lispval tm = kno_time2timestamp(base_time);
    lispval offset = KNO_INT(inc);
    value = kno_init_compound(NULL,mongo_timestamp_tag,0,2,tm,offset);
    break;}
  case BSON_TYPE_MAXKEY:
    value = maxkey; break;
  case BSON_TYPE_MINKEY:
    value = minkey; break;
  default:
    if (BSON_ITER_HOLDS_DOCUMENT(in)) {
      struct KNO_BSON_INPUT r; bson_iter_t child;
      bson_iter_recurse(in,&child);
      r.bson_iter = &child; r.bson_flags = b.bson_flags;
      r.bson_opts = b.bson_opts; r.bson_fieldmap = b.bson_fieldmap;
      value = kno_init_slotmap(NULL,0,NULL);
      while (bson_iter_next(&child))
	bson_read_step(r,flags,value,NULL);
      if (kno_test(value,dotcar_symbol,KNO_VOID)) {
	lispval car = kno_get(value,dotcar_symbol,KNO_VOID);
	lispval cdr = kno_get(value,dotcdr_symbol,KNO_VOID);
	kno_decref(value);
	value = kno_init_pair(NULL,car,cdr);}
      else if (kno_test(value,knotag_symbol,KNO_VOID)) {
	lispval tag = kno_get(value,knotag_symbol,KNO_VOID), compound;
	struct KNO_TYPEINFO *entry = kno_use_typeinfo(tag);
	lispval fields[16] = { KNO_VOID }, keys = kno_getkeys(value);
	int max = -1, i = 0, n, ok = 1;
	{KNO_DO_CHOICES(key,keys) {
	    if (KNO_FIXNUMP(key)) {
	      long long index = KNO_FIX2INT(key);
	      if ((index<0) || (index>=16)) {
		i = 0; while (i<16) {
		  lispval value = fields[i++];
		  kno_decref(value);}
		u8_logf(LOG_ERR,kno_BSON_Compound_Overflow,
			"Compound of type %q: %q",tag,value);
		KNO_STOP_DO_CHOICES;
		ok = 0;
		break;}
	      if (index>max) max = index;
	      fields[index]=kno_get(value,key,KNO_VOID);}}}
	if (ok) {
	  n = max+1;
	  if ((entry)&&(entry->type_consfn))
	    compound = entry->type_consfn(n,fields,entry);
	  else {
	    struct KNO_COMPOUND *c=
	      u8_malloc(sizeof(struct KNO_COMPOUND)+(n*LISPVAL_LEN));
	    lispval *cdata = &(c->compound_0);
	    kno_init_compound(c,tag,0,0);
	    c->compound_length = n;
	    memcpy(cdata,fields,n);
	    compound = LISP_CONS(c);}
	  kno_decref(value);
	  value = compound;}
	kno_decref(keys);
	kno_decref(tag);}
      else {}}
    else if (BSON_ITER_HOLDS_ARRAY(in)) {
      if (choiceslot)
	value = bson_read_choice(b,flags);
      else if (!(symslot))
	value = bson_read_vector(b,flags);
      else if (flags&KNO_MONGODB_PREFCHOICES)
	value = bson_read_choice(b,flags);
      else value = bson_read_generic(b,flags);}
    else {
      u8_logf(LOG_ERR,kno_BSON_Input_Error,
	      "Can't handle BSON type %d",bt);
      return;}}
  if (!(KNO_VOIDP(b.bson_fieldmap))) {
    struct KNO_STRING _tempkey;
    lispval tempkey = kno_init_string(&_tempkey,strlen(field),field);
    lispval mapfn = KNO_VOID, new_value = KNO_VOID;
    lispval fieldmap = b.bson_fieldmap;
    KNO_INIT_STACK_CONS(tempkey,kno_string_type);
    mapfn = kno_get(fieldmap,tempkey,KNO_VOID);
    if (KNO_VOIDP(mapfn)) {}
    else if (KNO_APPLICABLEP(mapfn))
      new_value = kno_apply(mapfn,1,&value);
    else if (KNO_TABLEP(mapfn))
      new_value = kno_get(mapfn,value,KNO_VOID);
    else if ((KNO_TRUEP(mapfn))&&(KNO_STRINGP(value)))
      new_value = kno_parse(KNO_CSTRING(value));
    else NO_ELSE;
    if (KNO_ABORTP(new_value)) {
      kno_clear_errors(1);
      new_value = KNO_VOID;}
    else if (new_value!=KNO_VOID) {
      lispval old_value = value;
      value = new_value;
      kno_decref(old_value);}
    else {}}
  /* For weird bugs */
  /* if (!(KNO_CHECK_ANY_PTR(value))) kno_raise("BadPtr","bson_read_step",NULL,KNO_VOID); */
  if (!(KNO_VOIDP(into))) kno_store(into,slotid,value);
  if (loc) *loc = value;
  else kno_decref(value);
}

static lispval bson_read_vector(KNO_BSON_INPUT b,int flags)
{
  struct KNO_BSON_INPUT r; bson_iter_t child;
  lispval result, *data = u8_alloc_n(16,lispval);
  lispval *write = data, *lim = data+16;
  if (flags < 0) flags = b.bson_flags;
  bson_iter_recurse(b.bson_iter,&child);
  r.bson_iter = &child; r.bson_flags = flags;
  r.bson_opts = b.bson_opts; r.bson_fieldmap = b.bson_fieldmap;
  while (bson_iter_next(&child)) {
    if (write>=lim) {
      int len = lim-data;
      int newlen = ((len<16384)?(len*2):(len+16384));
      lispval *newdata = u8_realloc_n(data,newlen,lispval);
      if (!(newdata)) u8_raise(kno_MallocFailed,"mongodb_cursor_read_vector",NULL);
      write = newdata+(write-data); lim = newdata+newlen; data = newdata;}
    bson_read_step(r,flags,KNO_VOID,write);
    write++;}
  result = kno_make_vector(write-data,data);
  u8_free(data);
  return result;
}

static lispval bson_read_choice(KNO_BSON_INPUT b,int flags)
{
  struct KNO_BSON_INPUT r; bson_iter_t child;
  lispval *data = u8_alloc_n(16,lispval), *write = data, *lim = data+16;
  if (flags < 0) flags = b.bson_flags;
  bson_iter_recurse(b.bson_iter,&child);
  r.bson_iter = &child; r.bson_flags = flags;
  r.bson_opts = b.bson_opts; r.bson_fieldmap = b.bson_fieldmap;
  while (bson_iter_next(&child)) {
    if (write>=lim) {
      int len = lim-data;
      int newlen = ((len<16384)?(len*2):(len+16384));
      lispval *newdata = u8_realloc_n(data,newlen,lispval);
      if (!(newdata)) u8_raise(kno_MallocFailed,"mongodb_cursor_read_vector",NULL);
      write = newdata+(write-data); lim = newdata+newlen; data = newdata;}
    if (BSON_ITER_HOLDS_ARRAY(&child)) {
      *write++=bson_read_vector(r,flags);}
    else {
      bson_read_step(r,flags,KNO_VOID,write);
      write++;}}
  if (write == data) {
    u8_free(data);
    return KNO_EMPTY_CHOICE;}
  else return kno_make_choice
	 (write-data,data,
	  KNO_CHOICE_DOSORT|KNO_CHOICE_COMPRESS|
	  KNO_CHOICE_FREEDATA|KNO_CHOICE_REALLOC);
}

static lispval bson_read_generic(KNO_BSON_INPUT b,int flags)
{
  struct KNO_BSON_INPUT r; bson_iter_t child; int ischoice=0;
  lispval *data = u8_alloc_n(16,lispval), *write = data, *lim = data+16;
  if (flags < 0) flags = b.bson_flags;
  bson_iter_recurse(b.bson_iter,&child);
  r.bson_iter = &child; r.bson_flags = flags;
  r.bson_opts = b.bson_opts; r.bson_fieldmap = b.bson_fieldmap;
  while (bson_iter_next(&child)) {
    if (write>=lim) {
      int len = lim-data;
      int newlen = ((len<16384)?(len*2):(len+16384));
      lispval *newdata = u8_realloc_n(data,newlen,lispval);
      if (!(newdata)) u8_raise(kno_MallocFailed,"mongodb_cursor_read_vector",NULL);
      write = newdata+(write-data); lim = newdata+newlen; data = newdata;}
    if (BSON_ITER_HOLDS_ARRAY(&child)) {
      *write++=bson_read_vector(r,flags);}
    else {
      bson_read_step(r,flags,KNO_VOID,write);
      if ( (write == data) && (*write==choice_tagsym) )
	ischoice=1;
      else if ( (write == data) && (KNO_STRINGP(*write)) &&
		(strcmp(KNO_STRDATA(*write),CHOICE_TAGSTRING_TEXT)==0) ) {
	kno_decref(*write);
	ischoice=1;}
      else write++;}}
  if (ischoice) {
    if (write == data) {
      u8_free(data);
      return KNO_EMPTY_CHOICE;}
    else return kno_make_choice
	   (write-data,data,
	    KNO_CHOICE_DOSORT|KNO_CHOICE_COMPRESS|
	    KNO_CHOICE_FREEDATA|KNO_CHOICE_REALLOC);}
  else {
    lispval result = kno_make_vector(write-data,data);
    u8_free(data);
    return result;}
}

KNO_EXPORT lispval kno_bson2lisp(bson_t *in,int flags,lispval opts)
{
  bson_iter_t iter;
  if (flags<0) flags = getflags(opts,KNO_MONGODB_DEFAULTS);
  memset(&iter,0,sizeof(bson_iter_t));
  if (bson_iter_init(&iter,in)) {
    lispval result, fieldmap = kno_getopt(opts,fieldmap_symbol,KNO_VOID);
    struct KNO_BSON_INPUT b;
    memset(&b,0,sizeof(struct KNO_BSON_INPUT));
    b.bson_iter = &iter; b.bson_flags = flags;
    b.bson_opts = opts; b.bson_fieldmap = fieldmap;
    result = kno_init_slotmap(NULL,0,NULL);
    while (bson_iter_next(&iter)) bson_read_step(b,flags,result,NULL);
    kno_decref(fieldmap);
    return result;}
  else return kno_err(kno_BSON_Input_Error,"kno_bson2lisp",NULL,KNO_VOID);
}


DEFC_PRIMN("mongovec",mongovec_lexpr,
	   KNO_VAR_ARGS|KNO_MIN_ARGS(0)|KNO_NDCALL,
	   "**undocumented**")
static lispval mongovec_lexpr(int n,kno_argvec values)
{
  return kno_init_compound_from_elts
    (NULL,mongovec_symbol,
     KNO_COMPOUND_INCREF|KNO_COMPOUND_SEQUENCE,
     n,(lispval *)values);
}

DEFC_PRIM("mongovec?",mongovecp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongovecp(lispval arg)
{
  if (KNO_COMPOUND_TYPEP(arg,mongovec_symbol))
    return KNO_TRUE;
  else return KNO_FALSE;
}

/* The MongoDB OPMAP */

/* The OPMAP translates symbols that correspond to MongoDB
   operations. Since the default slot transformation lowercases the
   symbol name, this table only needs to include the ones that have
   embedded uppercase letters.  */

static void add_to_mongo_opmap(u8_string keystring)
{
  lispval key = kno_getsym(keystring);
  struct KNO_KEYVAL *entry=
    kno_sortvec_insert(key,&mongo_opmap,
		       &mongo_opmap_size,
		       &mongo_opmap_space,
		       MONGO_OPMAP_MAX,
		       1);
  if (entry)
    entry->kv_val = kno_mkstring(keystring);
  else u8_logf(LOG_ERR,"Couldn't add %s to the mongo opmap",keystring);
}

static void init_mongo_opmap()
{
  mongo_opmap = u8_alloc_n(32,struct KNO_KEYVAL);
  mongo_opmap_space = 32;
  mongo_opmap_size = 0;
  add_to_mongo_opmap("$elemMatch");
  add_to_mongo_opmap("$ifNull");
  add_to_mongo_opmap("$setOnInsert");
  add_to_mongo_opmap("$currentDate");
  add_to_mongo_opmap("$indexStats");
  add_to_mongo_opmap("$addToSet");
  add_to_mongo_opmap("$setEquals");
  add_to_mongo_opmap("$setIntersection");
  add_to_mongo_opmap("$setUnion");
  add_to_mongo_opmap("$setDifference");
  add_to_mongo_opmap("$setIsSubset");
  add_to_mongo_opmap("$anyElementTrue");
  add_to_mongo_opmap("$allElementsTrue");
  add_to_mongo_opmap("$stdDevPop");
  add_to_mongo_opmap("$stdDevSamp");
  add_to_mongo_opmap("$toLower");
  add_to_mongo_opmap("$toUpper");
  add_to_mongo_opmap("$arrayElemAt");
  add_to_mongo_opmap("$concatArrays");
  add_to_mongo_opmap("$isArray");
  add_to_mongo_opmap("$dayOfYear");
  add_to_mongo_opmap("$dayOfMonth");
  add_to_mongo_opmap("$dayOfWeek");
  add_to_mongo_opmap("$pullAll");
  add_to_mongo_opmap("$pushAll");
  add_to_mongo_opmap("$comment");
  add_to_mongo_opmap("$geoNear");
  add_to_mongo_opmap("$geoWithin");
  add_to_mongo_opmap("$geoInserts");
  add_to_mongo_opmap("$nearSphere");
  add_to_mongo_opmap("$bitsAllSet");
  add_to_mongo_opmap("$bitsAllClear");
  add_to_mongo_opmap("$bitsAnySet");
  add_to_mongo_opmap("$bitsAnyClear");

  add_to_mongo_opmap("$or");
  add_to_mongo_opmap("$and");
  add_to_mongo_opmap("$not");
  add_to_mongo_opmap("$nor");

  add_to_mongo_opmap("$gt");
  add_to_mongo_opmap("$gte");
  add_to_mongo_opmap("$lt");
  add_to_mongo_opmap("$lte");

  add_to_mongo_opmap("$eq");
  add_to_mongo_opmap("$ne");
  add_to_mongo_opmap("$nin");
  add_to_mongo_opmap("$in");

  add_to_mongo_opmap("$exists");

  add_to_mongo_opmap("$maxScan");
  add_to_mongo_opmap("$maxTimeMS");
  add_to_mongo_opmap("$returnKey");
  add_to_mongo_opmap("$showDiskLoc");

}

/* Getting 'meta' information from connections */

static struct KNO_MONGODB_DATABASE *getdb(lispval arg,u8_context cxt)
{
  if (KNO_TYPEP(arg,kno_mongoc_server)) {
    return kno_consptr(struct KNO_MONGODB_DATABASE *,arg,kno_mongoc_server);}
  else if (KNO_TYPEP(arg,kno_mongoc_collection)) {
    struct KNO_MONGODB_COLLECTION *collection=
      kno_consptr(struct KNO_MONGODB_COLLECTION *,arg,kno_mongoc_collection);
    return (struct KNO_MONGODB_DATABASE *)collection->collection_db;}
  else if (KNO_TYPEP(arg,kno_mongoc_cursor)) {
    struct KNO_MONGODB_CURSOR *cursor=
      kno_consptr(struct KNO_MONGODB_CURSOR *,arg,kno_mongoc_cursor);
    struct KNO_MONGODB_COLLECTION *collection = CURSOR2COLL(cursor);
    return COLL2DB(collection);}
  else {
    kno_seterr(kno_TypeError,cxt,"MongoDB object",arg);
    return NULL;}
}


DEFC_PRIM("mongodb/dbname",mongodb_dbname,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_dbname(lispval arg)
{
  struct KNO_MONGODB_DATABASE *db = getdb(arg,"mongodb_dbname");
  if (db == NULL) return KNO_ERROR_VALUE;
  else return kno_mkstring(db->dbname);
}


DEFC_PRIM("mongodb/dbspec",mongodb_spec,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_spec(lispval arg)
{
  struct KNO_MONGODB_DATABASE *db = getdb(arg,"mongodb_spec");
  if (db == NULL) return KNO_ERROR_VALUE;
  else return kno_mkstring(db->dbspec);
}


DEFC_PRIM("mongodb/dburi",mongodb_uri,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_uri(lispval arg)
{
  struct KNO_MONGODB_DATABASE *db = getdb(arg,"mongodb_uri");
  if (db == NULL) return KNO_ERROR_VALUE;
  else return kno_mkstring(db->dburi);
}


DEFC_PRIM("mongodb/getopts",mongodb_getopts,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_getopts(lispval arg)
{
  lispval opts = KNO_VOID;
  if (KNO_TYPEP(arg,kno_mongoc_server)) {
    opts = (kno_consptr(struct KNO_MONGODB_DATABASE *,arg,kno_mongoc_server))
      ->dbopts;}
  else if (KNO_TYPEP(arg,kno_mongoc_collection)) {
    struct KNO_MONGODB_COLLECTION *collection=
      kno_consptr(struct KNO_MONGODB_COLLECTION *,arg,kno_mongoc_collection);
    opts = collection->collection_opts;}
  else if (KNO_TYPEP(arg,kno_mongoc_cursor)) {
    struct KNO_MONGODB_CURSOR *cursor=
      kno_consptr(struct KNO_MONGODB_CURSOR *,arg,kno_mongoc_cursor);
    opts = cursor->cursor_opts;}
  else {
    kno_seterr(kno_TypeError,"mongodb_opts","MongoDB object",arg);
    return KNO_ERROR_VALUE;}
  kno_incref(opts);
  return opts;
}


DEFC_PRIM("mongodb/getdb",mongodb_getdb,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_getdb(lispval arg)
{
  struct KNO_MONGODB_COLLECTION *collection = NULL;
  if (KNO_TYPEP(arg,kno_mongoc_server))
    return kno_incref(arg);
  else if (KNO_TYPEP(arg,kno_mongoc_collection))
    collection = kno_consptr(struct KNO_MONGODB_COLLECTION *,arg,kno_mongoc_collection);
  else if (KNO_TYPEP(arg,kno_mongoc_cursor)) {
    struct KNO_MONGODB_CURSOR *cursor=
      kno_consptr(struct KNO_MONGODB_CURSOR *,arg,kno_mongoc_cursor);
    collection = (struct KNO_MONGODB_COLLECTION *)cursor->cursor_coll;}
  else return kno_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
  if (collection)
    return kno_incref(collection->collection_db);
  else return KNO_FALSE;
}


DEFC_PRIM("collection/name",mongodb_collection_name,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_collection_name(lispval arg)
{
  struct KNO_MONGODB_COLLECTION *collection = NULL;
  if (KNO_TYPEP(arg,kno_mongoc_collection))
    collection = kno_consptr(struct KNO_MONGODB_COLLECTION *,arg,kno_mongoc_collection);
  else if (KNO_TYPEP(arg,kno_mongoc_cursor)) {
    struct KNO_MONGODB_CURSOR *cursor=
      kno_consptr(struct KNO_MONGODB_CURSOR *,arg,kno_mongoc_cursor);
    collection = (struct KNO_MONGODB_COLLECTION *)cursor->cursor_coll;}
  else return kno_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
  if (collection)
    return kno_make_string(NULL,-1,collection->collection_name);
  else return KNO_FALSE;
}

static int mongodb_getflags(lispval arg)
{
  if (KNO_TYPEP(arg,kno_mongoc_server)) {
    struct KNO_MONGODB_DATABASE *server = (struct KNO_MONGODB_DATABASE *)arg;
    return server->dbflags;}
  else if (KNO_TYPEP(arg,kno_mongoc_collection)) {
    struct KNO_MONGODB_COLLECTION *collection=
      kno_consptr(struct KNO_MONGODB_COLLECTION *,arg,kno_mongoc_collection);
    return collection->collection_flags;}
  else if (KNO_TYPEP(arg,kno_mongoc_cursor)) {
    struct KNO_MONGODB_CURSOR *cursor=
      kno_consptr(struct KNO_MONGODB_CURSOR *,arg,kno_mongoc_cursor);
    return cursor->cursor_flags;}
  else return -1;
}


DEFC_PRIM("mongodb/getcollection",mongodb_getcollection,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_getcollection(lispval arg)
{
  if (KNO_TYPEP(arg,kno_mongoc_collection))
    return kno_incref(arg);
  else if (KNO_TYPEP(arg,kno_mongoc_cursor)) {
    struct KNO_MONGODB_CURSOR *cursor=
      kno_consptr(struct KNO_MONGODB_CURSOR *,arg,kno_mongoc_cursor);
    return kno_incref(cursor->cursor_coll);}
  else return kno_type_error("MongoDB collection/cursor","mongodb_dbname",arg);
}


DEFC_PRIM("mongodb?",mongodbp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodbp(lispval arg)
{
  if (KNO_TYPEP(arg,kno_mongoc_server))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFC_PRIM("mongodb/collection?",mongodb_collectionp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_collectionp(lispval arg)
{
  if (KNO_TYPEP(arg,kno_mongoc_collection))
    return KNO_TRUE;
  else return KNO_FALSE;
}


DEFC_PRIM("mongodb/cursor?",mongodb_cursorp,
	  KNO_MAX_ARGS(1)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"arg",kno_any_type,KNO_VOID})
static lispval mongodb_cursorp(lispval arg)
{
  if (KNO_TYPEP(arg,kno_mongoc_cursor))
    return KNO_TRUE;
  else return KNO_FALSE;
}

static void add_string(lispval result,lispval field,u8_string value)
{
  if (value == NULL) return;
  else {
    lispval stringval = kno_mkstring(value);
    kno_add(result,field,stringval);
    kno_decref(stringval);
    return;}
}


DEFC_PRIM("mongodb/dbinfo",mongodb_getinfo,
	  KNO_MAX_ARGS(2)|KNO_MIN_ARGS(1),
	  "**undocumented**",
	  {"mongodb",kno_any_type,KNO_VOID},
	  {"field",kno_any_type,KNO_VOID})
static lispval mongodb_getinfo(lispval mongodb,lispval field)
{
  lispval result = kno_make_slotmap(10,0,NULL);
  struct KNO_MONGODB_DATABASE *db=
    kno_consptr(struct KNO_MONGODB_DATABASE *,mongodb,kno_mongoc_server);
  mongoc_uri_t *info = db->dburi_info;
  u8_string tmpstring;
  if ((tmpstring = mongoc_uri_get_database(info)))
    add_string(result,dbname_symbol,tmpstring);
  if ((tmpstring = mongoc_uri_get_username(info)))
    add_string(result,username_symbol,tmpstring);
  if ((tmpstring = mongoc_uri_get_auth_mechanism(info)))
    add_string(result,auth_symbol,tmpstring);
  if ((tmpstring = mongoc_uri_get_auth_source(info)))
    add_string(result,auth_symbol,tmpstring);
  {
    const mongoc_host_list_t *scan = mongoc_uri_get_hosts(info);
    while (scan) {
      add_string(result,hosts_symbol,scan->host);
      add_string(result,connections_symbol,scan->host_and_port);
      scan = scan->next;}
  }
  if (mongoc_uri_get_tls(info))
    kno_store(result,sslsym,KNO_TRUE);

  if ((KNO_VOIDP(field))||(KNO_FALSEP(field)))
    return result;
  else {
    lispval v = kno_get(result,field,KNO_EMPTY_CHOICE);
    kno_decref(result);
    return v;}
}

/* MongoDB logging */

static int getu8loglevel(mongoc_log_level_t l);
static int mongodb_ignore_loglevel = -1;

void mongoc_logger(mongoc_log_level_t l,const char *d,const char *m,void *u)
{
  int u8l = getu8loglevel(l);
  if (u8l <= LOG_CRIT)
    u8_logger(u8l,d,m);
  else if ((mongodb_loglevel >= 0)&&
	   (u8l <= mongodb_loglevel))
    u8_logger(-u8l,d,m);
  else if ((mongodb_ignore_loglevel >= 0)&&
	   (u8l > mongodb_ignore_loglevel))
    return;
  else u8_logger(u8l,d,m);
}

static int getu8loglevel(mongoc_log_level_t l)
{
  switch (l) {
  case MONGOC_LOG_LEVEL_ERROR:
    return LOG_ERR;
  case MONGOC_LOG_LEVEL_CRITICAL:
    return LOG_CRIT;
  case MONGOC_LOG_LEVEL_WARNING:
    return LOG_WARN;
  case MONGOC_LOG_LEVEL_MESSAGE:
    return LOG_NOTICE;
  case MONGOC_LOG_LEVEL_INFO:
    return LOG_INFO;
  case MONGOC_LOG_LEVEL_DEBUG:
    return LOG_DEBUG;
  case MONGOC_LOG_LEVEL_TRACE:
    return LOG_DETAIL;
  default:
    return LOG_WARN;}
}

/* Initialization */

static u8_byte mongoc_version_string[100];
static u8_byte *mongoc_version = mongoc_version_string;

KNO_EXPORT int kno_init_mongodb(void) KNO_LIBINIT_FN;
static long long int mongodb_initialized = 0;

#define DEFAULT_FLAGS (KNO_SHORT2LISP(KNO_MONGODB_DEFAULTS))

static lispval mongodb_module;

KNO_EXPORT int kno_init_mongodb()
{
  if (mongodb_initialized) return 0;
  mongodb_initialized = u8_millitime();

  init_mongo_opmap();

  mongodb_module = kno_new_cmodule("mongodb",0,kno_init_mongodb);

  idsym = kno_intern("_id");
  maxkey = kno_intern("mongomax");
  minkey = kno_intern("mongomin");
  oidtag = kno_intern("mongoid");
  mongofun = kno_intern("mongofun");
  mongouser = kno_intern("mongouser");
  mongomd5 = kno_intern("md5hash");

  skipsym = kno_intern("skip");
  limitsym = kno_intern("limit");
  batchsym = kno_intern("batch");
  fieldssym = kno_intern("fields");
  upsertsym = kno_intern("upsert");
  singlesym = kno_intern("single");
  wtimeoutsym = kno_intern("wtimeout");
  writesym = kno_intern("write");
  readsym = kno_intern("read");
  returnsym = kno_intern("return");
  originalsym = kno_intern("original");
  newsym = kno_intern("new");
  removesym = kno_intern("remove");
  softfailsym = kno_intern("softfail");

  sslsym = kno_intern("ssl");
  smoketest_sym = kno_intern("smoketest");
  certfile = kno_intern("certfile");
  certpass = kno_intern("certpass");
  cafilesym = kno_intern("cafile");
  cadirsym = kno_intern("cadir");
  crlsym = kno_intern("crlfile");

  mongovec_symbol = kno_intern("%mongovec");

  bsonflags = kno_intern("bson");
  raw = kno_intern("raw");
  slotify = kno_intern("slotify");
  stringkeys = kno_intern("stringkeys");
  colonize = kno_intern("colonize");
  rawstrings = kno_intern("rawstrings");
  logopsym = kno_intern("logops");
  choices = kno_intern("choices");
  nochoices = kno_intern("nochoices");
  fieldmap_symbol = kno_intern("fieldmap");

  dbname_symbol = kno_intern("dbname");
  username_symbol = kno_intern("username");
  auth_symbol = kno_intern("authentication");
  hosts_symbol = kno_intern("hosts");
  connections_symbol = kno_intern("connections");
  knotag_symbol = kno_intern("%knotag");
  knoparse_symbol = kno_intern("%knoparse");

  primarysym = kno_intern("primary");
  primarypsym = kno_intern("primary+");
  secondarysym = kno_intern("secondary");
  secondarypsym = kno_intern("secondary+");
  nearestsym = kno_intern("nearest");

  dotcar_symbol = kno_intern(">car>");
  dotcdr_symbol = kno_intern(">cdr>");

  poolmaxsym = kno_intern("poolmax");

  symslots_symbol = kno_intern("symslots");
  choiceslots_symbol = kno_intern("choiceslots");
  rawslots_symbol = kno_intern("rawslots");

  mongo_timestamp_tag = kno_intern("mongotime");

  choice_tagstring = knostring(CHOICE_TAGSTRING_TEXT);
  choice_tagsym = kno_intern(CHOICE_TAGSTRING_TEXT);

  kno_mongoc_server =
    kno_register_cons_type("mongoc_client",KNO_MONGOC_SERVER);
  kno_mongoc_collection =
    kno_register_cons_type("mongoc_collection",KNO_MONGOC_COLLECTION);
  kno_mongoc_cursor =
    kno_register_cons_type("mongoc_cursor",KNO_MONGOC_CURSOR);

  kno_recyclers[kno_mongoc_server]=recycle_server;
  kno_recyclers[kno_mongoc_collection]=recycle_collection;
  kno_recyclers[kno_mongoc_cursor]=recycle_cursor;

  kno_unparsers[kno_mongoc_server]=unparse_server;
  kno_unparsers[kno_mongoc_collection]=unparse_collection;
  kno_unparsers[kno_mongoc_cursor]=unparse_cursor;

  link_local_cprims();

  kno_register_config("MONGODB:FLAGS",
		      "Default flags (fixnum) for MongoDB/BSON processing",
		      kno_intconfig_get,kno_intconfig_set,&mongodb_defaults);

  kno_register_config("MONGODB:LOGLEVEL",
		      "Default flags (fixnum) for MongoDB/BSON processing",
		      kno_intconfig_get,kno_loglevelconfig_set,
		      &mongodb_loglevel);
  kno_register_config("MONGODB:MAXLOG",
		      "Controls which log messages are always discarded",
		      kno_intconfig_get,kno_loglevelconfig_set,
		      &mongodb_ignore_loglevel);
  kno_register_config("MONGODB:LOGOPS",
		      "Whether to log mongodb operations",
		      kno_boolconfig_get,kno_boolconfig_set,&logops);
  kno_register_config("MONGODB:LOGCMDS",
		      "Whether to log mongodb commands",
		      kno_boolconfig_get,kno_boolconfig_set,&logcmds);

  kno_register_config("MONGODB:SSL",
		      "Whether to default to SSL for MongoDB connections",
		      kno_boolconfig_get,kno_boolconfig_set,
		      &default_ssl);
  kno_register_config("MONGODB:CERT",
		      "Default certificate file to use for mongodb",
		      kno_sconfig_get,kno_realpath_config_set,
		      &default_certfile);
  kno_register_config("MONGODB:CAFILE",
		      "Default certificate file for use with MongoDB",
		      kno_sconfig_get,kno_realpath_config_set,
		      &default_cafile);
  kno_register_config("MONGODB:CADIR",
		      "Default certificate file directory for use with MongoDB",
		      kno_sconfig_get,kno_realdir_config_set,
		      &default_cadir);

  kno_register_config("MONGODB:MULTISLOTS",
		      "Which slots should always have vector values",
		      multislots_config_get,multislots_config_add,NULL);
  kno_register_config("MONGODB:CHOICESLOTS",
		      "Alias for MONGODB:MULTISLOTS: Which slots should "
		      "always have vector values",
		      multislots_config_get,multislots_config_add,NULL);

  kno_register_config("MONGODB:SOCKET_TIMEOUT",
		      "Default socket timeout for mongodb",
		      kno_intconfig_get,kno_intconfig_set,
		      &default_socket_timeout);
  kno_register_config("MONGODB:TIMEOUT",
		      "Default socket timeout for mongodb",
		      kno_intconfig_get,kno_intconfig_set,
		      &default_socket_timeout);
  kno_register_config("MONGODB:CONNECT:TIMEOUT",
		      "Default connection timeout for mongodb",
		      kno_intconfig_get,kno_intconfig_set,
		      &default_connect_timeout);
  kno_register_config("MONGODB:SERVER:TIMEOUT",
		      "Default server selection timeout for mongodb",
		      kno_intconfig_get,kno_intconfig_set,
		      &default_server_select_timeout);
  kno_register_config("MONGODB:MAXPOOLS",
		      "Default max connection pools limit for mongodb",
		      kno_intconfig_get,kno_intconfig_set,
		      &default_maxpools_limit);
  kno_register_config("MONGODB:THREADS:RECKLESS",
		      "Whether to ignore thread-safety for cursors",
		      kno_boolconfig_get,kno_boolconfig_set,
		      &reckless_threading);

  add_choiceslot(kno_intern("$each"));
  add_choiceslot(kno_intern("$in"));
  add_choiceslot(kno_intern("$nin"));
  add_choiceslot(kno_intern("$all"));
  add_choiceslot(kno_intern("$and"));
  add_choiceslot(kno_intern("$or"));
  add_choiceslot(kno_intern("$nor"));

  kno_finish_module(mongodb_module);

  mongoc_init();
  atexit(mongoc_cleanup);

  strcpy(mongoc_version_string,"libmongoc ");
  strcat(mongoc_version_string,MONGOC_VERSION_S);
  u8_register_source_file(mongoc_version_string);

  mongoc_log_set_handler(mongoc_logger,NULL);
  kno_register_config("MONGO:VERSION",
		      "The MongoDB C library version string",
		      kno_sconfig_get,NULL,
		      &mongoc_version);

  u8_register_source_file(_FILEINFO);

  strcpy(mongoc_version_string,"libmongoc ");
  strcat(mongoc_version_string,MONGOC_VERSION_S);
  u8_register_source_file(mongoc_version_string);

  return 1;
}

static void link_local_cprims()
{
  KNO_LINK_CPRIM("mongodb/open",mongodb_open,2,mongodb_module);
  KNO_LINK_CPRIM("mongodb?",mongodbp,1,mongodb_module);
  KNO_LINK_CPRIM("mongodb/cursor?",mongodb_cursorp,1,mongodb_module);
  KNO_LINK_CPRIM("mongodb/collection?",mongodb_collectionp,1,mongodb_module);
  KNO_LINK_CPRIM("mongodb/getdb",mongodb_getdb,1,mongodb_module);
  KNO_LINK_CPRIM("mongodb/getopts",mongodb_getopts,1,mongodb_module);

  KNO_LINK_CPRIM("mongodb/dburi",mongodb_uri,1,mongodb_module);
  KNO_LINK_CPRIM("mongodb/dbspec",mongodb_spec,1,mongodb_module);
  KNO_LINK_CPRIM("mongodb/dbname",mongodb_dbname,1,mongodb_module);
  KNO_LINK_CPRIM("mongodb/dbinfo",mongodb_getinfo,2,mongodb_module);

  KNO_LINK_CPRIM("mongovec?",mongovecp,1,mongodb_module);
  KNO_LINK_CPRIM("->mongovec",make_mongovec,1,mongodb_module);
  KNO_LINK_CPRIMN("mongovec",mongovec_lexpr,mongodb_module);
  KNO_LINK_CPRIM("mongodb/oid",mongodb_oidref,1,mongodb_module);

  KNO_LINK_CPRIM("collection/open",mongodb_collection,3,mongodb_module);
  KNO_LINK_CPRIM("collection/name",mongodb_collection_name,1,mongodb_module);
  KNO_LINK_CPRIM("collection/get",collection_get,3,mongodb_module);
  KNO_LINK_CPRIM("collection/count",collection_count,3,mongodb_module);
  KNO_LINK_CPRIM("collection/find",collection_find,3,mongodb_module);
  KNO_LINK_CPRIM("collection/modify!",collection_modify,4,mongodb_module);
  KNO_LINK_CPRIM("collection/upsert!",collection_upsert,4,mongodb_module);
  KNO_LINK_CPRIM("collection/update!",collection_update,4,mongodb_module);
  KNO_LINK_CPRIM("collection/remove!",collection_remove,3,mongodb_module);
  KNO_LINK_CPRIM("collection/insert!",collection_insert,3,mongodb_module);
  KNO_LINK_CPRIM("collection/open",mongodb_collection,3,mongodb_module);
  KNO_LINK_CPRIM("collection/oidslot",collection_oidslot,1,mongodb_module);
  KNO_LINK_ALIAS("mongodb/collection",mongodb_collection,mongodb_module);

  KNO_LINK_CPRIM("mongodb/cursor",mongodb_cursor,3,mongodb_module);
  KNO_LINK_CPRIM("cursor/done?",cursor_donep,1,mongodb_module);
  KNO_LINK_CPRIM("cursor/skipcount",cursor_skipcount,1,mongodb_module);
  KNO_LINK_CPRIM("cursor/readcount",cursor_readcount,1,mongodb_module);
  KNO_LINK_CPRIM("cursor/readvec",cursor_readvec,3,mongodb_module);
  KNO_LINK_CPRIM("cursor/read",cursor_read,3,mongodb_module);
  KNO_LINK_CPRIM("cursor/skip",cursor_skip,2,mongodb_module);
  KNO_LINK_CPRIM("cursor/close",cursor_close,1,mongodb_module);
  KNO_LINK_ALIAS("mongo/read->vector",cursor_readvec,mongodb_module);

  KNO_LINK_CPRIM("mongodb/getcollection",
		 mongodb_getcollection,1,mongodb_module);

  KNO_LINK_CPRIMN("mongodb/cmd",mongodb_simple_command,mongodb_module);
  KNO_LINK_CPRIMN("mongodb/results",mongodb_command,mongodb_module);

  KNO_LINK_ALIAS("mongo/oid",mongodb_oidref,mongodb_module);

  KNO_LINK_ALIAS("collection?",mongodb_collectionp,mongodb_module);

  KNO_LINK_ALIAS("cursor/open",mongodb_cursor,mongodb_module);
  KNO_LINK_ALIAS("cursor?",mongodb_cursorp,mongodb_module);

  KNO_LINK_ALIAS("mongo/open",mongodb_open,mongodb_module);
  KNO_LINK_ALIAS("mongo/collection",mongodb_collection,mongodb_module);
  KNO_LINK_ALIAS("mongo/collection?",mongodb_collectionp,mongodb_module);
  KNO_LINK_ALIAS("mongo/insert!",collection_insert,mongodb_module);
  KNO_LINK_ALIAS("mongo/insert!",collection_insert,mongodb_module);
  KNO_LINK_ALIAS("mongo/remove!",collection_remove,mongodb_module);
  KNO_LINK_ALIAS("mongo/update!",collection_update,mongodb_module);
  KNO_LINK_ALIAS("mongo/find",collection_find,mongodb_module);
  KNO_LINK_ALIAS("mongo/get",collection_get,mongodb_module);
  KNO_LINK_ALIAS("mongo/modify",collection_modify,mongodb_module);
  KNO_LINK_ALIAS("mongo/modify!",collection_modify,mongodb_module);
  KNO_LINK_ALIAS("mongo/results",mongodb_command,mongodb_module);
  KNO_LINK_ALIAS("mongo/name",mongodb_dbname,mongodb_module);
  KNO_LINK_ALIAS("mongo/spec",mongodb_spec,mongodb_module);
  KNO_LINK_ALIAS("mongo/uri",mongodb_uri,mongodb_module);
  KNO_LINK_ALIAS("mongo/dburi",mongodb_uri,mongodb_module);
  KNO_LINK_ALIAS("mongo/opts",mongodb_getopts,mongodb_module);
  KNO_LINK_ALIAS("mongo/info",mongodb_getinfo,mongodb_module);
  KNO_LINK_ALIAS("mongo/getcollection",mongodb_getcollection,mongodb_module);
  KNO_LINK_ALIAS("mongo/getdb",mongodb_getdb,mongodb_module);
  KNO_LINK_ALIAS("mongo?",mongodbp,mongodb_module);

  KNO_LINK_ALIAS("collection/modify",collection_modify,mongodb_module);
  KNO_LINK_ALIAS("mongodb/get",collection_get,mongodb_module);

  KNO_LINK_ALIAS("mongodb/spec",mongodb_spec,mongodb_module);
  KNO_LINK_ALIAS("mongodb/uri",mongodb_uri,mongodb_module);
  KNO_LINK_ALIAS("mongodb/opts",mongodb_getopts,mongodb_module);
}
