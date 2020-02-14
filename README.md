# kno-mongo

This contains the MongoDB C driver for KNO and includes beingmeta's
fork of the mongo-c-driver repository as a submodule. This builds a
version of the KNO driver module which is statically linked with the
'release' branch of beingmeta's fork. This is done because different
distributions are uneven with respect to their mongo-c-driver support.

While MongoDB has moved their server release to an open but non-free
license, mongo-c-driver remains licensed under the Apache License.

