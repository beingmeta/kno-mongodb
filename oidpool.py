
import pymongo
import bson
from bson.objectid import ObjectId

zb=(0).to_bytes(0,'big')
b4=(0).to_bytes(4,'big')

def abs_oid(hi,lo):
    return ObjectID(zb.join([b4,(hi).to_bytes(4,'big'),(lo).to_bytes(4,'big')]))

def rel_oid(objid,off,n_bits=None):
    if n_bits is None:
        n_bits = off.bit_length()
    elif off.bit_length() > n_bits:
        raise Exception("Pool reference overflow")
    n_bytes=(n_bits+(8-n_bits%8))//8
    # print("nbits=%s nb8=%s nbytes=%s"%(n_bits,n_bits%8,n_bytes))
    base_bytes=base.binary[0:-n_bytes]
    return ObjectId(zb.join([base_bytes,off.to_bytes(n_bytes,'big')]))

pools={}

def collection_id(collection):
    db=collection.database
    client=db.client
    return (db.name).join((client.address,collection.name))

class OIDPool():
    def __init__(self,collection,base=None,capacity=None,load=0):
        id=collection_id(collection)
        if id in pools:
            return pools[id]
        if "_pool" in collection:
            info=collection._pool
            if base is None:
                base=info.base
            elif base is not info.base:
                raise Exception("InconsistentPoolBase")
            if capacity is None:
                capacity=info.capacity
            elif capacity is not info.capacity:
                raise Exception("InconsistentPoolCapacity")
        elif base is None:
            raise Exception("MissingPoolBase")
        elif capacity is None:
            raise Exception("MissingPoolCapacity")
        poolbits=(capacity-1).bit_length()
        if poolbits%8 is not 0:
            raise Exception("OddSizedPool")
        self.id=id
        pools[id]=self
        self.base=base
        self.capacity=capacity
        self.load=load
        self.collection=collection
        self.poolbits=(capacity-1).bit_length()
    def oidref(offset):
        return rel_oid(self.base,offset,self.poolbits)
    def ref(offset):
        if offset < self.load:
            return rel_oid(self.base,offset,self.poolbits)
        self.load=self.collection['_pool'].load
        if offset < self.load:
            return rel_oid(self.base,offset,self.poolbits)
        else raise Exception("UnallocatedOID")
    def alloc(n=1):
        if self.load+n > self.capacity:
            raise Exception("PoolIsFull")
        info=collection.find_and_modify({_id: '_pool'},{"$inc": {"load": n}})
        base=self.base
        base_load=info.load
        if base_load+n > self.capacity:
            raise Exception("PoolIsFull")
        oids=()
        for i from 0 to n:
            oid=rel_oid(base,base_load+i)
            oids.push(oid)
        return oids
