"""A real ctypes-backed Arrow producer with observable ownership callbacks.

Descriptions below build physical arrays. They are NOT target import logic.
Buffers stay allocated until the fixture owner is discarded, but release
poisons values so accidental post-release reads are observable without UAF.
"""
import ctypes as C
from abi import ArrowArray,ArrowSchema,ArrayRelease,SchemaRelease

class Bundle:
    def __init__(self):
        self.keep=[];self.events=[];self._depth=0;self.value_buffers=[]
    def own(self,x): self.keep.append(x);return x
    def bitmap(self,valid):
        raw=bytearray((len(valid)+7)//8)
        for i,v in enumerate(valid):
            if v:raw[i//8]|=1<<(i%8)
        return self.own((C.c_uint8*len(raw))(*raw))
    def build(self,spec,label='root'):
        typ=spec['type'];name=spec.get('name','')
        fmt={'int':'l','str':'u','list':'+l','struct':'+s','dict':'i'}[typ]
        schema=self.own(ArrowSchema());array=self.own(ArrowArray())
        schema.format=fmt.encode();schema.name=name.encode();schema.flags=2
        array.offset=spec.get('offset',0);array.length=spec['length']
        valid=spec.get('valid')
        bitmap=self.bitmap(valid) if valid is not None else None
        array.null_count=-1 if valid is not None else 0
        buffers=[C.cast(bitmap,C.c_void_p).value if bitmap is not None else None]
        if typ in ('int','dict'):
            cls=C.c_int64 if typ=='int' else C.c_int32
            values=self.own((cls*len(spec['values']))(*spec['values']))
            buffers.append(C.cast(values,C.c_void_p).value);self.value_buffers.append(values)
        elif typ=='str':
            values=spec['values'];raw=b'';offsets=[0]
            for value in values:
                raw+=value.encode('utf-8');offsets.append(len(raw))
            indexes=self.own((C.c_int32*len(offsets))(*offsets))
            data=self.own((C.c_uint8*len(raw)).from_buffer_copy(raw))
            buffers.extend([C.cast(indexes,C.c_void_p).value,C.cast(data,C.c_void_p).value])
            self.value_buffers.append(data)
        elif typ=='list':
            indexes=self.own((C.c_int32*len(spec['offsets']))(*spec['offsets']))
            buffers.append(C.cast(indexes,C.c_void_p).value)
        children=[]
        for i,child in enumerate(spec.get('children',[])):
            children.append(self.build(child,label+'/'+str(i)))
        if children:
            sp=self.own((C.POINTER(ArrowSchema)*len(children))(*(C.pointer(x[0]) for x in children)))
            ap=self.own((C.POINTER(ArrowArray)*len(children))(*(C.pointer(x[1]) for x in children)))
            schema.children=sp;array.children=ap
        schema.n_children=array.n_children=len(children)
        dictionary=None
        if typ=='dict':
            dictionary=self.build(spec['dictionary'],label+'/dictionary')
            schema.dictionary=C.pointer(dictionary[0]);array.dictionary=C.pointer(dictionary[1])
        buf=self.own((C.c_void_p*len(buffers))(*buffers));array.buffers=buf;array.n_buffers=len(buffers)
        def release_array(ptr):
            obj=ptr.contents
            self.events.append({'kind':'array','node':label,'outside_parent':label!='root' and self._depth==0})
            self._depth+=1
            try:
                for _,child in children:
                    if child.release: child.release(C.pointer(child))
                if dictionary and dictionary[1].release:dictionary[1].release(C.pointer(dictionary[1]))
                obj.release=ArrayRelease()
                if label=='root':
                    for v in self.value_buffers:
                        if C.sizeof(v):C.memset(C.addressof(v),0xDD,C.sizeof(v))
            finally:self._depth-=1
        def release_schema(ptr):
            obj=ptr.contents
            self.events.append({'kind':'schema','node':label,'outside_parent':label!='root' and self._depth==0})
            self._depth+=1
            try:
                for child,_ in children:
                    if child.release:child.release(C.pointer(child))
                if dictionary and dictionary[0].release:dictionary[0].release(C.pointer(dictionary[0]))
                obj.release=SchemaRelease()
            finally:self._depth-=1
        array.release=self.own(ArrayRelease(release_array));schema.release=self.own(SchemaRelease(release_schema))
        return schema,array
    def export(self,spec):
        self.schema,self.array=self.build(spec);return self
    def close_unclaimed(self):
        if self.array.release:self.array.release(C.pointer(self.array))
        if self.schema.release:self.schema.release(C.pointer(self.schema))

def small():
    return Bundle().export({'type':'struct','offset':1,'length':3,'valid':[True,True,False,True],
      'children':[
        {'name':'id','type':'int','offset':1,'length':4,'values':[900,901,10,20,30], 'valid':[True]*5},
        {'name':'tag','type':'dict','offset':1,'length':4,'values':[0,0,1,0,2],
         'dictionary':{'type':'str','offset':1,'length':3,'values':['pad','red','蓝','empty'], 'valid':[True,True,True,False]}},
        {'name':'samples','type':'list','offset':1,'length':4,'offsets':[0,0,0,2,2,3],
         'children':[{'type':'int','offset':1,'length':3,'values':[999,5,6,7]}]}
      ]})

def large(n=200000):
    # Build raw C storage without an input list; all allocations precede adoption measurement.
    return Bundle().export({'type':'struct','length':n,'children':[
      {'name':'id','type':'int','length':n,'values':range(1000000,1000000+n)}]})
