import ctypes as C
import json
import sys
from producer import small,large

def main():
    b=small();a=b.array
    assert a.length==3 and a.offset==1 and a.n_children==3
    ints=C.cast(a.children[0].contents.buffers[1],C.POINTER(C.c_int64))
    assert [ints[i] for i in (2,3,4)]==[10,20,30]
    assert a.children[1].contents.dictionary.contents.offset==1
    assert b.events==[]
    b.close_unclaimed()
    assert not b.array.release and not b.schema.release
    assert sum(x['kind']=='array' and x['node']=='root' for x in b.events)==1
    assert sum(x['kind']=='schema' and x['node']=='root' for x in b.events)==1
    assert not any(x['outside_parent'] for x in b.events)
    z=large(17);assert z.array.length==17;z.close_unclaimed()
    print('ARROW_PRODUCER_BASELINE_OK')
    if len(sys.argv)>1 and sys.argv[1]=='demo':print(json.dumps({'fixture_expected_rows':[{'id':10,'tag':'蓝','samples':[5,6]},None,{'id':30,'tag':None,'samples':[7]}]},ensure_ascii=False))
if __name__=='__main__':main()
