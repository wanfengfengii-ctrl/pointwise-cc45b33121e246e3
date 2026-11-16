"""Real C callback source. Implements production only, not target import.
Each callback may run on a different caller thread, but concurrent source
callbacks violate the source contract. C buffers remain allocated for safety;
root array release poisons values, making premature release observable.
"""
import ctypes as C
import errno
import threading
from abi import ArrowArray,ArrowSchema,ArrayRelease,SchemaRelease
from stream_abi import ArrowArrayStream,GetSchema,GetNext,GetLastError,StreamRelease
from producer import small

class Source:
    def __init__(self,count=3,factory=small,fail_at=None,before_next=None,schema_factory=None):
        self.count=count;self.factory=factory;self.fail_at=fail_at
        self.schema_factory=schema_factory or factory
        self.before_next=before_next;self.bundles=[];self.schemas=[];self.events=[]
        self.position=0;self.closed=False;self.guard=threading.Lock();self.callbacks=[]
        self.error=C.create_string_buffer(b'configured source failure')
        self.stream=ArrowArrayStream()
        def guarded(name,operation):
            if not self.guard.acquire(False):
                self.events.append(('overlap',name));return errno.EBUSY
            try:
                if self.closed:
                    self.events.append(('after_close',name));return errno.EBADF
                self.events.append((name,self.position))
                return operation()
            except BaseException as exc:
                self.error=C.create_string_buffer(str(exc).encode())
                return errno.EIO
            finally:self.guard.release()
        def schema(stream,out):
            def operation():
                bundle=self.schema_factory();self.schemas.append(bundle)
                # The producer owns this unused array, but exports the schema.
                if bundle.array.release:bundle.array.release(C.pointer(bundle.array))
                C.memmove(out,C.byref(bundle.schema),C.sizeof(ArrowSchema))
                bundle.schema.release=SchemaRelease()
                return 0
            return guarded('schema',operation)
        def next_(stream,out):
            def operation():
                C.memset(out,0,C.sizeof(ArrowArray))
                if self.before_next:self.before_next(self.position)
                if self.position==self.fail_at:return errno.EIO
                if self.position>=self.count:return 0
                bundle=self.factory();self.bundles.append(bundle)
                # Export array root by value. Its producer-side root is moved.
                C.memmove(out,C.byref(bundle.array),C.sizeof(ArrowArray))
                bundle.array.release=ArrayRelease()
                # The array and schema are independent C ownership trees.
                if bundle.schema.release:bundle.schema.release(C.pointer(bundle.schema))
                self.position+=1
                return 0
            return guarded('next',operation)
        def error(stream):
            if not self.guard.acquire(False):
                self.events.append(('overlap','error'));return 0
            try:
                self.events.append(('error',self.position));return C.addressof(self.error)
            finally:self.guard.release()
        def release(stream):
            if not self.guard.acquire(False):
                self.events.append(('overlap','release'));return
            try:
                self.events.append(('release',self.position))
                self.closed=True;stream.contents.release=StreamRelease()
            finally:self.guard.release()
        self.callbacks=[GetSchema(schema),GetNext(next_),GetLastError(error),StreamRelease(release)]
        self.stream.get_schema,self.stream.get_next,self.stream.get_last_error,self.stream.release=self.callbacks
    @property
    def pointer(self):return C.pointer(self.stream)
