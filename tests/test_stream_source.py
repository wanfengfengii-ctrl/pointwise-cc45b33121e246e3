import ctypes as C
import errno,threading,unittest
from abi import ArrowArray,ArrowSchema
from stream_fixture import Source

class StreamProducerTests(unittest.TestCase):
    def test_move_roots_and_eos(self):
        source=Source(count=1);schema=ArrowSchema();array=ArrowArray();end=ArrowArray()
        self.assertEqual(source.stream.get_schema(source.pointer,C.byref(schema)),0)
        self.assertTrue(schema.release);self.assertFalse(source.schemas[0].schema.release)
        self.assertEqual(source.stream.get_next(source.pointer,C.byref(array)),0)
        self.assertEqual(array.length,3);self.assertTrue(array.release)
        self.assertFalse(source.bundles[0].array.release)
        self.assertEqual(source.stream.get_next(source.pointer,C.byref(end)),0)
        self.assertFalse(end.release)
        source.stream.release(source.pointer)
        self.assertFalse(source.stream.release);self.assertTrue(array.release)
        values=C.cast(array.children[0].contents.buffers[1],C.POINTER(C.c_int64))
        self.assertEqual(values[2],10)
        array.release(C.byref(array));schema.release(C.byref(schema))
        self.assertFalse(array.release);self.assertFalse(schema.release)
        self.assertEqual(sum(e['kind']=='array' and e['node']=='root' for e in source.bundles[0].events),1)
    def test_source_error_releases_no_borrowed_batch(self):
        source=Source(fail_at=0);array=ArrowArray()
        self.assertEqual(source.stream.get_next(source.pointer,C.byref(array)),errno.EIO)
        self.assertFalse(array.release)
        self.assertEqual(C.string_at(source.stream.get_last_error(source.pointer)),b'configured source failure')
        source.stream.release(source.pointer)
    def test_true_inflight_callback_and_overlap_probe(self):
        entered=threading.Event();allow=threading.Event()
        def hook(position):entered.set();assert allow.wait(5)
        source=Source(count=1,before_next=hook);array=ArrowArray();other=ArrowArray()
        thread=threading.Thread(target=lambda:source.stream.get_next(source.pointer,C.byref(array)))
        thread.start();self.assertTrue(entered.wait(5))
        self.assertEqual(source.stream.get_next(source.pointer,C.byref(other)),errno.EBUSY)
        allow.set();thread.join(5);self.assertFalse(thread.is_alive())
        array.release(C.byref(array));source.stream.release(source.pointer)
        self.assertEqual(source.events.count(('overlap','next')),1)
if __name__=='__main__':unittest.main()
