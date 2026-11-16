import ctypes as C
import errno
import threading
import time
import tracemalloc
import unittest

from producer import Bundle, large
from stream_fixture import Source
from stream_import import Batch, Reader, SourceError, open_stream

EXPECTED_ROWS = [
    {'id': 10, 'tag': '蓝', 'samples': [5, 6]},
    None,
    {'id': 30, 'tag': None, 'samples': [7]},
]


def root_releases(bundle, kind):
    return sum(e['kind'] == kind and e['node'] == 'root' for e in bundle.events)


def bad_source_events(source):
    return [e for e in source.events if e[0] in ('overlap', 'after_close')]


def stream_releases(source):
    return sum(1 for e in source.events if e[0] == 'release')


def nested_bundle():
    # Nonzero offsets at every level, a struct inside a list inside a struct,
    # a null struct element, and a dictionary index null at the top level.
    return Bundle().export({'type': 'struct', 'offset': 2, 'length': 2,
      'valid': [True, True, True, True],
      'children': [
        {'name': 'items', 'type': 'list', 'offset': 1, 'length': 5,
         'offsets': [9, 9, 9, 0, 2, 3],
         'children': [{'type': 'struct', 'offset': 1, 'length': 3,
           'valid': [True, True, False, True],
           'children': [
             {'name': 'k', 'type': 'int', 'length': 4, 'values': [7, 8, 9, 10]},
             {'name': 'tags', 'type': 'list', 'length': 4, 'offsets': [9, 1, 2, 2, 3],
              'children': [{'type': 'str', 'length': 3, 'values': ['a', 'b', 'c']}]}]}]},
        {'name': 'label', 'type': 'dict', 'offset': 2, 'length': 6,
         'values': [9, 9, 9, 9, 1, 0], 'valid': [True, True, True, True, True, False],
         'dictionary': {'type': 'str', 'offset': 1, 'length': 2,
                        'values': ['pad', 'hey', 'yo'], 'valid': [True, True, True]}}]})


class StreamImportLifecycleTests(unittest.TestCase):
    def test_rows_views_and_release_order(self):
        source = Source(count=1)
        reader = open_stream(source.pointer)
        self.assertIsInstance(reader, Reader)
        # The schema root moved to the importer and was released exactly once.
        self.assertFalse(source.schemas[0].schema.release)
        self.assertEqual(root_releases(source.schemas[0], 'schema'), 1)
        batch = reader.next()
        self.assertIsInstance(batch, Batch)
        # The array root moved; the producer side no longer holds it.
        self.assertFalse(source.bundles[0].array.release)
        self.assertEqual(batch.length, 3)
        self.assertEqual([batch.row(i) for i in range(3)], EXPECTED_ROWS)
        kept = batch.slice(1, 2)
        self.assertEqual(kept.length, 2)
        self.assertIsNone(kept.row(0))
        self.assertEqual(kept.row(1), EXPECTED_ROWS[2])
        deeper = kept.slice(1, 1)
        self.assertEqual(deeper.row(0), EXPECTED_ROWS[2])
        empty = batch.slice(0, 0)
        self.assertEqual(empty.length, 0)
        # Normal end closes the stream; next() keeps returning None.
        self.assertIsNone(reader.next())
        self.assertIsNone(reader.next())
        self.assertTrue(source.closed)
        reader.close()
        reader.close()
        # Delivered views survive Reader close; the array root is still held.
        self.assertEqual(deeper.row(0), EXPECTED_ROWS[2])
        bundle = source.bundles[0]
        self.assertEqual(root_releases(bundle, 'array'), 0)
        batch.close()
        self.assertEqual(root_releases(bundle, 'array'), 0)
        empty.close()
        deeper.close()
        self.assertEqual(root_releases(bundle, 'array'), 0)
        kept.close()
        # The last open view released the array root exactly once.
        self.assertEqual(root_releases(bundle, 'array'), 1)
        # No child or dictionary node was released outside its root callback.
        self.assertFalse(any(e['outside_parent']
                             for b in source.bundles + source.schemas for e in b.events))
        self.assertEqual(stream_releases(source), 1)
        self.assertEqual(bad_source_events(source), [])

    def test_nested_offsets_and_nulls(self):
        source = Source(count=1, factory=nested_bundle)
        with open_stream(source.pointer) as reader:
            batch = reader.next()
            self.assertEqual(batch.length, 2)
            self.assertEqual(batch.row(0),
                             {'items': [{'k': 8, 'tags': ['b']}, None], 'label': 'yo'})
            self.assertEqual(batch.row(1),
                             {'items': [{'k': 10, 'tags': ['c']}], 'label': None})
            view = batch.slice(1, 1)
            self.assertEqual(view.row(0),
                             {'items': [{'k': 10, 'tags': ['c']}], 'label': None})
            view.close()
            batch.close()
        self.assertEqual(root_releases(source.bundles[0], 'array'), 1)
        self.assertEqual(root_releases(source.schemas[0], 'schema'), 1)
        self.assertFalse(any(e['outside_parent']
                             for b in source.bundles + source.schemas for e in b.events))

    def test_zero_row_batch_is_a_batch_not_eos(self):
        def empty_batch():
            return Bundle().export({'type': 'struct', 'length': 0, 'children': [
                {'name': 'id', 'type': 'int', 'length': 0, 'values': []}]})
        source = Source(count=1, factory=empty_batch)
        with open_stream(source.pointer) as reader:
            batch = reader.next()
            self.assertIsNotNone(batch)
            self.assertEqual(batch.length, 0)
            with self.assertRaises(IndexError):
                batch.row(0)
            view = batch.slice(0, 0)
            self.assertEqual(view.length, 0)
            self.assertIsNone(reader.next())
            batch.close()
            # The zero-row batch root is released only after its last view.
            self.assertEqual(root_releases(source.bundles[0], 'array'), 0)
            view.close()
            self.assertEqual(root_releases(source.bundles[0], 'array'), 1)

    def test_index_and_close_rules(self):
        source = Source(count=1)
        reader = open_stream(source.pointer)
        batch = reader.next()
        for bad in (True, 1.0, '1', None):
            with self.assertRaises(TypeError):
                batch.row(bad)
            with self.assertRaises(TypeError):
                batch.slice(bad, 1)
            with self.assertRaises(TypeError):
                batch.slice(0, bad)
        with self.assertRaises(IndexError):
            batch.row(-1)
        with self.assertRaises(IndexError):
            batch.row(3)
        with self.assertRaises(IndexError):
            batch.slice(-1, 1)
        with self.assertRaises(IndexError):
            batch.slice(0, 4)
        with self.assertRaises(IndexError):
            batch.slice(2, 2)
        tail = batch.slice(3, 0)
        tail.close()
        batch.close()
        for op in (lambda: batch.length, lambda: batch.row(0), lambda: batch.slice(0, 1)):
            with self.assertRaises(ValueError):
                op()
        batch.close()
        reader.close()
        self.assertEqual(root_releases(source.bundles[0], 'array'), 1)
        self.assertEqual(bad_source_events(source), [])

    def test_with_blocks_close_on_exception(self):
        source = Source(count=1)

        class Boom(Exception):
            pass

        with self.assertRaises(Boom):
            with open_stream(source.pointer) as reader:
                with reader.next() as batch:
                    self.assertEqual(batch.row(0), EXPECTED_ROWS[0])
                    raise Boom()
        self.assertTrue(source.closed)
        self.assertEqual(root_releases(source.bundles[0], 'array'), 1)
        self.assertEqual(stream_releases(source), 1)

    def test_open_stream_rejects_non_pointer(self):
        with self.assertRaises(TypeError):
            open_stream(None)
        source = Source(count=0)
        with self.assertRaises(TypeError):
            open_stream(source.stream)
        source.stream.release(source.pointer)


class StreamImportErrorTests(unittest.TestCase):
    def test_source_error_raises_and_closes(self):
        source = Source(count=3, fail_at=1)
        reader = open_stream(source.pointer)
        batch = reader.next()
        self.assertEqual(batch.row(0), EXPECTED_ROWS[0])
        with self.assertRaises(SourceError) as ctx:
            reader.next()
        self.assertEqual(ctx.exception.code, errno.EIO)
        self.assertEqual(ctx.exception.message, 'configured source failure')
        # The error closed the source; later next() is a quiet None.
        self.assertIsNone(reader.next())
        self.assertTrue(source.closed)
        reader.close()
        self.assertEqual(stream_releases(source), 1)
        self.assertEqual(bad_source_events(source), [])
        # The batch delivered before the error is still fully usable.
        self.assertEqual([batch.row(i) for i in range(3)], EXPECTED_ROWS)
        batch.close()
        self.assertEqual(root_releases(source.bundles[0], 'array'), 1)

    def test_open_stream_schema_failure_releases_stream(self):
        def broken():
            raise RuntimeError('schema boom')

        source = Source(schema_factory=broken)
        with self.assertRaises(SourceError) as ctx:
            open_stream(source.pointer)
        self.assertEqual(ctx.exception.code, errno.EIO)
        self.assertEqual(ctx.exception.message, 'schema boom')
        self.assertTrue(source.closed)
        self.assertEqual(stream_releases(source), 1)
        self.assertEqual(bad_source_events(source), [])


class StreamImportConcurrencyTests(unittest.TestCase):
    def test_concurrent_next_drains_batches(self):
        count = 9
        source = Source(count=count)
        reader = open_stream(source.pointer)
        batches = []
        seen = threading.Lock()

        def consume():
            while True:
                batch = reader.next()
                if batch is None:
                    return
                with seen:
                    batches.append(batch)

        threads = [threading.Thread(target=consume) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(len(batches), count)
        self.assertEqual(len({id(b) for b in batches}), count)
        for batch in batches:
            self.assertEqual([batch.row(i) for i in range(3)], EXPECTED_ROWS)
        for batch in batches:
            batch.close()
        for bundle in source.bundles:
            self.assertEqual(root_releases(bundle, 'array'), 1)
        self.assertEqual(stream_releases(source), 1)
        self.assertEqual(bad_source_events(source), [])
        reader.close()

    def test_close_races_with_next(self):
        source = Source(count=1000, before_next=lambda position: time.sleep(0.001))
        reader = open_stream(source.pointer)
        batches = []
        seen = threading.Lock()

        def consume():
            while True:
                batch = reader.next()
                if batch is None:
                    return
                with seen:
                    batches.append(batch)

        threads = [threading.Thread(target=consume) for _ in range(3)]
        for t in threads:
            t.start()
        time.sleep(0.05)
        reader.close()
        for t in threads:
            t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertLessEqual(len(batches), 1000)
        for batch in batches:
            self.assertEqual(batch.row(0), EXPECTED_ROWS[0])
            batch.close()
        for bundle in source.bundles:
            self.assertEqual(root_releases(bundle, 'array'), 1)
        self.assertEqual(stream_releases(source), 1)
        self.assertEqual(bad_source_events(source), [])
        self.assertIsNone(reader.next())

    def test_close_waits_for_inflight_next(self):
        entered = threading.Event()
        allow = threading.Event()

        def hook(position):
            entered.set()
            self.assertTrue(allow.wait(5))

        source = Source(count=1, before_next=hook)
        reader = open_stream(source.pointer)
        result = []
        taker = threading.Thread(target=lambda: result.append(reader.next()))
        taker.start()
        self.assertTrue(entered.wait(5))
        closer = threading.Thread(target=reader.close)
        closer.start()
        time.sleep(0.05)
        # The in-flight get_next still owns the source; no release yet.
        self.assertEqual(stream_releases(source), 0)
        allow.set()
        taker.join(5)
        closer.join(5)
        self.assertFalse(taker.is_alive() or closer.is_alive())
        self.assertEqual(len(result), 1)
        batch = result[0]
        self.assertIsNotNone(batch)
        # Reader closed underneath it; the delivered batch is intact.
        self.assertEqual([batch.row(i) for i in range(3)], EXPECTED_ROWS)
        batch.close()
        self.assertEqual(root_releases(source.bundles[0], 'array'), 1)
        self.assertEqual(stream_releases(source), 1)
        self.assertEqual(bad_source_events(source), [])

    def test_concurrent_close_is_idempotent(self):
        source = Source(count=1)
        reader = open_stream(source.pointer)
        barrier = threading.Barrier(6)

        def close():
            barrier.wait(5)
            reader.close()

        threads = [threading.Thread(target=close) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(stream_releases(source), 1)
        self.assertEqual(bad_source_events(source), [])
        self.assertIsNone(reader.next())

    def test_concurrent_views_reads_and_closes(self):
        source = Source(count=1)
        reader = open_stream(source.pointer)
        batch = reader.next()
        reader.close()
        barrier = threading.Barrier(8)
        errors = []

        def worker(k):
            try:
                barrier.wait(5)
                for _ in range(50):
                    view = batch.slice(k % 3, 1)
                    self.assertEqual(view.row(0), EXPECTED_ROWS[k % 3])
                    view.close()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(k,)) for k in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(15)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(errors, [])
        self.assertEqual(root_releases(source.bundles[0], 'array'), 0)
        batch.close()
        self.assertEqual(root_releases(source.bundles[0], 'array'), 1)

    def test_reads_in_flight_block_array_release(self):
        data = large(5000)
        source = Source(count=1, factory=lambda: data, schema_factory=lambda: large(1))
        reader = open_stream(source.pointer)
        batch = reader.next()
        reader.close()
        view = batch.slice(0, 5000)
        stop = threading.Event()
        errors = []

        def reader_worker():
            try:
                while not stop.is_set():
                    for i in range(0, 5000, 7):
                        try:
                            value = view.row(i)
                        except ValueError:
                            return  # the view was closed underneath us; legal
                        # Release poisons buffers with 0xDD; a premature
                        # release during this read would be visible here.
                        self.assertEqual(value, {'id': 1000000 + i})
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=reader_worker) for _ in range(4)]
        for t in threads:
            t.start()
        time.sleep(0.05)
        batch.close()
        view.close()
        stop.set()
        for t in threads:
            t.join(15)
        self.assertFalse(any(t.is_alive() for t in threads))
        self.assertEqual(errors, [])
        self.assertEqual(root_releases(data, 'array'), 1)


class StreamImportMemoryTests(unittest.TestCase):
    def test_large_batch_peak_allocation(self):
        schema_bundle = large(1)
        data_bundle = large(200000)
        source = Source(count=1, factory=lambda: data_bundle,
                        schema_factory=lambda: schema_bundle)
        tracemalloc.start()
        try:
            base, _ = tracemalloc.get_traced_memory()
            reader = open_stream(source.pointer)
            batch = reader.next()
            self.assertEqual(batch.length, 200000)
            views = [batch.slice(i, 100) for i in range(100)]
            self.assertEqual(batch.row(0), {'id': 1000000})
            self.assertEqual(views[50].row(7), {'id': 1000057})
            self.assertEqual(views[99].row(99), {'id': 1000198})
            reader.close()
            for view in views:
                view.close()
            batch.close()
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLessEqual(peak - base, 4 * 1024 * 1024)
        self.assertEqual(root_releases(data_bundle, 'array'), 1)
        self.assertEqual(root_releases(schema_bundle, 'schema'), 1)
        self.assertEqual(stream_releases(source), 1)


if __name__ == '__main__':
    unittest.main()
