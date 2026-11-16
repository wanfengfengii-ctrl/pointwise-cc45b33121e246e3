"""Real Arrow C Stream consumer adapter (see CONTRACT.md).

Ownership model:
- open_stream(ptr) takes over the stream root exactly once. The Reader
  releases it exactly once -- on normal end, on source error, or on close --
  serialized with every other source callback, and never after close returns.
- The schema root is deep-copied into a pure-Python type plan at open_stream
  and released immediately; decoding depends only on the plan, so the schema
  root covers every later decode/view through that copy.
- Each get_next array root is owned by one _BatchState shared by the Batch
  and all its slices. The root is released exactly once, synchronously (never
  by the garbage collector), after the last open view closes and the last
  in-flight read finishes. Child and dictionary nodes are released only by
  the root callback, never traversed or released directly.
"""
import ctypes as C
import threading

from abi import ArrowArray, ArrowSchema
from stream_abi import ArrowArrayStream

__all__ = ['open_stream', 'Reader', 'Batch', 'SourceError']


class SourceError(Exception):
    """A source callback failed; carries the original code and diagnostics."""

    def __init__(self, code, message):
        super().__init__('source error %s: %s' % (code, message))
        self.code = code
        self.message = message


# ---------------------------------------------------------------- type plan

_FORMATS = {b'l': 'int', b'u': 'str', b'+l': 'list', b'+s': 'struct', b'i': 'dict'}


class _Plan:
    """Pure-Python copy of one schema node; decoding never needs the C schema."""

    __slots__ = ('kind', 'name', 'children', 'dictionary')

    def __init__(self, kind, name, children, dictionary):
        self.kind = kind
        self.name = name
        self.children = children
        self.dictionary = dictionary


def _build_plan(node):
    try:
        kind = _FORMATS[node.format]
    except KeyError:
        raise ValueError('unsupported Arrow format: %r' % (node.format,))
    name = node.name.decode('utf-8') if node.name else ''
    children = [_build_plan(node.children[i].contents) for i in range(node.n_children)]
    dictionary = _build_plan(node.dictionary.contents) if node.dictionary else None
    return _Plan(kind, name, children, dictionary)


# ------------------------------------------------------------------ decoding

def _valid(array, pos):
    bitmap = array.buffers[0]
    if not bitmap:
        return True
    return (C.cast(bitmap, C.POINTER(C.c_uint8))[pos >> 3] >> (pos & 7)) & 1 == 1


def _decode(plan, array, index):
    """Materialize one logical element as an independent Python value."""
    pos = array.offset + index
    if not _valid(array, pos):
        return None
    kind = plan.kind
    if kind == 'int':
        return C.cast(array.buffers[1], C.POINTER(C.c_int64))[pos]
    if kind == 'str':
        offsets = C.cast(array.buffers[1], C.POINTER(C.c_int32))
        start = offsets[pos]
        end = offsets[pos + 1]
        if end == start:
            return ''
        return C.string_at(array.buffers[2] + start, end - start).decode('utf-8')
    if kind == 'list':
        offsets = C.cast(array.buffers[1], C.POINTER(C.c_int32))
        child = array.children[0].contents
        child_plan = plan.children[0]
        return [_decode(child_plan, child, i) for i in range(offsets[pos], offsets[pos + 1])]
    if kind == 'struct':
        return {child_plan.name: _decode(child_plan, array.children[k].contents, pos)
                for k, child_plan in enumerate(plan.children)}
    if kind == 'dict':
        entry = C.cast(array.buffers[1], C.POINTER(C.c_int32))[pos]
        return _decode(plan.dictionary, array.dictionary.contents, entry)
    raise AssertionError('unreachable plan kind: %r' % (kind,))


def _as_index(value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError('index must be an integer, got %s' % type(value).__name__)
    return value


# ------------------------------------------------------------- batch storage

class _BatchState:
    """Shared ownership of one taken-over array root (batch plus all slices)."""

    __slots__ = ('array', 'lock', 'open_views', 'inflight', 'released')

    def __init__(self, array):
        self.array = array
        self.lock = threading.Lock()
        self.open_views = 1
        self.inflight = 0
        self.released = False

    def _release_needed_locked(self):
        if not self.released and self.open_views == 0 and self.inflight == 0:
            self.released = True
            return True
        return False

    def read_finished(self):
        with self.lock:
            self.inflight -= 1
            needed = self._release_needed_locked()
        if needed:
            self.release_array()

    def release_array(self):
        # The root callback releases the whole subtree; called exactly once.
        self.array.release(C.byref(self.array))


class Batch:
    """One live view onto a taken-over array; slices share the same root."""

    __slots__ = ('_state', '_plan', '_start', '_length', '_closed')

    def __init__(self, state, plan, start, length):
        self._state = state
        self._plan = plan
        self._start = start
        self._length = length
        self._closed = False

    def _check_open(self):
        if self._closed:
            raise ValueError('batch view is closed')

    @property
    def length(self):
        with self._state.lock:
            self._check_open()
            return self._length

    def row(self, index):
        index = _as_index(index)
        state = self._state
        with state.lock:
            self._check_open()
            if not 0 <= index < self._length:
                raise IndexError('row index out of range: %r' % (index,))
            state.inflight += 1
        try:
            return _decode(self._plan, state.array, self._start + index)
        finally:
            state.read_finished()

    def slice(self, start, length):
        start = _as_index(start)
        length = _as_index(length)
        state = self._state
        with state.lock:
            self._check_open()
            if start < 0 or length < 0 or start + length > self._length:
                raise IndexError('slice out of range: (%r, %r)' % (start, length))
            state.open_views += 1
            return Batch(state, self._plan, self._start + start, length)

    def close(self):
        state = self._state
        with state.lock:
            if self._closed:
                return
            self._closed = True
            state.open_views -= 1
            needed = state._release_needed_locked()
        if needed:
            state.release_array()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


# -------------------------------------------------------------------- reader

class Reader:
    """Consumes one taken-over ArrowArrayStream; next/close are thread-safe.

    A single lock serializes every source callback (get_next, get_last_error,
    release): source callbacks never overlap, diagnostics are copied before
    any other request can overwrite them, close() waits for an in-flight
    get_next, and no source call starts after close returns.
    """

    def __init__(self, stream, plan):
        self._stream = stream
        self._plan = plan
        self._lock = threading.Lock()
        self._finished = False
        self._released = False

    def next(self):
        with self._lock:
            if self._finished:
                return None
            out = ArrowArray()
            code = self._stream.contents.get_next(self._stream, C.byref(out))
            if code != 0:
                message = self._diagnostics_locked()
                self._finish_locked()
                raise SourceError(code, message)
            if not out.release:
                # Normal end of stream: empty root. Close the stream.
                self._finish_locked()
                return None
            # A zero-row batch still owns its root and must be released.
            return Batch(_BatchState(out), self._plan, 0, out.length)

    def close(self):
        with self._lock:
            self._finish_locked()

    def _diagnostics_locked(self):
        address = self._stream.contents.get_last_error(self._stream)
        if not address:
            return ''
        return C.string_at(address).decode('utf-8', 'replace')

    def _finish_locked(self):
        self._finished = True
        if not self._released:
            self._released = True
            release = self._stream.contents.release
            if release:
                release(self._stream)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False


def _release_stream_root(stream):
    release = stream.contents.release
    if release:
        release(stream)


def open_stream(stream):
    """Take over a POINTER(ArrowArrayStream) and return its Reader.

    The stream root is taken over whether or not get_schema succeeds; on
    failure the original diagnostics are copied, the root is released, and
    SourceError is raised instead of returning a Reader.
    """
    if not isinstance(stream, C.POINTER(ArrowArrayStream)):
        raise TypeError('expected POINTER(ArrowArrayStream), got %s' % type(stream).__name__)
    schema = ArrowSchema()
    code = stream.contents.get_schema(stream, C.byref(schema))
    if code != 0:
        address = stream.contents.get_last_error(stream)
        message = C.string_at(address).decode('utf-8', 'replace') if address else ''
        _release_stream_root(stream)
        raise SourceError(code, message)
    try:
        plan = _build_plan(schema)
    except BaseException:
        if schema.release:
            schema.release(C.byref(schema))
        _release_stream_root(stream)
        raise
    # The plan carries every type detail decoding needs; release the schema
    # root exactly once, right here. The root callback releases its subtree.
    if schema.release:
        schema.release(C.byref(schema))
    return Reader(stream, plan)
