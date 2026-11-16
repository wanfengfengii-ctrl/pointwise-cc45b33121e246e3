"""Arrow C Data Interface declarations; this file contains no importer."""
import ctypes as C
class ArrowSchema(C.Structure): pass
class ArrowArray(C.Structure): pass
SchemaRelease=C.CFUNCTYPE(None,C.POINTER(ArrowSchema))
ArrayRelease=C.CFUNCTYPE(None,C.POINTER(ArrowArray))
ArrowSchema._fields_=[('format',C.c_char_p),('name',C.c_char_p),('metadata',C.c_char_p),
 ('flags',C.c_int64),('n_children',C.c_int64),('children',C.POINTER(C.POINTER(ArrowSchema))),
 ('dictionary',C.POINTER(ArrowSchema)),('release',SchemaRelease),('private_data',C.c_void_p)]
ArrowArray._fields_=[('length',C.c_int64),('null_count',C.c_int64),('offset',C.c_int64),
 ('n_buffers',C.c_int64),('n_children',C.c_int64),('buffers',C.POINTER(C.c_void_p)),
 ('children',C.POINTER(C.POINTER(ArrowArray))),('dictionary',C.POINTER(ArrowArray)),
 ('release',ArrayRelease),('private_data',C.c_void_p)]
