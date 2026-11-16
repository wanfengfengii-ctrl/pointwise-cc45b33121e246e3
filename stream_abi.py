"""Arrow C Stream ABI declarations; no consuming adapter."""
import ctypes as C
from abi import ArrowSchema,ArrowArray
class ArrowArrayStream(C.Structure):pass
GetSchema=C.CFUNCTYPE(C.c_int,C.POINTER(ArrowArrayStream),C.POINTER(ArrowSchema))
GetNext=C.CFUNCTYPE(C.c_int,C.POINTER(ArrowArrayStream),C.POINTER(ArrowArray))
GetLastError=C.CFUNCTYPE(C.c_void_p,C.POINTER(ArrowArrayStream))
StreamRelease=C.CFUNCTYPE(None,C.POINTER(ArrowArrayStream))
ArrowArrayStream._fields_=[('get_schema',GetSchema),('get_next',GetNext),
 ('get_last_error',GetLastError),('release',StreamRelease),('private_data',C.c_void_p)]
