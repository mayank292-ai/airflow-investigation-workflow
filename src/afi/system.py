"""Read-only Windows resource probe. No process termination or OS changes."""
import ctypes


def memory_status():
    class MemoryStatus(ctypes.Structure):
        _fields_ = [('length', ctypes.c_ulong), ('load', ctypes.c_ulong),
                    ('total', ctypes.c_ulonglong), ('available', ctypes.c_ulonglong),
                    ('page_total', ctypes.c_ulonglong), ('page_available', ctypes.c_ulonglong),
                    ('virtual_total', ctypes.c_ulonglong), ('virtual_available', ctypes.c_ulonglong),
                    ('extended', ctypes.c_ulonglong)]
    memory = MemoryStatus()
    memory.length = ctypes.sizeof(memory)
    if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(memory)):
        raise OSError('Windows memory probe failed')
    return {'total_bytes': memory.total, 'available_bytes': memory.available,
            'load_percent': memory.load}
