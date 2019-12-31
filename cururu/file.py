from cururu.compression import pack_object, unpack_object


def save(filename, obj):
    """Compress and save a python object (Data, Transformer) as a file."""
    f = open(filename, 'wb')
    f.write(pack_object(obj))
    f.close()


def load(filename):
    """Load a compressed python object (Data, Transformer) from file."""
    f = open(filename, 'rb')
    res = unpack_object(f.read())
    f.close()
    return res
