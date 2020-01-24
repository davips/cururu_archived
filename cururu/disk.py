from pjdata.aux.compression import pack_data, unpack_data


def save(filename, obj):
    """Compress and save a python object (Data, Transformer) as a file."""
    f = open(filename, 'wb')
    f.write(pack_data(obj))
    f.close()


def load(filename):
    """Load a compressed python object (Data, Transformer) from file."""
    f = open(filename, 'rb')
    res = unpack_data(f.read())
    f.close()
    return res


def save_txt(filename, text):
    """Save text as a file."""
    f = open(filename, 'w')
    f.write(text)
    f.close()
