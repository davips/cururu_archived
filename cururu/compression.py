import _pickle as pickle
import lz4.frame as lz

import zstd as zs
# from numpy import ndarray
import numpy


def pack_data(obj):
    # pickled = pickle.dumps(obj) # 1169_airlines explodes here with RAM < ?
    if isinstance(obj, numpy.ndarray):
        h, w = obj.shape
        fast_reduced = lz.compress(obj.reshape(w * h), compression_level=1)
    else:
        pickled = pickle.dumps(obj)
        fast_reduced = lz.compress(b'Data'+pickled, compression_level=1)
    return zs.compress(fast_reduced)


def unpack_data(dump, w=None, h=None):
    decompressed = zs.decompress(dump)
    fast_decompressed = lz.decompress(decompressed)
    if fast_decompressed[:4] == b'Data':
        return pickle.loads(fast_decompressed[4:])
    else:
        return numpy.reshape(numpy.frombuffer(fast_decompressed), newshape=(h, w))

    # 1169_airlines explodes here with RAM < ?
    # return pickle.loads(fast_decompressed)


def pack_object(obj):
    """
    Nondeterministic (fast) parallel compression!
    Due to multithreading, blosc is nondeterministic and useless for UUIDs.
    :param obj:
    :return:
    """
    import blosc
    pickled = pickle.dumps(obj)
    fast_reduced = lz.compress(pickled, compression_level=1)
    return blosc.compress(fast_reduced,
                          shuffle=blosc.NOSHUFFLE, cname='zstd', clevel=3)


def unpack_object(dump):
    import blosc
    decompressed = blosc.decompress(dump)
    fast_decompressed = lz.decompress(decompressed)
    return pickle.loads(fast_decompressed)
