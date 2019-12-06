import _pickle as pickle
import lz4.frame as lz


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

