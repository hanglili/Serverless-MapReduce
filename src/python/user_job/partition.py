import hashlib


def partition(key, num_bins):
    """
    :param key: key of intermediate data which can be of any type
    :param num_bins: number of bins that a key could be partitioned to (this is equal to number of reducers)
    :returns an integer that denotes the bin assigned to this key
    """
    key_hashed = int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % 10**8
    return key_hashed % num_bins
