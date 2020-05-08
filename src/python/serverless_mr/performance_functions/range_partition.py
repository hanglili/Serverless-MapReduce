def partition(key, num_bins):
    """
    :param key: key of intermediate data which can be of any type
    :param num_bins: number of bins that a key could be partitioned to (this is equal to number of reducers)
    :returns an integer that denotes the bin assigned to this key
    """
    key_hashed = int(float(key) * num_bins)
    return key_hashed
