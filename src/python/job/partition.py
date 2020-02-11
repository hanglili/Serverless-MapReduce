def partition(key, num_bins):
    key_hashed = abs(hash(key)) % (10 ** 8)
    return key_hashed % num_bins
