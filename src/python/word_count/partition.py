import hashlib


def partition_function(key, num_bins):
    key_hashed = int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16) % 10**8
    return key_hashed % num_bins
