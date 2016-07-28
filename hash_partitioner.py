import binascii


class HashPartitioner:
    def get_partition(self, key, value, rds_count):
        b = str(key).encode('utf-8')
        hex = binascii.hexlify(b)
        n = int(hex, 16)

        return n % rds_count