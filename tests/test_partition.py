import unittest
from mapper import Mapper
from hash_partitioner import HashPartitioner


class TestPartition(unittest.TestCase):

    @staticmethod
    def create_map():
        return Mapper({"jt_addr": "http://localhost:11111", "base_dir": "/tmp/tst"}, None, "addr", "map")

    def test_partition_for_one(self):
        m = self.create_map()
        tuples = [('aa', 1), ('bb', 1), ('cc', 1)]
        r = m.partition(1, tuples)

        e = {1: [('aa', 1), ('bb', 1), ('cc', 1)]}
        self.assertDictEqual(e, r)

    def test_partition_for_two(self):
        m = self.create_map()
        tuples = [('mm', 1), ('cc', 1), ('bb', 1), ('aa', 1), ('mm', 1)]
        r = m.partition(2, tuples)

        e = {1: [('bb', 1)],
             2: [('aa', 1), ('cc', 1), ('mm', 1), ('mm', 1)]
             }

        self.assertDictEqual(e, r)

    def test_partition_for_tree(self):
        m = self.create_map()
        tuples = [('nlll', 1), ('moscow', 1), ('innopolis', 1), ('kazan', 1)]
        r = m.partition(3, tuples)

        e = {1: [('innopolis', 1)],
             2: [('moscow', 1)],
             3: [('kazan', 1), ('nlll', 1)]
             }

        self.assertDictEqual(e, r)

    def test_partitioning(self):
        p = HashPartitioner()
        self.assertEquals(2, p.get_partition("aa", 1, 3))
        self.assertEquals(1, p.get_partition("moasold", 1, 3))
        self.assertEquals(1, p.get_partition("bbsa", 1, 4))

if __name__ == '__main__':
    unittest.main()
