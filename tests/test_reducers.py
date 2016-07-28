import unittest
from map_libs.word_count import Reducer


class TestReducers(unittest.TestCase):
    def test_data_combine(self):
        data = [('a', 1), ('a', 1), ('a', 1), ('cc', 1), ('dd', 1), ('dd', 1)]
        r = Reducer()
        e = [('a', [1, 1, 1]), ('cc', [1]), ('dd', [1, 1])]
        self.assertListEqual(e, r.combine_data(data))

    def test_reduce_method_word_count(self):
        r = Reducer()
        r.tuples = []
        r.reduce("app", [1, 1, 1, 1, 1, 1, 1])
        self.assertListEqual([('app', 7)], r.tuples)

    def test_reduce_all(self):
        data = [('a', 1), ('a', 1), ('a', 1), ('cc', 1), ('dd', 1), ('dd', 1), ('zz', 1), ('zz', 1)]
        r = Reducer()

        e = [('a', 3), ('cc', 1), ('dd', 2), ('zz', 2)]
        self.assertListEqual(e, r.run_reduce(data))

if __name__ == '__main__':
    unittest.main()
