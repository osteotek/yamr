import unittest
from map_libs.word_count import Mapper


class TestMappers(unittest.TestCase):
    def test_base_word_count(self):
        m = Mapper()

        data = "aa yy jj aa"
        r = m.run_map(data)
        exp = [("aa", 1), ("yy", 1), ("jj", 1), ("aa", 1)]

        self.assertListEqual(exp, r)

    def test_base_word_count_with_spaces(self):
        m = Mapper()

        data = "  aa cc   yy jj   aa"
        r = m.run_map(data)
        exp = [("aa", 1), ("cc", 1), ("yy", 1), ("jj", 1), ("aa", 1)]

        self.assertListEqual(exp, r)

    def test_base_word_count_with_commas(self):
        m = Mapper()

        data = "  aa cc.   yy jj,   aa"
        r = m.run_map(data)
        exp = [("aa", 1), ("cc", 1), ("yy", 1), ("jj", 1), ("aa", 1)]

        self.assertListEqual(exp, r)

    def test_word_count_with_mappers(self):
        m = Mapper()

        data = "aa cc\naa bb"
        r = m.run_map(data)
        exp = [("aa", 1), ("cc", 1), ("aa", 1), ("bb", 1)]

        self.assertListEqual(exp, r)

if __name__ == '__main__':
    unittest.main()
