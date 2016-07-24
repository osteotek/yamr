import unittest
from map_libs.base_mapper import WordCountMapper


class TestMappers(unittest.TestCase):
    def test_base_word_count(self):
        m = WordCountMapper()

        data = "aa yy jj aa"
        r = m.run_map(data)
        exp = [("aa", 1), ("yy", 1), ("jj", 1), ("aa", 1)]

        self.assertListEqual(exp, r)

    def test_base_word_count_with_spaces(self):
        m = WordCountMapper()

        data = "  aa cc   yy jj   aa"
        r = m.run_map(data)
        exp = [("aa", 1), ("cc", 1), ("yy", 1), ("jj", 1), ("aa", 1)]

        self.assertListEqual(exp, r)

    def test_base_word_count_with_commas(self):
        m = WordCountMapper()

        data = "  aa cc.   yy jj,   aa"
        r = m.run_map(data)
        exp = [("aa", 1), ("cc", 1), ("yy", 1), ("jj", 1), ("aa", 1)]

        self.assertListEqual(exp, r)

if __name__ == '__main__':
    unittest.main()
