import unittest
from map_libs.word_count import Mapper
import map_libs.max_year_temp


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

    def test_max_temp(self):
        m = map_libs.max_year_temp.Mapper()

        data = "(201504, 31.2), (201503, 42)"

        r = m.run_map(data)
        exp = [(2015, 31.2), (2015, 42)]
        self.assertListEqual(exp, r)

    def test_max_temp_reducer(self):
        m = map_libs.max_year_temp.Reducer()

        data = [(2015, 31.2), (2015, 41), (2016, 11)]
        r = m.run_reduce(data)
        exp = [(2015, 41), (2016, 11)]
        self.assertListEqual(exp, r)


if __name__ == '__main__':
    unittest.main()
