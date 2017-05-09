import unittest

from runner import Source


class TestSource(unittest.TestCase):
    def test_filename_arg_with_no_regex(self):
        source = Source({})
        self.assertEqual('bnf_codes.csv', source.filename_arg('import_bnf_codes --filename bnf_codes.csv'))

    def test_filname_arg_with_regex(self):
        source = Source({})
        self.assertEqual('adqs_.*csv', source.filename_arg('import_adqs --filename adqs_.*csv'))


if __name__ == '__main__':
    unittest.main()
