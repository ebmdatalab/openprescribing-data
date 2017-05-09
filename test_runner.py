import unittest

from runner import Source

Source.LOG_PATH = 'test-data/log.json'


class TestSource(unittest.TestCase):
    def test_filename_arg_with_no_regex(self):
        source = Source({})
        self.assertEqual('bnf_codes.csv', source.filename_arg('import_bnf_codes --filename bnf_codes.csv'))

    def test_filname_arg_with_regex(self):
        source = Source({})
        self.assertEqual('adqs_.*csv', source.filename_arg('import_adqs --filename adqs_.*csv'))

    def test_imported_file_records(self):
        source = Source({
            'id': 'dummy_source'
        })
        records = source.imported_file_records('dummy_data.csv')
        self.assertEqual(2, len(records))
        self.assertTrue(records[0]['imported_at'] < records[1]['imported_at'])

    def test_imported_file_records_when_source_never_imported(self):
        source = Source({
            'id': 'new_source'
        })

        self.assertEqual([], source.imported_file_records('.*'))

    def test_imported_file_records_when_source_matching_regex_never_imported(self):
        source = Source({
            'id': 'dummy_source'
        })
        self.assertEqual([], source.imported_file_records('dummy_records.csv'))


if __name__ == '__main__':
    unittest.main()
