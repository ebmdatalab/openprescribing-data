import unittest
import os

os.environ['OPENP_DATA_BASEDIR'] = 'test-data'

from runner import Source

Source.LOG_PATH = 'test-data/log.json'


class TestSource(unittest.TestCase):
    def setUp(self):
        self.source = Source({
          'id': 'dummy_source',
          'importers': [
            'import_dummy_data --filename dummy_data.csv'
          ],
        })

    def test_filename_arg_with_no_regex(self):
        source = Source({})
        self.assertEqual('bnf_codes.csv', source.filename_arg('import_bnf_codes --filename bnf_codes.csv'))

    def test_filname_arg_with_regex(self):
        source = Source({})
        self.assertEqual('adqs_.*csv', source.filename_arg('import_adqs --filename adqs_.*csv'))

    def test_imported_file_records(self):
        records = self.source.imported_file_records('dummy_data.csv')
        self.assertEqual(2, len(records))
        self.assertTrue(records[0]['imported_at'] < records[1]['imported_at'])

    def test_imported_file_records_when_source_never_imported(self):
        source = Source({
          'id': 'new_source'
        })

        self.assertEqual([], source.imported_file_records('.*'))

    def test_imported_file_records_when_source_matching_regex_never_imported(self):
        self.assertEqual([], self.source.imported_file_records('dummy_records.csv'))

    def test_files_by_date(self):
        expected = [
            'test-data/dummy_source/2017_01/dummy_data.csv',
            'test-data/dummy_source/2017_02/dummy_data.csv',
            'test-data/dummy_source/2017_03/dummy_data.csv',
        ]
        self.assertEqual(expected, self.source.files_by_date(self.source['importers'][0]))

    def test_unimported_files(self):
        expected = [
            'test-data/dummy_source/2017_03/dummy_data.csv',
        ]
        self.assertEqual(expected, self.source.unimported_files(self.source['importers'][0]))

    def test_importer_cmds_with_latest_data(self):
        expected = [
            'import_dummy_data --filename test-data/dummy_source/2017_03/dummy_data.csv',
        ]
        self.assertEqual(expected, self.source.importer_cmds_with_latest_data())


if __name__ == '__main__':
    unittest.main()
