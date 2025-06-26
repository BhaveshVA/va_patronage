import unittest
from unittest.mock import MagicMock
from va_patronage.file_processor import FileProcessor

class TestFileProcessor(unittest.TestCase):
    def setUp(self):
        self.config = {
            'initial_cg_file': 'dummy.csv',
            'cg_source': 'dummy_cg',
            'scd_source': 'dummy_scd',
            'pt_old_source': 'dummy_pt_old',
            'pt_new_source': 'dummy_pt_new',
            'patronage_tablename': 'dummy_table',
            'patronage_table_location': 'dummy_loc',
            'fullname': 'dummy_full',
            'no_of_files': 0,
            'cg_start_datetime': '2025-01-01 00:00:00',
            'others_start_datetime': '2025-01-01 00:00:00',
            'identity_correlations_path': 'dummy_delta'
        }
        self.mock_spark = MagicMock()
        self.processor = FileProcessor(self.mock_spark, self.config)

    def test_source_directories(self):
        dirs = self.processor.source_directories()
        self.assertIsInstance(dirs, list)
        self.assertEqual(len(dirs), 4)
        self.assertEqual(dirs, [
            self.config['cg_source'],
            self.config['scd_source'],
            self.config['pt_old_source'],
            self.config['pt_new_source']
        ])

    def test_get_all_files(self):
        files = self.processor.get_all_files([], 0, 0)
        self.assertIsInstance(files, list)
        self.assertEqual(files, [])

if __name__ == "__main__":
    unittest.main()
