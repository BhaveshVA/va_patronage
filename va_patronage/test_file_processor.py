import unittest
from va_patronage.file_processor import FileProcessor

class TestFileProcessor(unittest.TestCase):
    def setUp(self):
        # Use minimal config, no Spark
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
        # Pass None for spark
        self.processor = FileProcessor(None, self.config)

    def test_source_directories(self):
        dirs = self.processor.source_directories()
        self.assertIsInstance(dirs, list)
        self.assertEqual(len(dirs), 4)

    def test_get_all_files(self):
        files = self.processor.get_all_files([], 0, 0)
        self.assertIsInstance(files, list)

if __name__ == "__main__":
    unittest.main()
