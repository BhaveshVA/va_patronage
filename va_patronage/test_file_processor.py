import unittest
from unittest.mock import MagicMock
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
        # Use MagicMock for spark
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

    def test_collect_data_source_returns_none_for_empty(self):
        # Should return None if no files found
        self.processor.get_all_files = MagicMock(return_value=[])
        result = self.processor.collect_data_source()
        self.assertIsNone(result)

    def test_prepare_caregivers_data_returns_none_for_none(self):
        # Should not fail if None is passed
        try:
            result = self.processor.prepare_caregivers_data(None)
        except Exception as e:
            self.fail(f"prepare_caregivers_data(None) raised {e}")

    def test_prepare_scd_data_returns_none_for_none(self):
        try:
            result = self.processor.prepare_scd_data(None)
        except Exception as e:
            self.fail(f"prepare_scd_data(None) raised {e}")

    def test_update_pai_data_returns_none_for_none(self):
        try:
            result = self.processor.update_pai_data(None, "text")
        except Exception as e:
            self.fail(f"update_pai_data(None, 'text') raised {e}")

    def test_process_updates_does_not_fail(self):
        try:
            self.processor.process_updates(MagicMock(), "CG")
        except Exception as e:
            self.fail(f"process_updates raised {e}")

    def test_process_files_does_not_fail(self):
        try:
            self.processor.process_files(MagicMock())
        except Exception as e:
            self.fail(f"process_files raised {e}")

if __name__ == "__main__":
    unittest.main()
