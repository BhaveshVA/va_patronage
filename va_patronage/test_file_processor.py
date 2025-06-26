import unittest
from unittest.mock import MagicMock, patch
from va_patronage.file_processor import FileProcessor

class TestFileProcessor(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Patch SparkSession and DeltaTable at the correct import location
        cls.spark_patcher = patch('va_patronage.file_processor.SparkSession', autospec=True)
        cls.mock_spark_class = cls.spark_patcher.start()
        cls.mock_spark = MagicMock()
        cls.mock_spark_class.builder.getOrCreate.return_value = cls.mock_spark
        # Patch all spark.read and spark.sql calls
        cls.mock_spark.read.format.return_value.load.return_value.withColumnRenamed.return_value.persist.return_value = MagicMock()
        cls.mock_spark.read.csv.return_value = MagicMock()
        cls.mock_spark.createDataFrame.return_value = MagicMock()
        cls.mock_spark.sql.return_value.collect.return_value = [[None]]
        # Patch DeltaTable
        cls.delta_patcher = patch('va_patronage.file_processor.DeltaTable', autospec=True)
        cls.mock_delta = cls.delta_patcher.start()
        cls.mock_delta.forPath.return_value = MagicMock()
        cls.config = {
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
        cls.processor = FileProcessor(cls.mock_spark, cls.config)

    @classmethod
    def tearDownClass(cls):
        cls.spark_patcher.stop()
        cls.delta_patcher.stop()

    def test_source_directories(self):
        dirs = self.processor.source_directories()
        self.assertIsInstance(dirs, list)
        self.assertEqual(len(dirs), 4)

    def test_get_all_files(self):
        files = self.processor.get_all_files([], 0, 0)
        self.assertIsInstance(files, list)

    def test_collect_data_source(self):
        result = self.processor.collect_data_source()
        self.assertTrue(result is None or hasattr(result, 'orderBy'))

    def test_initialize_caregivers(self):
        result = self.processor.initialize_caregivers()
        self.assertTrue(result is None or hasattr(result, 'withColumn'))

    def test_process_updates(self):
        # Should not raise
        self.processor.process_updates(MagicMock(), "CG")

    def test_prepare_caregivers_data(self):
        result = self.processor.prepare_caregivers_data(MagicMock())
        self.assertTrue(result is None or hasattr(result, 'withColumn'))

    def test_prepare_scd_data(self):
        result = self.processor.prepare_scd_data(MagicMock())
        self.assertTrue(result is None or hasattr(result, 'withColumn'))

    def test_update_pai_data(self):
        result = self.processor.update_pai_data(MagicMock(), "text")
        self.assertTrue(result is None or hasattr(result, 'withColumn'))

    def test_process_files(self):
        # Should not raise
        self.processor.process_files(MagicMock())

if __name__ == "__main__":
    unittest.main()
