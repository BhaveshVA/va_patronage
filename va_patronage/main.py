from pyspark.sql import SparkSession
from file_processor import FileProcessor

def main():
    spark = SparkSession.builder.getOrCreate()
    config = {
        'initial_cg_file': 'dbfs:/FileStore/All_Caregivers_InitialSeed_12182024_csv.csv',
        'cg_source': '/mnt/ci-carma/landing/',
        'scd_source': '/mnt/ci-vadir-shared/',
        'pt_old_source': '/mnt/ci-patronage/pai_landing/',
        'pt_new_source': '/mnt/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_PT/',
        'patronage_tablename': 'mypatronage_test111',
        'patronage_table_location': 'dbfs:/user/hive/warehouse/',
        'fullname': 'dbfs:/user/hive/warehouse/mypatronage_test111',
        'no_of_files': 0,  # This should be dynamically set if needed
        'cg_start_datetime': '2024-12-18 23:59:59',
        'others_start_datetime': '2025-04-01 00:00:00',
        'identity_correlations_path': '/mnt/Patronage/identity_correlations'
    }
    processor = FileProcessor(spark, config)
    # Example: processor.collect_data_source()

if __name__ == "__main__":
    main()
