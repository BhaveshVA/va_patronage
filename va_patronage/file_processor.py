from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import time

class FileProcessor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.initial_cg_file = config.get('initial_cg_file')
        self.cg_source = config.get('cg_source')
        self.scd_source = config.get('scd_source')
        self.pt_old_source = config.get('pt_old_source')
        self.pt_new_source = config.get('pt_new_source')
        self.patronage_tablename = config.get('patronage_tablename')
        self.patronage_table_location = config.get('patronage_table_location')
        self.fullname = config.get('fullname')
        self.no_of_files = config.get('no_of_files')
        self.cg_start_datetime = config.get('cg_start_datetime')
        self.others_start_datetime = config.get('others_start_datetime')
        self.identity_correlations_path = config.get('identity_correlations_path')
        self.icn_relationship = (
            spark.read.format("delta")
            .load(self.identity_correlations_path)
            .withColumnRenamed('MVIPersonICN', 'ICN')
        ).persist()

        # Schemas
        self.new_cg_schema = StructType([
            StructField("Discharge_Revocation_Date__c", StringType(), True),
            StructField("Caregiver_Status__c", StringType(), True),
            StructField("CreatedById", StringType(), True),
            StructField("Dispositioned_Date__c", StringType(), True),
            StructField("CARMA_Case_ID__c", StringType(), True),
            StructField("Applicant_Type__c", StringType(), True),
            StructField("CreatedDate", StringType(), True),
            StructField("Veteran_ICN__c", StringType(), True),
            StructField("Benefits_End_Date__c", StringType(), True),
            StructField("Caregiver_Id__c", StringType(), True),
            StructField("CARMA_Case_Number__c", StringType(), True),
            StructField("Caregiver_ICN__c", StringType(), True),
        ])
        self.scd_schema = StructType([
            StructField("PTCPNT_ID", StringType()),
            StructField("FILE_NBR", StringType()),
            StructField("LAST_NM", StringType()),
            StructField("FIRST_NM", StringType()),
            StructField("MIDDLE_NM", StringType()),
            StructField("SUFFIX_NM", StringType()),
            StructField("STA_NBR", StringType()),
            StructField("BRANCH_OF_SERV", StringType()),
            StructField("DATE_OF_BIRTH", StringType()),
            StructField("DATE_OF_DEATH", StringType()),
            StructField("VET_SSN_NBR", StringType()),
            StructField("SVC_NBR", StringType()),
            StructField("AMT_GROSS_OR_NET_AWARD", IntegerType()),
            StructField("AMT_NET_AWARD", IntegerType()),
            StructField("NET_AWARD_DATE", StringType()),
            StructField("SPECL_LAW_IND", IntegerType()),
            StructField("VET_SSN_VRFCTN_IND", IntegerType()),
            StructField("WIDOW_SSN_VRFCTN_IND", IntegerType()),
            StructField("PAYEE_SSN", StringType()),
            StructField("ADDRS_ONE_TEXT", StringType()),
            StructField("ADDRS_TWO_TEXT", StringType()),
            StructField("ADDRS_THREE_TEXT", StringType()),
            StructField("ADDRS_CITY_NM", StringType()),
            StructField("ADDRS_ST_CD", StringType()),
            StructField("ADDRS_ZIP_PREFIX_NBR", IntegerType()),
            StructField("MIL_POST_OFFICE_TYP_CD", StringType()),
            StructField("MIL_POSTAL_TYPE_CD", StringType()),
            StructField("COUNTRY_TYPE_CODE", IntegerType()),
            StructField("SUSPENSE_IND", IntegerType()),
            StructField("PAYEE_NBR", IntegerType()),
            StructField("EOD_DT", StringType()),
            StructField("RAD_DT", StringType()),
            StructField("ADDTNL_SVC_IND", StringType()),
            StructField("ENTLMT_CD", StringType()),
            StructField("DSCHRG_PAY_GRADE_NM", StringType()),
            StructField("AMT_OF_OTHER_RETIREMENT", IntegerType()),
            StructField("RSRVST_IND", StringType()),
            StructField("NBR_DAYS_ACTIVE_RESRV", IntegerType()),
            StructField("CMBNED_DEGREE_DSBLTY", StringType()),
            StructField("DSBL_DTR_DT", StringType()),
            StructField("DSBL_TYP_CD", StringType()),
            StructField("VA_SPCL_PROV_CD", IntegerType()),
        ])
        self.scd_schema1 = StructType([
            StructField("PTCPNT_ID", StringType()),
            StructField("CMBNED_DEGREE_DSBLTY", StringType()),
            StructField("DSBL_DTR_DT", StringType()),
        ])
        self.file_list_schema = StructType([
            StructField("path", StringType()),
            StructField("name", StringType()),
            StructField("size", StringType()),
            StructField("modificationTime", StringType()),
        ])

    def source_directories(self):
        return [self.cg_source, self.scd_source, self.pt_old_source, self.pt_new_source]

    def get_all_files(self, raw_file_folders, cg_unix_start_time, others_unix_start_time):
        # Recursively lists files at any depth inside a directory based on filtering criteria.
        all_files = []
        for folder in raw_file_folders:
            try:
                for dir_path in self.spark._jvm.dbutils.fs.ls(folder):
                    path = dir_path.path()
                    name = dir_path.name()
                    size = str(dir_path.size())
                    modificationTime = str(dir_path.modificationTime())
                    if name.startswith("caregiverevent") and name.endswith(".csv") and int(modificationTime) > cg_unix_start_time:
                        all_files.append({'path': path, 'name': name, 'size': size, 'modificationTime': modificationTime})
                    elif name.startswith("CPIDODIEX_") and name.endswith(".csv") and "NEW" not in name and int(modificationTime) > others_unix_start_time:
                        all_files.append({'path': path, 'name': name, 'size': size, 'modificationTime': modificationTime})
                    elif name.startswith("WRTS") and name.endswith(".txt") and int(modificationTime) > others_unix_start_time:
                        all_files.append({'path': path, 'name': name, 'size': size, 'modificationTime': modificationTime})
                    elif path.startswith("dbfs:/mnt/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_P") and name.endswith("parquet") and int(modificationTime) > others_unix_start_time:
                        all_files.append({'path': path, 'name': name, 'size': size, 'modificationTime': modificationTime})
            except Exception as e:
                continue
        return all_files

    def collect_data_source(self):
        raw_file_folders = self.source_directories()
        cg_unix_start_time = int(time.mktime(datetime.strptime(self.cg_start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000
        others_unix_start_time = int(time.mktime(datetime.strptime(self.others_start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000
        query = f"""
            SELECT COALESCE(MAX(SDP_Event_Created_Timestamp), TIMESTAMP('{others_unix_start_time}')) AS max_date 
            FROM {self.patronage_tablename}
        """
        max_processed_date = (
            datetime.fromtimestamp(others_unix_start_time / 1000)
            if self.no_of_files == 1
            else self.spark.sql(query).collect()[0][0]
        )
        now = datetime.now()
        yesterday_end_time = datetime(now.year, now.month, now.day) - timedelta(hours=4)
        yesterday_end_time_ts = int(yesterday_end_time.timestamp() * 1000)
        master_file_list = self.get_all_files(raw_file_folders, cg_unix_start_time, others_unix_start_time)
        if not master_file_list:
            return None
        file_list_df = self.spark.createDataFrame(master_file_list, self.file_list_schema)
        filtered_file_list_df = (
            file_list_df.withColumn("dateTime", to_timestamp(col("modificationTime") / 1000))
            .filter((col("dateTime") > max_processed_date) & (col("modificationTime") <= yesterday_end_time_ts))
        )
        parquet_PT_flag = (
            filtered_file_list_df.filter(filtered_file_list_df["path"].contains("parquet"))
            .orderBy(desc(col("modificationTime")))
            .limit(1)
        )
        all_other_files = filtered_file_list_df.filter(~filtered_file_list_df["path"].contains("parquet"))
        files_to_process_now = all_other_files.unionAll(parquet_PT_flag)
        if files_to_process_now.count() > 0:
            return files_to_process_now.orderBy(col("modificationTime"))
        else:
            return None

    def initialize_caregivers(self):
        new_cg_df = self.spark.read.csv(self.initial_cg_file, header=True, inferSchema=True)
        transformed_cg_df = new_cg_df.select(
            substring("ICN", 1, 10).alias("ICN"),
            "Applicant_Type",
            "Caregiver_Status",
            date_format("Status_Begin_Date", "yyyyMMdd").alias("Status_Begin_Date"),
            date_format("Status_Termination_Date", "yyyyMMdd").alias("Status_Termination_Date"),
            substring("Veteran_ICN", 1, 10).alias("Veteran_ICN"),
        )
        edipi_df = (
            broadcast(transformed_cg_df)
            .join(self.icn_relationship, ["ICN"], "left")
            .withColumn("filename", lit(self.initial_cg_file))
            .withColumn("SDP_Event_Created_Timestamp", lit(self.cg_start_datetime).cast(TimestampType()))
            .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
            .withColumn("PT_Indicator", lit(None).cast(StringType()))
            .withColumn("SC_Combined_Disability_Percentage", lit(None).cast(StringType()))
            .withColumn("RecordStatus", lit(True).cast(BooleanType()))
            .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
            .withColumn("Status_Last_Update", lit(None).cast(StringType()))
            .withColumn("sentToDoD", lit(False).cast(BooleanType()))
            .withColumn("Batch_CD", lit("CG").cast(StringType()))
        )
        return edipi_df

    def process_updates(self, edipi_df, file_type):
        # Define join, delta, columns_to_track, merge_conditions, concat_column as in notebook
        join_conditions = {
            "CG": (
                (col("ICN") == col("target_ICN"))
                & (col("Veteran_ICN") == col("target_Veteran_ICN"))
                & (col("Batch_CD") == col("target_Batch_CD"))
                & (col("Applicant_Type") == col("target_Applicant_Type"))
                & (col("target_RecordStatus") == True)
            ),
            "SCD": (
                (col("ICN") == col("target_ICN"))
                & (col("target_RecordStatus") == True)
                & (col("Batch_CD") == col("target_Batch_CD"))
            ),
        }
        delta_conditions = {
            "CG": xxhash64(
                col("Status_Begin_Date"),
                col("Status_Termination_Date"),
                col("Applicant_Type"),
                col("Caregiver_Status")
            ) != xxhash64(
                col("target_Status_Begin_Date"),
                col("target_Status_Termination_Date"),
                col("target_Applicant_Type"),
                col("target_Caregiver_Status")
            ),
            "SCD": xxhash64(
                col("SC_Combined_Disability_Percentage")
            ) != xxhash64(col("target_SC_Combined_Disability_Percentage")),
        }
        columns_to_track = {
            "CG": [
                ("Status_Begin_Date", "target_Status_Begin_Date"),
                ("Status_Termination_Date", "target_Status_Termination_Date"),
                ("Applicant_Type", "target_Applicant_Type"),
                ("Caregiver_Status", "target_Caregiver_Status")
            ],
            "SCD": [
                ("SC_Combined_Disability_Percentage", "target_SC_Combined_Disability_Percentage"),
            ],
            "PAI": [
                ("PT_Indicator", "target_PT_Indicator")
            ]
        }
        merge_conditions = {
            "CG": "concat(target.ICN, target.Veteran_ICN, target.Applicant_Type) = source.MERGEKEY and target.RecordStatus = True",
            "SCD": "((target.ICN = source.MERGEKEY) and (target.Batch_CD = source.Batch_CD) and (target.RecordStatus = True))"
        }
        concat_column = {
            "CG": concat(col("ICN"), col("Veteran_ICN"), col("Applicant_Type")),
            "SCD": col("ICN")
        }
        targetTable = DeltaTable.forPath(self.spark, self.fullname)
        targetDF = targetTable.toDF().filter((col("Batch_CD") == file_type) & (col("RecordStatus") == True))
        targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])
        upsert_df = None
        if file_type in ["CG", "SCD"]:
            joinDF = broadcast(edipi_df).join(targetDF, join_conditions[file_type], "leftouter")
            if file_type == "SCD":
                joinDF = (
                    joinDF.withColumn("Status_Last_Update", col("DSBL_DTR_DT"))
                    .withColumn("Status_Begin_Date", coalesce(col("target_Status_Begin_Date"), col("DSBL_DTR_DT")))
                    .withColumn("PT_Indicator", coalesce(joinDF["target_PT_Indicator"], lit("N")))
                )
            filterDF = joinDF.filter(delta_conditions[file_type])
            mergeDF = filterDF.withColumn("MERGEKEY", concat_column[file_type])
            dummyDF = filterDF.filter(col("target_ICN").isNotNull()).withColumn("MERGEKEY", lit(None))
            upsert_df = mergeDF.union(dummyDF)
        elif file_type == "PAI":
            upsert_df = edipi_df
        change_conditions = []
        for source_col, target_col in columns_to_track[file_type]:
            change_condition = when(
                xxhash64(coalesce(col(source_col), lit("Null"))) != xxhash64(coalesce(col(target_col), lit("Null"))),
                concat_ws(
                    " ",
                    lit(source_col),
                    lit("old value:"),
                    coalesce(col(target_col), lit("Null")),
                    lit("changed to new value:"),
                    coalesce(col(source_col), lit("Null")),
                ),
            ).otherwise(lit(""))
            change_conditions.append(change_condition)
        new_record_condition = when(col("target_icn").isNull(), lit("New Record")).otherwise(lit("Updated Record"))
        if upsert_df is None:
            return
        upsert_df = upsert_df.withColumn("RecordChangeStatus", new_record_condition)
        if len(change_conditions) > 0:
            change_log_col = concat_ws(" ", *[coalesce(cond, lit("")) for cond in change_conditions])
        else:
            change_log_col = lit("")
        upsert_df = upsert_df.withColumn("change_log", change_log_col)
        if file_type == "PAI":
            file_type = "SCD"
        targetTable.alias("target").merge(
            upsert_df.alias("source"), merge_conditions[file_type]
        ).whenMatchedUpdate(
            set={
                "RecordStatus": "False",
                "RecordLastUpdated": "source.SDP_Event_Created_Timestamp",
                "sentToDoD": "true",
                "RecordChangeStatus": lit("Expired Record"),
            }
        ).whenNotMatchedInsert(
            values={
                "edipi": "source.edipi",
                "ICN": "source.ICN",
                "Veteran_ICN": "source.Veteran_ICN",
                "Applicant_Type": "source.Applicant_Type",
                "Caregiver_Status": "source.Caregiver_Status",
                "participant_id": "source.participant_id",
                "Batch_CD": "source.Batch_CD",
                "SC_Combined_Disability_Percentage": "source.SC_Combined_Disability_Percentage",
                "PT_Indicator": "source.PT_Indicator",
                "Individual_Unemployability": "source.Individual_Unemployability",
                "Status_Begin_Date": "source.Status_Begin_Date",
                "Status_Last_Update": "source.Status_Last_Update",
                "Status_Termination_Date": "source.Status_Termination_Date",
                "SDP_Event_Created_Timestamp": "source.SDP_Event_Created_Timestamp",
                "RecordStatus": "true",
                "RecordLastUpdated": "source.RecordLastUpdated",
                "filename": "source.filename",
                "sentToDoD": "false",
                "change_log": "source.change_log",
                "RecordChangeStatus": "source.RecordChangeStatus",
            }
        ).execute()

    def prepare_caregivers_data(self, cg_csv_files):
        Window_Spec = Window.partitionBy("ICN", "Veteran_ICN", "Applicant_Type").orderBy(desc("Event_Created_Date"))
        cg_csv_files_to_process = cg_csv_files.select(collect_list("path")).first()[0]
        cg_df = (
            self.spark.read.schema(self.new_cg_schema)
            .csv(cg_csv_files_to_process, header=True, inferSchema=False)
            .selectExpr("*", "_metadata.file_name as filename", "_metadata.file_modification_time as SDP_Event_Created_Timestamp")
        )
        combined_cg_df = (
            cg_df.select(
                substring("Caregiver_ICN__c", 1, 10).alias("ICN"),
                substring("Veteran_ICN__c", 1, 10).alias("Veteran_ICN"),
                date_format("Benefits_End_Date__c", "yyyyMMdd").alias("Status_Termination_Date").cast(StringType()),
                col("Applicant_Type__c").alias("Applicant_Type"),
                col("Caregiver_Status__c").alias("Caregiver_Status"),
                date_format("Dispositioned_Date__c", "yyyyMMdd").alias("Status_Begin_Date").cast(StringType()),
                col("CreatedDate").cast("timestamp").alias("Event_Created_Date"),
                "filename",
                "SDP_Event_Created_Timestamp",
            )
        ).filter(col("Caregiver_ICN__c").isNotNull())
        edipi_df = (
            broadcast(combined_cg_df)
            .join(self.icn_relationship, ["ICN"], "left")
            .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
            .withColumn("PT_Indicator", lit(None).cast(StringType()))
            .withColumn("SC_Combined_Disability_Percentage", lit(None).cast(StringType()))
            .withColumn("RecordStatus", lit(True).cast(BooleanType()))
            .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
            .withColumn("Status_Last_Update", lit(None).cast(StringType()))
            .withColumn("sentToDoD", lit(False).cast(BooleanType()))
            .withColumn("Batch_CD", lit("CG").cast(StringType()))
            .withColumn("rank", rank().over(Window_Spec))
            .filter(col("rank") == 1)
            .dropDuplicates()
            .drop("rank", "va_profile_id", "record_updated_date")
        ).orderBy(col("Event_Created_Date"))
        return edipi_df

    def prepare_scd_data(self, row):
        file_name = getattr(row, 'path', None)
        if not file_name:
            return None
        if len(self.spark.read.csv(file_name).columns) != 3:
            schema = self.scd_schema
        else:
            schema = self.scd_schema1
        scd_updates_df = (
            self.spark.read.csv(file_name, schema=schema, header=True, inferSchema=False)
            .selectExpr(
                "PTCPNT_ID as participant_id", "CMBNED_DEGREE_DSBLTY", "DSBL_DTR_DT", "_metadata.file_name as filename", "_metadata.file_modification_time as SDP_Event_Created_Timestamp"
            )
            .withColumn("sentToDoD", lit(False).cast(BooleanType()))
            .withColumn(
                "SC_Combined_Disability_Percentage",
                lpad(
                    coalesce(
                        when(col("CMBNED_DEGREE_DSBLTY") == "", lit("000")).otherwise(col("CMBNED_DEGREE_DSBLTY"))
                    ),
                    3,
                    "0",
                ),
            )
            .withColumn(
                "DSBL_DTR_DT",
                when(col("DSBL_DTR_DT") == "", None).otherwise(date_format(to_date(col("DSBL_DTR_DT"), "MMddyyyy"), "yyyyMMdd")),
            )
        ).filter(col("DSBL_DTR_DT").isNotNull())
        Window_Spec = Window.partitionBy("participant_id").orderBy(desc("DSBL_DTR_DT"), desc("SC_Combined_Disability_Percentage"))
        edipi_df = (
            broadcast(scd_updates_df)
            .join(self.icn_relationship, ["participant_id"], "left")
            .withColumn("rank", rank().over(Window_Spec))
            .withColumn("Veteran_ICN", lit(None).cast(StringType()))
            .withColumn("Applicant_Type", lit(None).cast(StringType()))
            .withColumn("Caregiver_Status", lit(None).cast(StringType()))
            .withColumn("Individual_Unemployability", lit(None).cast(StringType()))
            .withColumn("Status_Termination_Date", lit(None).cast(StringType()))
            .withColumn("RecordLastUpdated", lit(None).cast(DateType()))
            .withColumn("Batch_CD", lit("SCD"))
            .withColumn("RecordStatus", lit(True).cast(BooleanType()))
            .filter(col("rank") == 1)
            .filter(col("ICN").isNotNull())
            .dropDuplicates()
            .drop("rank", "va_profile_id", "record_updated_date")
        )
        return edipi_df

    def update_pai_data(self, row, source_type):
        file_name = getattr(row, 'path', None)
        file_creation_dateTime = getattr(row, 'dateTime', None)
        raw_pai_df = None
        if source_type == "text" and file_name:
            raw_pai_df = (
                self.spark.read.csv(file_name, header=True, inferSchema=True)
                .selectExpr("*", "_metadata.file_name as filename", "_metadata.file_modification_time as SDP_Event_Created_Timestamp")
            )
        elif source_type == "table":
            file_creation_dateTime = getattr(row, 'dateTime', None)
            file_name = f"Updated from PA&I delta table on {file_creation_dateTime}"
            raw_pai_df = self.spark.read.format("delta").load(self.pt_new_source)
        if raw_pai_df is None:
            return None
        targetTable = DeltaTable.forPath(self.spark, self.fullname)
        targetDF = (
            targetTable.toDF()
            .filter("Batch_CD == 'SCD'")
            .filter("RecordStatus=='True'")
        )
        targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])
        pai_df = raw_pai_df.selectExpr("PTCPNT_VET_ID as participant_id", "PT_35_FLAG as source_PT_Indicator")
        joinDF = (
            pai_df.join(
                broadcast(targetDF),
                pai_df["participant_id"] == targetDF["target_participant_id"],
                "left",
            )
            .filter(targetDF["target_PT_Indicator"] == "N")
            .withColumn("filename", lit(file_name))
            .withColumn("SDP_Event_Created_Timestamp", lit(file_creation_dateTime))
        )
        filterDF = joinDF.filter(xxhash64(joinDF.source_PT_Indicator) != xxhash64(joinDF.target_PT_Indicator))
        mergeDF = filterDF.withColumn("MERGEKEY", filterDF.target_ICN)
        dummyDF = filterDF.filter("target_ICN is not null").withColumn("MERGEKEY", lit(None))
        paiDF = mergeDF.union(dummyDF)
        edipi_df = (
            paiDF.selectExpr(
                "target_edipi as edipi",
                "participant_id",
                "target_ICN",
                "MERGEKEY",
                "target_SC_Combined_Disability_Percentage as SC_Combined_Disability_Percentage",
                "target_Status_Begin_Date as Status_Begin_Date",
                "target_Status_Last_Update as Status_Last_Update",
                "SDP_Event_Created_Timestamp",
                "filename",
                "source_PT_Indicator",
                "target_PT_Indicator",
            )
            .withColumn("ICN", lit(col("target_ICN")))
            .withColumn("Veteran_ICN", lit(None))
            .withColumn("Applicant_Type", lit(None))
            .withColumn("Caregiver_Status", lit(None))
            .withColumn("Batch_CD", lit("SCD"))
            .withColumn("PT_Indicator", coalesce(col("source_PT_Indicator"), lit("N")))
            .withColumn("Individual_Unemployability", lit(None))
            .withColumn("Status_Termination_Date", lit(None))
            .withColumn("RecordLastUpdated", lit(None))
        )
        return edipi_df

    def process_files(self, files_to_process_now):
        cg_csv_files = files_to_process_now.filter(files_to_process_now["path"].contains("caregiverevent"))
        edipi_df = self.prepare_caregivers_data(cg_csv_files)
        self.process_updates(edipi_df, "CG")
        other_files = files_to_process_now.filter(~files_to_process_now["path"].contains("caregiverevent"))
        other_rows = other_files.collect()
        for row in other_rows:
            filename = row.path
            if "CPIDODIEX" in filename:
                edipi_df = self.prepare_scd_data(row)
                self.process_updates(edipi_df, "SCD")
            elif "WRTS" in filename:
                edipi_df = self.update_pai_data(row, "text")
                self.process_updates(edipi_df, "PAI")
            elif "parquet" in filename:
                edipi_df = self.update_pai_data(row, "table")
                self.process_updates(edipi_df, "PAI")
            else:
                pass
