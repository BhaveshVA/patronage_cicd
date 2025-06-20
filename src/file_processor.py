"""
FileProcessor class for Databricks ETL, extracted and modularized from notebook.
Follows PySpark and Delta Lake best practices, with configuration-driven approach.
"""
from delta.tables import DeltaTable
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Any
from datetime import datetime, timedelta
import time
import logging

class FileProcessor:
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        self.spark = spark
        self.config = config
        self.initial_cg_file = config["initial_cg_file"]
        self.cg_source = config["cg_source"]
        self.scd_source = config["scd_source"]
        self.pt_old_source = config["pt_old_source"]
        self.pt_new_source = config["pt_new_source"]
        self.patronage_tablename = config["patronage_tablename"]
        self.patronage_table_location = config["patronage_table_location"]
        self.fullname = self.patronage_table_location + self.patronage_tablename
        self.cg_start_datetime = config["cg_start_datetime"]
        self.others_start_datetime = config["others_start_datetime"]
        self.identity_correlations_path = config["identity_correlations_path"]
        self.icn_relationship = (
            spark.read.format("delta")
            .load(self.identity_correlations_path)
            .withColumnRenamed('MVIPersonICN', 'ICN')
            .persist()
        )
        self.no_of_files = self._get_no_of_files()
        self.logger = logging.getLogger(__name__)

    def _get_no_of_files(self) -> int:
        file_count_query = f"SELECT COALESCE(COUNT(DISTINCT filename), 0) AS count FROM DELTA.`{self.fullname}`"
        try:
            return self.spark.sql(file_count_query).collect()[0][0]
        except Exception:
            return 0

    def source_directories(self) -> List[str]:
        """
        Returns the list of raw file source directories.
        """
        return [self.cg_source, self.scd_source, self.pt_old_source, self.pt_new_source]

    def get_all_files(self, raw_file_folders: List[str], cg_unix_start_time: int, others_unix_start_time: int):
        """
        Recursively lists files at any depth inside a directory based on filtering criteria.
        Returns: generator of file metadata objects
        """
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(self.spark)
        for folder in raw_file_folders:
            for dir_path in dbutils.fs.ls(folder):
                if dir_path.isFile():
                    if (
                        (dir_path.name.startswith("caregiverevent") and dir_path.name.endswith(".csv") and dir_path.modificationTime > cg_unix_start_time)
                        or (dir_path.name.startswith("CPIDODIEX_") and dir_path.name.endswith(".csv") and "NEW" not in dir_path.name and dir_path.modificationTime > others_unix_start_time)
                        or (dir_path.name.startswith("WRTS") and dir_path.name.endswith(".txt") and dir_path.modificationTime > others_unix_start_time)
                        or (dir_path.path.startswith("dbfs:/mnt/ci-vba-edw-2/DeltaTables/DW_ADHOC_RECURR.DOD_PATRONAGE_SCD_P") and dir_path.name.endswith("parquet") and dir_path.modificationTime > others_unix_start_time)
                    ):
                        yield dir_path
                elif dir_path.isDir() and folder != dir_path.path:
                    yield from self.get_all_files([dir_path.path], cg_unix_start_time, others_unix_start_time)

    def collect_data_source(self):
        """
        Collects files from directories, filters them based on modification time, and returns a DataFrame.
        Returns: DataFrame with files ready to process.
        """
        from pyspark.sql.types import StructType, StructField, StringType
        raw_file_folders = self.source_directories()
        cg_unix_start_time = int(time.mktime(datetime.strptime(self.cg_start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000
        others_unix_start_time = int(time.mktime(datetime.strptime(self.others_start_datetime, '%Y-%m-%d %H:%M:%S').timetuple())) * 1000
        master_file_list = [file for file in self.get_all_files(raw_file_folders, cg_unix_start_time, others_unix_start_time)]
        file_list_schema = StructType([
            StructField("path", StringType()),
            StructField("name", StringType()),
            StructField("size", StringType()),
            StructField("modificationTime", StringType()),
        ])
        file_list_df = self.spark.createDataFrame([(f.path, f.name, str(f.size), str(f.modificationTime)) for f in master_file_list], file_list_schema)
        return file_list_df

    def initialize_caregivers(self):
        new_cg_df = self.spark.read.csv(
            self.initial_cg_file,
            header=True,
            inferSchema=True,
        )
        transformed_cg_df = new_cg_df.select(
            substring("ICN", 1, 10).alias("ICN"),
            "Applicant_Type",
            "Caregiver_Status",
            date_format("Status_Begin_Date", "yyyyMMdd").alias("Status_Begin_Date"),
            date_format("Status_Termination_Date", "yyyyMMdd").alias("Status_Termination_Date"),
            substring("Veteran_ICN", 1, 10).alias("Veteran_ICN"),
        )
        edipi_df = (
            transformed_cg_df
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

    def process_updates(self, edipi_df: DataFrame, file_type: str):
        """
        Upserts the input dataframe depending on input file type ('CG', 'PAI' or 'SCD').
        Uses Slowly Changing Dimensions type 2 logic that stores records that have been updated.
        Parameters: Pyspark dataframe and file type
        Returns: None
        """
        from pyspark.sql.functions import col, lit, coalesce, concat_ws, when, desc, rank, broadcast, lpad, to_date, date_format, xxhash64
        from pyspark.sql.window import Window
        # Define join, delta, and merge conditions as in notebook
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
        concat_column = {
            "CG": concat_ws("", col("ICN"), col("Veteran_ICN"), col("Applicant_Type")),
            "SCD": col("ICN")
        }
        merge_conditions = {
            "CG": "concat(target.ICN, target.Veteran_ICN, target.Applicant_Type) = source.MERGEKEY and target.RecordStatus = True",
            "SCD": "((target.ICN = source.MERGEKEY) and (target.Batch_CD = source.Batch_CD) and (target.RecordStatus = True))"
        }
        # Load the target Delta table
        targetTable = DeltaTable.forPath(self.spark, self.fullname)
        targetDF = targetTable.toDF().filter(
            (col("Batch_CD") == file_type) & (col("RecordStatus") == True)
        )
        targetDF = targetDF.select([col(c).alias(f"target_{c}") for c in targetDF.columns])
        # Join and filter for changes
        joinDF = edipi_df.join(targetDF, join_conditions[file_type], "leftouter")
        filterDF = joinDF.filter(delta_conditions[file_type])
        mergeDF = filterDF.withColumn("MERGEKEY", concat_column[file_type])
        # Change log
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
        new_record_condition = when(col("target_ICN").isNull(), lit("New Record")).otherwise(lit("Updated Record"))
        upsert_df = mergeDF.withColumn("RecordChangeStatus", new_record_condition)
        if len(change_conditions) > 0:
            change_log_col = concat_ws(" ", *[coalesce(cond, lit("")) for cond in change_conditions])
        else:
            change_log_col = lit("")
        upsert_df = upsert_df.withColumn("change_log", change_log_col)
        # Merge
        targetTable.alias("target").merge(
            upsert_df.alias("source"),
            merge_conditions[file_type]
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
    # ...existing code...

    def prepare_caregivers_data(self, cg_csv_files: DataFrame) -> DataFrame:
        """
        Filters caregivers filenames from input dataframe, aggregates data and returns dataframe
        Parameters: PySpark dataframe with all filenames and metadata that are not processed (upsert)
        Returns: Dataframe: Dataframe with required column names and edipi of a caregiver ready for upsert
        """
        self.logger.info(f"Upserting records from {cg_csv_files.count()} caregivers aggregated files")
        Window_Spec = Window.partitionBy(
            "ICN", "Veteran_ICN", "Applicant_Type"
        ).orderBy(desc("Event_Created_Date"))

        cg_csv_files_to_process = cg_csv_files.select(collect_list("path")).first()[0]
        cg_df = (
            self.spark.read.schema(self.config["new_cg_schema"])
            .csv(cg_csv_files_to_process, header=True, inferSchema=False)
            .selectExpr("*", "_metadata.file_name as filename", "_metadata.file_modification_time as SDP_Event_Created_Timestamp")
        )
        combined_cg_df = (
            cg_df.select(
                substring("Caregiver_ICN__c", 1, 10).alias("ICN"),
                substring("Veteran_ICN__c", 1, 10).alias("Veteran_ICN"),
                date_format("Benefits_End_Date__c", "yyyyMMdd")
                .alias("Status_Termination_Date")
                .cast(StringType()),
                col("Applicant_Type__c").alias("Applicant_Type"),
                col("Caregiver_Status__c").alias("Caregiver_Status"),
                date_format("Dispositioned_Date__c", "yyyyMMdd")
                .alias("Status_Begin_Date")
                .cast(StringType()),
                col("CreatedDate").cast("timestamp").alias("Event_Created_Date"),
                "filename",
                "SDP_Event_Created_Timestamp",
            )
        ).filter(col("Caregiver_ICN__c").isNotNull())
        edipi_df = (
            combined_cg_df
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

    def prepare_scd_data(self, row: Any) -> DataFrame:
        """
        Prepares SCD data from the input row. This is the disability % data.
        Parameters: Row of data from pyspark dataframe with filenames and metadata that are not processed (upsert)
        Returns: Dataframe: Dataframe with required column names and edipi of a Veteran ready for upsert
        """
        file_name = row.path
        self.logger.info(f"Upserting records from {file_name}")
        if len(self.spark.read.csv(file_name).columns) != 3:
            schema = self.config["scd_schema"]
        else:
            schema = self.config["scd_schema1"]
        scd_updates_df = (
            self.spark.read.csv(file_name, schema=schema, header=True, inferSchema=False)
            .selectExpr(
                "PTCPNT_ID as participant_id", "CMBNED_DEGREE_DSBLTY", "DSBL_DTR_DT", "_metadata.file_name as filename", "_metadata.file_modification_time as SDP_Event_Created_Timestamp "
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
        Window_Spec = Window.partitionBy(scd_updates_df["participant_id"]).orderBy(desc("DSBL_DTR_DT"), desc("SC_Combined_Disability_Percentage"))
        edipi_df = (
            scd_updates_df
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

    def update_pai_data(self, row: Any, source_type: str) -> DataFrame:
        """
        Prepares PT Indicator from the input row, transforms and updates a Veteran's PT_Indicator column in delta table.
        Parameters: Row of data from pyspark dataframe with metadata that are not processed (upsert),
                    source_type is 'text' means its a static file (old source)
                    source_type is 'table' means its a delta table (new source)
        Returns: Dataframe: Dataframe with required column names ready for upsert
        """
        if source_type == "text":
            file_name = row.path
            file_creation_dateTime = row.dateTime
            raw_pai_df = (
                self.spark.read.csv(file_name, header=True, inferSchema=True)
                .selectExpr("*", "_metadata.file_name as filename", "_metadata.file_modification_time as SDP_Event_Created_Timestamp")
            )
        elif source_type == "table":
            file_creation_dateTime = row.dateTime
            file_name = f"Updated from PA&I delta table on {file_creation_dateTime}"
            raw_pai_df = self.spark.read.format("delta").load(self.pt_new_source)
        self.logger.info(f"Updating PT Indicator")
        pai_df = raw_pai_df.selectExpr(
            "PTCPNT_VET_ID as participant_id", "PT_35_FLAG as source_PT_Indicator"
        )
        # Additional join and transformation logic would go here, as in the notebook
        # This is a stub for modularization
        return pai_df

    def process_files(self, files_to_process_now: DataFrame):
        """
        Segregates files based on filename and calls required function to process these files
        Parameters: PySpark dataframe with filenames and metadata that are not processed
        Returns: None
        """
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
