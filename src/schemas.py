"""
Schema definitions for the ETL process.
"""
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType, DateType

new_cg_schema = StructType([
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

scd_schema = StructType([
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

scd_schema1 = StructType([
    StructField("PTCPNT_ID", StringType()),
    StructField("CMBNED_DEGREE_DSBLTY", StringType()),
    StructField("DSBL_DTR_DT", StringType()),
])

pai_schema = StructType([
    StructField("EDI_PI", StringType()),
    StructField("SSN_NBR", StringType()),
    StructField("FILE_NBR", StringType()),
    StructField("PTCPNT_VET_ID", StringType()),
    StructField("LAST_NM", StringType()),
    StructField("FIRST_NM", StringType()),
    StructField("MIDDLE_NM", StringType()),
    StructField("PT35_RATING_DT", TimestampType()),
    StructField("PT35_PRMLGN_DT", TimestampType()),
    StructField("PT35_EFFECTIVE_DATE", TimestampType()),
    StructField("PT35_END_DATE", TimestampType()),
    StructField("PT_35_FLAG", StringType()),
    StructField("COMBND_DEGREE_PCT", StringType()),
])

file_list_schema = StructType([
    StructField("path", StringType()),
    StructField("name", StringType()),
    StructField("size", StringType()),
    StructField("modificationTime", StringType()),
])
