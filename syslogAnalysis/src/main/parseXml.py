# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, row_number, udf
from pyspark.sql.window import *
from pyspark.sql.types import *
import requests
import json

# custom schema definition
customSchema = StructType([ \
    StructField("Application_Number", LongType(), True), \
    StructField("category", ArrayType(StructType([StructField("element", StringType(), True)])), True), \
    StructField("Application_Type", StringType(), True), \
    StructField("country", StringType(), True)])

# initialize context
spark = SparkSession\
    .builder\
    .master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# 1. Developer should apply a schema on given xml file to read/parse xml file
# custom schema can be applied using .schema(customSchema), but as we are not interested in all the fields
# defining custom schema is unnecessary and cumbersome.
# Read xml file
xmlDf = spark.read.format("com.databricks.spark.xml")\
    .option("rootTag", "us-patent-grant") \
    .option("rowTag", "us-bibliographic-data-grant")\
    .load('C:\\data\\ipg101026.xml')
# .load('C:\\data\\ipg101026_1.xml', schema=customSchema)

# f. appStatus : developer should call given api by passing above Application_Number and fetch the value appStatus.
# getting app status for the application number


def getAppStatus(application_number):
    try:
        r = requests.post(url='https://ped.uspto.gov/api/queries',
                          json={"Accept: rchText": application_number, "qf": "applId"})
        return str(r.json()["queryResults"]["searchResponse"]["facet_counts"]["facet_fields"]["appStatus"])
    except:
        pass


# defining UDF
getAppStatusUdf = udf(getAppStatus)

# only 6 columns (Serial_Number, Application_Number, category, Application_Type, country, appStatus) we interested in
# d. Application_Type : value for this field is  "General", same for all records. apply udf function to achieve this.
# as per the requirement d, we cannot have udf with 0 parameters so I used 'General' as literal
# calling udf to get the app status and passing application number as parameter

# "application-reference.document-id.country", / "parties.agents.agent.addressbook.address.country"
# publication-reference.document-id.doc-number / application-reference.document-id.doc-number
xmlDfSelect = xmlDf.select("publication-reference.document-id.doc-number",
                           "references-cited.citation.category",
                           "parties.agents.agent.addressbook.address.country")\
    .withColumnRenamed("doc-number", "Application_Number")\
    .withColumn("Application_Type", lit("General"))\
    .withColumn("appStatus", getAppStatusUdf('Application_Number'))\
    .withColumn("Serial_Number", row_number().over(Window.orderBy('Application_Number')))\
    .cache()

# 2.Load above 6 fields into hive table and perform sample aggregation on these fields and store the results into another table.
"""
# write data to hive external table with partition if required in required file format
xmlDfSelect.write\
    .mode('Overwrite')\
    .parquet('external_table_path')\
    .partitionBy('country', 'category')\
    .saveAsTable()
"""

xmlDfSelect.createTempView("xmldf")
xmlTable = spark.sql("select Serial_Number, Application_Number, category, Application_Type, country, appStatus from xmldf")

# 3. finally visualising the resultant table fields in Zeppelin note book.
# using another paragraph with %sql in zeppelin we can write the expected visualization query against the table xmlTable
xmlTable.registerTempTable("xmlTable")
