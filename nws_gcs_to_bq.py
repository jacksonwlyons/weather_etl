"""
Extract NWS data from GCS bucket, perform transformations, and export transformed data to BQ table
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("JSONtoBigQuery2").getOrCreate()
# Set temp bucket
spark.conf.set("temporaryGcsBucket", "gs://dataproc-temp-us-central1-302225578058-ayifikec")
# Read the JSON file
gcs_json_file_path = "gs://nws-alerts/daily-alerts-load.json"
# Read JSON file into dataframe
df = spark.read.option("multiline", "true").json(gcs_json_file_path)

# Select necessary fields
df2 = df.select(
    col('properties.@id').alias('id'), 
                col('properties.@type').alias('type'), 
                col('properties.areaDesc'), 
                col('properties.geocode.SAME').alias('FIPS'),
                col('properties.affectedZones'),
                col('properties.sent'),
                col('properties.effective'),
                col('properties.onset'),
                col('properties.expires'),
                col('properties.ends'),
                col('properties.status'),
                col('properties.messageType'),
                col('properties.category'),
                col('properties.severity'),
                col('properties.certainty'),
                col('properties.urgency'),
                col('properties.event'),
                col('properties.senderName'),
                col('properties.headline'),
                col('properties.description'),
                col('properties.instruction'),
                col('properties.response'),
                col('properties.parameters')

)

# Chnage intermediate format to preserve array data types
spark.conf.set("spark.datasource.bigquery.intermediateFormat", "orc")

# Write the transformed DataFrame to BigQuery
df2.write \
    .format("bigquery") \
    .option("table", "instant-mind-445916-f0.nws_data.nws_alerts") \
    .mode("overwrite") \
    .save()

print ("Records ingested : '"+ str(df2.count()) +"', Target table: nws_data.nws_alerts") 
print ("Transformation job completed successfully")

spark.catalog.clearCache()
spark.stop()