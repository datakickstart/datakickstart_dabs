"""Python functions to test
These represent Python functions that you would keep in a Python file and import to test.
"""

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import expr

def read_autoloader(spark, path, checkpoint_location):
  schema = StructType([
      StructField("Year", IntegerType(), True),
      StructField("Month", IntegerType(), True),
      StructField("DayofMonth", IntegerType(), True),
      StructField("DayOfWeek", IntegerType(), True),
      StructField("DepTime", StringType(), True),
      StructField("CRSDepTime", IntegerType(), True),
      StructField("ArrTime", StringType(), True),
      StructField("CRSArrTime", IntegerType(), True),
      StructField("UniqueCarrier", StringType(), True),
      StructField("FlightNum", IntegerType(), True),
      StructField("TailNum", StringType(), True),
      StructField("ActualElapsedTime", StringType(), True),
      StructField("CRSElapsedTime", IntegerType(), True),
      StructField("AirTime", StringType(), True),
      StructField("ArrDelay", StringType(), True),
      StructField("DepDelay", StringType(), True),
      StructField("Origin", StringType(), True),
      StructField("Dest", StringType(), True),
      StructField("Distance", StringType(), True),
      StructField("TaxiIn", StringType(), True),
      StructField("TaxiOut", StringType(), True),
      StructField("Cancelled", IntegerType(), True),
      StructField("CancellationCode", StringType(), True),
      StructField("Diverted", IntegerType(), True),
      StructField("CarrierDelay", StringType(), True),
      StructField("WeatherDelay", StringType(), True),
      StructField("NASDelay", StringType(), True),
      StructField("SecurityDelay", StringType(), True),
      StructField("LateAircraftDelay", StringType(), True),
      StructField("IsArrDelayed", StringType(), True),
      StructField("IsDepDelayed", StringType(), True)
  ])
  
  streaming_df = spark.readStream.format("cloudFiles") \
      .option("cloudFiles.format", "csv") \
      .option("cloudFiles.includeExistingFiles", "true") \
      .schema(schema) \
      .load(path)

      # .option("cloudFiles.inferSchema", "true") \
      # .option("cloudFiles.schemaLocation", checkpoint_location+"/schema") \
  
  return streaming_df
      

def write_to_delta(df, dest_table, checkpoint_location):
  df.writeStream.format("delta").outputMode("append").option("checkpointLocation", checkpoint_location).toTable(dest_table)


def delay_type_transform(df):
  delay_expr = expr(
    """case when WeatherDelay != 'NA'
              then 'WeatherDelay'
            when NASDelay != 'NA'
              then 'NASDelay'
            when SecurityDelay != 'NA'
              then 'SecurityDelay'
            when LateAircraftDelay != 'NA'
              then 'LateAircraftDelay'
            when IsArrDelayed == 'YES' OR IsDepDelayed == 'YES'
              then 'UncategorizedDelay'
        end
  """)
  return df.withColumn("delay_type", delay_expr)

