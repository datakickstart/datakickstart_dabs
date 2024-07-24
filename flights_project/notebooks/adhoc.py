from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Unity Catalog Example") \
    .getOrCreate()

# Connect to Unity Catalog using catalog main
spark.sql("""Select WeatherDelay, NASDelay, SecurityDelay, LateAircraftDelay, IsArrDelayed 
from main.dustinvannoy_dev.flights_raw 
where WeatherDelay != 'NA' or NASDelay != 'NA' or SecurityDelay != 'NA' or LateAircraftDelay != 'NA'
limit 20""").show()

