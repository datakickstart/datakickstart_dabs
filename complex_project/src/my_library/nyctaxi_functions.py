from pyspark.sql import SparkSession

def get_taxis():
  print("Reading taxi trips data")
  spark = SparkSession.builder.getOrCreate()
  return spark.read.table("samples.nyctaxi.trips")

def save_summary(df):
  df2 = df.select(
        df.tpep_pickup_datetime.cast("date").alias("pickup_date"), 
        df.pickup_zip, 
        df.trip_distance,
        df.fare_amount
        )
  df2.createOrReplaceTempView("trip_tmp")

  df_agg = df.sparkSession.sql("""
          SELECT 
            pickup_date, 
            pickup_zip, 
            SUM(trip_distance) as trip_distance, 
            SUM(fare_amount) as fare_amount
          FROM trip_tmp
          GROUP BY pickup_date, pickup_zip
        """)
        
  df_agg.write.mode("overwrite").saveAsTable("main.dustinvannoy_dev.trip_summary")