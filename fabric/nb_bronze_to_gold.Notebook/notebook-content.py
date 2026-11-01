# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "615cb6d6-2968-47da-bec7-f2455c878629",
# META       "default_lakehouse_name": "lakehouse",
# META       "default_lakehouse_workspace_id": "c5e0c809-e69b-461d-aec8-19d28ce16c9d",
# META       "known_lakehouses": [
# META         {
# META           "id": "615cb6d6-2968-47da-bec7-f2455c878629"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reading the lakehouse table
trips = spark.table("silver.yellow_trips")

# Creating the dimensions
# Trip zones: From Files/bronze/reference/taxi_zone_lookup.csv
taxi_zone_lookup_dir = "Files/bronze/reference/taxi_zone_lookup.csv"
(
    spark.read.option("header", True).csv(taxi_zone_lookup_dir)
    .select(F.col("LocationID").cast("int").alias("zone_id"), "Borough", "Zone", "service_zone")
    .write.format("delta").mode("overwrite").saveAsTable("gold.dim_zone")
)

# Dates: Created from the date range of the data
dates = (
    trips.select(F.col("pickup_ts").alias("date")).distinct()
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day_of_week", F.date_format("date", "E"))
    .withColumn("is_weekend", F.dayofweek("date").isin(1,7)) # checking if the day of the week (numeric) is either 1 or 7
)
dates.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")

# Fact table: Fact about the trip from bronze layer trip records.
fact = (
    trips
    .withColumn("date", F.to_date("pickup_ts"))
    .withColumn("pickup_hour", F.hour("pickup_ts"))
    .select("date", "pickup_hour", "pickup_zone_id", "dropoff_zone_id", "payment_type",
           "passenger_count", "trip_distance", "trip_minutes",
           "fare_amount", "tip_amount", "tip_pct", "total_amount", "pickup_month")
)
fact.write.format("delta").mode("overwrite").partitionBy("pickup_month").saveAsTable("gold.fact_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
