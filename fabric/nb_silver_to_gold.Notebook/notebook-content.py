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

# Reading the lakehouse table
trips = spark.table("silver.yellow_trips")

# Creating the dimensions
# Trip zones: From Files/bronze/reference/taxi_zone_lookup.csv
taxi_zone_lookup_dir = "Files/bronze/reference/taxi_zone_lookup.csv"
(
    spark.read.option("header", True).csv(taxi_zone_lookup_dir)
    .select(F.col("LocationID").cast("int").alias("zone_id"), "Borough", "Zone", "service_zone")
    .withColumn("zone_key", F.col("zone_id").cast("string"))
    .write.format("delta").mode("overwrite").saveAsTable("gold.dim_zone")
)

# Dates: Created from the date range of the data
dates = (
    trips.select(F.col("pickup_ts").cast("date").alias("date")).distinct()
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMM"))
    .withColumn("day_of_week", F.date_format("date", "E"))
    .withColumn("dow_num", F.dayofweek("date"))      # 1 = Sunday
    .withColumn("is_weekend", F.dayofweek("date").isin(1,7)) # checking if the day of the week (numeric) is either 1 or 7
    .withColumn("year_month", F.date_format("date", "yyyy-MM"))
    
)
dates.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")

# Payment Types: Codes from the TLC data dictionary
# A numeric code signifying how the passenger paid for the trip. 
# 0 = Flex Fare trip 
# 1 = Credit card 
# 2 = Cash 
# 3 = No charge 
# 4 = Dispute 
# 5 = Unknown 
# 6 = Voided trip 
pay = spark.createDataFrame(
    [(1,"Credit card"),(2,"Cash"),(3,"No charge"),(4,"Dispute"),(5,"Unknown"),(6,"Voided trip")],
    ["payment_type","payment_name"])
pay.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable("gold.dim_payment_type")

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
