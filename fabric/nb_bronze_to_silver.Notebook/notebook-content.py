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

# PARAMETERS CELL ********************

# PARAMETER CELL
start_month = "2026-01"
end_month = "2026-03"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read raw files
raw = spark.read.parquet("Files/bronze/*.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Standardise column names and types
df = (raw
    .withColumnRenamed("Airport_fee", "airport_fee")
    .select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias('pickup_ts'),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_ts"),
        F.col("passenger_count").cast("int"),
        F.col("trip_distance").cast("double"),
        F.col("PULocationID").cast("int").alias("pickup_zone_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_zone_id"),
        F.col("payment_type").cast("int"),
        F.col("fare_amount").cast("double"),
        F.col("tip_amount").cast("double"),
        F.col("total_amount").cast("double"),
    ))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean the dataframe
################################
# Exclude trips with 0 or negative fare amount
# Exclude trips with 0 or negative distance
# Exclude trips with drop-off timestamp before pickup timestamp
# Only keep trips with 1 to 6 passengers
# Add a new column with month extracted from pickup timestamp
# Only keep trips in the parameter range
# Drop any duplicate rows/trips
# Calculate the trip duration
# Calculate the tip percentage over the total fare
################################

df = (df
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("dropoff_ts") > F.col("pickup_ts"))
    .filter(F.col("passenger_count").between(1,6))
    .withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
    .filter(F.col("pickup_month").between(start_month, end_month))
    .dropDuplicates()
    .withColumn("trip_minutes", (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long"))/60)
    .withColumn("tip_pct", F.when(F.col("fare_amount") > 0, F.col("tip_amount") / F.col("fare_amount")))
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save the dataframe
################################
# Saving the data as delta format.
# Ensuring that overwrite mode is selected, so that when same data comes in it's not duplicated.
# Data is also partitioned by the pickup_month (column newly created during transformation).
# This ensures fast retrieval of data in the delta format.
################################
(df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"pickup_month >= '{start_month}' AND pickup_month <= '{end_month}'")
    .partitionBy("pickup_month")
    .saveAsTable("silver.yellow_trips")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
