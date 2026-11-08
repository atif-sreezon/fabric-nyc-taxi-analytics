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
start_date = "2026-01-01"
end_date   = "2026-03-31"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

metrics = ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
           "precipitation_sum", "rain_sum", "snowfall_sum",
           "wind_speed_10m_max", "weather_code"]

raw = (spark.read.option("multiLine", "true")
       .json("Files/bronze/weather/*.json"))

# Creates a new DataFrame containing the selected column.
# In this case, the output will have one column named z.
# References the time array inside the daily struct.
# The alias "time" gives the array a name within the resulting struct.
# [F.col(f"daily.{m}").alias(m) for m in metrics]
# This is a Python list comprehension. It loops through the variable metrics and creates a column reference for each metric.
# The asterisk unpacks the list into individual arguments.
zipped = raw.select(F.arrays_zip(
    F.col("daily.time").alias("time"),
    *[F.col(f"daily.{m}").alias(m) for m in metrics]
).alias("z"))

days = zipped.select(
    F.explode("z").alias("r")
).select(F.col("r.time").alias("time"),
        *[F.col(f"r.{m}").alias(m) for m in metrics]
)

silver = (days
    .select(
        F.to_date("time").alias("date"),
        F.col("temperature_2m_max").cast("double").alias("temp_max_c"),
        F.col("temperature_2m_min").cast("double").alias("temp_min_c"),
        F.col("temperature_2m_mean").cast("double").alias("temp_mean_c"),
        F.col("precipitation_sum").cast("double").alias("precipitation_mm"),
        F.col("rain_sum").cast("double").alias("rain_mm"),
        F.col("snowfall_sum").cast("double").alias("snowfall_cm"),
        F.col("wind_speed_10m_max").cast("double").alias("wind_max_kmh"),
        F.col("weather_code").cast("int").alias("weather_code"))
    .filter(F.col("date").isNotNull())
    .filter(F.col("date").between(start_date, end_date))
    .dropDuplicates(["date"])                       # overlapping files re-pulled across runs
    .fillna({"precipitation_mm": 0.0, "rain_mm": 0.0, "snowfall_cm": 0.0})
)

(silver.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"date >= '{start_date}' AND date <= '{end_date}'")
    .saveAsTable("silver.weather_daily"))

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
