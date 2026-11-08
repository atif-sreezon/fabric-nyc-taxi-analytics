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

w = spark.table("silver.weather_daily")

gold = (w
    .withColumn("is_rainy", F.col("rain_mm") >= 1.0)
    .withColumn("is_snowy", F.col("snowfall_cm") > 0)
    .withColumn("precip_category",
        F.when(F.col("snowfall_cm") > 0, "Snow")
         .when(F.col("rain_mm") >= 10, "Heavy rain")
         .when(F.col("rain_mm") >= 1,  "Light rain")
         .otherwise("Dry"))
    .withColumn("temp_band",
        F.when(F.col("temp_mean_c") < 0,  "Freezing")
         .when(F.col("temp_mean_c") < 10, "Cold")
         .when(F.col("temp_mean_c") < 20, "Mild")
         .when(F.col("temp_mean_c") < 28, "Warm")
         .otherwise("Hot")))

(gold.write.format("delta").mode("overwrite")
     .saveAsTable("gold.fact_weather_daily"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
