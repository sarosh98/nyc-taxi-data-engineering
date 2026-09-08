# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6cdf0087-245c-4b63-875d-13269552fa96",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "b48f3b95-6043-42fb-b6ab-2206ae98dab8",
# META       "known_lakehouses": [
# META         {
# META           "id": "d38bb853-1399-4986-88f4-580d9aa155f8"
# META         },
# META         {
# META           "id": "6cdf0087-245c-4b63-875d-13269552fa96"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.table("LH_Bronze.dbo.bronze_yellow_trips")

print(f"Rows: {df.count()}")
print(f"Columns: {len(df.columns)}")

display(df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_zone = spark.read.table("LH_Bronze.dbo.bronze_taxi_zone")

print(f"Zone rows: {df_zone.count()}")
print(f"Zone columns: {len(df_zone.columns)}")

display(df_zone.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_zone.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_silver = df.withColumn(
    "trip_duration_minutes",
    F.round(
        (
            F.col("tpep_dropoff_datetime").cast("long")
            - F.col("tpep_pickup_datetime").cast("long")
        ) / 60,
        2
    )
)

display(
    df_silver.select(
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_duration_minutes"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df_silver = (
    df_silver

    .withColumn(
        "invalid_timestamp",
        F.when(
            F.col("tpep_pickup_datetime").isNull()
            | F.col("tpep_dropoff_datetime").isNull(),
            True
        ).otherwise(False)
    )

    .withColumn(
        "invalid_duration",
        F.when(
            F.col("trip_duration_minutes") <= 0,
            True
        ).otherwise(False)
    )

    .withColumn(
        "duration_over_24h",
        F.when(
            F.col("trip_duration_minutes") > 1440,
            True
        ).otherwise(False)
    )

    .withColumn(
        "invalid_passenger_count",
        F.when(
            F.col("passenger_count").isNotNull()
            & (F.col("passenger_count") <= 0),
            True
        ).otherwise(False)
    )

    .withColumn(
        "invalid_distance",
        F.when(
            F.col("trip_distance") <= 0,
            True
        ).otherwise(False)
    )

    .withColumn(
        "invalid_fare",
        F.when(
            F.col("fare_amount") <= 0,
            True
        ).otherwise(False)
    )

    .withColumn(
        "invalid_total_amount",
        F.when(
            F.col("total_amount") <= 0,
            True
        ).otherwise(False)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = df_silver.withColumn(
    "data_quality_status",
    F.when(
        F.col("invalid_timestamp")
        | F.col("invalid_duration")
        | F.col("duration_over_24h")
        | F.col("invalid_passenger_count")
        | F.col("invalid_distance")
        | F.col("invalid_fare")
        | F.col("invalid_total_amount"),
        "INVALID"
    ).otherwise("VALID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.select(
    "data_quality_status",
    "invalid_timestamp",
    "invalid_duration",
    "duration_over_24h",
    "invalid_passenger_count",
    "invalid_distance",
    "invalid_fare",
    "invalid_total_amount"
).show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

quality_columns = [
    "invalid_timestamp",
    "invalid_duration",
    "duration_over_24h",
    "invalid_passenger_count",
    "invalid_distance",
    "invalid_fare",
    "invalid_total_amount"
]

for column in quality_columns:
    count = df_silver.filter(F.col(column) == True).count()
    print(f"{column}: {count:,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver = df_silver.withColumn(
    "rejection_reason",
    F.concat_ws(
        " | ",
        F.when(F.col("invalid_timestamp"), "MISSING_TIMESTAMP"),
        F.when(F.col("invalid_duration"), "INVALID_TRIP_DURATION"),
        F.when(F.col("duration_over_24h"), "TRIP_DURATION_OVER_24_HOURS"),
        F.when(F.col("invalid_passenger_count"), "INVALID_PASSENGER_COUNT"),
        F.when(F.col("invalid_distance"), "INVALID_TRIP_DISTANCE"),
        F.when(F.col("invalid_fare"), "INVALID_FARE_AMOUNT"),
        F.when(F.col("invalid_total_amount"), "INVALID_TOTAL_AMOUNT")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    df_silver
    .filter(F.col("data_quality_status") == "INVALID")
    .select(
        "trip_distance",
        "trip_duration_minutes",
        "passenger_count",
        "fare_amount",
        "total_amount",
        "rejection_reason"
    )
    .limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver.groupBy(
    "rejection_reason"
).count().orderBy(
    F.desc("count")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips = (
    df_silver
    .filter(F.col("data_quality_status") == "VALID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Silver trips rows: {silver_trips.count():,}")
print(f"Silver trips columns: {len(silver_trips.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rejected_trips = (
    df_silver
    .filter(F.col("data_quality_status") == "INVALID")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Rejected trips rows: {rejected_trips.count():,}")
print(f"Rejected trips columns: {len(rejected_trips.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

total = silver_trips.count() + rejected_trips.count()

print(f"Silver:   {silver_trips.count():,}")
print(f"Rejected: {rejected_trips.count():,}")
print(f"Total:    {total:,}")
print(f"Bronze:   {df.count():,}")

assert total == df.count()

print("\n✓ Row reconciliation successful")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Silver columns:")
for i, column in enumerate(silver_trips.columns, 1):
    print(f"{i:2}. {column}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_final = silver_trips.select(
    F.col("VendorID").alias("vendor_id"),
    F.col("tpep_pickup_datetime").alias("pickup_datetime"),
    F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
    F.col("passenger_count"),
    F.col("trip_distance"),
    F.col("RatecodeID").alias("rate_code_id"),
    F.col("store_and_fwd_flag").alias("store_and_forward_flag"),
    F.col("PULocationID").alias("pickup_location_id"),
    F.col("DOLocationID").alias("dropoff_location_id"),
    F.col("payment_type"),
    F.col("fare_amount"),
    F.col("extra"),
    F.col("mta_tax"),
    F.col("tip_amount"),
    F.col("tolls_amount"),
    F.col("improvement_surcharge"),
    F.col("total_amount"),
    F.col("congestion_surcharge"),
    F.col("Airport_fee").alias("airport_fee"),
    F.col("cbd_congestion_fee"),
    F.col("source_file"),
    F.col("ingestion_timestamp"),
    F.col("batch_id"),
    F.col("trip_duration_minutes")
)

print(f"Rows: {silver_trips_final.count():,}")
print(f"Columns: {len(silver_trips_final.columns)}")

silver_trips_final.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rejected_trips_final = rejected_trips.select(
    F.col("VendorID").alias("vendor_id"),
    F.col("tpep_pickup_datetime").alias("pickup_datetime"),
    F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
    F.col("passenger_count"),
    F.col("trip_distance"),
    F.col("RatecodeID").alias("rate_code_id"),
    F.col("store_and_fwd_flag").alias("store_and_forward_flag"),
    F.col("PULocationID").alias("pickup_location_id"),
    F.col("DOLocationID").alias("dropoff_location_id"),
    F.col("payment_type"),
    F.col("fare_amount"),
    F.col("extra"),
    F.col("mta_tax"),
    F.col("tip_amount"),
    F.col("tolls_amount"),
    F.col("improvement_surcharge"),
    F.col("total_amount"),
    F.col("congestion_surcharge"),
    F.col("Airport_fee").alias("airport_fee"),
    F.col("cbd_congestion_fee"),
    F.col("source_file"),
    F.col("ingestion_timestamp"),
    F.col("batch_id"),
    F.col("trip_duration_minutes"),
    F.col("invalid_timestamp"),
    F.col("invalid_duration"),
    F.col("duration_over_24h"),
    F.col("invalid_passenger_count"),
    F.col("invalid_distance"),
    F.col("invalid_fare"),
    F.col("invalid_total_amount"),
    F.col("data_quality_status"),
    F.col("rejection_reason")
)

print(f"Rows: {rejected_trips_final.count():,}")
print(f"Columns: {len(rejected_trips_final.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

zone_lookup = df_zone.select(
    F.col("LocationID").cast("int").alias("location_id"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone"),
    F.col("service_zone")
)

print(f"Zone lookup rows: {zone_lookup.count():,}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pickup_lookup = zone_lookup.select(
    F.col("location_id").alias("pickup_location_id"),
    F.col("borough").alias("pickup_borough"),
    F.col("zone").alias("pickup_zone"),
    F.col("service_zone").alias("pickup_service_zone")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dropoff_lookup = zone_lookup.select(
    F.col("location_id").alias("dropoff_location_id"),
    F.col("borough").alias("dropoff_borough"),
    F.col("zone").alias("dropoff_zone"),
    F.col("service_zone").alias("dropoff_service_zone")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched = (
    silver_trips_final
    .join(
        pickup_lookup,
        on="pickup_location_id",
        how="left"
    )
    .join(
        dropoff_lookup,
        on="dropoff_location_id",
        how="left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Rows: {silver_trips_enriched.count():,}")
print(f"Columns: {len(silver_trips_enriched.columns)}")

display(
    silver_trips_enriched.select(
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "dropoff_location_id",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone"
    ).limit(10)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_trips_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

before_count = silver_trips_final.count()
after_count = silver_trips_enriched.count()

print(f"Before join: {before_count:,}")
print(f"After join:  {after_count:,}")

assert before_count == after_count

print("✓ Row count preserved")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched.select(
    F.sum(
        F.when(F.col("pickup_borough").isNull(), 1).otherwise(0)
    ).alias("missing_pickup_location"),
    F.sum(
        F.when(F.col("dropoff_borough").isNull(), 1).otherwise(0)
    ).alias("missing_dropoff_location")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_trips_enriched.select(
        "pickup_location_id",
        "pickup_borough",
        "pickup_zone",
        "pickup_service_zone",
        "dropoff_location_id",
        "dropoff_borough",
        "dropoff_zone",
        "dropoff_service_zone",
        "trip_distance",
        "trip_duration_minutes",
        "fare_amount",
        "total_amount"
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Total columns: {len(silver_trips_enriched.columns)}")

for i, column in enumerate(silver_trips_enriched.columns, 1):
    print(f"{i:2}. {column}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(silver_trips_enriched.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched = silver_trips_enriched.withColumn(
    "pickup_date",
    F.to_date("pickup_datetime")
)

silver_trips_enriched = silver_trips_enriched.withColumn(
    "pickup_hour",
    F.hour("pickup_datetime")
)

silver_trips_enriched = silver_trips_enriched.withColumn(
    "pickup_day_of_week",
    F.date_format("pickup_datetime", "EEEE")
)

silver_trips_enriched = silver_trips_enriched.withColumn(
    "is_weekend",
    F.when(
        F.dayofweek("pickup_datetime").isin([1, 7]),
        True
    ).otherwise(False)
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched = silver_trips_enriched.withColumn(
    "average_speed_mph",
    F.when(
        (F.col("trip_distance") > 0) &
        (F.col("trip_duration_minutes") > 0),
        F.round(
            F.col("trip_distance") /
            (F.col("trip_duration_minutes") / 60),
            2
        )
    ).otherwise(None)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(f"Rows: {silver_trips_enriched.count():,}")
print(f"Columns: {len(silver_trips_enriched.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    silver_trips_enriched.select(
        "pickup_datetime",
        "pickup_date",
        "pickup_hour",
        "pickup_day_of_week",
        "is_weekend",
        "trip_distance",
        "trip_duration_minutes",
        "average_speed_mph",
        "pickup_borough",
        "pickup_zone",
        "dropoff_borough",
        "dropoff_zone"
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Row count
print("Row count:", silver_trips_enriched.count())

# 2. Null checks on derived columns
silver_trips_enriched.select(
    F.count(F.when(F.col("pickup_date").isNull(), True)).alias("null_pickup_date"),
    F.count(F.when(F.col("pickup_hour").isNull(), True)).alias("null_pickup_hour"),
    F.count(F.when(F.col("pickup_day_of_week").isNull(), True)).alias("null_day_of_week"),
    F.count(F.when(F.col("is_weekend").isNull(), True)).alias("null_is_weekend"),
    F.count(F.when(F.col("average_speed_mph").isNull(), True)).alias("null_average_speed")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched.select(
    F.min("pickup_hour").alias("min_hour"),
    F.max("pickup_hour").alias("max_hour")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched.groupBy(
    "is_weekend"
).count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched.select(
    "trip_distance",
    "trip_duration_minutes",
    "average_speed_mph"
).filter(
    (F.col("trip_duration_minutes") > 0) &
    (F.col("trip_distance") > 0)
).withColumn(
    "expected_speed",
    F.round(
        F.col("trip_distance") /
        (F.col("trip_duration_minutes") / 60),
        2
    )
).filter(
    F.col("average_speed_mph") != F.col("expected_speed")
).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips_enriched.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_silver_check = spark.read.table("silver_trips")

print("Rows:", df_silver_check.count())
print("Columns:", len(df_silver_check.columns))

display(df_silver_check.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

rejected_trips_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_rejected_trips")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_rejected_check = spark.read.table("silver_rejected_trips")

print("Rows:", df_rejected_check.count())
print("Columns:", len(df_rejected_check.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_taxi_zone = df_zone.select(
    F.col("LocationID").cast("int").alias("location_id"),
    F.col("Borough").alias("borough"),
    F.col("Zone").alias("zone"),
    F.col("service_zone"),
    F.col("source_file"),
    F.col("ingestion_timestamp"),
    F.col("batch_id")
)

silver_taxi_zone.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_taxi_zone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_zone_check = spark.read.table("silver_taxi_zone")

print("Rows:", df_zone_check.count())
print("Columns:", len(df_zone_check.columns))

display(df_zone_check.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Current database:", spark.catalog.currentDatabase())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SHOW TABLES"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_count = spark.read.table("silver_trips").count()
rejected_count = spark.read.table("silver_rejected_trips").count()
bronze_count = spark.read.table(
    "LH_Bronze.dbo.bronze_yellow_trips"
).count()

print("Bronze:", bronze_count)
print("Silver:", silver_count)
print("Rejected:", rejected_count)
print("Silver + Rejected:", silver_count + rejected_count)

assert bronze_count == silver_count + rejected_count

print("✓ FULL RECONCILIATION PASSED")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_count = spark.read.table("LH_Silver.dbo.silver_trips").count()
print(silver_count)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
