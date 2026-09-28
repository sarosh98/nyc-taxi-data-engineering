# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6d4e1b8b-5efe-4c31-906b-dc704c4f9c83",
# META       "default_lakehouse_name": "LH_Gold",
# META       "default_lakehouse_workspace_id": "b48f3b95-6043-42fb-b6ab-2206ae98dab8",
# META       "known_lakehouses": [
# META         {
# META           "id": "d38bb853-1399-4986-88f4-580d9aa155f8"
# META         },
# META         {
# META           "id": "6cdf0087-245c-4b63-875d-13269552fa96"
# META         },
# META         {
# META           "id": "6d4e1b8b-5efe-4c31-906b-dc704c4f9c83"
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

# MARKDOWN ********************

# # Gold Layer

# CELL ********************

from pyspark.sql import functions as F

silver_trips = spark.read.table(
    "LH_Silver.dbo.silver_trips"
)

silver_zone = spark.read.table(
    "LH_Silver.dbo.silver_taxi_zone"
)

print("Silver trips:", silver_trips.count())
print("Silver trips columns:", len(silver_trips.columns))

print("Silver zones:", silver_zone.count())
print("Silver zones columns:", len(silver_zone.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips.select(
    F.min("pickup_date").alias("min_date"),
    F.max("pickup_date").alias("max_date")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_range = silver_trips.select(
    F.min("pickup_date").alias("min_date"),
    F.max("pickup_date").alias("max_date")
).first()

min_date = date_range["min_date"]
max_date = date_range["max_date"]

dim_date = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(min_date),
                F.lit(max_date),
                F.expr("INTERVAL 1 DAY")
            )
        ).alias("full_date")
    )
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("year", F.year("full_date"))
    .withColumn(
        "day_of_week",
        F.date_format("full_date", "EEEE")
    )
    .withColumn(
        "day_of_week_number",
        F.dayofweek("full_date")
    )
    .withColumn(
        "is_weekend",
        F.dayofweek("full_date").isin([1, 7])
    )
    .select(
        "date_key",
        "full_date",
        "day",
        "month",
        "month_name",
        "quarter",
        "year",
        "day_of_week",
        "day_of_week_number",
        "is_weekend"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rows:", dim_date.count())
print("Columns:", len(dim_date.columns))

display(
    dim_date.orderBy("full_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_date.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

pickup_location_source = spark.read.table(
    "LH_Silver.dbo.silver_taxi_zone"
)

print("Rows:", pickup_location_source.count())
print("Columns:", len(pickup_location_source.columns))

pickup_location_source.show(5, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_pickup_location = (
    pickup_location_source
    .select(
        F.col("location_id").alias("pickup_location_key"),
        F.col("borough"),
        F.col("zone"),
        F.col("service_zone")
    )
    .dropDuplicates(["pickup_location_key"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rows:", dim_pickup_location.count())
print("Columns:", len(dim_pickup_location.columns))

dim_pickup_location.orderBy("pickup_location_key").show(10, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Null keys:",
    dim_pickup_location
        .filter(F.col("pickup_location_key").isNull())
        .count()
)

print(
    "Duplicate keys:",
    dim_pickup_location.count()
    - dim_pickup_location.select("pickup_location_key").distinct().count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_dropoff_location = (
    pickup_location_source
    .select(
        F.col("location_id").alias("dropoff_location_key"),
        F.col("borough"),
        F.col("zone"),
        F.col("service_zone")
    )
    .dropDuplicates(["dropoff_location_key"])
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rows:", dim_dropoff_location.count())
print("Columns:", len(dim_dropoff_location.columns))

dim_dropoff_location.orderBy("dropoff_location_key").show(
    10,
    truncate=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Null keys:",
    dim_dropoff_location
        .filter(F.col("dropoff_location_key").isNull())
        .count()
)

print(
    "Duplicate keys:",
    dim_dropoff_location.count()
    - dim_dropoff_location.select("dropoff_location_key").distinct().count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_pickup_location.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("dim_pickup_location")

# dim_dropoff_location.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("dim_dropoff_location")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips.select(
    "payment_type"
).groupBy(
    "payment_type"
).count().orderBy(
    "payment_type"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

payment_type_mapping = [
    (0, "Flex Fare"),
    (1, "Credit Card"),
    (2, "Cash"),
    (3, "No Charge"),
    (4, "Dispute")
]

dim_payment_type = spark.createDataFrame(
    payment_type_mapping,
    ["payment_type_key", "payment_type_name"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rows:", dim_payment_type.count())
print("Columns:", len(dim_payment_type.columns))

dim_payment_type.orderBy("payment_type_key").show(
    truncate=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# dim_payment_type.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("dim_payment_type")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_trips.select(
    "vendor_id"
).groupBy(
    "vendor_id"
).count().orderBy(
    "vendor_id"
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

vendor_mapping = [
    (1, "Creative Mobile Technologies"),
    (2, "Curb Mobility"),
    (6, "Myle Technologies")
]

dim_vendor = spark.createDataFrame(
    vendor_mapping,
    ["vendor_key", "vendor_name"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_vendor_ids = (
    silver_trips
    .select("vendor_id")
    .distinct()
)

missing_vendors = (
    silver_vendor_ids
    .join(
        dim_vendor,
        silver_vendor_ids.vendor_id == dim_vendor.vendor_key,
        "left_anti"
    )
)

print(
    "Vendor IDs in Silver:",
    silver_vendor_ids.count()
)

print(
    "Missing from dimension:",
    missing_vendors.count()
)

missing_vendors.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_vendor.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dim_vendor")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_source = (
    silver_trips
    .withColumn(
        "trip_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("vendor_id").cast("string"),
                F.col("pickup_datetime").cast("string"),
                F.col("dropoff_datetime").cast("string"),
                F.col("pickup_location_id").cast("string"),
                F.col("dropoff_location_id").cast("string"),
                F.col("payment_type").cast("string"),
                F.col("trip_distance").cast("string"),
                F.col("total_amount").cast("string")
            ),
            256
        )
    )
    .withColumn(
        "date_key",
        F.date_format("pickup_date", "yyyyMMdd").cast("int")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Fact source rows:",
    fact_source.count()
)

print(
    "Null trip keys:",
    fact_source
        .filter(F.col("trip_key").isNull())
        .count()
)

print(
    "Duplicate trip keys:",
    fact_source.count()
    - fact_source.select("trip_key").distinct().count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_with_date = (
    fact_source
    .withColumn(
        "date_key",
        F.date_format("pickup_date", "yyyyMMdd").cast("int")
    )
)

print("Rows:", fact_with_date.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_date_gold = spark.read.table("dim_date").alias("d")
dim_pickup_gold = spark.read.table("dim_pickup_location").alias("pu")
dim_dropoff_gold = spark.read.table("dim_dropoff_location").alias("do")
dim_payment_gold = spark.read.table("dim_payment_type").alias("pt")
dim_vendor_gold = spark.read.table("dim_vendor").alias("v")


f = fact_source.alias("f")

fact_with_keys = (
    f
    .join(
        dim_date_gold,
        F.col("f.date_key") ==
        F.col("d.date_key"),
        "left"
    )
    .join(
        dim_pickup_gold,
        F.col("f.pickup_location_id") ==
        F.col("pu.pickup_location_key"),
        "left"
    )
    .join(
        dim_dropoff_gold,
        F.col("f.dropoff_location_id") ==
        F.col("do.dropoff_location_key"),
        "left"
    )
    .join(
        dim_payment_gold,
        F.col("f.payment_type") ==
        F.col("pt.payment_type_key"),
        "left"
    )
    .join(
        dim_vendor_gold,
        F.col("f.vendor_id") ==
        F.col("v.vendor_key"),
        "left"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rows before joins:", fact_source.count())
print("Rows after joins:", fact_with_keys.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Missing date matches:",
      fact_with_keys.filter(F.col("d.date_key").isNull()).count())

print("Missing pickup matches:",
      fact_with_keys.filter(F.col("pu.pickup_location_key").isNull()).count())

print("Missing dropoff matches:",
      fact_with_keys.filter(F.col("do.dropoff_location_key").isNull()).count())

print("Missing payment matches:",
      fact_with_keys.filter(F.col("pt.payment_type_key").isNull()).count())

print("Missing vendor matches:",
      fact_with_keys.filter(F.col("v.vendor_key").isNull()).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_taxi_trip = fact_with_keys.select(
    F.col("f.trip_key").alias("trip_key"),

    F.col("f.date_key").alias("date_key"),

    F.col("f.pickup_location_id").alias("pickup_location_key"),

    F.col("f.dropoff_location_id").alias("dropoff_location_key"),

    F.col("f.payment_type").alias("payment_type_key"),

    F.col("f.vendor_id").alias("vendor_key"),

    F.col("f.pickup_datetime").alias("pickup_datetime"),

    F.col("f.dropoff_datetime").alias("dropoff_datetime"),

    F.col("f.passenger_count").alias("passenger_count"),

    F.col("f.trip_distance").alias("trip_distance"),

    F.col("f.trip_duration_minutes").alias("trip_duration_minutes"),

    F.col("f.average_speed_mph").alias("average_speed_mph"),

    F.col("f.fare_amount").alias("fare_amount"),

    F.col("f.extra").alias("extra"),

    F.col("f.mta_tax").alias("mta_tax"),

    F.col("f.tip_amount").alias("tip_amount"),

    F.col("f.tolls_amount").alias("tolls_amount"),

    F.col("f.improvement_surcharge").alias("improvement_surcharge"),

    F.col("f.total_amount").alias("total_amount"),

    F.col("f.congestion_surcharge").alias("congestion_surcharge"),

    F.col("f.airport_fee").alias("airport_fee"),

    F.col("f.cbd_congestion_fee").alias("cbd_congestion_fee")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rows:", fact_taxi_trip.count())
print("Columns:", len(fact_taxi_trip.columns))

fact_taxi_trip.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# fact_taxi_trip.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("fact_taxi_trip")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("dim_date:", spark.read.table("dim_date").count())
print("dim_pickup_location:", spark.read.table("dim_pickup_location").count())
print("dim_dropoff_location:", spark.read.table("dim_dropoff_location").count())
print("dim_payment_type:", spark.read.table("dim_payment_type").count())
print("dim_vendor:", spark.read.table("dim_vendor").count())
print("fact_taxi_trip:", spark.read.table("fact_taxi_trip").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_trips = spark.read.table(
    "LH_Bronze.dbo.bronze_yellow_trips"
)

bronze_trips.select(
    "batch_id"
).groupBy(
    "batch_id"
).count().orderBy(
    "batch_id"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_trips.select(
    F.min("ingestion_timestamp").alias("first_ingestion"),
    F.max("ingestion_timestamp").alias("last_ingestion")
).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_trips.select(
    "source_file",
    "batch_id",
    "ingestion_timestamp"
).groupBy(
    "source_file",
    "batch_id"
).agg(
    F.count("*").alias("row_count"),
    F.min("ingestion_timestamp").alias("first_ingestion"),
    F.max("ingestion_timestamp").alias("last_ingestion")
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import Row
from pyspark.sql import functions as F

control_data = [
    Row(
        pipeline_name="nyc_taxi_incremental_pipeline",
        last_successful_batch_id="202601",
        last_successful_timestamp="2026-08-31 14:07:52.705387",
        status="SUCCESS",
        updated_at="2026-08-31 14:07:52.705387"
    )
]

control_df = (
    spark.createDataFrame(control_data)
    .withColumn(
        "last_successful_timestamp",
        F.to_timestamp("last_successful_timestamp")
    )
    .withColumn(
        "updated_at",
        F.to_timestamp("updated_at")
    )
)

# control_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("LH_Gold.dbo.pipeline_control")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.table(
    "LH_Gold.dbo.pipeline_control"
).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze_trips = spark.read.table(
    "LH_Bronze.dbo.bronze_yellow_trips"
)

test_batch_202602 = (
    bronze_trips
    .limit(10000)
    .withColumn("batch_id", F.lit("202602"))
    .withColumn(
        "ingestion_timestamp",
        F.current_timestamp()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_batch_202602.select(
    "batch_id",
    "source_file",
    "ingestion_timestamp"
).show(5, truncate=False)

print("Test batch rows:", test_batch_202602.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# test_batch_202602.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .saveAsTable("LH_Bronze.dbo.bronze_incremental_202602")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

incremental_bronze = spark.read.table(
    "LH_Bronze.dbo.bronze_incremental_202602"
)

print("Rows:", incremental_bronze.count())

incremental_bronze.select(
    "batch_id"
).distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

control = spark.read.table(
    "LH_Gold.dbo.pipeline_control"
)

last_successful_batch = (
    control
    .filter(
        F.col("pipeline_name") == "nyc_taxi_incremental_pipeline"
    )
    .select("last_successful_batch_id")
    .first()[0]
)

print("Last successful batch:", last_successful_batch)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

incremental_bronze = spark.read.table(
    "LH_Bronze.dbo.bronze_incremental_202602"
)

incremental_source = (
    incremental_bronze
    .filter(F.col("batch_id") == incoming_batch)
)

print("Incremental rows to process:", incremental_source.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read the incremental Bronze batch
incremental_bronze = spark.read.table(
    "LH_Bronze.dbo.bronze_incremental_202602"
)

# Identify the incoming batch dynamically
incoming_batch = (
    incremental_bronze
    .select("batch_id")
    .distinct()
    .orderBy(F.col("batch_id").desc())
    .first()[0]
)

print("Incoming batch:", incoming_batch)

# Extract only that batch
incremental_source = (
    incremental_bronze
    .filter(F.col("batch_id") == incoming_batch)
)

print("Incremental rows to process:", incremental_source.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

incremental_keys = (
    incremental_source
    .withColumn(
        "trip_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("VendorID").cast("string"),
                F.col("tpep_pickup_datetime").cast("string"),
                F.col("tpep_dropoff_datetime").cast("string"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
                F.col("payment_type").cast("string"),
                F.col("trip_distance").cast("string"),
                F.col("total_amount").cast("string")
            ),
            256
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact = spark.read.table(
    "LH_Gold.dbo.fact_taxi_trip"
)

existing_keys = (
    gold_fact
    .select("trip_key")
    .distinct()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

duplicate_check = (
    incremental_keys
    .join(
        existing_keys,
        on="trip_key",
        how="inner"
    )
)

print("Incoming rows:", incremental_keys.count())
print("Already existing in Gold:", duplicate_check.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
