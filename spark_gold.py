from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, current_timestamp, round,
    avg, sum as sum_, coalesce, lit
)

def create_spark():
    spark = (
        SparkSession.builder
        .appName("GoldSpark_BikeWeatherBvg")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_silver_tables(spark):
    nextbike = spark.table("silver.nextbike_states")
    weather = spark.table("silver.weather_observations")
    bvg = spark.table("silver.bvg_events")
    return nextbike, weather, bvg

def build_gold(nextbike, weather, bvg):
    nb = (
        nextbike
        .select(
            "station_id",
            "station_name",
            "area_id",
            "lat",
            "lon",
            "events_ts",
            "dt",
            "hour",
            "bikes_available",
            "slots_available",
            "is_reserved",
            "is_disabled"
        )
        .withColumn("slots_available", col("slots_available").cast("int"))
        .withColumn("bikes_available", col("bikes_available").cast("int"))
        .withColumn("is_reserved", col("is_reserved").cast("int"))
        .withColumn("is_disabled", col("is_disabled").cast("int"))
        .filter((col("station_id").isNotNull()) & (col("dt").isNotNull()) & (col("hour").isNotNull()))
    )

    w_agg = (
        weather
        .groupBy("dt", "hour")
        .agg(
            avg(col("temperature_avg").cast("double")).alias("temperature_avg"),
            avg(col("wind_speed_avg").cast("double")).alias("wind_speed_avg"),
            sum_(col("precipitation_sum").cast("double")).alias("precipitation_sum")
        )
    )

    b_agg = (
        bvg
        .withColumnRenamed("dt_partition", "dt")
        .groupBy("dt", "hour")
        .agg(
            sum_(col("trips_count").cast("long")).alias("trips_count"),
            avg(col("avg_delay_minutes").cast("double")).alias("avg_delay_minutes"),
            sum_(col("canceled_trips_count").cast("long")).alias("canceled_trips_count")
        )
    )

    nb_w = nb.join(w_agg, on=["dt", "hour"], how="left")
    nb_w_b = nb_w.join(b_agg, on=["dt", "hour"], how="left")

    denom = (coalesce(col("bikes_available"), lit(0)) + coalesce(col("slots_available"), lit(0)))

    gold = (
        nb_w_b
        .withColumn(
            "occupancy_ratio",
            when(denom > 0, col("bikes_available") / denom).otherwise(lit(None).cast("double"))
        )
        .withColumn("occupancy_ratio", round(col("occupancy_ratio"), 3))
        .withColumn("gold_ingestion_ts", current_timestamp())
        .select(
            "station_id",
            "station_name",
            "area_id",
            "lat",
            "lon",
            "events_ts",
            "dt",
            "hour",
            "bikes_available",
            "slots_available",
            "is_reserved",
            "is_disabled",
            "occupancy_ratio",
            "temperature_avg",
            "wind_speed_avg",
            "precipitation_sum",
            "trips_count",
            "avg_delay_minutes",
            "canceled_trips_count",
            "gold_ingestion_ts",
        )
    )

    return gold

def write_gold(spark, gold_df, table_name="bike_weather_bvg_features"):
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    (
        gold_df
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .saveAsTable(f"gold.{table_name}")
    )

if __name__ == "__main__":
    spark = create_spark()
    nextbike, weather, bvg = load_silver_tables(spark)
    gold_df = build_gold(nextbike, weather, bvg)
    write_gold(spark, gold_df)
    print("Gold table gold.bike_weather_bvg_features created.")
    spark.stop()
