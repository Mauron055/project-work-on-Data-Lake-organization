python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("DataLakeProject").getOrCreate()

# Загрузка таблицы событий
events_df = spark.read.parquet("/user/master/data/raw/events/events.parquet")

# Загрузка таблицы городов
geo_df = spark.read.csv("/user/master/data/raw/geo/geo.csv", header=True, inferSchema=True)

# Присоединение таблицы городов к таблице событий
events_with_geo_df = events_df.join(
    geo_df,
    on=[
        (F.abs(events_df.latitude - geo_df.latitude) < 0.01) &
        (F.abs(events_df.longitude - geo_df.longitude) < 0.01)
    ],
    how="left"
)

# Вычисление количества событий по городам за неделю и месяц
zone_events_df = events_with_geo_df.withColumn("week", F.weekofyear("TIME_UTC")) \
    .withColumn("month", F.month("TIME_UTC")) \
    .groupBy("month", "week", "city") \
    .agg(
        F.count("event_id").alias("week_message"),
        F.countDistinct("user_id").alias("week_user"),
        F.count("reaction_id").alias("week_reaction"),
        F.count("subscription_id").alias("week_subscription"),
        F.count("registration_id").alias("week_user"),  # Используем registration_id как proxy для новых пользователей
    )

# Вычисление месячных метрик с помощью оконных функций
zone_events_df = zone_events_df.withColumn(
    "month_message",
    F.sum("week_message").over(Window.partitionBy("city").orderBy("month", "week"))
).withColumn(
    "month_user",
    F.sum("week_user").over(Window.partitionBy("city").orderBy("month", "week"))
).withColumn(
    "month_reaction",
    F.sum("week_reaction").over(Window.partitionBy("city").orderBy("month", "week"))
).withColumn(
    "month_subscription",
    F.sum("week_subscription").over(Window.partitionBy("city").orderBy("month", "week"))
)

# Создание витрины zone_events
zone_events_df = zone_events_df.withColumnRenamed("city", "zone_id")

# Запись витрины в HDFS
zone_events_df.write.mode("overwrite").parquet("/user/master/data/processed/zone/zone_events.parquet")

# Вывод информации
print("Витрина zone_events создана успешно!")
