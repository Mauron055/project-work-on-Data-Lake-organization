from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

spark = SparkSession.builder.appName("DataLakeProject").getOrCreate()

# Загрузка таблицы событий
events_df = spark\"user_id",
    "channel_id",
    "latitude",
    "longitude",
    "timezone",
    "TIME_UTC"
).groupBy("channel_id", "user_id").agg(F.countDistinct("event_id").alias("count")).filter("count > 1") \
    .select(F.col("channel_id"), F.col("user_id"))

# Вычисление расстояния между пользователями в одной зоне
friend_recommendation_df = friend_recommendation_df.join(
    friend_recommendation_df.alias("friend"),
    on=[
        (F.col("channel_id") == F.col("friend.channel_id")) &
        (F.col("user_id") != F.col("friend.user_id"))
    ],
    how="inner"
)

# Вычисление локального времени
friend_recommendation_df = friend_recommendation_df.withColumn(
    "local_time",
    F.from_utc_timestamp(F.col("TIME_UTC"), F.col("timezone"))
)

# Очистка дубликатов и сортировка по user_id
friend_recommendation_df = friend_recommendation_df.select(
    F.least("user_id", "friend.user_id").alias("user_left"),
    F.greatest("user_id", "friend.user_id").alias("user_right"),
    "local_time",
    "channel_id"
).distinct().sort("user_left", "user_right")

# Добавление поля с датой расчета витрины
friend_recommendation_df = friend_recommendation_df.withColumn(
    "processed_dttm",
    F.lit(datetime.datetime.now())
)

# Запись витрины в HDFS
friend_recommendation_df.write.mode("overwrite").parquet("/user/master/data/analysis/friend_recommendation/friend_recommendation.parquet")

# Вывод информации
print("Витрина friend_recommendation создана успешно!")
