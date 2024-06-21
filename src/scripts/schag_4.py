from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

spark = SparkSession.builder.appName("DataLakeProject").getOrCreate()

# Загрузка таблицы событий
events_df = spark.read.parquet("/user/master/data/raw/events/events.parquet")

# Загрузка таблицы городов
geo_df = spark.read.csv("/user/master/data/raw/geo/geo.csv", header=True, inferSchema=True)

# Вычисление расстояния
def calculate_distance(lat1, lon1, lat2, lon2):
    """
    Функция для вычисления расстояния между двумя точками на Земле по формуле гаверсинуса.
    """
    R = 6371  # Радиус Земли в километрах
    lat1 = math.radians(lat1)
    lon1 = math.radians(lon1)
    lat2 = math.radians(lat2)
    lon2 = math.radians(lon2)
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

# Задание функции UDF для вычисления расстояния
calculate_distance_udf = F.udf(calculate_distance, FloatType())

# Находим пользователей, подписанных на одни каналы
candidates_df = events_df.select("user_id", "channel_id").filter("event_type == 'subscription'") \
    .groupBy("channel_id", "user_id").agg(F.countDistinct("event_id").alias("count")).filter("count > 1") \
    .select("channel_id", "user_id")

# Соединяем кандидатов, чтобы найти пары пользователей
candidates_df = candidates_df.join(
    candidates_df.alias("friend"),
    on=[
        (F.col("channel_id") == F.col("friend.channel_id")) &
        (F.col("user_id") != F.col("friend.user_id"))
    ],
    how="inner"
)

# Находим пары, которые не общались ранее
events_df = events_df.filter("event_type == 'message'")
contact_df = events_df.select("user_id", "recipient_id").withColumnRenamed("recipient_id", "user_right") \
    .union(events_df.select("recipient_id", "user_id").withColumnRenamed("recipient_id", "user_right"))
candidates_df = candidates_df.join(
    contact_df.select("user_id", "user_right"),
    on=["user_id", "user_right"],
    how="left_anti"
)

# Вычисляем расстояние между пользователями
candidates_df = candidates_df.withColumn(
    "user_left",
    F.least("user_id", "friend.user_id")
).withColumn(
    "user_right",
    F.greatest("user_id", "friend.user_id")
).drop("channel_id")

# Вычисляем локальное время 
candidates_df = candidates_df.withColumn(
    "local_time",
    F.from_utc_timestamp(F.col("TIME_UTC"), F.col("timezone"))
)

# Получаем последние координаты пользователей
last_coordinates_df = events_df.groupBy("user_id").agg(
    F.last("latitude").alias("latitude"),
    F.last("longitude").alias("longitude"),
    F.last("timezone").alias("timezone"),
    F.last("TIME_UTC").alias("TIME_UTC")
)

# Добавляем координаты для каждого пользователя
candidates_df = candidates_df.join(
    last_coordinates_df.alias("left_coordinates"),
    on=["user_left", "user_right"],
    how="left"
).join(
    last_coordinates_df.alias("right_coordinates"),
    on=["user_right"],
    how="left"
).withColumnRenamed("left_coordinates.latitude", "left_latitude") \
    .withColumnRenamed("left_coordinates.longitude", "left_longitude") \
    .withColumnRenamed("left_coordinates.timezone", "left_timezone") \
    .withColumnRenamed("left_coordinates.TIME_UTC", "left_TIME_UTC") \
    .withColumnRenamed("right_coordinates.latitude", "right_latitude") \
    .withColumnRenamed("right_coordinates.longitude", "right_longitude") \
    .withColumnRenamed("right_coordinates.timezone", "right_timezone") \
    .withColumnRenamed("right_coordinates.TIME_UTC", "right_TIME_UTC")

# Вычисляем расстояние между пользователями
candidates_df = candidates_df.withColumn(
    "distance",
    calculate_distance_udf(
        F.col("left_latitude"),
        F.col("left_longitude"),
        F.col("right_latitude"),
        F.col("right_longitude")
    )
)

# Фильтруем пользователей, находящихся на расстоянии менее 1 км
candidates_df = candidates_df.filter(F.col("distance") <= 1)

# Выбираем нужные колонки и добавляем поле с датой расчета витрины
friend_recommendation_df = candidates_df.select(
    "user_left",
    "user_right",
    "left_timezone",
    "right_timezone",
    "left_TIME_UTC",
    "right_TIME_UTC"
).withColumn(
    "local_time",
    F.from_utc_timestamp(F.col("left_TIME_UTC"), F.col("left_timezone"))
).withColumn(
    "processed_dttm",
    F.lit(datetime.datetime.now())
)

# Запись витрины в HDFS
friend_recommendation_df.write.mode("overwrite").parquet("/user/master/data/analysis/friend_recommendation/friend_recommendation.parquet")

# Вывод информации
print("Витрина friend_recommendation создана успешно!")
