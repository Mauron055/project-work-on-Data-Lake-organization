from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, IntegerType
from pyspark.sql.window import Window
import math

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

# Присоединение таблицы городов к таблице событий
events_with_geo_df = events_df.join(
    geo_df,
    on=[
        (F.abs(events_df.latitude - geo_df.latitude) < 0.01) &
        (F.abs(events_df.longitude - geo_df.longitude) < 0.01)
    ],
    how="left"
)

# Вычисление расстояния до центра каждого города
events_with_distance_df = events_with_geo_df.withColumn(
    "distance",
    calculate_distance_udf(
        F.col("latitude"),
        F.col("longitude"),
        F.col("city_latitude"),
        F.col("city_longitude")
    )
)

# Определение города, в котором произошло событие
events_with_city_df = events_with_distance_df.withColumn(
    "event_key",
    F.monotonically_increasing_id()
).withColumn(
    "row_number",
    F.row_number().over(Window.partitionBy("event_key").orderBy("distance"))
).filter("row_number == 1") \
    .select(
        "user_id",
        "TIME_UTC",
        "city",
        "latitude",
        "longitude",
        "timezone"
    ).withColumnRenamed("city", "act_city")

# Определение домашнего города 
# Для этого используем подход с группами непрерывного присутствия пользователя в городе.
# Этот подход подразумевает, что мы находим "пограничные" строки (когда пользователь менял город)
# и считаем разницу между датами этих строк.
# Последнее такое посещение не менее 27 дней и будет считаться домашним городом.

# Сначала создаем временные столбцы для вычисления разницы дат между "пограничными" строками.
events_with_city_df = events_with_city_df.withColumn(
    "previous_city",
    F.lag("act_city", 1).over(Window.partitionBy("user_id").orderBy("TIME_UTC"))
).withColumn(
    "previous_date",
    F.lag("TIME_UTC", 1).over(Window.partitionBy("user_id").orderBy("TIME_UTC"))
).withColumn(
    "days_in_city",
    F.when(
        F.col("previous_city") != F.col("act_city"),
        (F.col("TIME_UTC") - F.col("previous_date")).cast("long") / (1000 * 60 * 60 * 24)
    ).otherwise(None)
)

# После этого находим строки, где пользователь менял город, и выбираем последнюю такую строку
# с длительностью пребывания в предыдущем городе не менее 27 дней.
events_with_city_df = events_with_city_df.filter(
    F.col("previous_city") != F.col("act_city")
).withColumn(
    "row_number",
    F.row_number().over(Window.partitionBy("user_id").orderBy("TIME_UTC").rowsBetween(Window.unboundedPreceding, Window.currentRow))
).filter(
    F.col("row_number") == 1
).select(
    "user_id",
    "previous_city",
    "days_in_city"
).withColumnRenamed("previous_city", "home_city") \
    .filter("days_in_city >= 27")

# Объединяем две таблицы, чтобы получить окончательный датафрейм
events_with_city_df = events_with_city_df.join(
    events_with_city_df.select("user_id", "act_city").distinct(),
    on=["user_id"],
    how="inner"
)

# Вычисление количества посещенных городов
events_with_city_df = events_with_city_df.withColumn(
    "travel_count",
    F.countDistinct("act_city").over(Window.partitionBy("user_id"))
)

# Создание массива городов без дубликатов
events_with_city_df = events_with_city_df.withColumn(
    "travel_array",
    F.collect_set("act_city").over(Window.partitionBy("user_id").orderBy("TIME_UTC"))
)

# Вычисление локального времени
events_with_city_df = events_with_city_df.withColumn(
    "local_time",
    F.from_utc_timestamp(F.col("TIME_UTC"), F.col("timezone"))
)

# Создание витрины user_profile
user_profile_df = events_with_city_df.select(
    "user_id",
    "act_city",
    "home_city",
    "travel_count",
    "travel_array",
    "local_time"
).distinct()

# Запись витрины в HDFS
user_profile_df.write.mode("overwrite").parquet("/user/master/data/processed/user_profile/user_profile.parquet")

# Вывод информации
print("Витрина user_profile создана успешно!")
