from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)
from configs import (
    kafka_config,
    building_sensors_topic_name,
    temperature_alerts_topic_name,
)
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Створення SparkSession
spark = (
    SparkSession.builder.appName("SensorStreamProcessing")
    .master("local[*]")
    .getOrCreate()
)

# Визначення схеми даних
sensor_schema = StructType(
    [
        StructField("sensor_id", IntegerType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

# Зчитування потоку даних з Kafka
raw_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
    .option("subscribe", building_sensors_topic_name)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", "5")
    .option("kafka.security.protocol", "SASL_PLAINTEXT")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option(
        "kafka.sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
    )
    .load()
)

# Декодування даних
parsed_df = raw_stream_df.select(
    from_json(col("value").cast("string"), sensor_schema).alias("data")
).select("data.*")

# Застосування Sliding Window з Watermark
aggregated_df = (
    parsed_df.withWatermark("timestamp", "10 seconds")
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
    )
)

# Вывод в консоль
# query_console = (
#     aggregated_df.writeStream.outputMode("update")
#     .format("console")
#     .option("truncate", "false")
#     .option("checkpointLocation", "/tmp/checkpoints-2")
#     .start()
#     .awaitTermination()
# )

# Визначення схеми алертів
alerts_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("humidity_min", DoubleType(), True),
        StructField("humidity_max", DoubleType(), True),
        StructField("temperature_min", DoubleType(), True),
        StructField("temperature_max", DoubleType(), True),
        StructField("code", StringType(), True),
        StructField("message", StringType(), True),
    ]
)

# Читання потокових даних із CSV-файлу
# alerts_conditions_df = (
#     spark.readStream.option("sep", ",")
#     .option("header", True)
#     .option("maxFilesPerTrigger", 1)
#     .schema(alerts_schema)
#     .csv("CSV_files")
# )

alerts_conditions_df = (
    spark.read.option("sep", ",")  # ❗ Статическое чтение
    .option("header", True)
    .schema(alerts_schema)
    .csv("CSV_files")
)

# alerts_conditions_df.show(truncate=False)

# query = (
#     alerts_conditions_df.writeStream.outputMode(
#         "append"
#     )  # Або "complete" для повного виводу
#     .format("console")  # Вивід у консоль
#     .option("truncate", False)  # Вимкнення обрізання рядків
#     .start()
# )

# query.awaitTermination()

# Cross Join с условиями
# cross_joined_df = aggregated_df.crossJoin(alerts_conditions_df)
cross_joined_df = aggregated_df.alias("agg").join(
    alerts_conditions_df.alias("alerts"),
    (col("agg.avg_temperature") < col("alerts.temperature_max"))
    & (col("agg.avg_temperature") > col("alerts.temperature_min")),
    "inner",
)

query = (
    cross_joined_df.writeStream.outputMode(
        "append"
    )  # "append" або "complete", залежно від логіки
    .format("console")  # Вивід у консоль
    .option("truncate", False)  # Щоб бачити повні значення
    .start()
)

query.awaitTermination()

# Фильтрация по условиям
# filtered_alerts = cross_joined_df.filter(
#     (
#         (col("avg_humidity") < col("humidity_min"))
#         | (col("avg_humidity") > col("humidity_max"))
#         | (col("avg_temperature") < col("temperature_min"))
#         | (col("avg_temperature") > col("temperature_max"))
#     )
#     & ~(
#         (col("avg_temperature").between(30, 40)) & (col("avg_humidity").between(40, 60))
#     )  # Исключаем нормальные значения
# )

# Добавляем timestamp (текущее время)
# alerts_df = filtered_alerts.withColumn("timestamp", current_timestamp())

# Выбираем нужные колонки
# alerts_df = alerts_df.select(
#     "window", "avg_temperature", "avg_humidity", "code", "message", "timestamp"
# )

# Запись алертов в Kafka
# query_alerts = (
#     alerts_df.selectExpr("to_json(struct(*)) AS value")
#     .writeStream.format("kafka")
#     .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
#     .option("topic", temperature_alerts_topic_name)
#     .option("checkpointLocation", "/tmp/checkpoints-alerts")
#     .outputMode("append")
#     .start()
# )

# query_alerts = (
#     alerts_df.selectExpr("to_json(struct(*)) AS value")
#     .writeStream.format("kafka")
#     .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
#     .option("topic", temperature_alerts_topic_name)
#     .option("kafka.security.protocol", "SASL_PLAINTEXT")
#     .option("kafka.sasl.mechanism", "PLAIN")
#     .option(
#         "kafka.sasl.jaas.config",
#         "org.apache.kafka.common.security.plain.PlainLoginModule required username='admin' password='VawEzo1ikLtrA8Ug8THa';",
#     )
#     .option("checkpointLocation", "/tmp/checkpoints-alerts")
#     .outputMode("append")
#     .start()
#     .awaitTermination()
# )
