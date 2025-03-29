from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    avg,
    current_timestamp,
    expr,
    broadcast,
    lit,
    to_json,
    struct,
    date_format,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    StringType,
    TimestampType,
)
import os
import sys
from configs import (
    kafka_config,
    building_sensors_topic_name,
    temperature_alerts_topic_name,
    humidity_alerts_topic_name,
)
from helpers import check_csv_file


# Пакет, необхідний для читання Kafka зі Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

# Створення SparkSession
spark = (
    SparkSession.builder.appName("SensorStreamProcessing")
    .master("local[*]")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# Встановлення рівеня логуванн
spark.sparkContext.setLogLevel("WARN")

# Визначення схеми даних
sensor_schema = StructType(
    [
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
    ]
)

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

try:
    # Зчитування даних із CSV-файлу
    csv_paths = [
        "CSV_files/alerts_conditions.csv",
        "goit-de-hw-06/CSV_files/alerts_conditions.csv",
        "alerts_conditions.csv",
    ]

    csv_path = check_csv_file(csv_paths)

    # Якщо файл не знайдено, застосувати дефолтні параметри
    if not csv_path:
        print("Creating default alert conditions")
        default_data = [
            (1, 0.0, 40.0, -999, -999, 101, "It's too dry"),
            (2, 60.0, 100.0, -999, -999, 102, "It's too wet"),
            (3, -999, -999, -300.0, 30.0, 103, "It's too cold"),
            (4, -999, -999, 30.0, 300.0, 104, "It's too hot"),
        ]
        alerts_conditions_df = spark.createDataFrame(default_data, schema=alerts_schema)

    else:
        alerts_conditions_df = (
            spark.read.option("sep", ",")
            .option("header", True)
            .schema(alerts_schema)
            .csv(csv_path)
        )

    # Кешування для оптимізації продуктивності
    alerts_conditions_df = alerts_conditions_df.cache()

    # Зчитування потоку даних з Kafka
    raw_stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("subscribe", building_sensors_topic_name)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 100)
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
        )
        .load()
    )

    # Декодування даних з Kafka
    parsed_df = raw_stream_df.select(
        from_json(col("value").cast("string"), sensor_schema).alias("data")
    ).select("data.*")

    # Застосування Sliding Window з Watermark
    aggregated_df = (
        parsed_df.withWatermark("timestamp", "10 seconds")
        .groupBy(window(col("timestamp"), "1 minute", "30 seconds"))
        .agg(
            avg("temperature").alias("t_avg"),
            avg("humidity").alias("h_avg"),
        )
    )

    # Об'єднання даних з умовами алертів
    join_conditions = (
        (col("agg.t_avg") > col("alerts.temperature_min"))
        & (col("agg.t_avg") < col("alerts.temperature_max"))
    ) | (
        (col("agg.h_avg") > col("alerts.humidity_min"))
        & (col("agg.h_avg") < col("alerts.humidity_max"))
    )

    filtered_alerts = aggregated_df.alias("agg").join(
        broadcast(alerts_conditions_df).alias("alerts"), join_conditions, "inner"
    )

    # Фільтрація алертів температури (codes 103 and 104)
    temp_alerts = filtered_alerts.filter((col("code") == 103) | (col("code") == 104))

    # Фільтрація алертів вологості (codes 101 and 102)
    humidity_alerts = filtered_alerts.filter(
        (col("code") == 101) | (col("code") == 102)
    )

    # Підготовка даних для запису в Kafka: формування ключ-значення
    temp_output_df = temp_alerts.select(
        lit(None).cast("string").alias("key"),
        to_json(
            struct(
                struct(
                    date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss").alias(
                        "start"
                    ),
                    date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss").alias(
                        "end"
                    ),
                ).alias("window"),
                col("t_avg"),
                col("h_avg"),
                col("code"),
                col("message"),
                current_timestamp().alias("timestamp"),
            )
        ).alias("value"),
    )

    humidity_output_df = humidity_alerts.select(
        lit(None).cast("string").alias("key"),
        to_json(
            struct(
                struct(
                    date_format(col("window.start"), "yyyy-MM-dd'T'HH:mm:ss").alias(
                        "start"
                    ),
                    date_format(col("window.end"), "yyyy-MM-dd'T'HH:mm:ss").alias(
                        "end"
                    ),
                ).alias("window"),
                col("t_avg"),
                col("h_avg"),
                col("code"),
                col("message"),
                current_timestamp().alias("timestamp"),
            )
        ).alias("value"),
    )

    # Запис оброблених даних у Kafka-топіки
    temp_query = (
        temp_output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("topic", temperature_alerts_topic_name)
        .option("checkpointLocation", "/tmp/spark-checkpoint/temp-alerts")
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
        )
        .outputMode("append")
        .start()
    )

    humidity_query = (
        humidity_output_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_config["bootstrap_servers"][0])
        .option("topic", humidity_alerts_topic_name)
        .option("checkpointLocation", "/tmp/spark-checkpoint/humidity-alerts")
        .option("kafka.security.protocol", kafka_config["security_protocol"])
        .option("kafka.sasl.mechanism", kafka_config["sasl_mechanism"])
        .option(
            "kafka.sasl.jaas.config",
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";',
        )
        .outputMode("append")
        .start()
    )

    temp_query.awaitTermination()
    humidity_query.awaitTermination()

except Exception as e:
    print(f"An error occurred: {e}")
    import traceback

    traceback.print_exc()
    sys.exit(1)
