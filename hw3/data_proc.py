import os
import findspark
# findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from pyspark.sql.functions import col, sum
from pyspark.sql.window import Window
from pyspark.sql.functions import last
from pyspark.sql import functions as F
from pyspark.sql.functions import col, coalesce, lag, lit, expr, when


def find_double(df):
    duplicate_rows = df.groupBy(df.columns).count().filter("count > 1")
    print(f"Найдено полных дубликатов: {duplicate_rows.count()}")
    
    df = df.dropDuplicates()
    return df

def find_nan(df):
    missing_stats = df.select([
        F.sum(F.col(c).isNull().cast("int")).alias(c) 
        for c in df.columns
    ]).collect()[0]

    #for col_name, null_count in zip(df.columns, missing_stats):
    #    print(f"{col_name}: {null_count} пропущенных значений")

    median_amount = df.approxQuantile("tx_amount", [0.5], 0.01)[0]
    median_seconds = df.approxQuantile("tx_time_seconds", [0.5], 0.01)[0]
    median_days = df.approxQuantile("tx_time_days", [0.5], 0.01)[0]

    avg_interval = df.withColumn("time_diff", 
        F.unix_timestamp("tx_datetime") - F.unix_timestamp(F.lag("tx_datetime", 1).over(Window.orderBy("tranaction_id"))))
    avg_interval_seconds = avg_interval.agg(F.avg("time_diff")).collect()[0][0] or 1

    most_freq_customer = df.groupBy("customer_id").count().orderBy(F.desc("count")).first()[0]
    most_freq_terminal = df.groupBy("terminal_id").count().orderBy(F.desc("count")).first()[0]
    
    window_spec = Window.orderBy("tranaction_id")

    columns_with_nulls = [col_name for col_name, null_count in zip(df.columns, missing_stats) if null_count > 0]

    print("Колонки с пропусками:", columns_with_nulls)

    # 2. Создаем базовый DF
    df_clean = df

    # 3. Окно только если будем использовать lag()
    window_spec = Window.orderBy("tranaction_id") if "tranaction_id" in columns_with_nulls or "tx_datetime" in columns_with_nulls else None

    # 4. Применяем преобразования только к нужным колонкам
    if "tranaction_id" in columns_with_nulls:
        df_clean = df_clean.withColumn("is_transaction_id_imputed", F.col("tranaction_id").isNull()) \
                          .withColumn("tranaction_id", 
                                     F.coalesce(F.col("tranaction_id"),
                                               F.lag("tranaction_id", 1).over(window_spec) + 1))

    if "tx_datetime" in columns_with_nulls:
        df_clean = df_clean.withColumn("is_datetime_imputed", F.col("tx_datetime").isNull()) \
                          .withColumn("tx_datetime",
                                     F.coalesce(F.col("tx_datetime"),
                                               F.lag("tx_datetime", 1).over(window_spec) + 
                                               F.expr(f"INTERVAL {avg_interval_seconds} SECONDS")))

    if "customer_id" in columns_with_nulls:
        df_clean = df_clean.withColumn("is_customer_imputed", F.col("customer_id").isNull()) \
                          .withColumn("customer_id",
                                     F.coalesce(F.col("customer_id"), F.lit(most_freq_customer)))

    if "terminal_id" in columns_with_nulls:
        df_clean = df_clean.withColumn("is_terminal_imputed", F.col("terminal_id").isNull()) \
                          .withColumn("terminal_id",
                                     F.coalesce(F.col("terminal_id"), F.lit(most_freq_terminal)))

    if "tx_amount" in columns_with_nulls:
        df_clean = df_clean.withColumn("is_amount_imputed", F.col("tx_amount").isNull()) \
                          .withColumn("tx_amount",
                                     F.coalesce(F.col("tx_amount"), F.lit(median_amount)))

    if "tx_time_seconds" in columns_with_nulls:
        df_clean = df_clean.withColumn("is_time_seconds_imputed", F.col("tx_time_seconds").isNull()) \
                          .withColumn("tx_time_seconds",
                                     F.coalesce(F.col("tx_time_seconds"), F.lit(median_seconds)))

    if "tx_time_days" in columns_with_nulls:
        df_clean = df_clean.withColumn("is_time_days_imputed", F.col("tx_time_days").isNull()) \
                          .withColumn("tx_time_days",
                                     F.coalesce(F.col("tx_time_days"), F.lit(median_days)))

    # Фильтрация только если работали с датами
    if "tx_datetime" in columns_with_nulls:
        df_clean = df_clean.filter(F.col("tx_datetime").isNotNull())
    
    print(f"Всего строк обработано: {df_clean.count()}")
    print(f"Удалено строк из-за невозможности заполнить дату: {df.count() - df_clean.count()}")
    
    return df_clean

def find_emissions(df, column_name="tx_amount"):
    q1, q3 = df.approxQuantile(column_name, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower_bound = q1 - iqr_coef*iqr
    upper_bound = q3 + iqr_coef*iqr
    
    # 2. Считаем статистику выбросов
    # outliers_stats = df.agg(
    #     F.sum((F.col(column_name) < lower_bound).cast("int")).alias("low_outliers"),
    #     F.sum((F.col(column_name) > upper_bound).cast("int")).alias("high_outliers"),
    #     F.count(column_name).alias("total_values")
    # ).collect()[0]
    
    # total_outliers = outliers_stats[0] + outliers_stats[1]
    
    # print(f"Всего выбросов: {total_outliers}")
    
    # 3. Создаем колонку-флаг выбросов
    df_out = df.withColumn(
        f"is_{column_name}_outlier",
        (F.col(column_name) < lower_bound) | (F.col(column_name) > upper_bound)
    )
    
    # 4. Заменяем выбросы на граничные значения
    df_out = df_out.withColumn(
        column_name,
        F.when(F.col(column_name) < lower_bound, lower_bound)
         .when(F.col(column_name) > upper_bound, upper_bound)
         .otherwise(F.col(column_name))
    )
    
    return df_out


def main():
    spark = (SparkSession
        .builder
        .appName("Dataset Practice")
        .master("local[*]")
        .getOrCreate()
    )

    print(f"Spark Version: {spark.version}")

    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )

    path = "hdfs:/user/ubuntu/data"
    file_statuses = hadoop_fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
    file_list = [file.getPath().getName() for file in file_statuses]
    print(file_list)

    schema = StructType([
        StructField("tranaction_id", IntegerType()),
        StructField("tx_datetime", TimestampType()),
        StructField("customer_id", IntegerType()),
        StructField("terminal_id", IntegerType()),
        StructField("tx_amount", DoubleType()),
        StructField("tx_time_seconds", IntegerType()),
        StructField("tx_time_days", IntegerType()),
        StructField("tx_fraud", IntegerType()),
        StructField("tx_fraud_scenario", IntegerType())
    ])
    
    for file in file_list:
        print(f"Обработка файла {file}")
        name = os.path.basename(file)
        df = spark.read \
            .option("sep", ",") \
            .option("header", "true") \
            .option("comment", "#") \
            .schema(schema) \
            .csv(f"hdfs:/user/ubuntu/data/{file}")
        df = find_double(df)
        df = find_nan(df)
        df = find_emissions(df)
        df.write.mode('overwrite').parquet(f"hdfs:/user/ubuntu/data/{name}.parquet")

if __name__== "__main__":
    main()
