import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as F
import findspark

# findspark.init() 

def clean_dataframe(df, iqr_coef=1.5):
    # Удаляем полные дубликаты
    df = df.dropDuplicates()
    
    # Удаляем строки с NaN
    null_stats = df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])
    null_counts = null_stats.collect()[0].asDict()
    cols_with_nulls = [col for col, cnt in null_counts.items() if cnt > 0]

    if cols_with_nulls:
        print(f"Удаление строк с пропусками в колонках: {cols_with_nulls}")
        df = df.dropna(subset=cols_with_nulls)

    # Обработка выбросов по tx_amount
    q1, q3 = df.approxQuantile("tx_amount", [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lower, upper = q1 - iqr_coef * iqr, q3 + iqr_coef * iqr

    df = df.withColumn(
        "tx_amount",
        F.when(F.col("tx_amount") < lower, lower)
         .when(F.col("tx_amount") > upper, upper)
         .otherwise(F.col("tx_amount"))
    )

    return df

def main():
    spark = (SparkSession
        .builder
        .appName("Fast Data Cleaner")
        .master("local[*]")
        .getOrCreate()
    )

    print(f"Spark Version: {spark.version}")

    hadoop_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jsc.hadoopConfiguration()
    )
    path = "hdfs:/user/ubuntu/data"
    file_statuses = hadoop_fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(path))
    file_list = [file.getPath().getName() for file in file_statuses if file.getPath().getName().endswith(".txt")]

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
        print(f"Обработка файла: {file}")
        name = os.path.basename(file)
        df = spark.read \
            .option("sep", ",") \
            .option("header", "true") \
            .option("comment", "#") \
            .schema(schema) \
            .csv(f"hdfs:/user/ubuntu/data/{file}")

        df_cleaned = clean_dataframe(df)
        print(f"  Строк до очистки: {df.count()}")
        print(f"  Строк после очистки: {df_cleaned.count()}")

        if df_cleaned.count() == 0:
            print("После очистки DataFrame оказался пустым!")

        output_path = f"hdfs:/user/ubuntu/data/{name[:-4]}.parquet"
        df_cleaned.write.mode('overwrite').parquet(output_path)
        print(f"Сохранено в {output_path}")

if __name__ == "__main__":
    main()
