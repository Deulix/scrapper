import os
import subprocess

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace, when

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

spark = SparkSession.builder.appName("SparkScrapper").master("local[*]").getOrCreate()
subprocess.run("clear")


# def delete_non_number(column_name: str):
#     return regexp_replace(col(column_name), r"\D", "").cast("float")


# mapping = {
#     "price": delete_non_number("price"),
#     "price_with_off": delete_non_number("price_with_off"),
# }


categories = [
    "Оперативная память",
    "Процессор",
    "SSD",
    "Материнская плата",
    "Видеокарта",
]

regex = "^(" + "|".join(categories) + ")"

df = spark.read.json("products_raw_data.jsonl")
# df_cleaned = df.withColumn(
#     "price", regexp_replace("price", "руб.", "").cast("float")
# ).withColumn(
#     "price_with_off", regexp_replace("price_with_off", "руб.", "").cast("float")
# )

mapping = {
    "price": regexp_replace(regexp_replace("price", "руб.", ""), r"\s", "").cast(
        "float"
    ),
    "price_with_off": regexp_replace(
        regexp_replace("price_with_off", "руб.", ""), r"\s", ""
    ).cast("float"),
}

df_cleaned = df.withColumns(mapping)

df_final = (
    df_cleaned.withColumn(
        "price_final",
        when(col("price_with_off").isNotNull(), col("price_with_off")).otherwise(
            col("price")
        ),
    )
    .withColumn(
        "category",
        regexp_extract(col("name"), regex, 0),
    )
    .withColumn("name", regexp_replace("name", regex, ""))
    .withColumn(
        "capacity",
        regexp_extract("description", r"(\d+)\s?(?:ГБ|GB|Gb|Гб|ТБ|TB|Тб|Tb){1}", 1),
    )
    .withColumn("memory_type", regexp_extract("description", r"G?DDR\dX?", 0))
)
df_final.show()
df_final.write.options(header="True", delimiter=";").mode("overwrite").csv("results")
