import os
import subprocess
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, regexp_replace, round, when

from src.settings.config import (
    CAPACITY_REGEX,
    CATEGORY_REGEX,
    CLEAN_DATA_DIR,
    MEMORY_TYPE_REGEX,
    RAW_DATA_DIR,
    SOCKET_REGEX,
)

load_dotenv()
if sys.platform == "win32":
    if not os.getenv("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = "C:\\hadoop"
        os.environ["PATH"] += os.pathsep + "C:\\hadoop\\bin"

spark = SparkSession.builder.appName("SparkScrapper").master("local[*]").getOrCreate()
subprocess.run("clear")


df = spark.read.json((RAW_DATA_DIR / "products_raw_data.jsonl").as_posix())

mapping = {
    "price": regexp_replace(regexp_replace("price", "руб.", ""), r"\s", "").cast(
        "float"
    ),
    "price_with_off": regexp_replace(
        regexp_replace("price_with_off", "руб.", ""), r"\s", ""
    ).cast("float"),
}

df = (
    df.withColumns(mapping)
    .withColumn("description", regexp_replace("description", '[",]', " "))
    .withColumn(
        "price_final",
        when(col("price_with_off").isNotNull(), col("price_with_off")).otherwise(
            col("price")
        ),
    )
    .withColumn(
        "category",
        regexp_extract(col("name"), CATEGORY_REGEX, 0),
    )
    .withColumn("name", regexp_replace("name", CATEGORY_REGEX, ""))
    .withColumn("memory_type", regexp_extract("description", MEMORY_TYPE_REGEX, 0))
    .withColumn(
        "value",
        regexp_extract("description", CAPACITY_REGEX, 1).try_cast("float"),
    )
    .withColumn(
        "unit",
        regexp_extract("description", CAPACITY_REGEX, 2),
    )
    .withColumn(
        "capacity_gb",
        when(col("unit") == "ТБ", round(col("value") * 1024)).otherwise(col("value")),
    )
    .withColumn(
        "socket",
        regexp_extract("description", SOCKET_REGEX, 1),
    )
    .withColumn(
        "description",
        regexp_replace(
            regexp_replace(
                regexp_replace("description", SOCKET_REGEX, ""), CAPACITY_REGEX, ""
            ),
            MEMORY_TYPE_REGEX,
            "",
        ),
    )
).sort(col("category").asc(), col("price").desc())

df.show()
# df.write.options(header="True", delimiter=";").mode("overwrite").csv(
#     CLEAN_DATA_DIR.as_posix()
# )
df = df.toPandas()
df.to_csv(CLEAN_DATA_DIR / "data.csv")
