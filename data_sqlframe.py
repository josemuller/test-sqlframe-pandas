from sqlframe import activate

activate(engine="duckdb")

from pyspark.sql import SparkSession

from transformation.pyspark.data import data
from transformation.pyspark.func5 import func5
from transformation.pyspark.prep_func2 import prep_func2
from transformation.pyspark.prep_func3 import prep_func3
from transformation.pyspark.prep_func4 import prep_func4
from transformation.pyspark.prep_func1 import prep_func1

spark = SparkSession.builder.getOrCreate()

from time import time

df_func1 = spark.read.parquet("./transformation/raw_data/func1.parquet")
df_func2 = spark.read.parquet("./transformation/raw_data/func2.parquet")
df_func3 = spark.read.parquet("./transformation/raw_data/func3.parquet")
df_func4 = spark.read.parquet("./transformation/raw_data/func4.parquet")
df_func5 = spark.read.parquet("./transformation/raw_data/func5.parquet")

start = time()

df_func1 = prep_func1(df_func1)
df_func2 = prep_func2(df_func2)
df_func3 = prep_func3(df_func3)
df_func4 = prep_func4(df_func4)
df_func5 = func5(df_func5)

df = data(df_func2, df_func1, df_func3, df_func4, df_func5)
df.select(sorted(df.columns)).show(truncate=False)

print(f"SqlFrame[DuckDB] ran in {round((time() - start) * 1000, 0)} milliseconds ({round((time() - start), 2)} seconds)")
