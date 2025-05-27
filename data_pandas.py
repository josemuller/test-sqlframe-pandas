import pandas as pd
from tabulate import tabulate

from transformation.pandas.data import data
from transformation.pandas.func5 import func5
from transformation.pandas.prep_func2 import prep_func2
from transformation.pandas.prep_func3 import prep_func3
from transformation.pandas.prep_func4 import prep_func4
from transformation.pandas.prep_func1 import prep_func1

from time import time

df_func1 = pd.read_parquet("./transformation/raw_data/func1.parquet")
df_func2 = pd.read_parquet("./transformation/raw_data/func2.parquet")
df_func3 = pd.read_parquet("./transformation/raw_data/func3.parquet")
df_func4 = pd.read_parquet("./transformation/raw_data/func4.parquet")
df_func5 = pd.read_parquet("./transformation/raw_data/func5.parquet")

start = time()

df_func1 = prep_func1(df_func1)
df_func2 = prep_func2(df_func2)
df_func3 = prep_func3(df_func3)
df_func4 = prep_func4(df_func4)
df_func5 = func5(df_func5)

df = data(df_func2, df_func1, df_func3, df_func4, df_func5)
print(tabulate(df[sorted(df.columns)], headers=sorted(df.columns), tablefmt="grid", showindex=False))

print(f"Pandas ran in {round((time() - start) * 1000, 0)} milliseconds ({round((time() - start), 2)} seconds)")