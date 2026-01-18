import sys
import pandas as pd

print("arguments, ", sys.argv)
# day = sys.argv[1]
#print("running pipeline for day {day}")
month = int(sys.argv[1])
print(f"running pipeline for month {month}")
df = pd.DataFrame({"day": [1, 2], "number of passengers": [3, 4]})
df['month'] = month
print(df.head())

df.to_parquet(f"output_{month}.parquet")
print (f"hello pipeline, month={month}")
df.to_parquet(f"output_day_{sys.argv[1]}.parquet")