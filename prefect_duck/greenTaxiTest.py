import duckdb
from typing import List


def createMonthString(month: str):
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-{month:02d}.parquet'

def createMonthArray():
    months = [createMonthString(x) for x in range(1, 13)]  # '01', '02', ..., '12'
    return months

    
con = duckdb.connect('greenTAXITEST.duckdb')
parquet_files = createMonthArray()
print('parquet files', parquet_files)
con.execute("""
CREATE TABLE greenTAXI_YEAR AS
SELECT * FROM read_parquet(?)
""", [parquet_files])

