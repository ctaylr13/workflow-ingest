import duckdb
from typing import List

def createMonthString(taxi_type: str, month: int, year: str):
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet'

def createMonthArray():
    months = [createMonthString(x) for x in range(1, 13)]  # '01', '02', ..., '12'
    return months

def ingest_data(download_data: List[str]):
    total_files = len(download_data)
    print(f"Starting ingestion of {total_files} files.")
    for idx, file_url in enumerate(download_data, start=1):
        try:
            print(f"Processing file {idx}/{total_files}: {file_url}")
            con.execute("""
                CREATE TABLE yellowTAXI_YEAR AS
                SELECT * FROM read_parquet(?)
            """, [file_url])
            print(f"Successfully ingested {file_url}")
        except Exception as e:
            print(f"Error processing {file_url}: {e}")
    print("Ingestion process completed.")

con = duckdb.connect('greenTAXITEST.duckdb')
parquet_files = createMonthArray()
print('Parquet files to process:', parquet_files)
ingest_data(parquet_files)
