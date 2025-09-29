import duckdb
from typing import List
from prefect import flow

PREFECT_API_URL='http://127.0.0.1:4200/api'

@flow
def pipeLineStart():
    print('prefect pipeline started')
    return ''

@flow
def createMonthString(taxi_type: str, month: str, year: str):
    return f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet'

@flow
def createMonthArray(taxi_type: str, year: str):
    months = [createMonthString(taxi_type, str(x).zfill(2), year) for x in range(1, 13)]
    return months

@flow
def ingest_data(taxi_type: str, year: int, download_data: List[str]):
    total_files = len(download_data)
    print(f"Starting ingestion of {total_files} files.")
    for idx, file_url in enumerate(download_data, start=1):
        try:
            print(f"Processing file {idx}/{total_files}: {file_url}")
            con.execute(f"""
                CREATE TABLE {taxi_type}_{year} AS
                SELECT * FROM read_parquet(?)
            """, [file_url])
            print(f"Successfully ingested {file_url}")
        except Exception as e:
            print(f"Error processing {file_url}: {e}")
    print("Ingestion process completed.")

taxi_type = 'green'

green_2019 = createMonthArray('green', '2019')
green_2020 = createMonthArray('green', '2020')
green_2021 = createMonthArray('green', '2021')

yellow_2019 = createMonthArray('yellow', '2019')
yellow_2020 = createMonthArray('yellow', '2020')
yellow_2021 = createMonthArray('yellow', '2021')

con = duckdb.connect('chapter_2.duckdb')
ingest_data('green', '2019', green_2019)
ingest_data('green', '2020', green_2020)
ingest_data('green', '2021', green_2021)

ingest_data('yellow', '2019', yellow_2019)
ingest_data('yellow', '2020', yellow_2020)
ingest_data('yellow', '2021', yellow_2021)


pipeLineStart()
