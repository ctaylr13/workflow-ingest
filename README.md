goal is to use prefect & duckdb to ingest parquet files.

ingested nyc taxi data using duckdb & prefect workflows.

initially did it without prefect and as a single year worth of data.

upgraded flow to start two data pipelines at once to download 3 years worth of data (2019, 2020, 2021) for green and yellow taxis

deleteed the db because its hugh but running the ingest_data.py file will create the db for you if you have duckdb and prefect installed.

pretty easily configurable to be used with other url data
