import pandas as pd
import glob

# List of your parquet files -- adjust the pattern to match your filenames
parquet_files = [
    'yellow_tripdata_2024-01.parquet',
    'yellow_tripdata_2024-02.parquet',
    'yellow_tripdata_2024-03.parquet',
    'yellow_tripdata_2024-04.parquet',
    'yellow_tripdata_2024-05.parquet',
    'yellow_tripdata_2024-06.parquet',
]

# Read and concatenate all parquet files
dfs = [pd.read_parquet(file) for file in parquet_files]
combined_df = pd.concat(dfs, ignore_index=True)

# Save the combined DataFrame as a single parquet file
combined_df.to_parquet('yellow_trip_firsthalf.parquet', engine='pyarrow')

print("All files have been combined into 'combined_output.parquet'")
