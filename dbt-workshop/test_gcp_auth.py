from google.cloud import storage
from google.cloud import bigquery

def test_gcp_auth():
    # Test GCS access
    try:
        storage_client = storage.Client()
        buckets = list(storage_client.list_buckets())
        print("GCS Buckets:")
        for bucket in buckets:
            print(f" - {bucket.name}")
    except Exception as e:
        print(f"Error accessing GCS: {e}")

    # Test BigQuery access
    try:
        bigquery_client = bigquery.Client()
        datasets = list(bigquery_client.list_datasets())
        print("\nBigQuery Datasets:")
        for dataset in datasets:
            print(f" - {dataset.dataset_id}")
    except Exception as e:
        print(f"Error accessing BigQuery: {e}")

if __name__ == "__main__":
    test_gcp_auth()
