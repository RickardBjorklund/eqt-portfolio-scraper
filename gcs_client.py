import io

import pandas
from google.cloud import storage

from utils import time_function


@time_function
def fetch_enrichment_data(file_path):
    # Download the .json.gz file from Google Cloud Storage
    storage_client = storage.Client.create_anonymous_client()
    bucket = storage_client.bucket("motherbrain-external-test")
    blob = bucket.blob(file_path)
    file_buffer = io.BytesIO(blob.download_as_bytes())

    # Load the JSON data into a Pandas DataFrame
    df = pandas.read_json(file_buffer, lines=True, compression="gzip")

    return df
