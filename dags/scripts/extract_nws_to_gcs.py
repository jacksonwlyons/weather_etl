"""
Extract NWS data from NWS API, upload raw data to GCS bucket
"""

import requests
from requests.exceptions import HTTPError, Timeout, RequestException
import json
from google.cloud import storage

def make_nws_request(endpoint, user_agent):
    """
    Function to send HTTP request to NWS. 
    Source: https://www.pythonsnacks.com/p/a-guide-on-using-the-national-weather-service-api-with-python
    """
    headers = {
        "User-Agent": user_agent,
    }

    try:
        response = requests.get(
                       endpoint, 
                       headers=headers
                   )
        # Raise HTTPError for bad responses (4xx or 5xx)
        response.raise_for_status()
        return response.json()

    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err} - Status code: {response.status_code}")
    except Timeout as timeout_err:
        print(f"Request timed out: {timeout_err}")
    except RequestException as req_err:
        print(f"Request error: {req_err}")
    
    return None  # Return None if an error occurred

def upload_to_gcs(bucket_name, destination_blob_name, alert_data):
    """
    Function to upload raw json data to Google Cloud Storage bucket.
    """
    # Initialize a storage client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object from the bucket
    blob = bucket.blob(destination_blob_name)

    # Convert the JSON data to a string
    alert_data_str = json.dumps(alert_data['features'])

    # Upload the JSON string to the blob
    blob.upload_from_string(alert_data_str)

    print(f"File uploaded to {destination_blob_name} in bucket {bucket_name}.")


def nws_to_gcs():
    user_agent = 'nws_data_etl'
    # Get weather alerts for Michigan
    endpoint = 'https://api.weather.gov/alerts?area=MI'
    alert_data = make_nws_request(endpoint, user_agent)

    bucket_name = "nws-alerts"
    destination_blob_name = "daily-alerts-load.json"

    upload_to_gcs(bucket_name, destination_blob_name, alert_data) 

if __name__ == '__main__':
    nws_to_gcs()
