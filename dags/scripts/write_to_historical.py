"""
Add new records to historical table in BigQuery
"""

from google.cloud import bigquery

def run_query(query):
    client = bigquery.Client()
    try:
        query_job = client.query(query)
        results = query_job.result()
    except Exception as s:
        print(f'The error is {s}')
    return results

def main_historical():
    
    query = """
    INSERT INTO `instant-mind-445916-f0.nws_data.nws_alerts_historical`
    SELECT * EXCEPT(parameters) FROM `instant-mind-445916-f0.nws_data.nws_alerts`
    WHERE id NOT IN (
    SELECT id FROM `instant-mind-445916-f0.nws_data.nws_alerts_historical`
    ) 
    """

    run_query(query)

if __name__ == "__main__":
    main_historical() 