from google.cloud import bigquery

# Path to your JSON key file
key_path = "bigquery-key.json"

# Initialize the BigQuery client
client = bigquery.Client.from_service_account_json(key_path)

# Run a sample query
query = "SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` LIMIT 10"
query_job = client.query(query)

# Display results
for row in query_job:
    print(row.name)