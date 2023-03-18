import os
import pandas as pd
from google.cloud import bigquery

# Set environment variable to authenticate with GCP
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/path/to/your/credentials-key.json'

# Extract data from CSV file
input_csv = 'your_input_csv_file.csv'
data = pd.read_csv(input_csv)

# Transform data
# Perform your data transformations using pandas here
# For example, renaming columns, filtering, or aggregating data
data.rename(columns={'old_column_name': 'new_column_name'}, inplace=True)

# Load data to BigQuery
project_id = 'your-gcp-project-id'
dataset_id = 'your-bigquery-dataset-id'
table_id = 'your-bigquery-table-id'

client = bigquery.Client(project=project_id)
table_ref = client.dataset(dataset_id).table(table_id)

# Define your table schema if needed
# For example, [{'name': 'column_name', 'type': 'STRING', 'mode': 'NULLABLE'}]
schema = []

# Create the table if it doesn't exist
try:
    client.get_table(table_ref)
except Exception as e:
    table = bigquery.Table(table_ref, schema=schema)
    client.create_table(table)

# Load the data into the table
job_config = bigquery.LoadJobConfig()
job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
job_config.source_format = bigquery.SourceFormat.CSV
job_config.autodetect = True

with open(input_csv, 'rb') as file:
    job = client.load_table_from_file(file, table_ref, job_config=job_config)

job.result()  # Wait for the job to complete

print(f"Loaded {job.output_rows} rows to {project_id}:{dataset_id}.{table_id}")
