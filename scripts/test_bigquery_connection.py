import os
from dotenv import load_dotenv
from google.cloud import bigquery

# Load environment variables
load_dotenv()

# Set credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Initialize BigQuery client
client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT_ID"))

# Test dataset access
dataset_id = f"{client.project}.{os.getenv('BIGQUERY_DATASET')}"
dataset = client.get_dataset(dataset_id)  # API request

print(f"Connected to BigQuery dataset: {dataset_id}")
