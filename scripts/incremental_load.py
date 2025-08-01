
import os
from google.cloud import bigquery
from dotenv import load_dotenv
from google.cloud.bigquery import SchemaField

# Load environment variables
load_dotenv()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Initialize BigQuery client
client = bigquery.Client()

# Set your variables
project_id = "healthcare-rcm-467610"
dataset_id = "healthcare_rcm"
main_table = f"{project_id}.{dataset_id}.fact_claims"
staging_table = f"{project_id}.{dataset_id}.fact_claims_staging"
csv_path = "cleaned/fact_claims.csv"

# Load CSV to staging table
job_config = bigquery.LoadJobConfig(
    schema=[
            SchemaField("ClaimSK", "INTEGER"),
            SchemaField("ClaimID", "STRING"),
            SchemaField("PatientSK", "INTEGER"),
            SchemaField("ProviderSK", "INTEGER"),
            SchemaField("ProcedureCode", "INTEGER"),
            SchemaField("ClaimDate", "DATE"),
            SchemaField("ClaimDateKey", "STRING"),
            SchemaField("ClaimAmount", "FLOAT"),
            SchemaField("PaidAmount", "FLOAT"),
            SchemaField("CoveragePercent", "FLOAT"),
            SchemaField("PaymentStatus", "STRING"),
            SchemaField("HospitalID", "STRING"),

    ],
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)


with open(csv_path, "rb") as source_file:
    load_job = client.load_table_from_file(source_file, staging_table, job_config=job_config)
    load_job.result()
    print(f" Loaded staging table: {staging_table}")

# Run MERGE to insert only new records
merge_query = f"""
MERGE `{main_table}` T
USING `{staging_table}` S
ON T.ClaimID = S.ClaimID
WHEN NOT MATCHED THEN
  INSERT ROW
"""

merge_job = client.query(merge_query)
merge_job.result()
print(f" Incremental load completed into: {main_table}")
