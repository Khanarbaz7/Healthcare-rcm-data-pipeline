import os
from google.cloud import bigquery
from dotenv import load_dotenv
from google.cloud.bigquery import SchemaField

partition_schemas = {
    "fact_claims.csv": {
    "schema": [
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
    "partition_field": "ClaimDate",
    "clustering_fields": ["PatientSK", "ProcedureCode"]  
},
    "fact_transactions.csv": {
    "schema": [SchemaField("TransactionSK", "INTEGER"),
        SchemaField("TransactionID", "STRING"),
        SchemaField("ClaimID", "STRING"),
        SchemaField("PatientSK", "INTEGER"),
        SchemaField("ProviderSK", "INTEGER"),
        SchemaField("DeptID", "STRING"),
        SchemaField("VisitDate", "DATE"),
        SchemaField("VisitDateKey", "STRING"),
        SchemaField("ServiceDate", "DATE"),
        SchemaField("ServiceDateKey", "STRING"),
        SchemaField("PaidDate", "DATE"),
        SchemaField("PaidDateKey", "STRING"),
        SchemaField("VisitType", "STRING"),
        SchemaField("Amount", "FLOAT"),
        SchemaField("AmountType", "STRING"),
        SchemaField("PaidAmount", "FLOAT"),
        SchemaField("ProcedureCode", "INTEGER"),
        SchemaField("ICDCode", "STRING"),
        SchemaField("LineOfBusiness", "STRING"),
        SchemaField("HospitalID", "STRING"),],  
    "partition_field": "ServiceDate",  # 
    "clustering_fields": ["PatientSK", "ProviderSK"]
   }

}

# Load .env and initialize client
load_dotenv()
project_id = "healthcare-rcm-467610"
dataset_id = "healthcare_rcm"
client = bigquery.Client(project=project_id)

# Map CSV files to BigQuery tables
table_map = {
    "dim_patients.csv": "dim_patients",
    "dim_providers.csv": "dim_providers",
    "dim_procedures.csv": "dim_procedures",
    "dim_date.csv": "dim_date",
    "fact_claims.csv": "fact_claims",
    "fact_transactions.csv": "fact_transactions",
    "dim_patients_scd.csv": "dim_patients_scd"
}

# Path to cleaned data
cleaned_dir = "cleaned"

for filename, table_id in table_map.items():
    file_path = os.path.join(cleaned_dir, filename)

    if os.path.exists(file_path):
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        if filename in partition_schemas:
            schema_config = partition_schemas[filename]
            job_config = bigquery.LoadJobConfig(
                schema=schema_config["schema"],
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                write_disposition="WRITE_TRUNCATE",
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=schema_config["partition_field"]
                ),
                clustering_fields=schema_config.get("clustering_fields", [])
            )
        else:
            job_config = bigquery.LoadJobConfig(
                skip_leading_rows=1,
                autodetect=True,
                source_format=bigquery.SourceFormat.CSV,
                write_disposition="WRITE_TRUNCATE"
            )


        with open(file_path, "rb") as f:
            load_job = client.load_table_from_file(f, table_ref, job_config=job_config)
        load_job.result()  # Wait for job to finish
        print(f" Uploaded {filename} to {table_ref}")
    else:
        print(f" File not found: {file_path}")
