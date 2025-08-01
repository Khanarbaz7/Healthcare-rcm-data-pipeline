import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables (.env should have GOOGLE_APPLICATION_CREDENTIALS path)
load_dotenv()

project_id = "healthcare-rcm-467610"
dataset_id = "healthcare_rcm"

client = bigquery.Client(project=project_id)

def create_table(table_id, schema, partition_field=None, cluster_fields=None):
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(table_ref, schema=schema)

    if partition_field:
        table.time_partitioning = bigquery.TimePartitioning(field=partition_field)
    if cluster_fields:
        table.clustering_fields = cluster_fields

    table = client.create_table(table, exists_ok=True)
    print(f" Created table: {table_id}")

# ---------------------
# Dimension Tables
# ---------------------
create_table(
    "dim_patients",
    schema=[
        bigquery.SchemaField("UnifiedPatientID", "STRING"),
        bigquery.SchemaField("PatientSK", "INTEGER"),
        bigquery.SchemaField("FirstName", "STRING"),
        bigquery.SchemaField("LastName", "STRING"),
        bigquery.SchemaField("Gender", "STRING"),
        bigquery.SchemaField("DOB", "DATE"),
        bigquery.SchemaField("Age", "INTEGER"),
        bigquery.SchemaField("PhoneNumber", "STRING"),
        bigquery.SchemaField("Address", "STRING"),
        bigquery.SchemaField("HospitalID", "STRING")
    ],
    cluster_fields=["PatientSK"]
)

create_table(
    "dim_providers",
    schema=[
        bigquery.SchemaField("ProviderID", "STRING"),
        bigquery.SchemaField("ProviderSK", "INTEGER"),
        bigquery.SchemaField("FirstName", "STRING"),
        bigquery.SchemaField("LastName", "STRING"),
        bigquery.SchemaField("Specialty", "STRING"),
        bigquery.SchemaField("HospitalID", "STRING")
    ],
    cluster_fields=["ProviderSK"]
)

create_table(
    "dim_procedures",
    schema=[
        bigquery.SchemaField("ProcedureCode", "STRING"),
        bigquery.SchemaField("ProcedureDescription", "STRING")
    ]
)

create_table(
    "dim_date",
    schema=[
        bigquery.SchemaField("DateKey", "STRING"),
        bigquery.SchemaField("FullDate", "DATE"),
        bigquery.SchemaField("Year", "INTEGER"),
        bigquery.SchemaField("Month", "INTEGER"),
        bigquery.SchemaField("Quarter", "INTEGER"),
        bigquery.SchemaField("DayOfWeek", "STRING")
    ],
    cluster_fields=["Year", "Month"]
)

# ---------------------
# Fact Tables
# ---------------------
create_table(
    "fact_claims",
    schema=[
        bigquery.SchemaField("ClaimSK", "INTEGER"),
        bigquery.SchemaField("ClaimID", "STRING"),
        bigquery.SchemaField("UnifiedPatientID", "STRING"),
        bigquery.SchemaField("ClaimAmount", "FLOAT"),
        bigquery.SchemaField("PaidAmount", "FLOAT"),
        bigquery.SchemaField("CoveragePercent", "FLOAT"),
        bigquery.SchemaField("PaymentStatus", "STRING"),
        bigquery.SchemaField("ClaimDate", "DATE"),
        bigquery.SchemaField("ClaimDateKey", "STRING"),
        bigquery.SchemaField("ClaimYear", "INTEGER"),
        bigquery.SchemaField("ClaimMonth", "INTEGER"),
        bigquery.SchemaField("ClaimQuarter", "INTEGER"),
        bigquery.SchemaField("ClaimDayOfWeek", "STRING"),
        bigquery.SchemaField("ProcedureCode", "STRING"),
        bigquery.SchemaField("ProcedureDescription", "STRING"),
        bigquery.SchemaField("HospitalID", "STRING")
    ],
    partition_field="ClaimDate",
    cluster_fields=["UnifiedPatientID", "ProcedureCode"]
)

create_table(
    "fact_transactions",
    schema=[
        bigquery.SchemaField("TransactionSK", "INTEGER"),
        bigquery.SchemaField("TransactionID", "STRING"),
        bigquery.SchemaField("ClaimID", "STRING"),
        bigquery.SchemaField("PatientSK", "INTEGER"),
        bigquery.SchemaField("ProviderSK", "INTEGER"),
        bigquery.SchemaField("DeptID", "STRING"),
        bigquery.SchemaField("VisitDate", "DATE"),
        bigquery.SchemaField("VisitDateKey", "STRING"),
        bigquery.SchemaField("ServiceDate", "DATE"),
        bigquery.SchemaField("ServiceDateKey", "STRING"),
        bigquery.SchemaField("PaidDate", "DATE"),
        bigquery.SchemaField("PaidDateKey", "STRING"),
        bigquery.SchemaField("VisitType", "STRING"),
        bigquery.SchemaField("Amount", "FLOAT"),
        bigquery.SchemaField("AmountType", "STRING"),
        bigquery.SchemaField("PaidAmount", "FLOAT"),
        bigquery.SchemaField("ProcedureCode", "STRING"),
        bigquery.SchemaField("ICDCode", "STRING"),
        bigquery.SchemaField("LineOfBusiness", "STRING"),
        bigquery.SchemaField("HospitalID", "STRING")
    ],
    partition_field="ServiceDate",
    cluster_fields=["PatientSK", "ProviderSK"]
)

