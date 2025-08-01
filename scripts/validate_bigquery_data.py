from google.cloud import bigquery
from dotenv import load_dotenv
import os

# Load environment
load_dotenv()
project_id = "healthcare-rcm-467610"
dataset = "healthcare_rcm"
client = bigquery.Client(project=project_id)

# Validation Queries
queries = {
    "Row Count - dim_patients": f"SELECT COUNT(*) AS count FROM `{project_id}.{dataset}.dim_patients`",
    "Row Count - fact_claims": f"SELECT COUNT(*) AS count FROM `{project_id}.{dataset}.fact_claims`",
    "Row Count - fact_transactions": f"SELECT COUNT(*) AS count FROM `{project_id}.{dataset}.fact_transactions`",
    "Invalid PatientSK in Claims": f"""
        SELECT COUNT(*) AS invalid_fk FROM `{project_id}.{dataset}.fact_claims` fc
        LEFT JOIN `{project_id}.{dataset}.dim_patients` dp ON fc.PatientSK = dp.PatientSK
        WHERE dp.PatientSK IS NULL
    """,
    "Invalid ProviderSK in Transactions": f"""
        SELECT COUNT(*) AS invalid_fk FROM `{project_id}.{dataset}.fact_transactions` ft
        LEFT JOIN `{project_id}.{dataset}.dim_providers` dp ON ft.ProviderSK = dp.ProviderSK
        WHERE dp.ProviderSK IS NULL
    """,
    "Check NULLs in ClaimAmount (shouldn't be null)": f"""
        SELECT COUNT(*) AS nulls FROM `{project_id}.{dataset}.fact_claims` WHERE ClaimAmount IS NULL
    """,
    "Check ClaimDate format": f"""
        SELECT COUNT(*) AS invalid_dates 
        FROM `healthcare-rcm-467610.healthcare_rcm.fact_claims` 
        WHERE SAFE.PARSE_DATE('%Y-%m-%d', ClaimDate) IS NULL

    """
}

# Run validations
for description, query in queries.items():
    result = client.query(query).result()
    print(f"\n🔍 {description}")
    for row in result:
        print(f"    {dict(row)}")
