import pandas as pd
import os

print("Running Data Validation Checks...\n")

# Load dimension tables
dim_patients = pd.read_csv("cleaned/dim_patients.csv")
dim_providers = pd.read_csv("cleaned/dim_providers.csv")
dim_procedures = pd.read_csv("cleaned/dim_procedures.csv")
dim_date = pd.read_csv("cleaned/dim_date.csv")

# Load fact tables
fact_claims = pd.read_csv("cleaned/fact_claims.csv")
fact_transactions = pd.read_csv("cleaned/fact_transactions.csv")

# 1. Referential Integrity Checks
print("🔗 Referential Integrity Checks")
missing_patients_fc = fact_claims[~fact_claims["PatientSK"].isin(dim_patients["PatientSK"])]
missing_patients_ft = fact_transactions[~fact_transactions["PatientSK"].isin(dim_patients["PatientSK"])]

missing_providers = fact_transactions[~fact_transactions["ProviderSK"].isin(dim_providers["ProviderSK"])]
missing_procedures = fact_claims[~fact_claims["ProcedureCode"].isin(dim_procedures["ProcedureCode"])]

print(" - Fact Claims → Missing PatientSKs:", len(missing_patients_fc))
print(" - Fact Transactions → Missing PatientSKs:", len(missing_patients_ft))
print(" - Fact Transactions → Missing ProviderSKs:", len(missing_providers))
print(" - Fact Claims → Missing ProcedureCodes:", len(missing_procedures))

# 2. Orphan Check (NULL surrogate keys)
print("\n Orphan Key Check")
print(" - Nulls in fact_claims:")
print(fact_claims[["PatientSK", "ProcedureCode"]].isnull().sum())
print(" - Nulls in fact_transactions:")
print(fact_transactions[["PatientSK", "ProviderSK"]].isnull().sum())

# 3. Business Rule Validations
print("\n  Business Rule Validations")
print(" - ClaimAmount <= 0:", (fact_claims["ClaimAmount"] <= 0).sum())
print(" - PaidAmount < 0 in Claims:", (fact_claims["PaidAmount"] < 0).sum())

print(" - Transaction Amount <= 0:", (fact_transactions["Amount"] <= 0).sum())
print(" - PaidAmount < 0 in Transactions:", (fact_transactions["PaidAmount"] < 0).sum())

# Check for future dates
today = pd.Timestamp.today()
print(" - Claims with future ClaimDate:", (pd.to_datetime(fact_claims["ClaimDateKey"], format="%Y%m%d", errors='coerce') > today).sum())
print(" - Transactions with future ServiceDate:", (pd.to_datetime(fact_transactions["ServiceDate"], errors='coerce') > today).sum())

print("\n Data validation complete.")
