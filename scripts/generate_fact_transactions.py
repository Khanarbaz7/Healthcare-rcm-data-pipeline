import pandas as pd
import os

# Ensure cleaned dir exists

import os
print("Current Working Dir:", os.getcwd())
os.makedirs("cleaned", exist_ok=True)

# -------------------------
# Load source & dimension data
# -------------------------
transactions_a = pd.read_csv("extracted/hospital_a_transactions.csv")
transactions_b = pd.read_csv("extracted/hospital_b_transactions.csv")
patients_df = pd.read_csv("cleaned/patients_cleaned.csv")
providers_df = pd.read_csv("cleaned/dim_providers.csv")

# Add hospital IDs
transactions_a["HospitalID"] = "HospitalA"
transactions_b["HospitalID"] = "HospitalB"

# Combine
transactions = pd.concat([transactions_a, transactions_b], ignore_index=True)

# -------------------------
# Map PatientSK
# -------------------------
# -------------------------
# Recover PatientID for mapping
# -------------------------
patients_a = pd.read_csv("extracted/hospital_a_patients.csv")
patients_b = pd.read_csv("extracted/hospital_b_patients.csv")


patients_b.rename(columns={
    "ID": "PatientID",
    "F_Name": "FirstName",
    "L_Name": "LastName",
    "M_Name": "MiddleName"
}, inplace=True)

patients_a["UnifiedPatientID"] = patients_a["PatientID"].apply(lambda x: f"HA_{x}")
patients_b["UnifiedPatientID"] = patients_b["PatientID"].apply(lambda x: f"HB_{x}")

raw_patients = pd.concat([patients_a, patients_b])
patient_mapping = raw_patients[["UnifiedPatientID", "PatientID"]].merge(
    patients_df[["UnifiedPatientID", "PatientSK"]], on="UnifiedPatientID"
)

# Merge to get PatientSK using PatientID
transactions = transactions.merge(
    patient_mapping[["PatientID", "PatientSK"]],
    on="PatientID",
    how="left"
)

# -------------------------
# Map ProviderSK
# -------------------------
transactions = transactions.merge(
    providers_df[["ProviderID", "ProviderSK"]],
    on="ProviderID",
    how="left"
)

# -------------------------
# Generate Surrogate Key
# -------------------------
transactions["TransactionSK"] = transactions["TransactionID"].astype("category").cat.codes

# Optional: Add Date Keys
for date_col in ["VisitDate", "ServiceDate", "PaidDate"]:
    if date_col in transactions.columns:
        transactions[date_col + "Key"] = pd.to_datetime(transactions[date_col], errors='coerce').dt.strftime("%Y%m%d")

# -------------------------
# Select & Reorder Columns
# -------------------------
fact_transactions = transactions[[
    "TransactionSK", "TransactionID", "ClaimID", "PatientSK", "ProviderSK", "DeptID",
    "VisitDate", "VisitDateKey", "ServiceDate", "ServiceDateKey", "PaidDate", "PaidDateKey",
    "VisitType", "Amount", "AmountType", "PaidAmount", "ProcedureCode", "ICDCode",
    "LineOfBusiness", "HospitalID"
]]

# -------------------------
# Save
# -------------------------
fact_transactions.to_csv("cleaned/fact_transactions.csv", index=False)
print(" Saved fact_transactions.csv with", len(fact_transactions), "records")
