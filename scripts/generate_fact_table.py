import pandas as pd
import os

# Make sure cleaned dir exists
os.makedirs("cleaned", exist_ok=True)

  
# Load cleaned and dimension data
  
claims_df = pd.read_csv("cleaned/claims_cleaned.csv")
patients_df = pd.read_csv("cleaned/patients_cleaned.csv")
providers_dim = pd.read_csv("cleaned/dim_providers.csv")

  
# Map PatientSK using UnifiedPatientID
  
claims_df = claims_df.merge(
    patients_df[["UnifiedPatientID", "PatientSK"]],
    on="UnifiedPatientID",
    how="left"
)

  
# Map ProviderSK using ProviderID
  
claims_df = claims_df.merge(
    providers_dim[["ProviderID", "ProviderSK"]],
    on="ProviderID",
    how="left"
)

  
# Select columns for fact_claims
  
fact_claims = claims_df[[
    "ClaimSK", "ClaimID", "PatientSK", "ProviderSK", "ProcedureCode",
    "ClaimDateKey", "ClaimAmount", "PaidAmount", "CoveragePercent",
    "PaymentStatus", "HospitalID"
]]


# Save to cleaned folder
  
fact_claims.to_csv("cleaned/fact_claims.csv", index=False)
print(" Saved fact_claims.csv with", len(fact_claims), "records")

