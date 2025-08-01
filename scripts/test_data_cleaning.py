from data_integrator import DataIntegrator
from data_cleaner import DataCleaner

integrator = DataIntegrator()
cleaner = DataCleaner()

# Load raw data
patients_df = integrator.get_unified_patients()
claims_df = integrator.get_combined_claims()

# Clean
cleaned_patients = cleaner.clean_patients(patients_df)
print(f" Cleaned Patients: {len(cleaned_patients)}")

cleaned_claims = cleaner.clean_claims(claims_df)
print(f" Cleaned Claims: {len(cleaned_claims)}")

# Enrichment
enriched_patients = cleaner.enrich_patients(cleaned_patients)
print(f" Enriched Patients with Age: {enriched_patients[['UnifiedPatientID', 'Age']].head(3)}")

enriched_claims = cleaner.enrich_claims(cleaned_claims)
print(f" Enriched Claims with Coverage %, Payment Status, Time Dimensions:")
print(enriched_claims[["ClaimID", "CoveragePercent", "PaymentStatus", "ClaimYear", "ClaimMonth"]].head(3))

# Enrichment
enriched_patients = cleaner.enrich_patients(cleaned_patients)
enriched_claims = cleaner.enrich_claims(cleaned_claims)

print(" Final Cleaned Patient Columns:", enriched_patients.columns.tolist())
print(" Final Cleaned Claim Columns:", enriched_claims.columns.tolist())

