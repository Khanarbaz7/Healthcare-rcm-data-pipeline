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
