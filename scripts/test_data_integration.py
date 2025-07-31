from data_integrator import DataIntegrator

integrator = DataIntegrator()

unified_patients = integrator.get_unified_patients()
print(f"Unified Patient Records: {len(unified_patients)}")

combined_claims = integrator.get_combined_claims()
print(f"Combined Claims Records: {len(combined_claims)}")
