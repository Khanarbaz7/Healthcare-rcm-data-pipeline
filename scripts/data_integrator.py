from data_extractor import DataExtractor
from claims_extractor import ClaimsExtractor
import pandas as pd

class DataIntegrator:
    def __init__(self):
        self.extractor_a = DataExtractor("hospital_a_db")
        self.extractor_b = DataExtractor("hospital_b_db")
        self.claims = ClaimsExtractor("data/claims")

    def get_unified_patients(self):
        df_a = self.extractor_a.extract_table("patients", source="HospitalA")
        df_b = self.extractor_b.extract_table("patients", source="HospitalB")

        # Clean column names
        df_a.columns = df_a.columns.str.strip()
        df_b.columns = df_b.columns.str.strip()
        df_b.rename(columns={
        "ID": "PatientID",
        "F_Name": "FirstName",
        "L_Name": "LastName",
        "M_Name": "MiddleName"
        }, inplace=True)
        
        print("\n Columns in Hospital A:", df_a.columns.tolist())
        print("\n Columns in Hospital B:", df_b.columns.tolist())

        # Confirm column exists before using
        if "PatientID" not in df_b.columns:
            raise ValueError("  'PatientID' not found in df_b after cleaning.")

        df_a["UnifiedPatientID"] = "HA_" + df_a["PatientID"].astype(str)
        df_b["UnifiedPatientID"] = "HB_" + df_b["PatientID"].astype(str)

        return pd.concat([df_a, df_b], ignore_index=True)



    def get_combined_claims(self):
        return self.claims.extract_claims()
