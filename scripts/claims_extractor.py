import os
import pandas as pd

REQUIRED_COLUMNS = [
    "ClaimID", "PatientID", "ProviderID", "ClaimDate",
    "ClaimAmount", "PaidAmount", "ClaimStatus"
]


class ClaimsExtractor:
    def __init__(self, claims_folder_path):
        self.folder = claims_folder_path

    def extract_claims(self):
        all_dfs = []
        for file in os.listdir(self.folder):
            if file.endswith(".csv"):
                file_path = os.path.join(self.folder, file)
                try:
                    df = pd.read_csv(file_path)
                    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
                    if missing_cols:
                        print(f"{file} missing columns: {missing_cols}")
                        continue
                    print(f" {file} → {len(df)} rows loaded")
                    all_dfs.append(df)
                except Exception as e:
                    print(f" Error loading {file}: {e}")
        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()
