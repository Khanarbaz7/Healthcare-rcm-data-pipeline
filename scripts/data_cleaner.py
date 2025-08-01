import pandas as pd

class DataCleaner:

    def clean_patients(self, df):
        df = df.copy()

        # Drop duplicates
        df.drop_duplicates(subset="UnifiedPatientID", inplace=True)

        # Convert DOB to datetime
        df["DOB"] = pd.to_datetime(df["DOB"], errors="coerce")

        # Standardize Gender (e.g., M/F → Male/Female)
        df["Gender"] = df["Gender"].str.strip().str.upper()
        df["Gender"] = df["Gender"].map({"M": "Male", "F": "Female"})

        # Drop rows with null PatientID or DOB
        df.dropna(subset=["UnifiedPatientID", "DOB"], inplace=True)

        return df

    def clean_claims(self, df):
        df = df.copy()

        # Drop duplicates
        df.drop_duplicates(subset="ClaimID", inplace=True)

        # Convert dates
        for col in ["ClaimDate", "ServiceDate", "InsertDate", "ModifiedDate"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        # Drop null ClaimIDs
        df.dropna(subset=["ClaimID"], inplace=True)

        return df
