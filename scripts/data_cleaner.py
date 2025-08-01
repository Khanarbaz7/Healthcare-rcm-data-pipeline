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
    
    def enrich_patients(self, df):
        df = df.copy()
        today = pd.to_datetime("today")
        df["Age"] = df["DOB"].apply(lambda dob: today.year - dob.year if pd.notnull(dob) else None)
        return df

    def enrich_claims(self, df):
        df = df.copy()

        # Compute coverage percentage
        df["CoveragePercent"] = (df["PaidAmount"] / df["ClaimAmount"]) * 100

        # Categorize payment status
        def categorize_status(row):
            if pd.isna(row["PaidAmount"]) or row["PaidAmount"] == 0:
                return "Pending"
            elif row["PaidAmount"] == row["ClaimAmount"]:
                return "Paid"
            elif row["PaidAmount"] < row["ClaimAmount"]:
                return "Partial"
            else:
                return "Overpaid"

        df["PaymentStatus"] = df.apply(categorize_status, axis=1)

        # Add time dimensions from ClaimDate
        df["ClaimYear"] = df["ClaimDate"].dt.year
        df["ClaimMonth"] = df["ClaimDate"].dt.month
        df["ClaimQuarter"] = df["ClaimDate"].dt.quarter
        df["ClaimDayOfWeek"] = df["ClaimDate"].dt.day_name()

        return df