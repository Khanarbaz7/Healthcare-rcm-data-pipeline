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
        df["PatientSK"] = df["UnifiedPatientID"].astype("category").cat.codes
        return df[["UnifiedPatientID", "PatientSK", "FirstName", "LastName", "Gender", "DOB", "Age", "HospitalID", "Address"]]


    def enrich_claims(self, df):
        df = df.copy()

        # Only enrich ProcedureCode if it exists
        if "ProcedureCode" in df.columns:
            procedure_map = {
                1001: "MRI Scan",
                1002: "X-Ray",
                1003: "Blood Test",
                1004: "Ultrasound",
                1005: "CT Scan"
            }
            df["ProcedureDescription"] = df["ProcedureCode"].map(procedure_map)
        else:
            # Add empty columns if missing
            df["ProcedureCode"] = None
            df["ProcedureDescription"] = None

        # Compute coverage percentage
        df["CoveragePercent"] = (df["PaidAmount"] / df["ClaimAmount"]) * 100

        # Surrogate key
        df["ClaimSK"] = df["ClaimID"].astype("category").cat.codes
        df["ClaimDateKey"] = df["ClaimDate"].dt.strftime("%Y%m%d")

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

        # Add time dimensions
        df["ClaimYear"] = df["ClaimDate"].dt.year
        df["ClaimMonth"] = df["ClaimDate"].dt.month
        df["ClaimQuarter"] = df["ClaimDate"].dt.quarter
        df["ClaimDayOfWeek"] = df["ClaimDate"].dt.day_name()

        return df[[  
            "ClaimSK", "ClaimID", "UnifiedPatientID", "ClaimAmount", "PaidAmount", "CoveragePercent",
            "PaymentStatus", "ClaimDate", "ClaimDateKey", "ClaimYear", "ClaimMonth", 
            "ClaimQuarter", "ClaimDayOfWeek", "ProcedureCode", "ProcedureDescription", "HospitalID"
        ]]

