
import pandas as pd
from db_connector import get_mysql_engine
import os

# Ensure cleaned directory exists
os.makedirs("cleaned", exist_ok=True)


# 1. dim_patients

patients_df = pd.read_csv("cleaned/patients_cleaned.csv")
dim_patients = patients_df[[
    "PatientSK", "UnifiedPatientID", "FirstName", "LastName", 
    "Gender", "DOB", "Age", "HospitalID", "Address"
]]
dim_patients.to_csv("cleaned/dim_patients.csv", index=False)
print(" Saved dim_patients.csv")


# 2. dim_providers (Hospital A + B)

def load_providers_from_db(db_name, hospital_id):
    engine = get_mysql_engine(db_name)
    query = "SELECT * FROM providers"
    df = pd.read_sql(query, engine)
    df["HospitalID"] = hospital_id
    return df

prov_a = load_providers_from_db("hospital_a_db", "HospitalA")
prov_b = load_providers_from_db("hospital_b_db", "HospitalB")
providers_df = pd.concat([prov_a, prov_b], ignore_index=True)

providers_df["ProviderSK"] = providers_df["ProviderID"].astype("category").cat.codes

dim_providers = providers_df[[
    "ProviderSK", "ProviderID", "FirstName", "LastName", 
    "Specialization", "DeptID", "NPI", "HospitalID"
]]
dim_providers.to_csv("cleaned/dim_providers.csv", index=False)
print(" Saved dim_providers.csv")

# 3. dim_procedures (manual)

procedure_map = {
    1001: "MRI Scan",
    1002: "X-Ray",
    1003: "Blood Test",
    1004: "Ultrasound",
    1005: "CT Scan"
}
dim_procedures = pd.DataFrame(list(procedure_map.items()), columns=["ProcedureCode", "ProcedureDescription"])
dim_procedures.to_csv("cleaned/dim_procedures.csv", index=False)
print(" Saved dim_procedures.csv")


# 4. dim_date (from ClaimDate)

claims_df = pd.read_csv("cleaned/claims_cleaned.csv", parse_dates=["ClaimDate"])
dim_date = claims_df[["ClaimDate"]].drop_duplicates().rename(columns={"ClaimDate": "Date"})

dim_date["DateKey"] = dim_date["Date"].dt.strftime("%Y%m%d").astype(int)
dim_date["Year"] = dim_date["Date"].dt.year
dim_date["Month"] = dim_date["Date"].dt.month
dim_date["Quarter"] = dim_date["Date"].dt.quarter
dim_date["DayName"] = dim_date["Date"].dt.day_name()

dim_date = dim_date[["DateKey", "Date", "Year", "Month", "Quarter", "DayName"]]
dim_date.to_csv("cleaned/dim_date.csv", index=False)
print(" Saved dim_date.csv")
