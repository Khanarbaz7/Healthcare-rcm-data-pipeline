import pandas as pd
import os
from etl_logger import setup_logger

logger = setup_logger("generate_fact_transactions")

try:
    logger.info("Current Working Dir: " + os.getcwd())
    os.makedirs("cleaned", exist_ok=True)

    logger.info(" Loading source & dimension data...")
    transactions_a = pd.read_csv("extracted/hospital_a_transactions.csv")
    transactions_b = pd.read_csv("extracted/hospital_b_transactions.csv")
    patients_df = pd.read_csv("cleaned/patients_cleaned.csv")
    providers_df = pd.read_csv("cleaned/dim_providers.csv")

    transactions_a["HospitalID"] = "HospitalA"
    transactions_b["HospitalID"] = "HospitalB"
    transactions = pd.concat([transactions_a, transactions_b], ignore_index=True)

    # Patient mapping
    logger.info("Mapping PatientSKs...")
    patients_a = pd.read_csv("extracted/hospital_a_patients.csv")
    patients_b = pd.read_csv("extracted/hospital_b_patients.csv")

    patients_b.rename(columns={
        "ID": "PatientID",
        "F_Name": "FirstName",
        "L_Name": "LastName",
        "M_Name": "MiddleName"
    }, inplace=True)

    patients_a["UnifiedPatientID"] = patients_a["PatientID"].apply(lambda x: f"HA_{x}")
    patients_b["UnifiedPatientID"] = patients_b["PatientID"].apply(lambda x: f"HB_{x}")

    raw_patients = pd.concat([patients_a, patients_b])
    patient_mapping = raw_patients[["UnifiedPatientID", "PatientID"]].merge(
        patients_df[["UnifiedPatientID", "PatientSK"]], on="UnifiedPatientID"
    )

    transactions = transactions.merge(
        patient_mapping[["PatientID", "PatientSK"]],
        on="PatientID",
        how="left"
    )

    # Provider mapping
    logger.info("Unique ProviderIDs in transactions (raw):")
    logger.info(transactions["ProviderID"].unique()[:10])

    logger.info(" Unique ProviderIDs in dim_providers:")
    logger.info(providers_df["ProviderID"].unique()[:10])

    transactions = transactions.merge(
        providers_df[["ProviderID", "ProviderSK"]],
        on="ProviderID",
        how="left"
    )

    missing_patient = transactions["PatientSK"].isna().sum()
    missing_providers = transactions["ProviderSK"].isna().sum()
    logger.info(f" Missing PatientSKs: {missing_patient}")
    logger.info(f" Missing ProviderSKs: {missing_providers}")
    if missing_providers > 0:
        logger.warning(" Warning: Some ProviderIDs in transactions do not match dim_providers.")

    transactions["ProviderSK"] = transactions["ProviderSK"].fillna(-1).astype("int64")

    # Transaction surrogate key
    transactions["TransactionSK"] = transactions["TransactionID"].astype("category").cat.codes

    for date_col in ["VisitDate", "ServiceDate", "PaidDate"]:
        if date_col in transactions.columns:
            transactions[date_col + "Key"] = pd.to_datetime(transactions[date_col], errors='coerce').dt.strftime("%Y%m%d")

    fact_transactions = transactions[[ 
        "TransactionSK", "TransactionID", "ClaimID", "PatientSK", "ProviderSK", "DeptID",
        "VisitDate", "VisitDateKey", "ServiceDate", "ServiceDateKey", "PaidDate", "PaidDateKey",
        "VisitType", "Amount", "AmountType", "PaidAmount", "ProcedureCode", "ICDCode",
        "LineOfBusiness", "HospitalID"
    ]]

    fact_transactions.to_csv("cleaned/fact_transactions.csv", index=False)
    logger.info(f" Saved fact_transactions.csv with {len(fact_transactions)} records")

except Exception as e:
    logger.error(f" ETL failed: {str(e)}")
