
import pandas as pd
import os
from datetime import datetime

# Load new cleaned patient data (simulate new incoming data)
new_data = pd.read_csv("cleaned/patients_cleaned.csv")

# Load existing SCD table
scd_path = "cleaned/dim_patients_scd.csv"
if os.path.exists(scd_path):
    scd_data = pd.read_csv(scd_path)
else:
    print("SCD file not found. Run initial SCD load first.")
    exit()

# Normalize date formats
today = datetime.today().strftime('%Y-%m-%d')
new_data["EffectiveDate"] = today
new_data["ExpiryDate"] = "9999-12-31"
new_data["IsCurrent"] = True
new_data["Version"] = 1

# Columns to track for changes (excluding SCD metadata)
tracked_cols = ["FirstName", "LastName", "PhoneNumber", "Address", "Gender", "DOB", "HospitalID"]

# Records to update
updates = []

for _, new_row in new_data.iterrows():
    pid = new_row["UnifiedPatientID"]
    existing = scd_data[(scd_data["UnifiedPatientID"] == pid) & (scd_data["IsCurrent"] == True)]

    if existing.empty:
        # New patient → add directly
        updates.append(new_row)
    else:
        # Compare tracked columns
        current = existing.iloc[0]
        changed = any(new_row[col] != current[col] for col in tracked_cols)

        if changed:
            # Expire current record
            scd_data.loc[(scd_data["UnifiedPatientID"] == pid) & (scd_data["IsCurrent"] == True), ["IsCurrent", "ExpiryDate"]] = [False, today]

            # Add new versioned record
            new_row["Version"] = int(current["Version"]) + 1
            updates.append(new_row)


# Append updates to SCD data
if updates:
    updates_df = pd.DataFrame(updates)
    scd_updated = pd.concat([scd_data, updates_df], ignore_index=True)
    scd_updated.to_csv(scd_path, index=False)
    print(f"SCD updated with {len(updates)} changes.")
else:
    print("No changes detected. SCD unchanged.")
