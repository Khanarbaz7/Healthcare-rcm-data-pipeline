# scripts/test_extraction.py

from data_extractor import DataExtractor

extractor_a = DataExtractor("hospital_a_db")
patients_a = extractor_a.extract_table("patients")
transactions_a = extractor_a.extract_table("transactions")

extractor_b = DataExtractor("hospital_b_db")
patients_b = extractor_b.extract_table("patients")
df_providers_a = extractor_a.extract_table("providers")
df_encounters_a = extractor_a.extract_table("encounters")

df_providers_b = extractor_b.extract_table("providers")
df_encounters_b = extractor_b.extract_table("encounters")

