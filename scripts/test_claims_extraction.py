from claims_extractor import ClaimsExtractor

extractor = ClaimsExtractor("data/claims")
claims_df = extractor.extract_claims()

print(f"\n Final Combined Claims Data: {len(claims_df)} rows")
