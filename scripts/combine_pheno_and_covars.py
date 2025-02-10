import pandas as pd
import sys

# Step 1: Define file paths
covariates_file = sys.argv[1]  # Now contains both covariates and phenotypes
pcs_file = sys.argv[2]
output_file = sys.argv[3]

# Step 2: Load datasets
covariates = pd.read_csv(covariates_file, dtype=str, sep="\t")
pcs = pd.read_csv(pcs_file, dtype=str, sep="\t", quotechar='"')

# Step 3: Rename the first column of PCS to "person_id" if needed
if pcs.columns[0] != "person_id":
    print(f"\n[DEBUG] Renaming first column of PCS ({pcs.columns[0]}) to 'person_id'")
    pcs.rename(columns={pcs.columns[0]: "person_id"}, inplace=True)

# Step 4: Process the PCS file (split "scores" column into individual PCs)
if "scores" in pcs.columns:
    pcs_scores = pcs["scores"].str.strip("[]").str.split(",", expand=True)
    pcs_scores.columns = [f"PC{i+1}" for i in range(pcs_scores.shape[1])]
    pcs = pd.concat([pcs["person_id"], pcs_scores], axis=1)

print("\n[DEBUG] PCS after processing:")
print(pcs.head(3))

# Step 5: Ensure "person_id" column is consistent across all datasets
for df_name, df in zip(["Covariates+Phenotypes", "PCS"], [covariates, pcs]):
    if "person_id" in df.columns:
        df["person_id"] = df["person_id"].str.strip()
    else:
        print(f"\n[ERROR] 'person_id' column not found in {df_name}!")
        print(f"Columns in {df_name}: {df.columns.tolist()}")

# Step 6: Perform strict inner join
merged = covariates.merge(pcs, on="person_id", how="inner")

print("\n[DEBUG] First 3 rows of merged DataFrame:")
print(merged.head(3))

# Step 7: Handle missing values dynamically
for col in merged.columns:
    merged[col].fillna("", inplace=True)

# Step 8: Write the final output file
merged.to_csv(output_file, index=False)
print(f"\nFinal merged data written to: {output_file}")
