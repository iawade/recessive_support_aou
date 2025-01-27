import pandas as pd
import sys

# Step 1: Define file paths
covariates_file = sys.argv[1]
pheno_file = sys.argv[2]
pcs_file = sys.argv[3]
output_file = sys.argv[4]

# Step 2: Dynamically determine column names
covariates = pd.read_csv(covariates_file, dtype=str, nrows=0)
pheno = pd.read_csv(pheno_file, dtype=str, sep="\t", nrows=0)
pcs = pd.read_csv(pcs_file, dtype=str, sep="\t", quotechar='"', nrows=0)

covariates_columns = covariates.columns.tolist()
pheno_columns = pheno.columns.tolist()
pcs_columns = pcs.columns.tolist()

print("\n[DEBUG] Initial column names:")
print(f"Covariates columns: {covariates_columns}")
print(f"Pheno columns: {pheno_columns}")
print(f"PCS columns: {pcs_columns}")

# Step 3: Load datasets
covariates = pd.read_csv(covariates_file, dtype=str)
pheno = pd.read_csv(pheno_file, dtype=str, sep="\t")

# Fix: Properly read the PCS file, handling quoted strings
pcs = pd.read_csv(pcs_file, dtype=str, sep="\t", quotechar='"')

# Step 4: Rename the "s" column in PCS to "person_id"
if pcs.columns[0] != "person_id":
    print(f"\n[DEBUG] Renaming first column of PCS ({pcs.columns[0]}) to 'person_id'")
    pcs.rename(columns={pcs.columns[0]: "person_id"}, inplace=True)

# Step 5: Process the PCS file
# Split "scores" column into individual PCs
if "scores" in pcs.columns:
    pcs_scores = pcs["scores"].str.strip("[]").str.split(",", expand=True)
    pcs_scores.columns = [f"PC{i+1}" for i in range(pcs_scores.shape[1])]
    pcs = pd.concat([pcs["person_id"], pcs_scores], axis=1)

print("\n[DEBUG] PCS after processing:")
print(pcs.head(3))

# Step 6: Ensure "person_id" column is consistent across all datasets
for df_name, df in zip(["Covariates", "Pheno", "PCS"], [covariates, pheno, pcs]):
    if "person_id" in df.columns:
        df["person_id"] = df["person_id"].str.strip()
    else:
        print(f"\n[ERROR] 'person_id' column not found in {df_name}!")
        print(f"Columns in {df_name}: {df.columns.tolist()}")

# Step 7: Perform strict inner joins
merged = covariates.merge(pheno, on="person_id", how="inner")
merged = merged.merge(pcs, on="person_id", how="inner")

print("\n[DEBUG] First 3 rows of merged DataFrame:")
print(merged.head(3))

# Step 8: Handle missing values dynamically
phenotype_column = pheno.columns[1] if len(pheno.columns) > 1 else None
if phenotype_column and phenotype_column in merged.columns:
    merged[phenotype_column].fillna("", inplace=True)

if "scores" in pcs.columns:
    for col in pcs_scores.columns:
        merged[col].fillna("", inplace=True)

# Step 9: Write the final output file
merged.to_csv(output_file, index=False)
print(f"\nFinal merged data written to: {output_file}")

