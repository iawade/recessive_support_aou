import pandas as pd
import json
import gzip

# Load the phenotype mapping from JSON
with open("../updated_pilot_phenotypes_with_phecode.json") as f:
    phenotype_data = json.load(f)

# Create mappings from phecode to phenotype_ID and sex specificity
phecode_to_id = {}
phecode_to_sex = {}

for entry in phenotype_data:
    phecodes = str(entry.get("phecode", "")).split(", ")  # Ensure it's a string
    for phecode in phecodes:
        if phecode:  # Skip empty values
            phecode_to_id[phecode] = entry["phenotype_ID"]
            sex_spec = entry["sex_specific_run"].strip().lower()
            if sex_spec in ["male", "female"]:
                phecode_to_sex[phecode] = sex_spec
            else:
                phecode_to_sex[phecode] = "FALSE"  # Default to FALSE

# Read the input file
input_file = "combined_sorted_results.txt"
output_file = "aou_recessive_combined_results.txt.gz"

# Define the columns to keep
columns_to_keep = [
    "MarkerID", "AC_Allele2", "p.value", "BETA", "SE", "Is.SPA", "Tstat",
    "var", "N", "N_case", "N_ctrl", "Ancestry", "Trait", "phecode", "VariantClass"
]

# Read the file while handling missing or extra spaces in the header
df = pd.read_csv(input_file, sep="\t")

# Censor AC_Allele2 values less than 40
df["AC_Allele2"] = df["AC_Allele2"].apply(lambda x: "<40" if pd.notna(x) and x < 40 else x)

# Preserve the original phecode column
df["phecode"] = df["Trait"]

# Replace phecode with phenotype_ID
df["Trait"] = df["phecode"].apply(lambda x: phecode_to_id.get(str(x), x) if pd.notna(x) else "UNKNOWN")

# Assign sex specificity based on phecode
df["Sex_Specific"] = df["phecode"].apply(lambda x: phecode_to_sex.get(str(x), "FALSE") if pd.notna(x) else "FALSE")

# Select only necessary columns
df = df[columns_to_keep + ["Sex_Specific"]]

# Save as gzipped file
df.to_csv(output_file, sep="\t", index=False, compression="gzip")

print(f"Processed data saved to {output_file}")
