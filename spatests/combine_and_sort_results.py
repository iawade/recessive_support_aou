import pandas as pd
import glob
import re

# Define the file pattern
file_pattern = "aou_snp_*_step2_*_vars_*.txt"
files = [f for f in glob.glob(file_pattern) if "ADDITIVE" not in f]


dfs = []

# Process each file
for file in files:
    # Extract ancestry, trait, and variant class from filename
    match = re.search(r'aou_snp_(.*?)_step2_(.*?)_vars_(.*?)\.txt', file)
    if not match:
        continue
    ancestry, trait, variant_class = match.groups()

    # Read file
    df = pd.read_csv(file, sep="\t")

    # Add extracted metadata as new columns
    df["Ancestry"] = ancestry
    df["Trait"] = trait
    df["VariantClass"] = variant_class

    # Apply filtering if columns exist
    if "N_case" in df.columns:
        df["N_case"] = pd.to_numeric(df["N_case"], errors="coerce")
        df = df[df["N_case"] >= 100]  # Keep only if N_case >= 100

    if "AF_Allele2" in df.columns:
        df["AF_Allele2"] = pd.to_numeric(df["AF_Allele2"], errors="coerce")
        df = df[df["AF_Allele2"] < 0.5]  # Keep only if AF_Allele2 < 0.5


    dfs.append(df)

# Combine all dataframes
combined_df = pd.concat(dfs, ignore_index=True)

# Convert p.value to numeric (handle potential errors)
combined_df["p.value"] = pd.to_numeric(combined_df["p.value"], errors="coerce")

# Sort by p.value (ascending)
sorted_df = combined_df.sort_values(by="p.value", ascending=True)

# Save to a new file
sorted_df.to_csv("combined_sorted_results.txt", sep="\t", index=False)

print("Files merged and sorted. Output saved as 'combined_sorted_results.txt'.")

