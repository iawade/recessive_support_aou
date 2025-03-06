import pandas as pd
import glob
import re

# Define the file pattern (excluding ADDITIVE)
file_pattern = "aou_snp_*_step2_*_vars_*.txt"
files = glob.glob(file_pattern)

# Set to store unique combinations
unique_combos = set()

# Process each file
for file in files:
    # Extract ancestry, trait, and variant class from filename
    match = re.search(r'aou_snp_(.*?)_step2_(.*?)_vars_(.*?)\.txt', file)
    if not match:
        continue
    ancestry, trait, variant_class = match.groups()

    # Read file
    df = pd.read_csv(file, sep="\t")

    # Check if required columns exist
    if "MarkerID" not in df.columns or "AF_Allele2" not in df.columns:
        print(f"Skipping {file}: Missing required columns.")
        continue

    # Filter for AF_Allele2 >= 0.5
    filtered_df = df[df["AF_Allele2"] >= 0.5]

    # Extract unique gene + ancestry + variant class
    for gene in filtered_df["MarkerID"].unique():
        unique_combos.add((gene, ancestry, variant_class))

# Convert set to DataFrame
output_df = pd.DataFrame(unique_combos, columns=["Gene", "Ancestry", "VariantClass"])

# Save to file
output_df.to_csv("high_AF_combos.txt", sep="\t", index=False)

print("Extraction complete. Results saved in 'high_AF_combos.txt'.")
