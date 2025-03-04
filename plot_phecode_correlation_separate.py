import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr

def extract_phecodes(json_file):
    """Extract phenotype IDs where there are two phecodes."""
    with open(json_file, "r") as f:
        phenotypes = json.load(f)

    two_phecode_phenos = []
    for pheno in phenotypes:
        if "phecode" in pheno and isinstance(pheno["phecode"], str):
            phecodes = pheno["phecode"].split(",")
            if len(phecodes) == 2:
                two_phecode_phenos.append((pheno["phenotype_ID"], phecodes[0].strip(), phecodes[1].strip()))

    return two_phecode_phenos

def plot_correlation(csv_file, json_file):
    """Generate scatter plot and compute correlation for phecodes."""
    # Load data
    df = pd.read_csv(csv_file)

    # Extract relevant phecodes
    phecode_pairs = extract_phecodes(json_file)

    for phenotype_ID, phe1, phe2 in phecode_pairs:
        if phe1 not in df.columns or phe2 not in df.columns:
            print(f"Skipping {phenotype_ID}: {phe1} or {phe2} not in CSV.")
            continue

        # Convert to boolean count (True/False)
        df_subset = df[[phe1, phe2]].apply(lambda col: col.astype(str).str.lower() == "true")
        df_counts = df_subset.sum()

        # Scatter plot
        plt.figure(figsize=(6,6))
        sns.scatterplot(x=df_subset[phe1], y=df_subset[phe2], alpha=0.6)
        plt.xlabel(f"Cases in {phe1}")
        plt.ylabel(f"Cases in {phe2}")
        plt.title(f"{phenotype_ID}: {phe1} vs {phe2}")

        # Compute correlation
        correlation, p_value = pearsonr(df_subset[phe1].astype(int), df_subset[phe2].astype(int))
        print(f"{phenotype_ID}: Correlation = {correlation:.4f}, p-value = {p_value:.4g}")

        # Save plot
        plt.savefig(f"{phenotype_ID}_{phe1}_vs_{phe2}.png", dpi=300)
        plt.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python plot_phecode_correlation.py <json_file> <csv_file>")
        sys.exit(1)

    plot_correlation(sys.argv[2], sys.argv[1])
