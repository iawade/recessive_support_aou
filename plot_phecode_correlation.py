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
    """Plot all phecode pairs on the same scatter plot, add x=y line, and compute correlation."""
    # Load data
    df = pd.read_csv(csv_file)

    # Extract relevant phecodes
    phecode_pairs = extract_phecodes(json_file)

    # Store data for plotting
    plot_data = []

    for phenotype_ID, phe1, phe2 in phecode_pairs:
        if phe1 not in df.columns or phe2 not in df.columns:
            print(f"Skipping {phenotype_ID}: {phe1} or {phe2} not in CSV.")
            continue

        # Convert to boolean count (True/False)
        df_subset = df[[phe1, phe2]].apply(lambda col: col.astype(str).str.lower() == "true")
        df_counts = df_subset.sum()

        # Store for plotting
        plot_data.append({
            "phenotype_ID": phenotype_ID,
            "phecode_1": phe1,
            "phecode_2": phe2,
            "cases_1": df_counts[phe1],
            "cases_2": df_counts[phe2]
        })

    # Convert to DataFrame
    plot_df = pd.DataFrame(plot_data)

    if plot_df.empty:
        print("No valid phecode pairs found in the data.")
        return

    # Sort data for better visualization
    plot_df = plot_df.sort_values(by="cases_1")

    # Scatter plot
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=plot_df["cases_1"], y=plot_df["cases_2"], alpha=0.7)

    # Add x=y line
    max_val = max(plot_df["cases_1"].max(), plot_df["cases_2"].max())  # Get max range
    plt.plot([0, max_val], [0, max_val], linestyle="--", color="red", label="x = y")

    # Label each point with its phecodes
    for _, row in plot_df.iterrows():
        plt.text(row["cases_1"], row["cases_2"], f"{row['phecode_1']}\n{row['phecode_2']}",
                 fontsize=8, ha="right", va="bottom")

    plt.xlabel("Cases in Phecode 1")
    plt.ylabel("Cases in Phecode 2")
    plt.title("Phecode Case Count Correlation")
    plt.legend()

    # Compute overall correlation across all pairs
    correlation, p_value = pearsonr(plot_df["cases_1"], plot_df["cases_2"])
    print(f"Overall Correlation: {correlation:.4f}, p-value: {p_value:.4g}")

    # Save the plot
    plt.savefig("phecode_correlation.png", dpi=300)
    plt.show()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python plot_phecode_correlation.py <json_file> <csv_file>")
        sys.exit(1)

    plot_correlation(sys.argv[2], sys.argv[1])
