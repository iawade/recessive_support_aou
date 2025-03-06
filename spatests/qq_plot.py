import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import re

def plot_qq(input_file):
    # Extract ancestry, trait, and variant class from filename
    match = re.search(r'aou_snp_(.*?)_step2_(.*?)_vars_(.*?)\.txt', input_file)
    if not match:
        print(f"Filename format not recognized: {input_file}")
        sys.exit(1)
    
    ancestry, trait, variant_class = match.groups()
    output_file = f"qqplot_{ancestry}_{trait}_{variant_class}.png"

    # Read the file
    df = pd.read_csv(input_file, delim_whitespace=True)
    # Filter out rows where AF_Allele2 > 0.5
    df = df[df["AF_Allele2"] < 0.5]

    # Extract p-values
    p_values = df["p.value"].dropna()

    # Compute expected quantiles
    n = len(p_values)
    expected = -np.log10((np.arange(1, n + 1) / (n + 1)))
    observed = -np.log10(np.sort(p_values))

    # Construct title
    title = f"QQ Plot: {ancestry} - {trait} - {variant_class}"
    
    # If N_case column exists, add to title
    if "N_case" in df.columns:
        total_cases = df["N_case"].median()  # 
        title += f" (N_cases: {total_cases})"

    # Generate QQ plot
    plt.figure(figsize=(6,6))
    plt.scatter(expected, observed, edgecolor='black', alpha=0.6)
    plt.plot([expected[0], expected[-1]], [expected[0], expected[-1]], color="red", linestyle="--")
    plt.xlabel("Expected -log10(p)")
    plt.ylabel("Observed -log10(p)")
    plt.title(title)
    plt.grid(True)

    # Save plot
    plt.savefig(output_file, dpi=300)
    plt.close()
    print(f"Saved QQ plot: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python qq_plot.py <input_file>")
        sys.exit(1)
    plot_qq(sys.argv[1])
