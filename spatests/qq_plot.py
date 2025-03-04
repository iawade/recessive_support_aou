import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as stats

def plot_qq(input_file):
    # Extract output name
    output_name = input_file.split("step2_")[1].rsplit(".txt", 1)[0]
    output_file = f"{output_name}.png"

    # Read the file
    df = pd.read_csv(input_file, delim_whitespace=True)
    
    # Extract p-values
    p_values = df["p.value"]

    # Compute expected quantiles
    n = len(p_values)
    expected = -np.log10((np.arange(1, n+1) / (n+1)))
    observed = -np.log10(np.sort(p_values))

    # Generate QQ plot
    plt.figure(figsize=(6,6))
    plt.scatter(expected, observed, edgecolor='black', alpha=0.6)
    plt.plot([expected[0], expected[-1]], [expected[0], expected[-1]], color="red", linestyle="--")
    plt.xlabel("Expected -log10(p)")
    plt.ylabel("Observed -log10(p)")
    plt.title("QQ Plot")
    plt.grid(True)

    # Save plot
    plt.savefig(output_file, dpi=300)
    print(f"Saved QQ plot: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python qq_plot.py <input_file>")
        sys.exit(1)
    plot_qq(sys.argv[1])
