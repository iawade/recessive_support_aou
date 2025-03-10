import pandas as pd

# Load the data
df = pd.read_csv("combined_sorted_results.txt", sep="\t")

# Filter rows where p.value < 1e-7
df_filtered = df[df["p.value"] < 1e-7]

# Save to hits.tsv
df_filtered.to_csv("hits.tsv", sep="\t", index=False)
