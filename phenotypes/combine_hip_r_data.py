import pandas as pd

# Load the main dataset
df1 = pd.read_csv("combined_data.csv")

# Load the procedure status dataset
df2 = pd.read_csv("procedure_concept_4134857_status.csv")

# Merge on 'person_id'
merged_df = df1.merge(df2, on="person_id", how="left")

# Save back as combined_data.csv without quotes in column names
merged_df.to_csv("combined_data.csv", index=False)
