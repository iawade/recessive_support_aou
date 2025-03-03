import pandas as pd

# Load the preprocessed CSV data
df = pd.read_csv("measurement_3004410_preprocessed.csv")

# Filter out rows with unwanted unit values
df_filtered = df[~df['unit_concept_name'].isin([
    'No matching concept', 'no value', 'Percentage of total', 'unit', 'Seconds per CO', 'gram per deciliter'
])]

# Conversion of 'milligram per deciliter' to percent
df_filtered.loc[df_filtered['unit_concept_name'] == 'milligram per deciliter', 'value_as_number'] = (
    (df_filtered['value_as_number'] + 46.7) / 28.7
)

# Filter out rows where 'value_as_number' is greater than 100
df_filtered = df_filtered[df_filtered['value_as_number'] <= 50]


# Calculate the median value per person_id
df_median = df_filtered.groupby('person_id')['value_as_number'].median().reset_index()

# Save the result to a new CSV file
df_median.to_csv('measurement_3004410_summary.csv', index=False)

print("Median per person_id calculated and saved to 'measurement_3004410_summary.csv'.")
