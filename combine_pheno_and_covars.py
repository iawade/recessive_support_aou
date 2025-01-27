import pandas as pd

# Step 1: Load the datasets
covariates_file = "sorted_covariates.csv"
diabetes_file = "sorted_diabetes.tsv"

# Load covariates (CSV) and diabetes (TSV)
covariates = pd.read_csv(covariates_file, header=None, names=["person_id", "age", "age2", "age_sex", "age2_sex", "sex"], dtype={"person_id": str})
diabetes = pd.read_csv(diabetes_file, header=None, names=["person_id", "T2Diab"], sep="\t", dtype={"person_id": str})

# Print the first few rows of each file to verify that they are loaded correctly
print("Covariates Data:")
print(covariates.head())  # Inspect the first few rows of the covariates data

print("\nDiabetes Data:")
print(diabetes.head())  # Inspect the first few rows of the diabetes data

# Step 2: Strip any leading/trailing spaces from person_id to ensure proper matching
covariates["person_id"] = covariates["person_id"].str.strip()
diabetes["person_id"] = diabetes["person_id"].str.strip()

# Step 3: Merge the two datasets on "person_id", using a left join
merged = pd.merge(covariates, diabetes, on="person_id", how="left")

# Print the merged dataframe to check the result
print("\nMerged Data (Before Handling Missing T2Diab):")
print(merged.head())  # Print the merged data before filling missing values

# Step 4: Ensure missing values are handled correctly (if no match, T2Diab will be NaN, then replace with "")
merged["T2Diab"].fillna("", inplace=True)

# Print the data after filling missing T2Diab values
print("\nMerged Data (After Handling Missing T2Diab):")
print(merged.head())  # Print the merged data after handling missing values

# Step 5: Reorder columns and write the final output to CSV
merged = merged[["person_id", "T2Diab", "age", "age2", "age_sex", "age2_sex", "sex"]]

# Print final output before writing it to a file
print("\nFinal Merged Data (Ready to Write to CSV):")
print(merged.head())  # Inspect the final data that will be written to CSV

# Write to final output file
output_file = "diabetes_and_covariates.csv"
merged.to_csv(output_file, index=False)

# Step 6: Inspect the result by printing the output file path
print(f"\nFinal output written to: {output_file}")
