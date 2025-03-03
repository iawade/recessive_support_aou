import pandas as pd

# Load the CSV file
df = pd.read_csv("demographics_table_unbinned.csv")

# Drop missing values in 'age_at_cdr' or 'sex_at_birth'
df = df.dropna(subset=["age_at_cdr", "sex_at_birth"])

# Exclude people with age > 100
df = df[df["age_at_cdr"] <= 100]

# Exclude unwanted sex values
invalid_sex_values = {
    "Intersex",
    "I prefer not to answer",
    "PMI: Skip",
    "Sex At Birth: Sex At Birth None Of These",
    "Unknown"
}
df = df[df["sex_at_birth"].isin(["Female", "Male"])]

# Map 'Female' to 1 and 'Male' to 0
df["sex"] = df["sex_at_birth"].map({"Female": 1, "Male": 0}).astype(int)

# Convert age to int and calculate new columns
df["age"] = df["age_at_cdr"].astype(int)
df["age2"] = (df["age"] ** 2).astype(int)
df["age_sex"] = (df["age"] * df["sex"]).astype(int)
df["age2_sex"] = (df["age2"] * df["sex"]).astype(int)

# Select final columns
df = df[["person_id", "age", "age2", "age_sex", "age2_sex", "sex"]]

# Save to a new CSV file
df.to_csv("demographics_filtered_covariates.csv", index=False)

print("Transformation complete! Output saved")

