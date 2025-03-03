import pandas as pd
import statsmodels.api as sm

# Load the data
df = pd.read_csv("physical_measurement_table_v8.csv")

# Calculate BMI (weight in kg, height in cm)
df['height_m'] = df['height'] / 100  # Convert height to meters
df['BMI'] = df['weight'] / (df['height_m'] ** 2)

# Calculate Waist-to-Hip Ratio (WHR)
df['waist_hip_ratio'] = df['waist-circumference-mean'] / df['hip-circumference-mean']

# Drop rows with missing data in necessary columns
df_clean = df.dropna(subset=['BMI', 'waist_hip_ratio'])

# Prepare the data for linear regression
X = df_clean['BMI']  # Independent variable (BMI)
Y = df_clean['waist_hip_ratio']  # Dependent variable (Waist-to-Hip Ratio)

# Add a constant to the independent variable (for the intercept)
X = sm.add_constant(X)

# Run the linear regression
model = sm.OLS(Y, X).fit()

# Get the residuals, which are the WHRadjBMI values
df_clean['WHRadjBMI'] = model.resid

# Select the necessary columns and output to CSV
df_clean[['person_id', 'BMI', 'WHRadjBMI']].to_csv('output_bmi_whradjbmi.csv', index=False)

print("Output saved to 'output_bmi_whradjbmi.csv'")

