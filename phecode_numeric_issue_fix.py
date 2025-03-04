import pandas as pd
import json

# File paths
csv_file = "combined_data.csv"
json_file = "pilot_phenotypes_with_phecode.json"
updated_csv_file = "updated_combined_data.csv"
updated_json_file = "updated_pilot_phenotypes_with_phecode.json"

# Update CSV column names
def update_csv_columns(csv_file):
    # Load the CSV file
    df = pd.read_csv(csv_file)

    # Iterate over the columns and add 'phe_' prefix to numeric column names
    df.columns = [f"phe_{col}" if str(col).isnumeric() else col for col in df.columns]

    # Save the updated CSV
    df.to_csv(updated_csv_file, index=False)
    print("CSV column names updated!")

# Update JSON phenotype codes
def update_json_phenotypes(json_file):
    # Load the JSON file
    with open(json_file, "r") as f:
        phenotype_data = json.load(f)

    # Function to add 'phe_' prefix to numeric codes
    def update_phecodes(entry):
        if isinstance(entry.get("phecode"), list):
            # If phecode is a list, prefix each item
            entry["phecode"] = [f"phe_{code}" if str(code).isnumeric() else code for code in entry["phecode"]]
        elif isinstance(entry.get("phecode"), str) and entry["phecode"].isnumeric():
            # If phecode is a single numeric string, add the prefix
            entry["phecode"] = f"phe_{entry['phecode']}"
        return entry

    # Update each phenotype entry in the JSON
    phenotype_data = [update_phecodes(entry) for entry in phenotype_data]

    # Save the updated JSON
    with open(updated_json_file, "w") as f:
        json.dump(phenotype_data, f, indent=4)

    print("Phenotype codes updated!")

# Run both update functions
update_csv_columns(csv_file)
update_json_phenotypes(json_file)
