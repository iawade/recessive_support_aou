import json
import pandas as pd

# Load JSON data
with open("pilot_phenotypes_updated.json", "r") as f:
    json_data = json.load(f)

# Load CSV data
csv_data = pd.read_csv("brava_44_phenotype_code.csv")

# Create mapping from brava_code to aou_code (phecode)
brava_mapping = dict(zip(csv_data["brava_code"], csv_data["aou_code"]))

# Update JSON with phecode
for entry in json_data:
    phenotype_id = entry.get("phenotype_ID")
    if phenotype_id in brava_mapping:
        entry["phecode"] = brava_mapping[phenotype_id]

# Save updated JSON
with open("pilot_phenotypes_with_phecode.json", "w") as f:
    json.dump(json_data, f, indent=2)
