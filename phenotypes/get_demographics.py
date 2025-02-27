import pandas as pd
from google.cloud import bigquery
#import pandas_gbq

dataset = "fc-aou-cdr-prod-ct.C2024Q3R5"

demographics_df = pd.read_gbq(f'''SELECT DISTINCT person_id, age_at_cdr, sex_at_birth, 
                                FROM `{dataset}.cb_search_person`''')

demographics_df.to_csv('demographics_table_unbinned.csv', index = False)

age_bins = [0,29,39,49,59,69,79,89,200]
age_labels = ["18-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89", "90+"]
demographics_df['age_group'] = pd.cut(demographics_df['age_at_cdr']
                                      , bins=age_bins, labels=age_labels, include_lowest=True)

demographics_df.to_csv('demographics_table.csv', index = False)
