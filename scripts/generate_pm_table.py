# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Background ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#The physical measurement table contains height, weight, heart rate, hip circumference, waist circumference
#    , blood pressure systolic, and blood pressure diastolic values per participant.
#The program calculated mean values were used for all the measurements except for height and weight. 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Code to generate the PFHH table~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
import pandas as pd
#dataset = %env WORKSPACE_CDR
dataset = "fc-aou-cdr-prod-ct.C2024Q3R5"

calculated_pm_df = pd.read_gbq(f'''SELECT DISTINCT person_id, measurement_source_value, value_as_number
                                    FROM `{dataset}.measurement` 
                                    WHERE LOWER(measurement_source_value) IN ('weight', 'height'
                                                                            , 'waist-circumference-mean'
                                                                            , 'hip-circumference-mean'
                                                                            , 'heart-rate-mean'
                                                                            , 'blood-pressure-diastolic-mean'
                                                                            , 'blood-pressure-systolic-mean')''')
#pm_df = calculated_pm_df.pivot(index = ['person_id'], columns = 'measurement_source_value'
#                        , values= 'value_as_number').reset_index()

pm_df = calculated_pm_df.pivot_table(
    index='person_id',
    columns='measurement_source_value',
    values='value_as_number',
    aggfunc='median'  # or 'min', 'max', 'first', etc., depending on your use case
).reset_index()

pm_df.to_csv('physical_measurement_table', index = False)
            
