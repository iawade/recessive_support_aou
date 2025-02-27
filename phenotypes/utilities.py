
import pandas as pd
import os
import subprocess
from google.cloud import bigquery
import nbformat as nbf
from nbconvert.preprocessors import CellExecutionError, ExecutePreprocessor
import shutil
import time
from datetime import datetime

client = bigquery.Client()

def client_read_gbq(query, dataset = os.getenv('WORKSPACE_CDR')):
    
    job_config = bigquery.QueryJobConfig(default_dataset=dataset)
    query_job = client.query(query, job_config =job_config)  # API request
    df = query_job.result().to_dataframe()

    return df

def make_clickable(url, name):
    return f'<a href="{url}">{name}</a>'

def click(df):
    DF = df.copy()
    DF['notebook_url'] = DF.apply(lambda x: make_clickable(x['notebook_url'], x['notebook_name']), axis=1)
    DF = DF.drop('notebook_name', axis= 1)
    return DF.style.format({'url': make_clickable})

def get_phe_dd(dtype):
    phe_dd = { #[index file input, datafile, col_name]
        'lab':['lab_measurements_index_raw.csv', 'lab_measurements_index.csv', 'measurement_concept_id']
        , 'phecode':['phecode_info.csv', 'mcc2_phecode_table.csv', 'phecode']
        , 'phecodex':['phecodex_info.csv', 'mcc2_phecodex_table.csv', 'phecodex']
        , 'physical_measurement':['', 'physical_measurement_table.csv', 'measurement_source_value']
        , 'drug':['', 'r_drug_table.csv', 'concept_code']
        , 'pfhh':['', 'pfhh_survey_table.csv', 'concept_id']
        }
    return phe_dd[dtype]

def create_phenotype_index_file(dtype, save = True, BUCKET  = os.getenv('WORKSPACE_BUCKET')):
    
    # create phenotype index for all datatypes, except lab
    #BUCKET = os.getenv('WORKSPACE_BUCKET')
    dtype = dtype.lower()
    bucket_nb_phe = f'{BUCKET}/notebooks/phenotype_data'    
    
    phe_dd = get_phe_dd(dtype)
                       
    if dtype in ['phecode', 'phecodex','lab']:
        phe_index = pd.read_csv(f'{bucket_nb_phe}/{phe_dd[0]}', float_precision='high')
    else:
        concepts = pd.read_csv(f'{bucket_nb_phe}/{phe_dd[1]}', nrows = 0).drop('person_id', axis = 1).columns
        query_dd = {
            'physical_measurement': """SELECT DISTINCT concept_id, LOWER(concept_name) as concept_name
                                        , LOWER(measurement_source_value) as measurement_source_value
                                        FROM `measurement` 
                                        JOIN `concept` on measurement_concept_id = concept_id
                                        WHERE LOWER(measurement_source_value) IN {concepts} """

            , 'drug': """SELECT DISTINCT concept_code, LOWER(concept_name) as concept_name 
                         FROM `concept` WHERE concept_code IN {concepts} 
                         AND domain_id = 'Drug' """

            , 'pfhh': """SELECT DISTINCT concept_id, LOWER(concept_name) as concept_name
                          , REPLACE(REPLACE(LOWER(concept_name)
                             , 'including yourself, who in your family has had', '')
                                ,'select all that apply.', '') as short_name
                        FROM `concept` 
                        WHERE CAST(concept_id AS STRING) IN {concepts} """
        }  
             
        query = query_dd[dtype].format(concepts = tuple(concepts))
        phe_index = client_read_gbq(query).sort_values('concept_name')
    
    ####################
    pwd = os.getenv('PWD')
    nspace = os.getenv('WORKSPACE_NAMESPACE')
    wk_id = nspace+'/'+pwd.split('workspaces/')[1]
    workspace_link = f'https://workbench.researchallofus.org/workspaces/{wk_id}/analysis/preview'
    
    phe_index['notebook_name'] = [get_notebook_name(dtype, i) for i in phe_index[phe_dd[2]]]
    phe_index['notebook_url'] = workspace_link+'/'+phe_index['notebook_name']
    
    if save == True:
        index_filename =phe_dd[1].replace('table','index')
        phe_index.to_csv(f'{bucket_nb_phe}/{index_filename}', index = False)
        print(f'Saved {index_filename} to bucket.')
    
    return phe_index

    # create_phenotype_index_file(dtype = 'physical_measurement')
    # create_phenotype_index_file(dtype = 'pfhh')
    # create_phenotype_index_file(dtype = 'drug')
    # create_phenotype_index_file(dtype = 'phecode')
    # create_phenotype_index_file(dtype = 'phecodex')
    # create_phenotype_index_file(dtype = 'lab')

def get_labs_notebook_code(datatype, concept_id, phenotype_name):
    
#################################### LABS ##########################################################
    phenotype_name_str = phenotype_name.title()
    concept_str_intro = concept_id.replace(', '+concept_id.split(', ')[-1], ' and '+concept_id.split(', ')[-1])
    mrkd_intro = f"""
<div class='alert alert-info' style='font-family:Arial; font-size:20px' ><center><font size="6"> Lab Measurement(s) {concept_str_intro}: <br /><br /> <font size="5"> {phenotype_name_str}</font></center></div> </font></center></div>

This notebook details how the data for laboratory measurement(s) {concept_str_intro} was curated for All by All analysis. The sections are as follows:

- **Notebook setup**: Import Unifier python library, read reference tables into memory.
- **Unit harmonization and outlier removal**: Select measurement of interest, harmonize all reported units to a standard unit, and remove outlier values.
- **Data visualization**: Plot the data before and after unit harmonization to assess efficacy and identify the need for further quality control processes.
- **Quality control**: Drop data for the selected laboratory measurement at sites with data quality concerns after unit harmonization, as needed.
- **Changing outlier boundaries**: Change the thresholds used to define outliers for the selected laboratory measurement, as needed.
- **Participant-level summary**: Obtain a table of participant-level summary (min, median, max, latest, and count of measurements) from the curated data.


**Note:** This notebook has been automatically generated. If you wish to rerun it, you need to select the '*Python 3*' kernel (top menu) for the code to run. You would also need to run the set up cell in the '0_ReadMe' notebook once.
    
# Notebook setup
In this section you will import the Unifier python library and read reference tables into memory. """   
        
    code1 = """from unifier_functions import *
from IPython.core.interactiveshell import InteractiveShell

# Read in tables for unit processing
tables = read_tables(pipeline_version='v1_0')
metadata = tables['metadata']
unit_map_table = tables['unit_map']
unit_reduce_table = tables['unit_reduce']"""
        
    mrkd1 = """# Unit harmonization and outlier removal
In this section you will perform unit harmonization for measurement(s) of interest and remove outlier values.

## Select measurement of interest
The cell below is used to select the measurement of interest by defining the measurement_concept_id.

Please note that multiple measurement_concept_id's can be assigned to process the data for these measurements as seen in Example 2."""
        
    code2 = f"""# Define m_cid as a list of integers containing the OMOP measurement_concept_id for the measurement of interest
m_cid = [{concept_id}]

# Query CDR v7 Controlled Tier
measurement = measurement_data(m_cid)

# Summarize OMOP query
query_summary(measurement)"""      
        
    mrkd2 = """# Harmonize units and drop outlier values
The cell below will harmonize all units to a standard unit as defined in the resource *metadata* by applying conversion factors specified in the resource *unit_reduce_table. Outlier values are dropped based on upper and lower thresholds in the metadata* resource.

The lifecycle of the selected measurement unit and value data is detailed in the *df_harmonized* dataframe.

The processed data is written as the *df_final* dataframe."""

    code3 = """# Preprocess the input measurement dataframe and ensure it meets the proper format for Unifier
preprocessed = preprocess(measurement)

# Produce dataframe with lifecycle of measurement unit harmonization and labeling missing/outlier values
df_harmonized = harmonize(preprocessed, metadata, unit_map_table, unit_reduce_table)

# Final dataframe with measurement data after completing unit harmonization and dropping outliers
df_final = trim(df_harmonized)

# Descriptive statistics after outliers are removed
unitdata = units_dist(df_harmonized)"""        
        
    mrkd3 = """The cell below will plot all data for the selected *measurement_concept_id* in the original reported units. 
The vertical red line indicates the upper threshold used to define outliers but is only applicable on the plot for values in the standard unit.

The standard unit and outlier thresholds for each measurement included in All by All are defined in the *metadata* table."""
        
    code4 = """InteractiveShell.ast_node_interactivity = "all"

max_value = metadata[metadata['measurement_concept_id']==m_cid[0]]['maximum_value'].iloc[0]
unit_list = df_harmonized['assigned_unit'].unique()
sorted_unit_list = np.sort(unit_list)
value_type = 'unedited_value_as_number'

print("You can change bin size using the 'bin_factor' argument.")
for unit_name in sorted_unit_list:
    plot_hist_single_lab_unit(df_harmonized, df_harmonized['lab_name'].iloc[0], unit_name, value_type
                                ,maximum_value=max_value,low_cutoff=0, high_cutoff=max_value*10, bin_factor=100)
"""    
        
    mrkd4 = """# Data visualization
In this section you will plot measurement data before and after unit harmonization and outlier filtering. 
The data will be formatted as boxplots and stratified by electronic health record site and unit type. This will help visualize the data to assess the effectiveness of the Unifier workflow on preparing measurement data in a research-ready format.

## Visualization prior to unit harmonization and outlier filtering
The cell below plots the original data in their reported units without processing. These plots do exclude frequencies of missing values and missing value equivalents (i.e. value_as_number == 10,000,000 AND value_as_number == 100,000,000).
"""        
    code5 = """#bplot_filtered displays all values for the lab measurement in the original units
bplot_filtered = boxplot(df_harmonized, 'annotated', fill_col='assigned_unit', value_col='unedited_value_as_number')
bplot_filtered"""
        
    mrkd5 = """## Visualization after data processing
The cell below plots the data which were able to be harmonized to the standard unit and were not labeled as outliers."""
        
    code6 = """# bplot_final displays only non-outlier values which were able to be harmonized to the standard unit
bplot_final = boxplot(df_final, 'final', fill_col='unit', value_col='value_as_number')
bplot_final"""

    #####
    mrkd6 = """# Quality Control
This section of the notebook allows the user to drop the data from a site which did not harmonize correctly when applicable. 
In the first line of code below, specfic site(s) can be inputted to drop all data corresponding to the site. The dataframe containing processed data is updated and are re-plotted to allow visualization of the data following removal of specific site(s) data. 
"""        
    code7 = """#Drop all data from sites which did not harmonize correctly, if applicable
## Multiple sites can be inputted as a list to drop values corresponding to all sites listed.
df_harmonized_dropped = drop_ehr_site(df_harmonized, which_sites = [537])

#Regenerate df_trimmed using the updated df_harmonized, if applicable
df_trimmed_dropped = trim(df_harmonized_dropped)

#Plot bplot_final again after removing sites which did not harmonize correctly
bplot_final = boxplot(df_trimmed_dropped, 'final', fill_col='unit', value_col='value_as_number')
bplot_final"""
   ######

    mrkd7 = """# Changing outlier boundaries
This section of code allows users to update the upper and lower outlier thresholds as needed. 
Sections 1 - 3 of the notebook should be re-run using the updated_metadata table after updating the bounds to curate the lab measurements using the newly defined bounds.

The cell below outputs the original minimum and maximum value for the lab measurement defined in the metadata table."""
      
    code8 = """#metadata[metadata['lab_name']=='hematocrit']"""
    
    mrkd8 = """The cell below should be used to update outlier bounds. The third argument defines the minimum (lower) bound and the fourth argument defines the maximum (upper) bound. The values for the upper and lower bounds should be specified in the standard units."""
    code9 = """#updated_metadata = update_outlier_bounds(metadata, 'hematocrit', None, 1000)"""        
        
    mrkd9 = """This cell outputs the updated minimum and maximum value for the lab measurement defined in the metadata table."""
    code10 = """#updated_metadata[updated_metadata['lab_name']=='hematocrit']"""
        
    mrkd10 = """# Participant-level summary
The cell below summarizes the curated data at the participant level."""
    
    code11 = f"""pid_summary = pid_level_summary([{concept_id}], df_trimmed_dropped)
pid_summary.head()"""
    
    
 #########   
    sites_drop_dd = {'3019897': '[537]',
                     '3024929': '[310, 537, 609]',
                     '3008342': '[537]',
                     '3013869': '[250, 537]',
                     '3014576': '[537]',
                     '3013682': '[243, 250, 609]',
                     '3045716': '[609]',
                     '3013721': '[250]',
                     '3006923': '[250]',
                     '3035995': '[278, 609]',
                     '3007359': '[321]',
                     '3027114': '[243]',
                     '3009201': '[144]',
                     '3015632': '[250, 537, 609]',
                     '3018311': '[250, 433, 537, 734]',
                     '3026910': '[250, 930]',
                     '3016436': '[412, 962]',
                     '3001420': '[148, 250, 679]',
                     '3045440, 3023939': '[734, 925]',
                     '3043359, 3027450': '[433, 734, 925, 962]',
                     '3045792, 3016921': '[734, 925]',
                     '3001008': '[321, 497]',
                     '3035124': '[194, 195, 321, 433, 497]',
                     '3035583': '[194, 195, 250, 433, 397]',
                     '3022621': '[250, 537]'
                    }
    
    if concept_id not in list(sites_drop_dd.keys()):
        mrkd6 = mrkd6+"""\n\n**Not applicable for this lab**. No EHR sites have been dropped."""
        code7 = """"""
        code11 = code11.replace('df_trimmed_dropped', 'df_final')

    else:
        code7 = code7.replace('[537]', sites_drop_dd[concept_id])
               
    nb_code = [nbf.v4.new_markdown_cell(mrkd_intro)
                , nbf.v4.new_code_cell(code1), nbf.v4.new_markdown_cell(mrkd1)
                , nbf.v4.new_code_cell(code2), nbf.v4.new_markdown_cell(mrkd2)
                , nbf.v4.new_code_cell(code3), nbf.v4.new_markdown_cell(mrkd3)
                , nbf.v4.new_code_cell(code4), nbf.v4.new_markdown_cell(mrkd4)
                , nbf.v4.new_code_cell(code5), nbf.v4.new_markdown_cell(mrkd5)
                , nbf.v4.new_code_cell(code6), nbf.v4.new_markdown_cell(mrkd6)
                , nbf.v4.new_code_cell(code7), nbf.v4.new_markdown_cell(mrkd7)
                , nbf.v4.new_code_cell(code8), nbf.v4.new_markdown_cell(mrkd8)
                , nbf.v4.new_code_cell(code9), nbf.v4.new_markdown_cell(mrkd9)
                , nbf.v4.new_code_cell(code10), nbf.v4.new_markdown_cell(mrkd10)
                , nbf.v4.new_code_cell(code11)
              ]
    
    return nb_code
    
def get_notebook_code(datatype, concept_id, phenotype_name):
    
############################################# OTHERS ###############################################
    phenotype_name_str = phenotype_name.replace(' select all that apply.', '').title()
    dtype_mk = datatype.title().replace('Pfhh', 'PFHH').replace('Physical_Measurement', '')
    cid_mk = str(concept_id).replace('-', ' ').title()

    mrkd_intro = f"""
<div class='alert alert-info' style='font-family:Arial; font-size:20px' ><center><font size="6"> {cid_mk} {dtype_mk} Phenotype Summary: <br /><br /> <font size="5"> {phenotype_name_str}</font></center></div>

This notebook includes graphical summaries of cases and controls (see '0_ReadMe') for the {cid_mk} {dtype_mk} phenotype. 

**Note:**
- If you have not already, **run the set up in the '0_ReadMe' once** before running the notebooks in this workspace.
- This notebook has been automatically generated. If you wish to rerun it, you may need to choose the R kernel (top menu).
"""   
    mrkd_setup = f"""# Import summary functions
source('cat_data_summary_functions.R'); source('utilities.R')"""

#######
    mrkd_cat_sum_intro = f"""# Summary    
Below are summaries of cases and controls for the {cid_mk} {datatype} phenotype:
- Overall summaries by 1) age, 2) sex at birth, and 3) ancestry.
- Detailed summaries by 1) sex at birth and ancestry and 2) age and ancestry. """ 
    
    code_cat = f"""categorical_data_summary(concept_id = '{concept_id}', datatype = '{datatype}')"""

#######
    mrkd_cont_sum_intro = f"""# Summary   
Below are summaries for {cid_mk} {dtype_mk}.
- A table of descriptive statistics.
- An histogram and a boxplot.
- Demographic counts: 1) age, sex at birth, ancestry, 4) sex at birth and ancestry and 5) age and ancestry."""
    
    code_cont = f"""pm_data_summary(concept_name = '{concept_id}')"""
    
################################################# create notebook  ##################################################
    r_code_dd = {"drug":[mrkd_intro, mrkd_setup, mrkd_cat_sum_intro, code_cat]
                ,"phecode":[mrkd_intro.replace(cid_mk, f'Phecode {cid_mk}').replace(f'{dtype_mk} Phenotype ','')
                                 , mrkd_setup, mrkd_cat_sum_intro, code_cat]
                ,"phecodex":[mrkd_intro.replace(cid_mk, f'PhecodeX {cid_mk}').replace(f'{dtype_mk} Phenotype ','')
                                  , mrkd_setup, mrkd_cat_sum_intro, code_cat]
                ,"pfhh":[mrkd_intro, mrkd_setup, mrkd_cat_sum_intro.replace('FALSE', 'TRUE'),code_cat]
                ,"physical_measurement":[mrkd_intro
                                        , mrkd_setup.replace('cat_data_summary_functions.R', 'cont_data_summary_functions.R')
                                        , mrkd_cont_sum_intro, code_cont]}     
    ##########
    nb_code = [nbf.v4.new_markdown_cell(r_code_dd[datatype][0]), nbf.v4.new_code_cell(r_code_dd[datatype][1])
                       , nbf.v4.new_markdown_cell(r_code_dd[datatype][2]), nbf.v4.new_code_cell(r_code_dd[datatype][3])]
        
    return nb_code
        
def get_notebook_name(datatype, concept_id):
    if datatype == 'physical_measurement':
        pm_name = str(concept_id).lower().replace('-','_')
        notebook_name = f"{pm_name}_summary.ipynb"
    elif datatype == 'lab': 
        concept_id_str = str(concept_id).replace(', ','_')
        notebook_name = f"measurement_{concept_id_str}.ipynb"
    else: notebook_name = f"{datatype}_{str(concept_id)}_summary.ipynb"
    
    return notebook_name

def generate_notebook(concept_id, datatype, notebook_name, phenotype_name):
    
    # initiate notebook
    nb = nbf.v4.new_notebook() 
    
    # write notebook code to cells
    if datatype == 'lab': nb['cells'] = get_labs_notebook_code(datatype, concept_id, phenotype_name)
    else: nb['cells'] = get_notebook_code(datatype, concept_id, phenotype_name)
    
    # Save notebook to bucket
    bucket = os.getenv('WORKSPACE_BUCKET')
    nbf.write(nb, notebook_name)
    os.system(f"gsutil cp {notebook_name} {bucket}/notebooks/")    

def get_kernel(kernel):
    return 'ir' if kernel.lower() == 'r' else 'python3'

def run_notebook(NOTEBOOK_TO_RUN, KERNEL = 'r'):

    KERNEL_NAME = get_kernel(KERNEL)    
    OUTPUT_NOTEBOOK = NOTEBOOK_TO_RUN
    bucket = os.getenv('WORKSPACE_BUCKET')
    
    with open(NOTEBOOK_TO_RUN) as f_in:
        nb = nbf.read(f_in, as_version=4)
        ep = ExecutePreprocessor(timeout=-1, kernel_name=KERNEL_NAME)
        out = ep.preprocess(nb, {'metadata': {'path': ''}})
        
    with open(OUTPUT_NOTEBOOK, mode='w', encoding='utf-8') as f_out:
        nbf.write(nb, f_out)
        os.system(f"gsutil cp {OUTPUT_NOTEBOOK} {bucket}/notebooks/{OUTPUT_NOTEBOOK}")
        
    print(f'Ran and saved {OUTPUT_NOTEBOOK}.')
    
def generate_and_run_all_nb(datatype, kernel = 'r', concept_ids = None):
    
    phe_dd = get_phe_dd(datatype)
    phe_filename = phe_dd[1]
    index_filename = phe_filename.replace('table', 'index')
    index_df = pd.read_csv(index_filename) #change later to read freom bucket
    cid_col = phe_dd[2]
    
    #combines all functions to genrate and run all the notebooks for the workspaces (for every phenotype)
    if concept_ids is None:
        #phe_filename = get_phe_dd(datatype)[1]    
        concept_ids =  pd.read_csv(phe_filename, nrows=0).drop('person_id', axis =1).columns
        concept_ids  = [c for c in concept_ids if c !='sex']
        
    start = datetime.now()
    n = 1
    print(n)
    for concept_id in concept_ids:
        phenotype_name = index_df[index_df[cid_col] == concept_id].reset_index(drop = True).concept_name.values[0]
        notebook_name = get_notebook_name(datatype, concept_id)
        generate_notebook(concept_id, datatype, notebook_name, phenotype_name)
        run_notebook(notebook_name, KERNEL = kernel)
        n = n+1
        print(n)

    print('DONE.')
    end = datetime.now()
    totaltime = end-start
    print(totaltime)
    
    
# drug_bucket = 'gs://fc-secure-15736bb7-7fa3-4bf8-8870-8f043b195ca5'
# pfhh_bucket = 'gs://fc-secure-b00c0e03-c1e7-47f3-b069-6760e9ac8081'
# phex_bucket = 'gs://fc-secure-2607142e-b58a-40b2-9174-5cfed82a428b'
# phe_bucket = 'gs://fc-secure-78e9e728-962a-4d97-812c-527cbfd95f3f'
# pm_bucket = 'gs://fc-secure-264cb142-0acd-4173-8fda-a256d09f3149'
# lab_bucket = 'gs://fc-secure-93cc8e81-154f-4d54-ab68-4af358dfa5da'

# !gsutil -m cp utilities.py {drug_bucket}/notebooks/code
# !gsutil -m cp utilities.py {pfhh_bucket}/notebooks/code
# !gsutil -m cp utilities.py {phe_bucket}/notebooks/code
# !gsutil -m cp utilities.py {phex_bucket}/notebooks/code
# !gsutil -m cp utilities.py {pm_bucket}/notebooks/code
# !gsutil -m cp utilities.py {lab_bucket}/notebooks/code

# !gsutil -m cp cat_data_summary_functions.R {drug_bucket}/notebooks/code
# !gsutil -m cp cat_data_summary_functions.R {pfhh_bucket}/notebooks/code
# !gsutil -m cp cat_data_summary_functions.R {phe_bucket}/notebooks/code
# !gsutil -m cp cat_data_summary_functions.R {phex_bucket}/notebooks/code
