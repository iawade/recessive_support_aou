
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Background ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# The phecode phenotypes used in All by All are participant level phecodeX condition data (True/False).
# The table was created as follows. 
# Based on the phecode map, participants with a condition were assigned values of True (case) 
# and participant who do not have a condition were assigned a value of False (control). 
# A minimum case count of 2 was used for both phecode and phecodeX conditions. 

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ Code to generate the phecode table ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# export LD_LIBRARY_PATH=/home/jupyter/anaconda3/lib:$LD_LIBRARY_PATH # may need this

library(tidyverse)
if(!require(PheWAS)) devtools::install_github("PheWAS/PheWAS")
library(PheWAS)
library(bigrquery)  # BigQuery R client.

## BigQuery setup.
BILLING_PROJECT_ID <- Sys.getenv('GOOGLE_PROJECT')
# Get the BigQuery curated dataset for the current workspace context.
# CDR <- Sys.getenv('WORKSPACE_CDR')
# .C2024Q3R5
CDR <- "fc-aou-cdr-prod-ct.C2024Q3R5"

# Bucket
WORKSPACE_BUCKET <- Sys.getenv('WORKSPACE_BUCKET')

#Query
bq <- function(query) {bq_table_download(bq_project_query(
    BILLING_PROJECT_ID, page_size = 25000,
    query=query, default_dataset = CDR ))
}

#temporary python setup
library(reticulate)
use_python("/home/jupyter/conda_envs/python_general/bin/python", required = TRUE) # not interoperable anymore - but works for me!
reticulate::py_config()
bigquery=import("google.cloud.bigquery")
client = bigquery$Client()

phecodeX_labels = read.csv('phecodeX_R_labels.csv')
phecodeX_rollup_map = read.csv('phecodeX_R_rollup_map.csv')
phecodeX_map = read.csv('phecodeX_R_map.csv')

# Create the phecode map
# expanded_phecode = inner_join(PheWAS::phecode_map, rename(PheWAS::phecode_rollup_map, phecode=code)) %>% transmute(vocabulary_id, concept_code=code, phecode=phecode_unrolled)
expanded_phecode = inner_join(phecodeX_map, rename(phecodeX_rollup_map, phecode=code)) %>% transmute(vocabulary_id, concept_code=code, phecode=phecode_unrolled) # above is phecode, this line is phecode X

write_csv(expanded_phecode, file="expanded_phecode.csv")
system2("gsutil",args = c("cp","expanded_phecode.csv",WORKSPACE_BUCKET))

phecode = bigquery$ExternalConfig('CSV')
phecode$source_uris = c(sprintf("%s/expanded_phecode.csv",WORKSPACE_BUCKET))
phecode$schema = c(
    bigquery$SchemaField('vocabulary_id', 'STRING'),
    bigquery$SchemaField('concept_code', 'STRING'),
    bigquery$SchemaField('phecode', 'STRING')
)
py_set_attr(phecode$options,"skip_leading_rows", "1")  # optionally skip header row

icds=sprintf("with all_codes as (select * from (
    select distinct person_id, vocabulary_id, concept_code, condition_start_date as date
    from condition_occurrence join concept on (condition_source_concept_id=concept_id)
    where vocabulary_id in ('ICD9CM','ICD10CM')
    union distinct
    select distinct person_id, vocabulary_id, concept_code, observation_date as date
    from observation join concept on (observation_source_concept_id=concept_id)
    where vocabulary_id in ('ICD9CM','ICD10CM')
    union distinct
    select distinct person_id, vocabulary_id, concept_code, procedure_date as date
    from procedure_occurrence join concept on (procedure_source_concept_id=concept_id)
    where vocabulary_id in ('ICD9CM','ICD10CM')
    union distinct
    select distinct person_id, vocabulary_id, concept_code, measurement_date as date
    from measurement join concept on (measurement_source_concept_id=concept_id)
    where vocabulary_id in ('ICD9CM','ICD10CM')
    ))
    select distinct person_id, phecode, count(distinct date) as code_count from
    all_codes join expanded_phecode using (vocabulary_id, concept_code)
    group by person_id, phecode
    ")

job_config = bigquery$QueryJobConfig()
job_config$table_definitions = c("expanded_phecode" = phecode)
job_config$default_dataset = CDR
query_job = client$query(icds, job_config=job_config)  # API request

data = query_job$to_dataframe()

# Get individuals with EHR data (for control list)
job_config = bigquery$QueryJobConfig()
job_config$default_dataset = CDR
query_job = client$query("SELECT DISTINCT person_id FROM cb_search_person WHERE has_ehr_data = 1"
                        , job_config=job_config)

ehr_inds = query_job$to_dataframe()

ehr_inds %>% group_by(person_id) %>% summarize(n_site=n()) %>% group_by(n_site) %>% summarize(n())

ehr_person_ids= (ehr_inds %>% select(person_id) %>% distinct())[[1]]


job_config = bigquery$QueryJobConfig()
job_config$default_dataset = CDR
query_job = client$query("select person_id, 
case when sex_at_birth_concept_id=45878463 then 'F'
when sex_at_birth_concept_id=45880669 then 'M'
else '0'
end as sex
from person", job_config=job_config)  # API request

sex_info = query_job$to_dataframe()

phe_table = createPhenotypes(data %>% transmute(person_id, vocabulary_id="phecode",phecode, code_count), 
                             translate=FALSE, min.code.count=2, add.phecode.exclusions=FALSE,
                             id.sex=sex_info,full.population.ids=ehr_person_ids)
                             
write_csv(phe_table,file="mcc2_phecode_table_v8_phecodesX.csv")

