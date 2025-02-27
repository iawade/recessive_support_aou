# Load required libraries
library(bigrquery)
library(stringr)

WORKSPACE_CDR <- "fc-aou-cdr-prod-ct.C2024Q3R5"
CURRENT_DATASET <- Sys.getenv('WORKSPACE_CDR')

# Function to download data from BigQuery
download_data <- function(query) {
  tb <- bq_project_query(Sys.getenv('GOOGLE_PROJECT'), str_glue(query))
  bq_table_download(tb, bigint = c("integer", "integer64", "numeric", "character"), page_size = 5000)
}

# Query to get participant IDs and their "Sex at Birth" responses
#query <- str_glue("
#  SELECT 
#    obs.person_id, 
#    concept.concept_name AS sex_at_birth
#  FROM `{CURRENT_DATASET}.observation` AS obs
#  JOIN `{CURRENT_DATASET}.concept` AS concept
#    ON obs.value_source_concept_id = concept.concept_id
#  WHERE obs.observation_concept_id = 1585845
#")

query <- str_glue("
  SELECT
    obs.person_id,
    concept.concept_name AS sex_at_birth
  FROM `{CURRENT_DATASET}.observation` AS obs
  JOIN `{CURRENT_DATASET}.concept` AS concept
    ON obs.value_source_concept_id = concept.concept_id
  WHERE obs.observation_concept_id = 1585845
")

# Download data
participant_sex_table <- download_data(query)

# Save as CSV
write.csv(participant_sex_table, "participant_sex_at_birth.csv", row.names = FALSE)

