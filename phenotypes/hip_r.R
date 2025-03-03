library(tidyverse)
library(bigrquery)  # BigQuery R client

## BigQuery setup.
BILLING_PROJECT_ID <- Sys.getenv('GOOGLE_PROJECT')
CDR <- "fc-aou-cdr-prod-ct.C2024Q3R5"  # Replace with your actual dataset if different

# Bucket
WORKSPACE_BUCKET <- Sys.getenv('WORKSPACE_BUCKET')

# Query function for BigQuery
bq <- function(query) {
  bq_table_download(bq_project_query(
    BILLING_PROJECT_ID, page_size = 25000,
    query=query, default_dataset = CDR
  ))
}

# SQL Query to retrieve relevant procedure occurrence data
procedure_query = sprintf("
    SELECT
        procedure_occurrence_id,
        person_id,
        procedure_concept_id,
        procedure_date,
        procedure_datetime,
        procedure_type_concept_id,
        modifier_concept_id,
        quantity,
        provider_id,
        visit_occurrence_id,
        procedure_source_value,
        procedure_source_concept_id,
    FROM
        `%s.procedure_occurrence`
    WHERE
        procedure_concept_id IN (4134857, 398010007)
", CDR)

# Download procedure occurrence data
procedure_data = bq(procedure_query)

# Optional: If you want additional fields from the person table, you can join the `person` table
person_query = sprintf("
    SELECT
        person_id,
        CASE
            WHEN sex_at_birth_concept_id = 45878463 THEN 'F'
            WHEN sex_at_birth_concept_id = 45880669 THEN 'M'
            ELSE '0'
        END AS sex
    FROM
        `%s.person`
", CDR)

# Download person data
person_data = bq(person_query)

# Combine the procedure data with the person sex information (if needed)
combined_data = full_join(procedure_data, person_data, by = "person_id")


# Assuming `data` contains `person_id` and `procedure_concept_id`
# Filter for procedure concept 4134857 and create TRUE/FALSE for each person_id
result <- combined_data %>%
  mutate(is_concept_4134857 = ifelse(procedure_concept_id %in%  c(4134857, 398010007), TRUE, FALSE)) %>%
  select(person_id, is_concept_4134857) %>%
  distinct()  # Get unique person_id entries

# Save result to a CSV file
write.csv(result, "procedure_concept_4134857_status.csv", row.names = FALSE)

# View the combined data
head(combined_data)

# Save the combined data as a CSV file if needed
# write_csv(combined_data, "procedure_data_with_person_info.csv")
