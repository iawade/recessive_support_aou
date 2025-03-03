library(tidyverse)

# Read all the input files
output_bmi_whradjbmi <- read.csv("output_bmi_whradjbmi.csv")
binary_brava_44_phecode <- read.csv("binary_brava_44_phecode_and_X.csv")
physical_measurement <- read.csv("physical_measurement_table_v8.csv")
demographics_covariates <- read.csv("demographics_filtered_covariates.csv")

# Read all measurement summary files
measurement_files <- list.files(pattern = "measurement_.*_summary.csv")

# Function to process measurement summary files, extract 'person_id' and 'median' columns, and rename 'median' column
measurement_data <- lapply(measurement_files, function(file) {
  df <- read.csv(file)
  
  # Select person_id and median columns
  df_summary <- df %>%
    select(person_id, median)
  
  # Rename the 'median' column based on the file name
  new_colname <- gsub("measurement_(\\d+)_summary.csv", "\\1", file)  # Extract person_id for new column name
  colnames(df_summary)[2] <- paste0("median_", new_colname)  # Rename the median column
  
  return(df_summary)
})

# Combine all the measurement data into one dataframe
measurement_combined <- Reduce(function(x, y) full_join(x, y, by = "person_id"), measurement_data)

# Merge all the data by person_id (using full joins and ensuring demographics_covariates is first)
final_data <- demographics_covariates %>%
  full_join(output_bmi_whradjbmi, by = "person_id") %>%
  full_join(binary_brava_44_phecode, by = "person_id") %>%
  full_join(physical_measurement, by = "person_id") %>%
  full_join(measurement_combined, by = "person_id")

# Optionally, you can write the final combined data to a CSV file
write.csv(final_data, "combined_data.csv", row.names = FALSE)
