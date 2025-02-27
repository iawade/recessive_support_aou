library(dplyr)

# Load participant sex data
participant_sex <- read.csv("participant_sex_at_birth.csv", stringsAsFactors = FALSE)

# Modify the 'sex_at_birth' values
participant_sex$sex_at_birth <- recode(participant_sex$sex_at_birth,
                                       "Sex At Birth: Male" = "M",
                                       "Sex At Birth: Female" = "F",
                                       "Sex At Birth: Intersex" = "",
                                       "Sex At Birth: Sex At Birth None Of These" = "",
                                       "PMI: Skip" = "PMI: Skip")  # or exclude it based on your needs

# Load the CRC data (make sure the column names are correct)
crc_data <- read.csv("aou_CRC_v8.tsv", stringsAsFactors = FALSE)

# Check and standardize the column names (if necessary)
names(participant_sex)[names(participant_sex) == "person_id"] <- "person_id"  # Make sure column names match
names(crc_data)[names(crc_data) == "person_id"] <- "person_id"  # Same here

# Ensure person_id columns are the same type (numeric or character)
participant_sex$person_id <- as.character(participant_sex$person_id)
crc_data$person_id <- as.character(crc_data$person_id)

# Merge the data based on 'person_id'
merged_data <- merge(crc_data, participant_sex, by = "person_id", all.x = TRUE)

# Save the merged data to a new file
write.csv(merged_data, "merged_crc_sex_at_birth.csv", row.names = FALSE, quote = FALSE)
# Optionally, save as TSV
# write_tsv(merged_data, "merged_crc_sex_at_birth.tsv")
