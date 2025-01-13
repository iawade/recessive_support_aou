#!/bin/bash

#eval "$(conda shell.bash hook)"
#conda activate biallelic_effects

INPUT_STEM="$1"
INCLUSION_SAMPLES="$2"
OUTPUT_PREFIX="$3"

# Extract file names from the input stem
INPUT_BED="$INPUT_STEM.bed"
INPUT_BIM="$INPUT_STEM.bim"
INPUT_FAM="$INPUT_STEM.fam"

# Function to check if the file exists locally before attempting download
#download_if_not_exists() {
#    local file_url="$1"
#    local file_name=$(basename "$file_url")
#
#    if [ ! -f "$file_name" ]; then
#        echo "Downloading $file_name..."
#        gsutil -u "$GOOGLE_PROJECT" cp "$file_url" .
#    else
#        echo "$file_name already exists. Skipping download."
#    fi
#}

# Check if each file exists, and download if necessary
#download_if_not_exists "$INPUT_BED"
#download_if_not_exists "$INPUT_BIM"
#download_if_not_exists "$INPUT_FAM"

# Run PLINK to filter samples based on the keep list
plink --bfile "$(basename "$INPUT_STEM")" \
      --keep "$INCLUSION_SAMPLES" \
      --make-bed \
      --out "$OUTPUT_PREFIX"

# Check if the filtering was successful
#if [ $? -eq 0 ]; then
#    echo "Filtering complete. Output files are:"
#    echo "$OUTPUT_PREFIX.bed"
#    echo "$OUTPUT_PREFIX.bim"
#    echo "$OUTPUT_PREFIX.fam"
#else
#    echo "Error: PLINK filtering failed."
#fi

# Delete the downloaded files from the current folder
#echo "Deleting temporary files..."
#rm $(basename "$INPUT_STEM")*

