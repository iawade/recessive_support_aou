#!/bin/bash

INPUT_STEM="$1"

# Extract file names from the input stem
INPUT_BED="$INPUT_STEM.bed"
INPUT_BIM="$INPUT_STEM.bim"
INPUT_FAM="$INPUT_STEM.fam"

# Function to check if the file exists locally before attempting download
download_if_not_exists() {
    local file_url="$1"
    local file_name=$(basename "$file_url")

    if [ ! -f "ancestry/$file_name" ]; then
        echo "Downloading $file_name..."
        gsutil -u "$GOOGLE_PROJECT" cp "$file_url" ancestry
    else
        echo "$file_name already exists. Skipping download."
    fi
}

# Check if each file exists, and download if necessary
download_if_not_exists "$INPUT_BED"
download_if_not_exists "$INPUT_BIM"
download_if_not_exists "$INPUT_FAM"

if [ -f ancestry/$(basename $INPUT_BED) ] && [ -f ancestry/$(basename $INPUT_BIM) ] && [ -f ancestry/$(basename $INPUT_FAM) ]; then
	touch downloaded_data.txt
else
    echo "Error: Download failed."
fi

