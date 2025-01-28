#!/bin/bash

eval "$(conda shell.bash hook)"
conda activate biallelic_effects

INPUT_STEM="$1"
INCLUSION_SAMPLES="$2"
OUTPUT="${3%.bed}" 

# Generate a unique identifier (e.g., timestamp + random number for safety)
UNIQUE_ID="$(basename "${INCLUSION_SAMPLES}" .tsv)"

# Extract file names from the input args
INPUT_BED="$INPUT_STEM.bed"
INPUT_BIM="$INPUT_STEM.bim"
INPUT_FAM="$INPUT_STEM.fam"
FILTERED_STEM="merged_${UNIQUE_ID}_plink"

ANCESTRY_DIR="sample_selection"              # Directory containing site parts

# Temporary file names
TMP_PART1="tmp_chr1-7_${UNIQUE_ID}"
TMP_PART2="tmp_chr8-24_${UNIQUE_ID}"
TMP_MERGED="tmp_merged_${UNIQUE_ID}"


# Step 1: Subset chromosomes 1-7 and filter samples
if [ ! -f "$TMP_PART1.bed" ]; then
    echo "Creating $TMP_PART1..."
    plink --bfile "$INPUT_STEM" --chr 1-7 --keep "$INCLUSION_SAMPLES" --make-bed --out "$TMP_PART1" --threads 4 --memory 20000  --hwe 1e-50
else
    echo "$TMP_PART1 already exists, skipping..."
fi

# Step 2: Subset chromosomes 8-22, X, and Y and filter samples
if [ ! -f "$TMP_PART2.bed" ]; then
    echo "Creating $TMP_PART2..."
    plink --bfile "$INPUT_STEM" --chr 8-24 --keep "$INCLUSION_SAMPLES" --make-bed --out "$TMP_PART2" --threads 4 --memory 20000 --hwe 1e-50
else
    echo "$TMP_PART2 already exists, skipping..."
fi

# Step 3: Export PLINK files to BCF format using PLINK 2.0
if [ ! -f "$TMP_PART1.bcf" ]; then
    echo "Converting $TMP_PART1 to BCF format..."
    plink2 --bfile "$TMP_PART1" --export bcf --out "$TMP_PART1"
else
    echo "$TMP_PART1.bcf already exists, skipping conversion..."
fi

if [ ! -f "$TMP_PART2.bcf" ]; then
    echo "Converting $TMP_PART2 to BCF format..."
    plink2 --bfile "$TMP_PART2" --export bcf --out "$TMP_PART2"
else
    echo "$TMP_PART2.bcf already exists, skipping conversion..."
fi

# Step 4: Index the BCF files using bcftools
if [ ! -f "$TMP_PART1.bcf.csi" ]; then
    echo "Indexing $TMP_PART1.bcf..."
    bcftools index --threads 4 "$TMP_PART1.bcf"
else
    echo "$TMP_PART1.bcf is already indexed, skipping..."
fi

if [ ! -f "$TMP_PART2.bcf.csi" ]; then
    echo "Indexing $TMP_PART2.bcf..."
    bcftools index --threads 4 "$TMP_PART2.bcf"
else
    echo "$TMP_PART2.bcf is already indexed, skipping..."
fi

# Step 5: Concatenate the BCF files using bcftools
if [ ! -f "$TMP_MERGED.bcf" ]; then
    echo "Concatenating BCF files using bcftools..."
    bcftools concat "$TMP_PART1.bcf" "$TMP_PART2.bcf" -O b -o "$TMP_MERGED.bcf" \
	    -W --threads 4
else
    echo "$TMP_MERGED.bcf already exists, skipping concatenation..."
fi

# Step 6: Convert the concatenated BCF back to PLINK format
if [ ! -f "$FILTERED_STEM" ]; then
    echo "Converting concatenated BCF back to PLINK format..."
    plink --bcf "$TMP_MERGED.bcf" --make-bed --out "$FILTERED_STEM"
else
    echo "$FILTERED_STEM already exists, skipping BCF to PLINK conversion..."
fi

# Safer delete
if [ -f "${FILTERED_STEM}" ]; then
    echo "Cleaning up temporary files..."
    rm -f "${TMP_PART1}.bed" "${TMP_PART1}.bim" "${TMP_PART1}.fam" "${TMP_PART1}.log" "${TMP_PART1}.nosex" "${TMP_PART1}.bcf" "${TMP_PART1}.bcf.csi"
    rm -f "${TMP_PART2}.bed" "${TMP_PART2}.bim" "${TMP_PART2}.fam" "${TMP_PART2}.log" "${TMP_PART2}.nosex" "${TMP_PART2}.bcf" "${TMP_PART2}.bcf.csi" 
    rm -f "${TMP_MERGED}.bed" "${TMP_MERGED}.bim" "${TMP_MERGED}.fam" "${TMP_MERGED}.log" "${TMP_MERGED}.nosex" "${TMP_MERGED}.bcf" "${TMP_MERGED}.bcf.csi"
    echo "Temporary files removed."
else
    echo "Output file ${FILTERED_STEM} not found. Skipping cleanup."
fi

LD_OUTPUT=${OUTPUT}

# SNP-wise LD pruning (window size 50 SNPs, step size 5 SNPs, r^2 threshold 0.5)
plink --bfile "${FILTERED_STEM}" \
      --indep-pairwise 50 5 0.05 \
      --out "${OUTPUT/.bed/ld_int}" \
      --threads 4

# Filter the dataset to keep SNPs from SNP-wise pruning
plink --bfile "${FILTERED_STEM}" \
      --extract "${OUTPUT/.bed/ld_int}.prune.in" \
      --make-bed \
      --out "${OUTPUT/.bed/ld_int}" \
      --threads 4

# Define variant counts
variants_lessthan_20_MAC=2000
variants_greaterthan_20_MAC=2000

# Get the count of variants in the merged PLINK file
plink \
    --bfile "${OUTPUT/.bed/ld_int}" \
    --freq counts \
    --out "${OUTPUT}"

# Select random variants in the specified MAC ranges
cat <(
    tail -n +2 "${OUTPUT}.frq.counts" \
    | awk '(($6-$5) < 20 && ($6-$5) >= 10) || ($5 < 20 && $5 >= 10) {print $2}' \
    | shuf -n "${variants_lessthan_20_MAC}"
) \
<(
    tail -n +2 "${OUTPUT}.frq.counts" \
    | awk '$5 >= 20 && ($6-$5) >= 20 {print $2}' \
    | shuf -n "${variants_greaterthan_20_MAC}"
) > "${OUTPUT}.markerid.list"

# Get the actual number of variants in the specified MAC ranges
actual_variants_lessthan_20_MAC=$(awk '(($6-$5) < 20 && ($6-$5) >= 10) || ($5 < 20 && $5 >= 10)' "${OUTPUT}.frq.counts" | wc -l)
actual_variants_greaterthan_20_MAC=$(awk '$5 >= 20 && ($6-$5) >= 20' "${OUTPUT}.frq.counts" | wc -l)

# Check if the required variants exist
if [[ $variants_lessthan_20_MAC -gt $actual_variants_lessthan_20_MAC ]]; then
    echo "Error: ${actual_variants_lessthan_20_MAC} variants (MAC<20) found - less than the required ${variants_lessthan_20_MAC} variants."
    exit 1
elif [[ $variants_greaterthan_20_MAC -gt $actual_variants_greaterthan_20_MAC ]]; then
    echo "Error: ${actual_variants_greaterthan_20_MAC} variants (MAC>20) found - less than the required ${variants_greaterthan_20_MAC} variants."
    exit 1
fi

# Extract markers from the large PLINK file using the list generated above
plink \
    --bfile "${OUTPUT/.bed/ld_int}" \
    --extract "${OUTPUT}.markerid.list" \
    --make-bed \
    --out "${OUTPUT}"

