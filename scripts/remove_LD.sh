#!/bin/bash

source /home/jupyter/anaconda3/etc/profile.d/conda.sh
conda activate biallelic_effects

# Input and output variables
INPUT_BFILE="${1%.bed}"       # Input file prefix (strip .bed if provided)
OUTPUT_SNP_WISE="${2%.bed}"  # Output prefix for SNP-wise pruning
OUTPUT_LENGTH_WISE="${3%.bed}"  # Output prefix for length-wise pruning

# SNP-wise LD pruning (window size 50 SNPs, step size 5 SNPs, r^2 threshold 0.5)
plink --bfile "${INPUT_BFILE}" \
      --indep-pairwise 50 5 0.05 \
      --out "${OUTPUT_SNP_WISE}" \
      --threads 4

# Length-based LD pruning (window size 1000 kb, step size 5 SNPs, r^2 threshold 0.1)
plink --bfile "${INPUT_BFILE}" \
      --indep-pairwise 1000'kb' 1 0.1 \
      --out "${OUTPUT_LENGTH_WISE}" \
      --threads 4

# Filter the dataset to keep SNPs from SNP-wise pruning
plink --bfile "${INPUT_BFILE}" \
      --extract "${OUTPUT_SNP_WISE}.prune.in" \
      --make-bed \
      --out "${OUTPUT_SNP_WISE}" \
      --threads 4

# Filter the dataset to keep SNPs from length-based pruning
plink --bfile "${INPUT_BFILE}" \
      --extract "${OUTPUT_LENGTH_WISE}.prune.in" \
      --make-bed \
      --out "${OUTPUT_LENGTH_WISE}" \
      --threads 4

