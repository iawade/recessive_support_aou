#!/bin/bash

# Activate conda environment
source /home/jupyter/anaconda3/etc/profile.d/conda.sh
conda activate biallelic_effects

# Set memory limits to unlimited
ulimit -v unlimited
ulimit -m unlimited

free -h

cat /proc/meminfo | grep -i "free"
cat /proc/meminfo | grep -i "slab"

# Input and output variables
INPUT_BFILE="${1%.bed}"       # Input file prefix (strip .bed if provided)
OUTPUT="${2%_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx}"

# Run the createSparseGRM.R script
createSparseGRM.R \
    --plinkFile="$INPUT_BFILE" \
    --nThreads=32 \
    --outputPrefix="$OUTPUT" \
    --numRandomMarkerforSparseKin=5000 \
    --relatednessCutoff=0.05 \

