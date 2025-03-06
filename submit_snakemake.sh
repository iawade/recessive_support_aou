#!/bin/bash

# Set the maximum number of jobs
MAX_JOBS=$(nproc)

# Path to the Snakemake workflow file
WORKFLOW_FILE="workflow/generate_sparse_grm.smk"

# Remove the pipeline completion marker if it exists
if [ -f "pipeline_complete.txt" ]; then
    echo "Removing existing pipeline_complete.txt..."
    rm -f "pipeline_complete.txt"
fi

mkdir -p ld_prune logs make_sparse_grm sample_selection PCA nullglmm spatests

source /home/jupyter/anaconda3/etc/profile.d/conda.sh
conda activate biallelic_effects

# Run Snakemake with the specified options
echo "Starting a run of Snakemake workflow..."
snakemake --snakefile "$WORKFLOW_FILE" --cores "$MAX_JOBS" --jobs "$MAX_JOBS" --max-status-checks-per-second 0.01 \
    --rerun-triggers mtime input \
    --nolock \
    --keep-going \
    -n \
    --rerun-incomplete \
    2>&1 | tee snakemake_run.log 

echo "Run complete."

