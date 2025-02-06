#!/bin/bash

# Set the maximum number of jobs
MAX_JOBS=1

# Path to the Snakemake workflow file
WORKFLOW_FILE="workflow/generate_sparse_grm.smk"

# Remove the pipeline completion marker if it exists
if [ -f "pipeline_complete.txt" ]; then
    echo "Removing existing pipeline_complete.txt..."
    rm -f "pipeline_complete.txt"
fi

# Log memory usage
LOG_FILE="memory_monitor.log"
echo "Starting memory monitoring..." > "$LOG_FILE"

# Function to log memory usage
log_memory() {
    echo "$(date): $(free -h | grep 'Mem')" >> "$LOG_FILE"
}

# Start a background process to monitor memory every 5 seconds
monitor_memory() {
    while true; do
        log_memory
        sleep 5
    done
}

# Start monitoring in the background
monitor_memory &
MONITOR_PID=$!

mkdir -p ld_prune logs make_sparse_grm sample_selection PCA nullglmm

source /home/jupyter/anaconda3/etc/profile.d/conda.sh
conda activate biallelic_effects

# Run Snakemake with the specified options
echo "Starting a run of Snakemake workflow..."
snakemake --snakefile "$WORKFLOW_FILE" --cores "$MAX_JOBS" --jobs 1 --max-status-checks-per-second 0.01 \
    --rerun-triggers mtime input \
    --touch

echo "Run complete."

# Stop the background memory monitor after the script finishes
kill $MONITOR_PID
echo "Memory monitoring stopped." >> "$LOG_FILE"

