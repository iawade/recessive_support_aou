#!/usr/bin/env python3

import hail as hl
import sys
import os

# Retrieve input PLINK file path from command-line arguments
input_bed = os.environ['INPUT_BED']  # PLINK .bed file path
output_scores = os.environ['OUTPUT']  # Output path for PCA scores (e.g., CSV or TSV file)
flagged_samples_file = os.environ['EXCLUSIONS_LIST']

# Initialize Hail with appropriate resources
hl.init()

# Import PLINK files
plink_data = hl.import_plink(
    bed=input_bed,
    bim=input_bed.replace(".bed", ".bim"),
    fam=input_bed.replace(".bed", ".fam"),
    reference_genome='GRCh38'
)

# Import the flagged samples file as a Hail Table
related_samples_to_remove = hl.import_table(flagged_samples_file, types={'sample_id': hl.tstr})

# Collect the sample IDs into a Python set
flagged_samples_list = related_samples_to_remove.sample_id.collect()
flagged_samples_set = hl.set(flagged_samples_list)  # Convert list to Hail set

# Filter the MatrixTable based on sample IDs
plink_data_filtered = plink_data.filter_cols(flagged_samples_set.contains(plink_data.s), keep=False)
print(f"Samples used in PCA: {plink_data_filtered.count_cols()}")

print("MatrixTable loaded, starting PCA...")

# Compute PCA (first 20 PCs)
eigenvalues, scores, loadings = hl.hwe_normalized_pca(plink_data_filtered.GT, k=20, compute_loadings=True)

# Calculate allele frequencies for the reference dataset
plink_data_filtered = plink_data_filtered.annotate_rows(af=hl.agg.mean(plink_data_filtered.GT.n_alt_alleles()) / 2)

# Annotate loadings with allele frequencies
loadings_ht = loadings_ht.annotate(af=plink_data_filtered.rows()[loadings_ht.key].af)

print("PCA computed, starting projection of new genotypes...")

# Project new genotypes onto loadings (using the loadings and allele frequencies from the PCA)
mt_to_project = plink_data.filter_cols(flagged_samples_set.contains(plink_data.s), keep=True)
projected_ht = pc_project(mt_to_project.GT, loadings_ht.loadings, loadings_ht.af)

# Combine PCA scores (original + projected)
original_scores = loadings_ht.select('scores')  # Assuming `loadings_ht` has the PCA scores
projected_scores = projected_ht.select('scores')  # Projected PCA scores

# Append original and projected scores
combined_scores = original_scores.union(projected_scores)

# Save the combined PCA scores
combined_scores.export(output_scores)

# Save eigenvalues
eigenvalues_path = output_scores.replace(".tsv", "_eigenvalues.tsv")
with open(eigenvalues_path, 'w') as f:
    f.write("PC\tEigenvalue\n")
    for i, eigenvalue in enumerate(loadings_ht.eigenvalues):
        f.write(f"{i + 1}\t{eigenvalue}\n")

# Save loadings
loadings_path = output_scores.replace(".tsv", "_loadings.tsv")
loadings_ht.export(loadings_path)

print(f"PCA scores saved to {output_scores}")
print(f"PCA eigenvalues saved to {eigenvalues_path}")
print(f"PCA loadings saved to {loadings_path}")

