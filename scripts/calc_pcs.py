import hail as hl
import sys
import os

# Retrieve input PLINK file path from command-line arguments
input_bed = sys.argv[1]  # PLINK .bed file path
output_scores = sys.argv[2]  # Output path for PCA scores (e.g., CSV or TSV file)
flagged_samples_file = sys.argv[3]  # Path to ancestry/relatedness flagged samples file

# Define output paths for intermediate files
loadings_path = output_scores.replace(".tsv", "_loadings.ht")
af_path = output_scores.replace(".tsv", "_allele_frequencies.ht")
scores_path = output_scores.replace(".tsv", "_scores.ht")
eigenvalues_path = output_scores.replace(".tsv", "_eigenvalues.tsv")

# Initialize Hail
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
flagged_samples_set = hl.set(flagged_samples_list)

# Split data: unrelated for PCA, related for projection
plink_data_unrelated = plink_data.filter_cols(flagged_samples_set.contains(plink_data.s), keep=False)
plink_data_related = plink_data.filter_cols(flagged_samples_set.contains(plink_data.s), keep=True)

print(f"Unrelated samples used for PCA: {plink_data_unrelated.count_cols()}")
print(f"Related samples to be projected: {plink_data_related.count_cols()}")

# Check if PCA results already exist
if os.path.exists(loadings_path) and os.path.exists(af_path) and os.path.exists(eigenvalues_path) and os.path.exists(scores_path):
    print("Loading previously computed PCA results...")
    loadings_ht = hl.read_table(loadings_path)
    af_ht = hl.read_table(af_path)
    scores_ht = hl.read_table(scores_path)

    # Ensure allele frequencies are available for projection
    loadings_ht = loadings_ht.annotate(af=af_ht[loadings_ht.key].af)

    # Read eigenvalues from file
    with open(eigenvalues_path, 'r') as f:
        next(f)  # Skip header
        eigenvalues = [float(line.strip().split("\t")[1]) for line in f]

else:
    print("Computing PCA on unrelated samples...")
    eigenvalues, scores_ht, loadings_ht = hl.hwe_normalized_pca(plink_data_unrelated.GT, k=20, compute_loadings=True)

    # Compute allele frequencies
    plink_data_unrelated = plink_data_unrelated.annotate_rows(af=hl.agg.mean(plink_data_unrelated.GT.n_alt_alleles()) / 2)

    # Annotate loadings with allele frequencies
    loadings_ht = loadings_ht.annotate(af=plink_data_unrelated.rows()[loadings_ht.key].af)

    # Save PCA results
    loadings_ht.write(loadings_path, overwrite=True)
    af_ht = plink_data_unrelated.rows().select("af")
    af_ht.write(af_path, overwrite=True)
    scores_ht.write(scores_path, overwrite=True)

    # Save eigenvalues
    with open(eigenvalues_path, 'w') as f:
        f.write("PC\tEigenvalue\n")
        for i, eigenvalue in enumerate(eigenvalues):
            f.write(f"{i + 1}\t{eigenvalue}\n")

print("PCA computation done or reused, starting projection of related samples...")

# Perform PCA projection on related samples
projected_scores_ht = hl.experimental.pc_project(plink_data_related.GT, loadings_ht.loadings, loadings_ht.af)

# Append original and projected scores
combined_scores = scores_ht.union(projected_scores_ht)

# Save the combined PCA scores
combined_scores.export(output_scores)

print(f"PCA scores saved to {output_scores}")
print(f"PCA eigenvalues saved to {eigenvalues_path}")
print(f"PCA loadings saved to {loadings_path}")
