import hail as hl
import sys

# Retrieve input PLINK file path from command-line arguments
input_bed = sys.argv[1]  # PLINK .bed file path
output_scores = sys.argv[2]  # Output path for PCA scores (e.g., CSV or TSV file)

# Initialize Hail with appropriate resources
hl.init(
    master='local[32]',  # Use 32 threads
    spark_conf={'spark.driver.memory': '100g'}  # Allocate 100 GB RAM
)

# Import PLINK files
plink_data = hl.import_plink(
    bed=input_bed,
    bim=input_bed.replace(".bed", ".bim"),
    fam=input_bed.replace(".bed", ".fam"),
    reference_genome='GRCh38'
)

print("MatrixTable loaded, starting PCA...")

# Compute PCA (first 20 PCs)
eigenvalues, scores, loadings = hl.hwe_normalized_pca(plink_data.GT, k=20, compute_loadings=True)

print("PCA computed, starting export...")

# Save PCA scores (samples x PCs) as a TSV or CSV file
scores.export(output_scores)

# Save eigenvalues
eigenvalues_path = output_scores.replace(".tsv", "_eigenvalues.tsv")
with open(eigenvalues_path, 'w') as f:
    f.write("PC\tEigenvalue\n")
    for i, eigenvalue in enumerate(eigenvalues):
        f.write(f"{i + 1}\t{eigenvalue}\n")

# Save loadings
# Convert loadings MatrixTable to Table for export
loadings_path = output_scores.replace(".tsv", "_loadings.tsv")
loadings.export(loadings_path)

print(f"PCA scores saved to {output_scores}")
print(f"PCA eigenvalues saved to {eigenvalues_path}")
print(f"PCA loadings saved to {loadings_path}")

