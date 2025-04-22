import argparse

template = """
# Define filenames
VCF="vcf/aou.exome_split.v8.{ancestry}.qced.autosomes.AAF05.popmax05.{variant_class}.recessive.vcf.bgz"
MODELFILE="{model_file}"
VARIANCE_RATIO="{variance_ratio}"
SPARSE_GRM="make_sparse_grm/allofus_array_{ancestry}_snp_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx"
SAMPLE_IDS="nullglmm/sample_ids_{ancestry}_snp_{phenotype}.txt"
OUTPUT="{output_file}"

# Upload input files to the cloud
gsutil -u $GOOGLE_PROJECT cp -n $VCF $WORKSPACE_BUCKET/data/saige/run/spatests/vcf/
gsutil -u $GOOGLE_PROJECT cp -n $VCF.csi $WORKSPACE_BUCKET/data/saige/run/spatests/vcf/
gsutil -u $GOOGLE_PROJECT cp -n $MODELFILE $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp -n $VARIANCE_RATIO $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp -n $SPARSE_GRM $WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/
gsutil -u $GOOGLE_PROJECT cp -n $SPARSE_GRM.sampleIDs.txt $WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/
gsutil -u $GOOGLE_PROJECT cp -n $SAMPLE_IDS $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp scripts/spa_tests_wrapper_dsub.sh $WORKSPACE_BUCKET/data/saige/run/scripts/

# Run dsub job
source ~/aou_dsub.bash

aou_dsub \\
    --name "spatests_dsub" \\
    --image "wzhou88/saige:1.4.2" \\
    --input VCF="$WORKSPACE_BUCKET/data/saige/run/spatests/vcf/aou.exome_split.v8.{ancestry}.qced.autosomes.AAF05.popmax05.{variant_class}.recessive.vcf.bgz" \\
    --input CSI="$WORKSPACE_BUCKET/data/saige/run/spatests/vcf/aou.exome_split.v8.{ancestry}.qced.autosomes.AAF05.popmax05.{variant_class}.recessive.vcf.bgz.csi" \\
    --input MODELFILE="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{model_file}" \\
    --input VARIANCE_RATIO="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{variance_ratio}" \\
    --input GRM="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{ancestry}_snp_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx" \\
    --input GRM_IDS="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{ancestry}_snp_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx.sampleIDs.txt" \\
    --input SAMPLE_IDS="$WORKSPACE_BUCKET/data/saige/run/nullglmm/sample_ids_{ancestry}_snp_{phenotype}.txt" \\
    --output OUTPUT="$WORKSPACE_BUCKET/data/saige/run/spatests/{output_file}" \\
    --env MIN_MAC="{min_mac}" \\
    --script "$WORKSPACE_BUCKET/data/saige/run/scripts/spa_tests_wrapper_dsub.sh" \\
    --logging "$WORKSPACE_BUCKET/data/saige/run/logging" \\
    --disk-size 2000 \\
    --boot-disk-size 50 \\
    --wait

# Download the output file
gsutil -u $GOOGLE_PROJECT cp "$WORKSPACE_BUCKET/data/saige/run/spatests/{output_file}" .
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate dsub command for SAIGE SPA tests")
    parser.add_argument("--ancestry", required=True, help="e.g. amr, afr, eur")
    parser.add_argument("--phenotype", required=True, help="e.g. Height, asthma")
    parser.add_argument("--variant_class", required=True, help="e.g. pLoF, missense, synonymous")
    parser.add_argument("--model_file", required=True, help="RDA file from null model step")
    parser.add_argument("--variance_ratio", required=True, help="Variance ratio file")
    parser.add_argument("--output_file", required=True, help="Desired name for output .txt file")
    parser.add_argument("--min_mac", default="0.5", help="Minimum minor allele count")

    args = parser.parse_args()

    command = template.format(
        ancestry=args.ancestry,
        phenotype=args.phenotype,
        variant_class=args.variant_class,
        model_file=args.model_file,
        variance_ratio=args.variance_ratio,
        output_file=args.output_file,
        min_mac=args.min_mac
    )

    print(command)
