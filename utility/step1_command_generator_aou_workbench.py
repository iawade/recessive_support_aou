import argparse

template = """
# Define filenames
BED="nullglmm/allofus_array_{ancestry}_for_vr.bed"
BIM="nullglmm/allofus_array_{ancestry}_for_vr.bim"
FAM="nullglmm/allofus_array_{ancestry}_for_vr.fam"
PCA_COVAR="{pca_covar_file}"
GRM="make_sparse_grm/allofus_array_{ancestry}_snp_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx"
SAMPLE_IDS="nullglmm/sample_ids_{ancestry}_snp_{phenotype}.txt"

# Upload input files to the cloud
gsutil -u $GOOGLE_PROJECT cp -n $BED $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp -n $BIM $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp -n $FAM $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp $PCA_COVAR $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp -n $GRM $WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/
gsutil -u $GOOGLE_PROJECT cp -n $GRM.sampleIDs.txt $WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/
gsutil -u $GOOGLE_PROJECT cp -n $SAMPLE_IDS $WORKSPACE_BUCKET/data/saige/run/nullglmm/
gsutil -u $GOOGLE_PROJECT cp scripts/fit_null_glmm_wrapper_dsub.sh $WORKSPACE_BUCKET/data/saige/run/scripts/

# Run dsub job
source ~/aou_dsub.bash

aou_dsub \\
    --name "fitnullglmm_dsub" \\
    --image "wzhou88/saige:1.4.2" \\
    --input BED="$WORKSPACE_BUCKET/data/saige/run/nullglmm/allofus_array_{ancestry}_for_vr.bed" \\
    --input BIM="$WORKSPACE_BUCKET/data/saige/run/nullglmm/allofus_array_{ancestry}_for_vr.bim" \\
    --input FAM="$WORKSPACE_BUCKET/data/saige/run/nullglmm/allofus_array_{ancestry}_for_vr.fam" \\
    --input PCA_COVAR="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{pca_covar_file}" \\
    --input GRM="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{ancestry}_snp_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx" \\
    --input GRM_IDS="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{ancestry}_snp_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx.sampleIDs.txt" \\
    --input SAMPLE_IDS="$WORKSPACE_BUCKET/data/saige/run/nullglmm/sample_ids_{ancestry}_snp_{phenotype}.txt" \\
    --output VARIANCE_RATIO="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{output_stem}.varianceRatio.txt" \\
    --output RDA="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{output_stem}.rda" \\
    --env TRAIT_TYPE="{trait_type}" \\
    --env INV_NORMALISE="{inv_norm}" \\
    --env PHENOCOL="{phenotype}" \\
    --env COVAR_COLLIST="{covars}" \\
    --env CATEG_COVAR_COLLIST="{categ_covar_collist}" \\
    --env SAMPLE_ID_COL="person_id" \\
    --env TOL="{tol}" \\
    --env FEMALEONLY="" \\
    --script "$WORKSPACE_BUCKET/data/saige/run/scripts/fit_null_glmm_wrapper_dsub.sh" \\
    --logging "$WORKSPACE_BUCKET/data/saige/run/logging" \\
    --disk-size 2000 \\
    --boot-disk-size 50 \\
    --min-ram 32 \\
    --min-cores 8 \\
    --env "SPARK_DRIVER_MEMORY=4g" \\
    --env "SPARK_EXECUTOR_MEMORY=8g" \\
    --env "SPARK_DRIVER_EXTRA_JAVA_OPTIONS=-Xmx4g" \\
    --env "SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS=-Xmx8g" \\
    --wait

# Download the output files
gsutil -u $GOOGLE_PROJECT cp $WORKSPACE_BUCKET/data/saige/run/nullglmm/{output_stem}.varianceRatio.txt .
gsutil -u $GOOGLE_PROJECT  cp $WORKSPACE_BUCKET/data/saige/run/nullglmm/{output_stem}.rda .
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate dsub command for SAIGE null model fitting")
    parser.add_argument("--ancestry", required=True, help="e.g. amr, afr, eur")
    parser.add_argument("--phenotype", required=True, help="e.g. Height, asthma")
    parser.add_argument("--pca_covar_file", required=True, help="")
    parser.add_argument("--output_stem", required=True, help="")
    parser.add_argument("--covars", required=True, help="age,age2,age_sex,age2_sex,sex,PC1,PC2,PC3,PC4,PC5,PC6,PC7,PC8,PC9,PC10,PC11,PC12,PC13,PC14,PC15,PC16,PC17,PC18,PC19,PC20, plus any extra")
    parser.add_argument("--categ_covar_collist", required=True, help="sex, any extra")
    parser.add_argument("--trait_type", required=True, choices=["quantitative", "binary"])
    parser.add_argument("--inv_norm", required=True, choices=["TRUE","FALSE"])
    parser.add_argument("--tol", required=True, help="0.02 or 1e-05")

    args = parser.parse_args()

    command = template.format(
        ancestry=args.ancestry,
        phenotype=args.phenotype,
        pca_covar_file=args.pca_covar_file,
        output_stem=args.output_stem,
        categ_covar_collist=args.categ_covar_collist,
        covars=args.covars,
        trait_type=args.trait_type,
        inv_norm=args.inv_norm,
        tol=args.tol
    )

    print(command)
