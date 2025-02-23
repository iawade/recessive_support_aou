configfile: "config/config.yaml"

# Quick pipeline for generating sparse GRM on All of Us data (NOT using dsub)
prune_methods = ['snp']
# ancestries = ['afr', 'amr', 'eas', 'eur', 'mid']
phenotypes = config['phenotype_code'] 

# Assess chrom based on what's in vcf folder
# could get acestries from this folder but want upstream steps to work without vcf already generated
CHROMS, ancestries = glob_wildcards("vcf/aou.exome_split.v8.{chrom}.{ancestry}.qc.maf05.popmax05.pLoF_damaging_missense.recessive.vcf.bgz")

import json

# Load the phenotype data from the JSON file
with open(config["json"]) as f:
    phenotype_data = json.load(f)

# Access the values for each phenotype_code from the JSON
phenotype_list = []

for pheno_code in phenotypes:  # Use the newly defined `phenotypes`
    match = next((item for item in phenotype_data if item["phenotype_ID"] == pheno_code), None)
    if match:
        phenotype_list.append(match)
    else:
        print(f"Warning: Phenotype ID '{pheno_code}' not found in JSON.")

# Now process all phenotypes
# Example of handling each phenotype's details
trait_type = {p["phenotype_ID"]: p["trait_type"] for p in phenotype_list}
invnormalise = {p["phenotype_ID"]: p["invnormalise"] for p in phenotype_list}
tol = {p["phenotype_ID"]: p["tol"] for p in phenotype_list}

# Target Rule for Completion of Pipeline
rule all:
    input:
        "removed_array_data.txt",
        expand(
		"spatests/aou_{prune_method}_{ancestry}_{phenotype_code}_{chrom}.txt",
            	ancestry=ancestries, 
            	prune_method=prune_methods, 
            	phenotype_code=phenotypes,
		chrom=CHROMS
        )
    output:
        "pipeline_complete.txt"
    shell:
        "touch {output}"

rule download_array_data:
    output:
        "downloaded_data.txt"
    shell:
        "bash scripts/download_array_data.sh gs://fc-aou-datasets-controlled/v7/microarray/plink_v7.1/arrays"
    # Should put path in config - e.g. for new releases

# Filter related and flagged samples and split array data by ancestry (p > 0.75)
# relies on tsv already set up with all exclusions
# TODO need to update some of the filepaths 
rule filter_and_split_ancestry:
    input:
        "downloaded_data.txt",
        "sample_selection/{ancestry}.tsv"
    output:
        "sample_selection/allofus_array_{ancestry}.bed"
    log:
        "logs/filter_ancestry_{ancestry}.log" 
    shell:
        "bash scripts/filter_ancestry.sh ancestry/arrays {input[1]} {output}"

rule create_plink_for_vr:
    input:
        "downloaded_data.txt",
        "sample_selection/{ancestry}.tsv"
    output:
        "nullglmm/allofus_array_{ancestry}_for_vr.bed"
    shell:
        "bash scripts/create_plink_file_for_vr.sh ancestry/arrays {input[1]} {output}"

rule delete_array_data:
    input:
        expand("sample_selection/allofus_array_{ancestry}.bed", ancestry=ancestries),
        expand("nullglmm/allofus_array_{ancestry}_for_vr.bed", ancestry=ancestries)
    output:
        "removed_array_data.txt"
    shell:
        "bash scripts/remove_array_data.sh"

rule LD_prune:
    input:
        "sample_selection/allofus_array_{ancestry}.bed"
    output:
        "ld_prune/allofus_array_{ancestry}_snp_wise.bed",
        "ld_prune/allofus_array_{ancestry}_length_wise.bed"
    shell:
        "bash scripts/remove_LD.sh {input} {output[0]} {output[1]}"

rule create_GRM:
    input:
        "ld_prune/allofus_array_{ancestry}_{prune_method}_wise.bed"
    output:
        "make_sparse_grm/allofus_array_{ancestry}_{prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx"
    log:
        "logs/create_sparse_grm_{ancestry}_{prune_method}.log"
    shell:
        "bash scripts/make_sparse_GRM_wrapper.sh {input} {output}"
# TODO need to make nthreads a parameter



rule calc_pcs:
    input:
        "ld_prune/allofus_array_{ancestry}_{prune_method}_wise.bed"
    output:
        "PCA/allofus_array_{ancestry}_{prune_method}_wise_pca.tsv"
    params:
        bim=lambda wildcards: f"ld_prune/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise.bim",
        fam=lambda wildcards: f"ld_prune/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise.fam",
        eigenv=lambda wildcards: f"PCA/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_eigenvalues.tsv",
        loadings=lambda wildcards: f"PCA/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_loadings.tsv"
    log:
        "logs/convert_to_hail_{ancestry}_{prune_method}.log"
    shell:
        """
        # Ensure the necessary files are uploaded to the cloud (if local)
        gsutil -u $GOOGLE_PROJECT cp {input} $WORKSPACE_BUCKET/data/saige/run/ld_prune/
        gsutil -u $GOOGLE_PROJECT cp {params.bim} $WORKSPACE_BUCKET/data/saige/run/ld_prune/
        gsutil -u $GOOGLE_PROJECT cp {params.fam} $WORKSPACE_BUCKET/data/saige/run/ld_prune/
        gsutil -u $GOOGLE_PROJECT cp scripts/calc_pcs_dsub.py $WORKSPACE_BUCKET/data/saige/run/scripts/

        # Run the dsub job on Google Cloud
        source ~/aou_dsub.bash
        aou_dsub \
            --name "pcs_dsub" \
            --image "hailgenetics/hail:0.2.133-py3.10" \
            --input INPUT_BED="$WORKSPACE_BUCKET/data/saige/run/ld_prune/{wildcards.ancestry}_{wildcards.prune_method}_wise.bed" \
            --input BIM="$WORKSPACE_BUCKET/data/saige/run/ld_prune/{wildcards.ancestry}_{wildcards.prune_method}_wise.bim" \
            --input FAM="$WORKSPACE_BUCKET/data/saige/run/ld_prune/{wildcards.ancestry}_{wildcards.prune_method}_wise.fam" \
            --output OUTPUT="$WORKSPACE_BUCKET/saige/run/{output}" \
            --output OUTPUT_eigenvalues="$WORKSPACE_BUCKET/data/saige/run/{params.eigenv}" \
            --output OUTPUT_loadings="$WORKSPACE_BUCKET/data/saige/run/{params.loadings}" \
            --script "$WORKSPACE_BUCKET/data/saige/run/scripts/calc_pcs_dsub.py" \
            --logging "$WORKSPACE_BUCKET/data/saige/run/logging" \
            --disk-size 2000 \
            --boot-disk-size 50 \
            --min-ram 128 \
            --min-cores 32 \
            --env "SPARK_DRIVER_MEMORY=32g" \
            --env "SPARK_EXECUTOR_MEMORY=64g" \
            --env "SPARK_DRIVER_EXTRA_JAVA_OPTIONS=-Xmx32g" \
            --env "SPARK_EXECUTOR_EXTRA_JAVA_OPTIONS=-Xmx64g" \
	    --wait

        # Download the result files from Google Cloud
        gsutil -u $GOOGLE_PROJECT cp $WORKSPACE_BUCKET/saige/run/{output} {output}
        gsutil -u $GOOGLE_PROJECT cp $WORKSPACE_BUCKET/data/saige/run/{params.eigenv} {params.eigenv}
        gsutil -u $GOOGLE_PROJECT cp $WORKSPACE_BUCKET/data/saige/run/{params.loadings} {params.loadings}
        """

rule combine_covars:
    input:
        "PCA/allofus_array_{ancestry}_{prune_method}_wise_pca.tsv"
    output:
        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.csv",
        "nullglmm/sample_ids_{ancestry}_{prune_method}_{phenotype_code}.txt"  # Output file for sample_ids
    params:
        #phenotype_code=config["phenotype_code"],
        phenotype_file=config["phenotype_file"],
        covariates_file=config["covariates_file"]
    shell:
        """
        python scripts/combine_pheno_and_covars.py {params.phenotype_file} {input} {output[0]}

        # Extract the sample_ids (those with phenotype data and correct ancestry)
        # Assuming `person_id` is the identifier in your covariates file and this matches `sample_id`
        cut -d "," -f1 {output[0]} > {output[1]}  # Save sample IDs to a text file
        """

#rule fitnullglmm:
#    input:
#        "nullglmm/allofus_array_{ancestry}_for_vr.bed",
#        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.csv",
#        "make_sparse_grm/allofus_array_{ancestry}_{prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx",
#        "nullglmm/sample_ids_{ancestry}_{prune_method}_{phenotype_code}.txt"  # The file with sample_ids
#    output:
#        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.varianceRatio.txt"
#	"nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.rda"
#    params:
#        covarcollist=config["covarcollist"],
#        categcovarcollist=config["categcovarcollist"],
#        sampleidcol=config["sampleidcol"],
#        n_threads=config["n_threads_step_one"],
#        trait_type=lambda wildcards: trait_type[wildcards.phenotype_code],   # Extract the correct trait type
#        invnormalise=lambda wildcards: invnormalise[wildcards.phenotype_code],  # Extract invnormalise value
#        tol=lambda wildcards: tol[wildcards.phenotype_code]   # Extract the correct tol value
#    shell:
#        """
#        bash scripts/fit_null_glmm_wrapper.sh {input[0]} {output[0]} {input[2]} {input[1]} {params.trait_type} {params.invnormalise} {wildcards.phenotype_code} {params.covarcollist} {params.categcovarcollist} {params.sampleidcol} {params.tol} {input[3]}
#        """

rule fitnullglmm:
    input:
        "nullglmm/allofus_array_{ancestry}_for_vr.bed",
        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.csv",
        "make_sparse_grm/allofus_array_{ancestry}_{prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx",
        "nullglmm/sample_ids_{ancestry}_{prune_method}_{phenotype_code}.txt"  # The file with sample_ids
    output:
        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.varianceRatio.txt",
        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.rda"
    params:
        bim=lambda wildcards: f"nullglmm/allofus_array_{wildcards.ancestry}_for_vr.bim",
        fam=lambda wildcards: f"nullglmm/allofus_array_{wildcards.ancestry}_for_vr.fam",
        covarcollist=config["covarcollist"],
        categcovarcollist=config["categcovarcollist"],
        sampleidcol=config["sampleidcol"],
        n_threads=config["n_threads_step_one"],
        phenotype_code=lambda wildcards: f"{wildcards.phenotype_code}",
        trait_type=lambda wildcards: trait_type[wildcards.phenotype_code],   # Extract the correct trait type
        invnormalise=lambda wildcards: invnormalise[wildcards.phenotype_code],  # Extract invnormalise value
        tol=lambda wildcards: tol[wildcards.phenotype_code]   # Extract the correct tol value
    shell:
        """
	# Define filenames
        BED="{input[0]}"
	BIM="{params.bim}"
        FAM="{params.fam}"
        PCA_COVAR="{input[1]}"
        GRM="{input[2]}"
        SAMPLE_IDS="{input[3]}"

        # Upload input files to the cloud
        gsutil -u $GOOGLE_PROJECT  cp -n $BED $WORKSPACE_BUCKET/data/saige/run/nullglmm/
        gsutil -u $GOOGLE_PROJECT cp -n $BIM $WORKSPACE_BUCKET/data/saige/run/nullglmm/
        gsutil -u $GOOGLE_PROJECT cp -n $FAM $WORKSPACE_BUCKET/data/saige/run/nullglmm/
        gsutil -u $GOOGLE_PROJECT cp -n $PCA_COVAR $WORKSPACE_BUCKET/data/saige/run/nullglmm/
        gsutil -u $GOOGLE_PROJECT cp -n $GRM $WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/
        gsutil -u $GOOGLE_PROJECT cp -n $GRM.sampleIDs.txt $WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/
        gsutil -u $GOOGLE_PROJECT cp -n $SAMPLE_IDS $WORKSPACE_BUCKET/data/saige/run/nullglmm/
	gsutil -u $GOOGLE_PROJECT cp scripts/fit_null_glmm_wrapper_dsub.sh $WORKSPACE_BUCKET/data/saige/run/scripts/
        
	echo "{params.phenotype_code}"

        # Run dsub job
        source ~/aou_dsub.bash
        aou_dsub \
            --name "fitnullglmm_dsub" \
            --image "wzhou88/saige:1.4.2" \
            --input BED="$WORKSPACE_BUCKET/data/saige/run/nullglmm/allofus_array_{wildcards.ancestry}_for_vr.bed" \
            --input BIM="$WORKSPACE_BUCKET/data/saige/run/nullglmm/allofus_array_{wildcards.ancestry}_for_vr.bim" \
            --input FAM="$WORKSPACE_BUCKET/data/saige/run/nullglmm/allofus_array_{wildcards.ancestry}_for_vr.fam" \
            --input PCA_COVAR="$WORKSPACE_BUCKET/data/saige/run/nullglmm/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_covariates_{wildcards.phenotype_code}.csv" \
            --input GRM="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx" \
            --input GRM_IDS="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx.sampleIDs.txt" \
            --input SAMPLE_IDS="$WORKSPACE_BUCKET/data/saige/run/nullglmm/sample_ids_{wildcards.ancestry}_{wildcards.prune_method}_{wildcards.phenotype_code}.txt" \
            --output VARIANCE_RATIO="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_covariates_{wildcards.phenotype_code}.varianceRatio.txt" \
            --output RDA="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_covariates_{wildcards.phenotype_code}.rda" \
            --env TRAIT_TYPE="{params.trait_type}" \
            --env INV_NORMALISE="{params.invnormalise}" \
            --env PHENOCOL="{wildcards.phenotype_code}" \
            --env COVAR_COLLIST="{params.covarcollist}" \
            --env CATEG_COVAR_COLLIST="{params.categcovarcollist}" \
            --env SAMPLE_ID_COL="{params.sampleidcol}" \
            --env TOL="{params.tol}" \
            --script "$WORKSPACE_BUCKET/data/saige/run/scripts/fit_null_glmm_wrapper_dsub.sh" \
            --logging "$WORKSPACE_BUCKET/data/saige/run/logging" \
            --disk-size 2000 \
            --boot-disk-size 50 \
            --wait

        # Download the output files
        gsutil -u $GOOGLE_PROJECT cp $WORKSPACE_BUCKET/data/saige/run/nullglmm/{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_covariates_{wildcards.phenotype_code}.varianceRatio.txt {output[0]}
        gsutil -u $GOOGLE_PROJECT  cp $WORKSPACE_BUCKET/data/saige/run/nullglmm/{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_covariates_{wildcards.phenotype_code}.rda {output[1]}
        """

rule spatests:
    input:
        "vcf/aou.exome_split.v8.{chrom}.{ancestry}.qc.maf05.popmax05.pLoF_damaging_missense.recessive.vcf.bgz",
	"nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.rda",
	"nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.varianceRatio.txt",
        "make_sparse_grm/allofus_array_{ancestry}_{prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx",
        "nullglmm/sample_ids_{ancestry}_{prune_method}_{phenotype_code}.txt"  # The file with sample_ids
    output:
        "spatests/aou_{prune_method}_{ancestry}_{phenotype_code}_{chrom}.txt"
    params:
        min_mac=config["min_mac"],
    shell:
        """
	 # Define filenames
        VCF="{input[0]}"
        MODELFILE="{input[1]}"
        VARIANCE_RATIO="{input[2]}"
        SPARSE_GRM="{input[3]}"
        SAMPLE_IDS="{input[4]}"

        # Upload input files to the cloud
        gsutil -u $GOOGLE_PROJECT  cp $VCF $WORKSPACE_BUCKET/data/saige/run/spatests/vcf/
        gsutil -u $GOOGLE_PROJECT cp $VCF.csi $WORKSPACE_BUCKET/data/saige/run/spatests/vcf/
        gsutil -u $GOOGLE_PROJECT cp scripts/spa_tests_wrapper_dsub.sh $WORKSPACE_BUCKET/data/saige/run/scripts/

        source ~/aou_dsub.bash
        aou_dsub \
            --name "spatests_dsub" \
            --image "wzhou88/saige:1.4.2" \
            --input VCF="$WORKSPACE_BUCKET/data/saige/run/spatests/$VCF" \
            --input CSI="$WORKSPACE_BUCKET/data/saige/run/spatests/$VCF.csi" \
            --input VARIANCE_RATIO="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_covariates_{wildcards.phenotype_code}.varianceRatio.txt" \
            --input MODELFILE="$WORKSPACE_BUCKET/data/saige/run/nullglmm/{wildcards.ancestry}_{wildcards.prune_method}_wise_pca_covariates_{wildcards.phenotype_code}.rda" \
            --input GRM="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx" \
            --input GRM_IDS="$WORKSPACE_BUCKET/data/saige/run/make_sparse_grm/allofus_array_{wildcards.ancestry}_{wildcards.prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx.sampleIDs.txt" \
            --input SAMPLE_IDS="$WORKSPACE_BUCKET/data/saige/run/nullglmm/sample_ids_{wildcards.ancestry}_{wildcards.prune_method}_{wildcards.phenotype_code}.txt" \
            --output OUTPUT="$WORKSPACE_BUCKET/data/saige/run/spatests/aou_{wildcards.prune_method}_{wildcards.ancestry}_{wildcards.phenotype_code}_{wildcards.chrom}.txt" \
            --env MIN_MAC="{params.min_mac}" \
            --env CHROM="{wildcards.chrom}" \
            --script "$WORKSPACE_BUCKET/data/saige/run/scripts/spa_tests_wrapper_dsub.sh" \
            --logging "$WORKSPACE_BUCKET/data/saige/run/logging" \
            --disk-size 2000 \
            --boot-disk-size 50 \
            --wait

        # Download the output files
        gsutil -u $GOOGLE_PROJECT  cp $WORKSPACE_BUCKET/data/saige/run/spatests/aou_{wildcards.prune_method}_{wildcards.ancestry}_{wildcards.phenotype_code}_{wildcards.chrom}.txt {output}

        """

