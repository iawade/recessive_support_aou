configfile: "config/config.yaml"

# Quick pipeline for generating sparse GRM on All of Us data (NOT using dsub)

prune_methods = ['snp']
ancestries = ['amr', 'eas', 'sas']

import json

# Load the phenotype data from the JSON file
with open(config["json"]) as f:
    phenotype_data = json.load(f)

# Access the values for the phenotype_code from the JSON
phenotype = next(item for item in phenotype_data if item["phenotype_ID"] == config["phenotype_code"])

# Now you can access the specific values from `phenotype`
trait_type = phenotype["trait_type"]
invnormalise = phenotype["invnormalise"]
tol = phenotype["tol"]

# Target Rule for Completion of Pipeline
rule all:
    input:
        "removed_array_data.txt",
        expand("nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}_fitNullGLMM_results.txt", ancestry=ancestries, prune_method=prune_methods, phenotype_code=config["phenotype_code"])
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

rule delete_array_data:
    input:
        expand("sample_selection/allofus_array_{ancestry}.bed", ancestry=glob_wildcards("sample_selection/{ancestry}.tsv").ancestry)
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
    log:
        "logs/convert_to_hail_{ancestry}_{prune_method}.log"
    shell:
        "python scripts/calc_pcs.py {input} {output}"

rule combine_covars:
    input:
        "PCA/allofus_array_{ancestry}_{prune_method}_wise_pca.tsv"
    output:
        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.csv",
        "nullglmm/sample_ids_{ancestry}_{prune_method}_{phenotype_code}.txt"  # Output file for sample_ids
    params:
        phenotype_code=config["phenotype_code"],
        phenotype_file=config["phenotype_file"],
        covariates_file=config["covariates_file"]
    shell:
        """
        python scripts/combine_pheno_and_covars.py {params.covariates_file} {params.phenotype_file} {input} {output}

        # Extract the sample_ids (those with phenotype data and correct ancestry)
        # Assuming `person_id` is the identifier in your covariates file and this matches `sample_id`
        cut -f1 {output[0]} > {output[1]}  # Save sample IDs to a text file
        """
rule fitnullglmm:
    input:
        "ld_prune/allofus_array_{ancestry}_{prune_method}_wise.bed",
        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}.csv",
        "make_sparse_grm/allofus_array_{ancestry}_{prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx",
        "nullglmm/sample_ids_{ancestry}_{prune_method}_{phenotype_code}.txt"  # The file with sample_ids
    output:
        "nullglmm/allofus_array_{ancestry}_{prune_method}_wise_pca_covariates_{phenotype_code}_fitNullGLMM_results.txt"
    params:
        phenocol=config["phenotype_code"],
        covarcollist=config["covarcollist"],
        categcovarcollist=config["categcovarcollist"],
        sampleidcol=config["sampleidcol"],
        n_threads=config["n_threads_step_one"],
        trait_type=trait_type,
        invnormalise=invnormalise,
        tol=tol
    shell:
        """
        bash scripts/fit_null_glmm_wrapper.sh {input[0]} {output} {input[2]} {input[1]} {params.trait_type} {params.invnormalise} {params.phenocol} {params.covarcollist} {params.categcovarcollist} {params.sampleidcol} {params.tol} --SampleIDIncludeFile={input[3]}
        """

