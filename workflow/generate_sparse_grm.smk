# Quick pipeline for generating sparse GRM on All of Us data (NOT using dsub)

# Define a list of pruning methods
prune_methods = ['snp']

# Target Rule for Completion of Pipeline
rule all:
    input:
        "removed_array_data.txt",
        expand("make_sparse_grm/allofus_array_{ancestry}_{prune_method}_wise_relatednessCutoff_0.05_5000_randomMarkersUsed.sparseGRM.mtx", ancestry=glob_wildcards("sample_selection/{ancestry}.tsv").ancestry, prune_method=prune_methods),
        expand("PCA/allofus_array_{ancestry}_{prune_method}_wise_pca.tsv", ancestry=glob_wildcards("sample_selection/{ancestry}.tsv").ancestry, prune_method=prune_methods),
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

# Filter related and flagged smaples and split array data by ancestry (p > 0.75)
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

rule calc_pcs:
    input:
        "ld_prune/allofus_array_{ancestry}_{prune_method}_wise.bed"
    output:
        "PCA/allofus_array_{ancestry}_{prune_method}_wise_pca.tsv"
    log:
        "logs/convert_to_hail_{ancestry}_{prune_method}.log"
    shell:
        "python scripts/calc_pcs.py {input} {output}"
