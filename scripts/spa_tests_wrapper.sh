#!/bin/bash

# Activate conda environment
source /home/jupyter/anaconda3/etc/profile.d/conda.sh
conda activate biallelic_effects

# Input and output variables
VCF="${1}" 
OUT="${2}"
CHR="$3"
MIN_MAC="$4"
MODELFILE="$5"
VARIANCERATIO="$6"
SPARSEGRM="$7"
SPARSEGRMID="${SPARSEGRM}.sampleIDs.txt"
SUBSAMPLES="$8"

step2_SPAtests.R \
        --vcfFile=${VCF} \
        --vcfFileIndex="${VCF}.csi"\
        --vcfField="DS" \
        --chrom="$CHR" \
        --minMAF=0 \
        --minMAC=${MIN_MAC} \
        --GMMATmodelFile=${MODELFILE} \
        --varianceRatioFile=${VARIANCERATIO} \
        --sparseGRMFile=${SPARSEGRM} \
        --sparseGRMSampleIDFile=${SPARSEGRMID} \
        --SampleFile=${SUBSAMPLES} \
        --LOCO=FALSE \
        --is_Firth_beta=TRUE \
        --SPAcutoff=0.5 \
        --pCutoffforFirth=0.10 \
        --is_output_moreDetails=TRUE \
        --is_fastTest=TRUE \
        --SAIGEOutputFile=${OUT} \
        --dosage_zerod_MAC_cutoff=0 \
        --dosage_zerod_cutoff=0 \
        --dosage_zerod_MAC_cutoff=0 

