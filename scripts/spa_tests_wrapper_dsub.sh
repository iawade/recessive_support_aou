#!/bin/bash

# Input and output variables
VCF=$VCF
OUT="$OUTPUT"
CHR="$CHROM"
MIN_MAC="$MIN_MAC"
MODELFILE="$MODELFILE"
VARIANCERATIO="$VARIANCE_RATIO"
SPARSEGRM="$GRM"
SPARSEGRMID="$GRM_IDS"
SUBSAMPLES="$SAMPLE_IDS"

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

