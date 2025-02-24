#!/bin/bash

OUT=$(basename "${OUT_FILE}" .txt)
TESTTYPE="${TESTTYPE}"
VCF="${IN_VCF}"
PLINK=""
MODELFILE="${IN_MODELFILE}"
VARIANCERATIO="${IN_VARIANCERATIO}"
SPARSEGRM="${IN_SPARSEGRM}"
SPARSEGRMID="${IN_SPARSEGRMID}"
GROUPFILE="${GROUPFILE}"
SINGULARITY=false     # Add this
CHR="${CHR}"         # Add this
ANNOTATIONS="${ANNOTATIONS}"  # Add this
export VCF


# Checks
if [[ ${TESTTYPE} == "" ]]; then
  echo "Test type not set"
  exit 1
fi

if [[ ${PLINK} == "" ]] && [[ ${VCF} == "" ]]; then
  echo "plink files plink.{bim,bed,fam} and vcf not set"
  exit 1
fi

if [[ ${SPARSEGRM} == "" ]]; then
  echo "sparse GRM .mtx file not set"
  exit 1
fi

if [[ ${SPARSEGRMID} == "" ]]; then
  echo "sparse GRM ID file not set"
  exit 1
fi

if [[ ${MODELFILE} == "" ]]; then
  echo "model file not set"
  exit 1
fi

if [[ ${VARIANCERATIO} == "" ]]; then
  echo "variance ration file not set"
  exit 1
fi

if [[ $GROUPFILE == "" ]] && [[ ${TESTTYPE} == "group" ]]; then
  echo "attempting to run group tests without an annotation file"
  exit 1
fi

if [[ $ANNOTATIONS == "" ]] && [[ ${TESTTYPE} == "group" ]]; then
  echo "attempting to run group tests without selected annotations"
  exit 1
fi

if [[ $SUBSAMPLES != "" ]]; then
  SUBSAMPLES="${SUBSAMPLES}"
fi

if [[ $OUT = "out" ]]; then
  echo "Warning: outputPrefix not set, setting outputPrefix to 'out'. Check that this will not overwrite existing files."
fi

echo "OUT               = ${OUT}"
echo "TESTTYPE          = ${TESTTYPE}"
echo "PLINK             = ${PLINK}.{bim/bed/fam}"
echo "MODELFILE         = ${MODELFILE}"
echo "VARIANCERATIO     = ${VARIANCERATIO}"
echo "GROUPFILE         = ${GROUPFILE}"
echo "ANNOTATIONS       = ${ANNOTATIONS}"
echo "SPARSEGRM         = ${SPARSEGRM}"
echo "SPARSEGRMID       = ${SPARSEGRMID}"

# For debugging
set -exo pipefail

## Set up directories
WD=$( pwd )

# Get number of threads
n_threads=$(( $(nproc --all) - 1 ))

## Set up directories
WD=$( pwd )


if [[ "$TESTTYPE" = "variant" ]]; then
  echo "variant testing"
  min_mac="4"
  GROUPFILE=""
else
  echo "gene testing"
  min_mac="0.5"
fi

if [[ ${PLINK} != "" ]]; then
  BED=${PLINK}".bed"
  BIM=${PLINK}".bim"
  FAM=${PLINK}".fam"
  VCF=""
elif [[ ${VCF} != "" ]]; then 
  BED=""
  BIM=""
  FAM="" 
else
  echo "No plink or vcf found!"
  exit 1
fi

ls ${VCF}

cmd="Rscript /usr/local/bin/step2_SPAtests.R \
        --bedFile=$BED \
        --bimFile=$BIM \
        --famFile=$FAM \
        --groupFile=$GROUPFILE \
        --annotation_in_groupTest=$ANNOTATIONS \
        --vcfFile=${VCF} \
        --vcfFileIndex=${VCF}.csi \
        --vcfField=DS \
        --chrom=$CHR \
        --minMAF=0 \
        --minMAC=${min_mac} \
        --GMMATmodelFile=${MODELFILE} \
        --varianceRatioFile=${VARIANCERATIO} \
        --sparseGRMFile=${SPARSEGRM} \
        --sparseGRMSampleIDFile=${SPARSEGRMID} \
        --subSampleFile=${SUBSAMPLES} \
        --LOCO=FALSE \
        --is_Firth_beta=TRUE \
        --SPAcutoff=0.5 \
        --pCutoffforFirth=0.10 \
        --is_output_moreDetails=TRUE \
        --is_fastTest=TRUE \
        --is_output_markerList_in_groupTest=TRUE \
        --is_single_in_groupTest=TRUE \
        --maxMAF_in_groupTest=0.0001,0.001,0.01 \
        --SAIGEOutputFile=step2_output.txt"

echo "Running command: $cmd"
eval $cmd

# Move output to the expected dsub output location
mv step2_output.txt $(dirname "${OUT_FILE}")/$(basename "${OUT_FILE}")
