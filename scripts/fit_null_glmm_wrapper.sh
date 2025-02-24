#!/bin/bash

# Activate conda environment
source /home/jupyter/anaconda3/etc/profile.d/conda.sh
conda activate biallelic_effects

# Input and output variables
GENOTYPE_PLINK="${1%.bed}"       # Input file prefix (strip .bed if provided)
OUTPUT_PREFIX="${2%.varianceRatio.txt}"
SPARSE_GRM="$3"
PHENOFILE="$4"
TRAITTYPE="$5"
INVNORMALISE="$6"
PHENOCOL="$7"
COVARCOLLIST="$8"
CATEGCOVARCOLLIST="$9"
SAMPLEIDCOL="${10}"
TOL="${11}"
SAMPLEIDS="${12}"

step1_fitNULLGLMM.R \
      --plinkFile="${GENOTYPE_PLINK}" \
      --relatednessCutoff=0.05 \
      --sparseGRMFile=${SPARSE_GRM} \
      --sparseGRMSampleIDFile=${SPARSE_GRM}.sampleIDs.txt \
      --useSparseGRMtoFitNULL=TRUE \
      --phenoFile=${PHENOFILE} \
      --skipVarianceRatioEstimation=FALSE \
      --traitType=${TRAITTYPE} \
      --invNormalize=${INVNORMALISE} \
      --phenoCol=""${PHENOCOL}"" \
      --covarColList=""${COVARCOLLIST}"" \
      --qCovarColList=""${CATEGCOVARCOLLIST}"" \
      --sampleIDColinphenoFile=${SAMPLEIDCOL} \
      --outputPrefix="${OUTPUT_PREFIX}" \
      --IsOverwriteVarianceRatioFile=TRUE \
      --nThreads=4 \
      --isCateVarianceRatio=TRUE \
      --tol=${TOL} \
      --SampleIDIncludeFile=${SAMPLEIDS} \
      --isCovariateOffset=FALSE
