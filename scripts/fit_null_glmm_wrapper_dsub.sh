#!/bin/bash

# Input and output variables
GENOTYPE_PLINK="${BED%.bed}"       # Input file prefix (strip .bed if provided)
OUTPUT_PREFIX="${VARIANCE_RATIO%.varianceRatio.txt}"
SPARSE_GRM="$GRM"
SPARSE_GRM_IDS="$GRM_IDS"
PHENOFILE="$PCA_COVAR"
TRAITTYPE="$TRAIT_TYPE"
INVNORMALISE="$INV_NORMALISE"

PHENOCOL="$PHENOCOL"

COVARCOLLIST="$COVAR_COLLIST"
CATEGCOVARCOLLIST="$CATEG_COVAR_COLLIST"
SAMPLEIDCOL="$SAMPLE_ID_COL"
TOL="$TOL"
SAMPLEIDS="$SAMPLEIDS"

Rscript /usr/local/bin/step1_fitNULLGLMM.R \
      --plinkFile="${GENOTYPE_PLINK}" \
      --relatednessCutoff=0.05 \
      --sparseGRMFile=${SPARSE_GRM} \
      --sparseGRMSampleIDFile=${SPARSE_GRM_IDS} \
      --useSparseGRMtoFitNULL=TRUE \
      --phenoFile=${PHENOFILE} \
      --skipVarianceRatioEstimation FALSE \
      --traitType=${TRAITTYPE} \
      --invNormalize=${INVNORMALISE} \
      --phenoCol=${PHENOCOL} \
      --covarColList="${COVARCOLLIST}" \
      --qCovarColList="${CATEGCOVARCOLLIST}" \
      --sampleIDColinphenoFile=${SAMPLEIDCOL} \
      --outputPrefix="${OUTPUT_PREFIX}" \
      --IsOverwriteVarianceRatioFile=TRUE \
      --nThreads=4 \
      --isCateVarianceRatio=TRUE \
      --tol=${TOL} \
      $( [[ "$FEMALEONLY" == "TRUE" ]] && echo "--FemaleOnly=TRUE --sexCol=sex --FemaleCode=1.0" )
      #--SampleIDIncludeFile=${SAMPLEIDS} \
      #--isCovariateOffset TRUE
