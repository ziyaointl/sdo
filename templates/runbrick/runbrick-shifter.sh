#! /bin/bash

# Script for running the legacypipe code within a Shifter container at NERSC
# Arguments:
# {0}: LEGACY_SURVEY_DIR
# {1}: ncores
# {2}: stage
# {3}: run argument / telescope
# {4}: maxmem, in KB (93750000 total for knl, 125000000 total for haswell)
# {5}: additional params
# {6}: '--writestage <writestage>'


# Burst-buffer!
#if [ "x$DW_PERSISTENT_STRIPED_DR9" == x ]; then
# No burst buffer -- use scratch

# # DR10a --
# # using survey-ccds-dr10-v3-cut.kd.fits
# # aka Eddie's PROPID-cut CCD list
# export LEGACY_SURVEY_DIR=$CSCRATCH/dr10a
# outdir=$LEGACY_SURVEY_DIR/out-v3-cut
#
# # DR10 (uncut)
# export LEGACY_SURVEY_DIR=$COSMO/work/legacysurvey/dr10
# outdir=$CSCRATCH/dr10-mem

# Using (depth-cut) v4 CCDs file
export LEGACY_SURVEY_DIR={0}
outdir=$LEGACY_SURVEY_DIR/

export CACHE_DIR=$CSCRATCH/dr10-cache

#export GAIA_CAT_DIR=/global/cfs/cdirs/desi/target/gaia_edr3/healpix
export GAIA_CAT_DIR=$CSCRATCH/gaia-edr3-healpix/healpix
export GAIA_CAT_PREFIX=healpix
export GAIA_CAT_SCHEME=nested
export GAIA_CAT_VER=E

#export DUST_DIR=/global/cfs/cdirs/cosmo/data/dust/v0_1
export DUST_DIR=$CSCRATCH/dr10-cache/dust-v0_1
#export UNWISE_COADDS_DIR=/global/cfs/cdirs/cosmo/work/wise/outputs/merge/neo7/fulldepth:/global/cfs/cdirs/cosmo/data/unwise/allwise/unwise-coadds/fulldepth
export UNWISE_COADDS_DIR=/global/cfs/cdirs/cosmo/data/unwise/neo7/unwise-coadds/fulldepth:/global/cfs/cdirs/cosmo/data/unwise/allwise/unwise-coadds/fulldepth
export UNWISE_COADDS_TIMERESOLVED_DIR=/global/cfs/cdirs/cosmo/work/wise/outputs/merge/neo7
#export UNWISE_MODEL_SKY_DIR=/global/cfs/cdirs/cosmo/work/wise/unwise_catalog/dr3/mod
export UNWISE_MODEL_SKY_DIR=/global/cfs/cdirs/cosmo/data/unwise/neo7/unwise-catalog/mod

#export TYCHO2_KD_DIR=/global/cfs/cdirs/cosmo/staging/tycho2
#export LARGEGALAXIES_CAT=/global/cfs/cdirs/cosmo/staging/largegalaxies/v3.0/SGA-ellipse-v3.0.kd.fits
export TYCHO2_KD_DIR=$CSCRATCH/dr10-cache/tycho2
export LARGEGALAXIES_CAT=$CSCRATCH/dr10-cache/SGA-ellipse-v3.0.kd.fits
export SKY_TEMPLATE_DIR=$CSCRATCH/dr10-cache/calib/sky_pattern
unset BLOB_MASK_DIR
unset PS1CAT_DIR
unset GALEX_DIR

# Don't add ~/.local/ to Python's sys.path
export PYTHONNOUSERSITE=1
# Force MKL single-threaded
# https://software.intel.com/en-us/articles/using-threaded-intel-mkl-in-multi-thread-application
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=1
# To avoid problems with MPI and Python multiprocessing
export MPICH_GNI_FORK_MODE=FULLCOPY
export KMP_AFFINITY=disabled

ncores={1}

brick="$1"
bri=$(echo $brick | head -c 3)

mkdir -p "$outdir/logs/$bri"
mkdir -p "$outdir/metrics/$bri"
mkdir -p "$outdir/pickles/$bri"
log="$outdir/logs/$bri/$brick.log"
echo Logging to: "$log"
#echo Running on $(hostname)

# # Config directory nonsense
export TMPCACHE=$(mktemp -d)
mkdir $TMPCACHE/cache
mkdir $TMPCACHE/config
# astropy
export XDG_CACHE_HOME=$TMPCACHE/cache
export XDG_CONFIG_HOME=$TMPCACHE/config
mkdir $XDG_CACHE_HOME/astropy
cp -r $HOME/.astropy/cache $XDG_CACHE_HOME/astropy
mkdir $XDG_CONFIG_HOME/astropy
cp -r $HOME/.astropy/config $XDG_CONFIG_HOME/astropy
# matplotlib
export MPLCONFIGDIR=$TMPCACHE/matplotlib
mkdir $MPLCONFIGDIR
cp -r $HOME/.config/matplotlib $MPLCONFIGDIR

echo -e "\n\n\n" >> "$log"
echo "-----------------------------------------------------------------------------------------" >> "$log"
echo -e "\nStarting on $(hostname)\n" >> "$log"
echo "-----------------------------------------------------------------------------------------" >> "$log"

python -O $LEGACYPIPE_DIR/legacypipe/runbrick.py \
     --brick "$brick" \
     --skip \
     --skip-calibs \
     --threads "${ncores}" \
     --stage {2} {6} \
     --bands g,r,i,z \
     --rgb-stretch 1.5 \
     --nsatur 2 \
     --survey-dir "$LEGACY_SURVEY_DIR" \
     --cache-dir "$CACHE_DIR" \
     --outdir "$outdir" \
     --checkpoint "${outdir}/checkpoints/${bri}/checkpoint-${brick}.pickle" \
     --checkpoint-period 120 \
     --pickle "${outdir}/pickles/${bri}/runbrick-%(brick)s-%%(stage)s.pickle" \
     --no-wise-ceres \
     --release 10200 \
     --cache-outliers \
     --max-memory-gb 20 \
     {5} $2 \
     >> "$log" 2>&1

#--zoom 1000 2000 1000 2000 \
# 8 threads -> 14 gb
#--run south \
#     --ps "${outdir}/metrics/${bri}/ps-${brick}-${SLURM_JOB_ID}.fits" \
#     --ps-t0

# Save the return value from the python command -- otherwise we
# exit 0 because the rm succeeds!
status=$?

# /Config directory nonsense
rm -R $TMPCACHE

exit $status


# QDO_BATCH_PROFILE=cori-shifter qdo launch -v tst 1 --cores_per_worker 8 --walltime=30:00 --batchqueue=debug --keep_env --batchopts "--image=docker:dstndstn/legacypipe:intel" --script "/src/legacypipe/bin/runbrick-shifter.sh"
