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


# we're not using the burst-buffer, but here's how one would use it, where "DR9" is the name of your BB:
#if [ "x$DW_PERSISTENT_STRIPED_DR9" == x ]; then

export SCR=/global/cscratch1/sd/dstn

# Using (depth-cut) v4 CCDs file, and v3 skies
export LEGACY_SURVEY_DIR={0}
outdir=$LEGACY_SURVEY_DIR/

export CACHE_DIR=$SCR/dr10-cache

#export GAIA_CAT_DIR=/global/cfs/cdirs/desi/target/gaia_edr3/healpix
export GAIA_CAT_DIR=$SCR/gaia-edr3-healpix/healpix
export GAIA_CAT_PREFIX=healpix
export GAIA_CAT_SCHEME=nested
export GAIA_CAT_VER=E

#export DUST_DIR=/global/cfs/cdirs/cosmo/data/dust/v0_1
export DUST_DIR=$CACHE_DIR/dust-v0_1
export UNWISE_COADDS_DIR=/global/cfs/cdirs/cosmo/data/unwise/neo7/unwise-coadds/fulldepth:/global/cfs/cdirs/cosmo/data/unwise/allwise/unwise-coadds/fulldepth
export UNWISE_COADDS_TIMERESOLVED_DIR=/global/cfs/cdirs/cosmo/work/wise/outputs/merge/neo7
export UNWISE_MODEL_SKY_DIR=/global/cfs/cdirs/cosmo/data/unwise/neo7/unwise-catalog/mod

#export TYCHO2_KD_DIR=/global/cfs/cdirs/cosmo/staging/tycho2
#export LARGEGALAXIES_CAT=/global/cfs/cdirs/cosmo/staging/largegalaxies/v3.0/SGA-ellipse-v3.0.kd.fits
export TYCHO2_KD_DIR=$CACHE_DIR/tycho2
export LARGEGALAXIES_CAT=$CACHE_DIR/SGA-ellipse-v3.0.kd.fits
export SKY_TEMPLATE_DIR=$CACHE_DIR/calib/sky_pattern
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
     --release 10300 \
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
