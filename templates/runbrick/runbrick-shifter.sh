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
# {7}: outdir

if [ ${RO_CFS_PATH}x == x ]; then
    echo The environment variable RO_CFS_PATH is not set.
    echo This script only works inside a Shifter container, on a compute node,
    echo   using the \"shifter --module=ro-cfs\" command-line argument.
    exit -1
fi

export SCR=/global/cscratch1/sd/dstn

#$RO_CFS_PATH = /ro-cfs/
export COSMO=$RO_CFS_PATH/cosmo

export LEGACY_SURVEY_DIR={0}
export outdir={7}

export GAIA_CAT_DIR=$COSMO/data/gaia/edr3/healpix
export GAIA_CAT_PREFIX=healpix
export GAIA_CAT_SCHEME=nested
export GAIA_CAT_VER=E

export DUST_DIR=$COSMO/data/dust/v0_1
export UNWISE_COADDS_DIR=$COSMO/data/unwise/neo7/unwise-coadds/fulldepth:$COSMO/data/unwise/allwise/unwise-coadds/fulldepth
export UNWISE_COADDS_TIMERESOLVED_DIR=$COSMO/work/wise/outputs/merge/neo7
export UNWISE_MODEL_SKY_DIR=$COSMO/data/unwise/neo7/unwise-catalog/mod

export TYCHO2_KD_DIR=$COSMO/staging/tycho2
export LARGEGALAXIES_CAT=$COSMO/staging/largegalaxies/v3.0/SGA-ellipse-v3.0.kd.fits
export SKY_TEMPLATE_DIR=$COSMO/work/legacysurvey/dr10/calib/sky_pattern

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
     --outdir "$outdir" \
     --checkpoint "${outdir}/checkpoints/${bri}/checkpoint-${brick}.pickle" \
     --checkpoint-period 120 \
     --pickle "${outdir}/pickles/${bri}/runbrick-%(brick)s-%%(stage)s.pickle" \
     --no-wise-ceres \
     --release 10000 \
     --cache-outliers \
     --max-memory-gb 20 \
     {5} $2 \
     >> "$log" 2>&1

# Save the return value from the python command -- otherwise we
# exit 0 because the rm succeeds!
status=$?

if [ "{2}" = "writecat" ]; then
    # tractor file exists and program exited normally
    if [ -f ${outdir}/tractor/${bri}/brick-${brick}.sha256sum ] && [ $status -eq 0 ]; then
        echo "$brick finished, removing checkpoints"
        python $LEGACYPIPE_DIR/legacypipe/rmckpt.py --brick $brick --outdir $outdir
    else
        echo "$brick did not finish, runbrick error code $status"
        #status=-1 # In case status -eq 0 but tractor file not found
    fi
fi

# /Config directory nonsense
rm -R $TMPCACHE

exit $status


# QDO_BATCH_PROFILE=cori-shifter qdo launch -v tst 1 --cores_per_worker 8 --walltime=30:00 --batchqueue=debug --keep_env --batchopts "--image=docker:dstndstn/legacypipe:intel" --script "/src/legacypipe/bin/runbrick-shifter.sh"
