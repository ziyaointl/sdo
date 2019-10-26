#! /bin/bash

# Script for running the legacypipe code within a Shifter container at NERSC
# with burst buffer!
# This merges some contents from legacypipe-env and runbrick.sh
# Arguments:
# {0}: LEGACY_SURVEY_DIR
# {1}: ncores
# {2}: stage and write-stage
# {3}: run argument / telescope
# {4}: maxmem, in KB (93750000 total for knl, 125000000 total for haswell)

# export LEGACY_SURVEY_DIR=/global/cscratch1/sd/ziyaoz/farm-playground
export LEGACY_SURVEY_DIR={0}

# Burst-buffer!
if [ x$DW_PERSISTENT_STRIPED_DR8 == x ]; then
  BB=${LEGACY_SURVEY_DIR}/
else
  BB=$DW_PERSISTENT_STRIPED_DR8
fi

UNWISE_PSF_DIR=/src/unwise_psf
BLOB_MASK_DIR=/global/project/projectdirs/cosmo/work/legacysurvey/dr8/south
export DUST_DIR=/global/project/projectdirs/cosmo/data/dust/v0_1
export UNWISE_COADDS_DIR=/global/project/projectdirs/cosmo/work/wise/outputs/merge/neo5/fulldepth:/global/project/projectdirs/cosmo/data/unwise/allwise/unwise-coadds/fulldepth
export UNWISE_COADDS_TIMERESOLVED_DIR=/global/project/projectdirs/cosmo/work/wise/outputs/merge/neo5
export GAIA_CAT_DIR=/global/project/projectdirs/cosmo/work/gaia/chunks-gaia-dr2-astrom-2
export GAIA_CAT_VER=2
export TYCHO2_KD_DIR=/global/project/projectdirs/cosmo/staging/tycho2
export LARGEGALAXIES_DIR=/global/project/projectdirs/cosmo/staging/largegalaxies/v2.0
export LARGEGALAXIES_CAT=/global/project/projectdirs/cosmo/staging/largegalaxies/v3.0/LSLGA-v3.0.kd.fits
export PS1CAT_DIR=/global/project/projectdirs/cosmo/work/ps1/cats/chunks-qz-star-v3

export PYTHONPATH=/usr/local/lib/python:/usr/local/lib/python3.6/dist-packages:.:${UNWISE_PSF_DIR}/py

# Don't add ~/.local/ to Python's sys.path
export PYTHONNOUSERSITE=1

# Force MKL single-threaded
# https://software.intel.com/en-us/articles/using-threaded-intel-mkl-in-multi-thread-application
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=1

# To avoid problems with MPI and Python multiprocessing
export MPICH_GNI_FORK_MODE=FULLCOPY
export KMP_AFFINITY=disabled

# Try limiting memory to avoid killing the whole MPI job...
# 16 is the default for both Edison and Cori: it corresponds
# to 3 and 4 bricks per node respectively.
ncores={1}
ulimit -Sv {4}

# Reduce the number of cores so that a task doesn't use too much memory.
# Using more threads than the number of physical cores usually causes the
# job to run out of memory.
#ncores=8

cd /src/legacypipe/py

outdir=${BB}

brick="$1"

bri=$(echo $brick | head -c 3)
mkdir -p $outdir/logs/$bri
log="$outdir/logs/$bri/$brick.log"

mkdir -p $outdir/metrics/$bri

echo Logging to: $log
echo Running on $(hostname)

echo -e "\n\n\n" >> $log
echo "-----------------------------------------------------------------------------------------" >> $log
echo "PWD: $(pwd)" >> $log
echo >> $log
ulimit -a >> $log
echo >> $log
#tmplog="/tmp/$brick.log"

echo -e "\nStarting on $(hostname)\n" >> $log
echo "-----------------------------------------------------------------------------------------" >> $log

python -O legacypipe/runbrick.py \
     --brick $brick \
     --skip \
     --threads ${ncores} \
     --checkpoint ${outdir}/checkpoints/checkpoint-${brick}.pickle \
     --pickle "${outdir}/pickles/${bri}/runbrick-%(brick)s-%%(stage)s.pickle" \
     --unwise-coadds \
     --outdir $outdir \
     --ps "${outdir}/metrics/${bri}/ps-${brick}-${SLURM_JOB_ID}.fits" \
     --ps-t0 $(date "+%s") \
     --write-stage {2} \
     --stage {2} \
     --blob-mask-dir ${BLOB_MASK_DIR} \
     --run {3} \
    >> $log 2>&1

#     --cache-dir $cachedir \
#     --bail-out \
#     --write-stage srcs \
#     --zoom 100 300 100 300 \

status=$?
#cat $tmplog >> $log

#python legacypipe/rmckpt.py --brick $brick --outdir $outdir

exit $status


# QDO_BATCH_PROFILE=cori-shifter qdo launch -v dr8-north1 60 --cores_per_worker 8 --walltime=30:00 --batchqueue=debug --keep_env --batchopts "--image=docker:legacysurvey/legacypipe:nersc-dr8.1.2" --script "/scratch1/scratchdirs/desiproc/dr8/runbrick-shifter.sh"
