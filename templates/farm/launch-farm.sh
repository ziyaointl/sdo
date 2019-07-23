#!/bin/bash

# Parameters
# {0} LEGACY_SURVEY_DIR / outdir
# {1} QNAME
# {2} SDO_SCRIPT_DIR

###Dependencies
source {2}/mpi_bugfix.sh
source {2}/qdo_login.sh
###

###OPTIONS
# FARM_SCRIPT='/src/legacypipe/py/legacypipe/farm.py'
# This is a temporary solution. Package farm.py with container!
FARM_SCRIPT='/global/cscratch1/sd/ziyaoz/sdo/farm.py'
FARM_OUTDIR='{0}/checkpoints/checkpoint-%(brick)s.pickle'
FARM_INDIR='{0}/pickles/%(brick).3s/runbrick-%(brick)s-srcs.pickle'
QNAME='{1}'
###

###Run farm
cd /src/legacypipe/py
python -u ${FARM_SCRIPT} --pickle ${FARM_INDIR} --big drop --inthreads 4 --checkpoint $FARM_OUTDIR $QNAME
###

###Remove Queue
#qdo delete $QNAME --force
###
