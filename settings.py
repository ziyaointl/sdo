# Every time this settings file is changed
# Please run cleanup.sh and init.py

import os
MAX_RETRIES = 1 # Number of times to run qdo recover when pending tasks reaches 0
LEGACY_SURVEY_DIR = "/global/cscratch1/sd/ziyaoz/sdo-test"
SDO_DIR = "/global/cscratch1/sd/ziyaoz/sdo"
IMAGE_TAG = "nersc-dr8.3.2"
SDO_SCRIPT_DIR = os.path.join(SDO_DIR, 'scripts')
QDO_LOGIN_PATH = '/global/cscratch1/sd/ziyaoz/farm/qdo_login.sh'
TELESCOPE = '90prime-mosaic' # or decam
PUBLIC_REPORT_PATH = '/global/project/projectdirs/desi/www/users/ziyaoz/sdo/'
            # In addition to storing reports to reports/history and reports/current,
            # also copy them to this location
SUMMARY_FORMAT = 'classic' # Choice between classic and simple
HASWELL_MEM = 125000000
KNL_MEM = 93750000

# Queue names
PREFIX='ziyao-'
PREFARM_QNAME = PREFIX + 'prefarm'
PREFARM_SCAVENGER_ONE_QNAME = PREFIX + 'prefarm-scavenger-1'
PREFARM_SCAVENGER_TWO_QNAME = PREFIX + 'prefarm-scavenger-2'
FARM_QNAME = PREFIX + 'farm'
POSTFARM_QNAME = PREFIX + 'postfarm'
POSTFARM_SCAVENGER_ONE_QNAME = PREFIX + 'postfarm-scavenger-1'
POSTFARM_SCAVENGER_TWO_QNAME = PREFIX + 'postfarm-scavenger-2'

# Allocations
KNL_ACCT = 'm3431'
HASWELL_ACCT = 'desi'
