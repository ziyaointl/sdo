# Every time this settings file is changed
# It's a *really* good idea to run cleanup.sh and init.py

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

# Queue names
PREFARM_QNAME = 'ziyao-prefarm'
FARM_QNAME = 'ziyao-farm'
POSTFARM_QNAME = 'ziyao-postfarm'
POSTFARM_SCAVENGER_ONE_QNAME = 'ziyao-postfarm-scavenger-1'
POSTFARM_SCAVENGER_TWO_QNAME = 'ziyao-postfarm-scavenger-2'

# Allocations
KNL_ACCT = 'desi'
HASWELL_ACCT = 'desi'
