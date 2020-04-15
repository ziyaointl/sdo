# Every time this settings file is changed
# Please run cleanup.sh and init.py

import os
MAX_RETRIES = 1 # Number of times to run qdo recover when pending tasks reaches 0
LEGACY_SURVEY_DIR = "/global/cscratch1/sd/ziyaoz/dr9f/south"
SDO_DIR = "/global/cscratch1/sd/ziyaoz/sdo-dr9f-south"
IMAGE_TAG = "dr9.3.4"
SDO_SCRIPT_DIR = os.path.join(SDO_DIR, 'scripts')
QDO_LOGIN_PATH = '/global/cscratch1/sd/ziyaoz/farm/qdo_login.sh'
TELESCOPE = 'south' # south or south
PUBLIC_REPORT_PATH = '/global/project/projectdirs/desi/www/users/ziyaoz/dr9f/south'
            # In addition to storing reports to reports/history and reports/current,
            # also copy them to this location
SUMMARY_FORMAT = 'simple' # Choice between classic and simple
HASWELL_MEM = 125000000
KNL_MEM = 93750000

# Queue names
PREFIX='ziyao-dr9f-south-'
PREFARM_QNAME = PREFIX + 'prefarm'
PREFARM_SCAVENGER_ONE_QNAME = PREFIX + 'prefarm-scavenger-1'
PREFARM_SCAVENGER_TWO_QNAME = PREFIX + 'prefarm-scavenger-2'
FARM_QNAME = PREFIX + 'farm'
POSTFARM_QNAME = PREFIX + 'postfarm'
POSTFARM_SCAVENGER_ONE_QNAME = PREFIX + 'postfarm-scavenger-1'
POSTFARM_SCAVENGER_TWO_QNAME = PREFIX + 'postfarm-scavenger-2'

# Allocations
KNL_ACCT = 'm3592'
HASWELL_ACCT = 'm3592'

# Burst Buffer
# How to use:
# 1. Request a persistent allocation
# 2. Setup symlinks in that allocation using an interative node
# 3. Point LEGACY_SURVEY_DIR to that directory (should be an environment variable + folder)
# 4. Flip the following BURST_BUFFER variable to True
# 5. Update burst file config file location if neccessary
BURST_BUFFER = False
BBF = '/global/homes/z/ziyaoz/bbf.conf'
