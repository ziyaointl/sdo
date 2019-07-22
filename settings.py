import os
MAX_RETRIES = 3 # Number of times to run qdo recover when pending tasks reaches 0
OUTPUT_DIR_PREFIX = "dr9"
LEGACY_SURVEY_DIR = "/global/cscratch1/sd/ziyaoz/farm-playground"
SDO_DIR = "/global/cscratch1/sd/ziyaoz/sdo"
IMAGE_TAG = "nersc-dr8.3.2"
SDO_SCRIPT_DIR = os.path.join(SDO_DIR, 'scripts')
QDO_LOGIN_PATH = '/global/cscratch1/sd/ziyaoz/farm/qdo_login.sh'

# Queue names
PREFARM_QNAME = 'ziyao-prefarm'
FARM_QNAME = 'ziyao-farm'
POSTFARM_QNAME = 'ziyao-postfarm'
POSTFARM_SCAVENGER_ONE_QNAME = 'ziyao-postfarm-scavenger-1'
POSTFARM_SCAVENGER_TWO_QNAME = 'ziyao-postfarm-scavenger-2'
