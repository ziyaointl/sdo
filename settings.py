import os
MAX_RETRIES = 3 # Number of times to run qdo recover when pending tasks reaches 0
OUTPUT_DIR_PREFIX = "dr9"
LEGACY_SURVEY_DIR = "/global/cscratch1/sd/ziyaoz/farm-playground"
SDO_DIR = "/global/cscratch1/sd/ziyaoz/sdo"
IMAGE_TAG = "nersc-dr8.3.2"
SDO_SCRIPT_DIR = os.path.join(SDO_DIR, 'scripts')

# Queue names
PREFARM_QNAME = 'prefarm'
FARM_QNAME = 'farm'
POSTFARM_QNAME = 'postfarm'
POSTFARM_SCAVENGER_ONE_QNAME = 'postfarm-scavenger-1'
POSTFARM_SCAVENGER_TWO_QNAME = 'postfarm-scavenger-2'
