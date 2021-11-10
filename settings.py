# Every time this settings file is changed
# Please run cleanup.sh and init.py

import os

### User defined fields
MAX_RETRIES = 1 # Number of times to run qdo recover when pending tasks reaches 0
LEGACY_SURVEY_DIR = "/global/cscratch1/sd/ziyaoz/dr9m2/south"
SDO_DIR = "/global/cscratch1/sd/ziyaoz/sdo-dr9m2-south"
IMAGE_TAG = "docker:legacysurvey/legacypipe:DR9.6.8" # "docker:legacysurvey/legacypipe:DR9.6.7"
QDO_LOGIN_PATH = '/global/cscratch1/sd/ziyaoz/farm/qdo_login.sh'
TELESCOPE = 'south'
PUBLIC_REPORT_PATH = '/global/cfs/cdirs/desi/users/ziyaoz/dr9m2/south'
            # In addition to storing reports to reports/history and reports/current,
            # also copy them to this location
EXTRA_PARAMS = '--release 9010' # Additional parameter to be passed to runbrick ONLY

### Generated fields
SDO_SCRIPT_DIR = os.path.join(SDO_DIR, 'scripts')
