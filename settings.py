# Every time this settings file is changed
# Please run cleanup.sh and init.py

import os

### User defined fields
MAX_RETRIES = 1 # Number of times to run qdo recover when pending tasks reaches 0
LEGACY_SURVEY_DIR = "$COSMO/work/legacysurvey/dr10-rocfs"
OUTDIR = "/global/cscratch1/sd/ziyaoz/dr10/south"
SDO_DIR = "/global/cscratch1/sd/ziyaoz/sdo-dr10"
IMAGE_TAG = "docker:legacysurvey/legacypipe:DR9.10.1"
TELESCOPE = 'south'
PUBLIC_REPORT_PATH = '/global/cfs/cdirs/desi/users/ziyaoz/dr10/south'
            # In addition to storing reports to reports/history and reports/current,
            # also copy them to this location
EXTRA_PARAMS = '' # Additional parameter to be passed to runbrick ONLY

### Generated fields
SDO_SCRIPT_DIR = os.path.join(SDO_DIR, 'scripts')
