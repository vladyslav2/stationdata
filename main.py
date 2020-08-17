"""
This app has in the stationdata/ folder all the code needed to execute the
workflow. It is organized in this way so that it can be packaged as a Python
package and later installed in the VM workers executing the job. The root
directory for the example contains just a "driver" script to launch the job
and the setup.py file needed to create a package.
The advantages for organizing the code is that large projects will naturally
evolve beyond just one module and you will have to make sure the additional
modules are present in the worker.

In Python Dataflow, using the --setup_file option when submitting a job, will
trigger creating a source distribution (as if running python setup.py sdist) and
then staging the resulting tarball in the staging area. The workers, upon
startup, will install the tarball.
Below is a complete command line for running the stationdata workflow remotely as
an example:
python stationdata_main.py \
  --job_name juliaset-$USER \
  --project YOUR-PROJECT \
  --region GCE-REGION \
  --runner DataflowRunner \
  --setup_file ./setup.py \
  --staging_location gs://YOUR-BUCKET/juliaset/staging \
  --temp_location gs://YOUR-BUCKET/juliaset/temp \
  --coordinate_output gs://YOUR-BUCKET/juliaset/out \
  --grid_size 20

python stationdata_main.py \
  --runner=dataflow \
  --temp_location=gs://${GCLOUD_PROJECT_ID}-dataflow-temp-${PRODUCTION_TYPE}/ \
  --staging_location=gs://${GCLOUD_PROJECT_ID}-dataflow-staging-${PRODUCTION_TYPE}/ \
  --input_topic=${IOT_TOPIC_TELEMETRY_ID} \
  --project=${GCLOUD_PROJECT_ID} \
  --bq_data_set=${BIGQUERY_DATASET} \
  --bq_raw_data=${BIGQUERY_RAWDATA_TABLE} \
  --bq_heart_rate=${BIGQUERY_HEARTRATE_TABLE} \
  --worker_machine_type=n1-standard-1 \
  --region=us-central1 \
  --setup_file=./setup.py

"""

# pytype: skip-file

from __future__ import absolute_import

import sys
import logging
import os
import time

sys.path.append('/dataflow/template/stationdata')
# sys.path.append('/dataflow')
logging.error('about to start')

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    
    print('started')
    logging.error(os.system('which python'))
    logging.error(os.system('pip freeze'))
    logging.error(os.system('ls -la /dataflow/template'))
    logging.error(os.system('ls -la /dataflow'))
    logging.error(os.system('ls -la /usr/local/lib/python3*/site-packages/'))
    logging.error(','.join(sys.path))
    print('waiting')
    time.sleep(200)
    try:
        from stationdata import run
        run.run(sys.argv)
    except:
        import run
        run.run(sys.argv)
