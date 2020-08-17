#!/bin/bash

python ./main.py \
  --runner=${RUNNER} \
  --temp_location=gs://${PROJECT_ID}-dataflows/temp/${PRODUCTION_TYPE}/ \
  --staging_location=gs://${PROJECT_ID}-dataflows/staging/${PRODUCTION_TYPE}/ \
  --iot_topic_telemetry=${IOT_TOPIC_TELEMETRY} \
  --iot_topic_dead_letter=${IOT_TOPIC_DEAD_LETTER} \
  --cloud_topic_workout=${CLOUD_TOPIC_WORKOUT} \
  --project=${PROJECT_ID} \
  --bq_data_set=${BIGQUERY_DATASET} \
  --bq_firmware=${BIGQUERY_FIRMWARE_TABLE} \
  --bq_heart_rate=${BIGQUERY_HEARTRATE_TABLE} \
  --worker_machine_type=n1-standard-1 \
  --autoscaling_algorithm=THROUGHPUT_BASED \
  --num_workers=1 \
  --max_num_workers=8 \
  --disk_size_gb=10 \
  --job_name=${JOB_NAME} \
  --region=${REGION} \
  --usePublicIps=false \
  --setup_file=./setup.py \
  $1 # Be wary when updating Dataflow streaming pipelines. You can do it, but Dataflow performs a capability check first. If the change is too complex, it will be rejected and the update will not happen.
  # --update

# toDo
# dataflow monitoring https://cloud.google.com/dataflow/docs/guides/using-cloud-monitoring

