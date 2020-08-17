#!/bin/bash
set -e

CMD=$1

case ${CMD} in
cloud-config)
  echo "Installing development infostructure"
  gsutil mb -p ${PROJECT_ID} -l us gs://${PROJECT_ID}-dataflows
  echo "Creating bigQuery dataset"
  bq --location=us-west2 mk \
    --dataset \
    --description "IoT data storage" \
    ${PROJECT_ID}:${BIGQUERY_DATASET}
  echo "Creating pus/sub topics"
  gcloud beta pubsub topics create projects/${PROJECT_ID}/topics/${IOT_TOPIC_TELEMETRY}
  gcloud beta pubsub topics create projects/${PROJECT_ID}/topics/${IOT_TOPIC_STATE}
  gcloud beta pubsub topics create projects/${PROJECT_ID}/topics/${IOT_TOPIC_DEAD_LETTER}
  gcloud beta pubsub subscriptions create \
    --topic="projects/${PROJECT_ID}/topics/${IOT_TOPIC_DEAD_LETTER}"
    ${IOT_TOPIC_DEAD_LETTER}
  gcloud beta pubsub subscriptions create \
    --topic="projects/${PROJECT_ID}/topics/${IOT_TOPIC_TELEMETRY}"
    ${IOT_TOPIC_TELEMETRY}-backup
  gcloud beta pubsub topics create projects/${PROJECT_ID}/topics/${CLOUD_TOPIC_WORKOUT}
  echo "Creating IoT registry"
  gcloud beta iot registries create ${IOT_REGISTRY} \
      --project=${PROJECT_ID} \
      --region=${REGION} \
      --log-level=${IOT_LOG_LEVEL} \
      --event-notification-config=topic=projects/${PROJECT_ID}/topics/${IOT_TOPIC_TELEMETRY} \
      --state-pubsub-topic=projects/${PROJECT_ID}/topics/${IOT_TOPIC_STATE}
  ;;

pubsub-local)
  gcloud beta emulators pubsub start --project=${PROJECT_ID} --host-port=${PUBSUB_EMULATOR_HOST}
  ;;

pubsub-create)
  python google/publisher.py ${PROJECT_ID} create ${IOT_TOPIC_TELEMETRY}
  python google/subscriber.py ${PROJECT_ID} create ${IOT_TOPIC_TELEMETRY} ${IOT_TOPIC_TELEMETRY}-backup
  python google/publisher.py ${PROJECT_ID} create ${CLOUD_TOPIC_WORKOUT}
  python google/publisher.py ${PROJECT_ID} create ${IOT_TOPIC_DEAD_LETTER}
  python google/subscriber.py ${PROJECT_ID} create ${IOT_TOPIC_DEAD_LETTER} ${IOT_TOPIC_DEAD_LETTER}
  python google/subscriber.py ${PROJECT_ID} create ${CLOUD_TOPIC_WORKOUT} ${CLOUD_TOPIC_WORKOUT}-history
  ;;

pubsub-listen)
  python google/subscriber.py ${PROJECT_ID} receive ${CLOUD_TOPIC_WORKOUT}-history
  ;;

pubsub-backup)
  python google/subscriber.py ${PROJECT_ID} receive ${IOT_TOPIC_TELEMETRY}-backup
  ;;

create-template)
  export GIT_HASH=`git rev-parse --short HEAD`
  export TEMPLATE_IMAGE_LATEST="gcr.io/$PROJECT_ID/dataflow:latest"
  export TEMPLATE_IMAGE_HASH="gcr.io/$PROJECT_ID/dataflow:$GIT_HASH"
  docker build -t $TEMPLATE_IMAGE_LATEST -t $TEMPLATE_IMAGE_HASH .
  docker push $TEMPLATE_IMAGE_LATEST
  docker push $TEMPLATE_IMAGE_HASH
  
  export TEMPLATE_FOLDER_LATEST="gs://formelife-binaries-master/dataflow/template-latest.json"
  gcloud beta dataflow flex-template build $TEMPLATE_FOLDER_LATEST \
    --image "$TEMPLATE_IMAGE_LATEST" \
    --sdk-language "PYTHON" \
    --metadata-file "metadata.json"

  export TEMPLATE_FOLDER_HASH="gs://formelife-binaries-master/dataflow/template-$GIT_HASH.json"
  gcloud beta dataflow flex-template build $TEMPLATE_FOLDER_HASH \
    --image "$TEMPLATE_IMAGE_HASH" \
    --sdk-language "PYTHON" \
    --metadata-file "metadata.json"

  # gcloud beta dataflow flex-template run "dataflow-flex-123" \
  #  --template-file-gcs-location "$TEMPLATE_FOLDER_HASH" \
  #  --parameters "iot_topic_telemetry=${IOT_TOPIC_TELEMETRY},iot_topic_dead_letter=${IOT_TOPIC_DEAD_LETTER},cloud_topic_workout=${CLOUD_TOPIC_WORKOUT},project=${PROJECT_ID},bq_data_set=${BIGQUERY_DATASET},bq_firmware=${BIGQUERY_FIRMWARE_TABLE},bq_heart_rate=${BIGQUERY_HEARTRATE_TABLE},region=${REGION}"
  ;;

install)
  echo "Creating virtual envoiroment into venv folder"
  virtualenv --python=python3 env
  source env/bin/activate
  echo "Installing requirements"
  pip install -r requirements.txt
  pip install -r requirements-dev.txt
  echo "Copy pre commit hook"
  cd etc/
  python update_hook.py
  ;;

lint)
  pylint stationdata
  ;;

unit)
  python -m pytest tests/unit_test.py
  ;;

test)
  python -m pytest tests/unit_test.py &&
  python tests/integration_test.py
  ;;

help)
  @echo "Run cloud-config | install | lint | help"
  ;;
esac
