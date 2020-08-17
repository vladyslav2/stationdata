# dataflow stream

### Requirements
- python 3.7 or higher (`brew install python3`)
- gcloud 209.0.0 or higher (`brew install google-cloud-sdk`)

### Quick install
1. Navigate to dataflow directly, and execute:
2. `./make.sh cloud-config` to create required infrostructure on the cloud
3. `./make.sh install` to install python requements

## Run unit tests
Unit tests does not require any additional steps

1. `./make.sh install` - make sure to install all requirements
2. `./make.sh unit` - run unit tests

## Deploying dataflow

1. source the correct env file from ../../env-files/. Example `source ../env-files/iot-stating.env`
2. Start and deploy dataflow `./start.sh`
3. You can close terminal after dataflow successfully started on the cloud


## General steps for local integration and manual tests

1. source test envs `source ../../env-files/iot-local.env`
2. Install [pubsub emulator](https://cloud.google.com/pubsub/docs/emulator)
3. Set up envs `$(gcloud beta emulators pubsub env-init)`
4. Run local pubsub `gcloud beta emulators pubsub start --project=${GCLOUD_PROJECT_ID}`
5. Create virtual python environment `virtualenv --python=python3 venv`
6. Activate virtual python environment `source venv/bin/activate`
7. Install python dependencies `pip install -r requirements.txt`
8. Create telemetry topic `python google/publisher.py ${GCLOUD_PROJECT_ID} create ${IOT_TOPIC_TELEMETRY_ID}`
8. Create historical topic `python google/publisher.py ${GCLOUD_PROJECT_ID} create ${WORKOUT_TOPIC_ID}`
9. Create subscription `python google/subscriber.py ${GCLOUD_PROJECT_ID} create ${WORKOUT_TOPIC_ID} history`
 
## Manual tests
Start dataflow locally, listen to pubsub and send some data

1. Start local pubsub `./make.sh pubsub-local`
2. Create topics and subscription `./make.sh pubsub-create`
3. Start dataflow `./start.sh`
4. Listen to the subscription `./make.sh pubsub-listen`
5. Send data `python tests/sendData.py`

## Integration tests
Integration tests will send data and check if bigquery, pubsub received it.  \
Integration tests can be executed agains local or cloud pus/subs but they have to exist

11. `python tests/integration_tests.py`


## Cloud infrostructure requements
Every step is executed by `./make.sh cloud-config` command

### Google storage
create storage bucket for dataflow to run
  `gsutil mb -p ${GCLOUD_PROJECT_ID} -l us gs://${GCLOUD_PROJECT_ID}-dataflows`

### BigQuery data sets
Create biquery dataset
```
bq --location=us-west2 mk \
--dataset \
--description "IoT data storage" \
${GCLOUD_PROJECT_ID}:${BIGQUERY_DATASET}
```
 - all tables dataflow will create automatically as needed
 - More command line options can be found [here](https://cloud.google.com/bigquery/docs/datasets)

### Pub/Sub topics
- `gcloud beta pubsub topics create projects/${GCLOUD_PROJECT_ID}/topics/${IOT_TOPIC_TELEMETRY_ID}`
- `gcloud beta pubsub topics create projects/${GCLOUD_PROJECT_ID}/topics/${IOT_TOPIC_STATE_ID}`
- `gcloud beta pubsub topics create projects/${GCLOUD_PROJECT_ID}/topics/${WORKOUT_TOPIC_ID}`

### IoT registry
```
  gcloud beta iot registries create ${IOT_REGISTRY_ID} \
        --project=${GCLOUD_PROJECT_ID} \
        --region=${GCLOUD_REGION} \
        --log-level=${IOT_LOG_LEVEL} \
        --event-notification-config=topic=projects/${GCLOUD_PROJECT_ID}/topics/${IOT_TOPIC_TELEMETRY_ID} \
        --state-pubsub-topic=projects/${GCLOUD_PROJECT_ID}/topics/${IOT_TOPIC_STATE_ID}
```

## Misc

Retrieve pubsub data: \
You need to use an already created pubsub subscription to use this command.
`gcloud alpha pubsub subscriptions pull history --limit=200 --auto-ack`
where history is a name of subscription we created at step 8

cancell running dataflows
`gcloud beta dataflow jobs list --region=${REGION} | grep Running | awk '{print $1}' | xargs gcloud beta dataflow jobs cancel --region=${REGION}`
