import dlt
import pendulum
from dlt.sources.rest_api import rest_api_resources
from datetime import timedelta, datetime, timezone
from airflow.decorators import dag
from dlt.helpers.airflow_helper import PipelineTasksGroup
from tenacity import Retrying, stop_after_attempt
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import os

load_dotenv()

SERVICE_ACCOUNT = os.getenv("SERVICE_ACCOUNT")
SCOPES = os.getenv("SCOPES").split(",")
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT,
    scopes=SCOPES
)
credentials.refresh(Request())
ACCESS_TOKEN = credentials.token
print("Access Token:", ACCESS_TOKEN)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# CHANGE THIS URL "https://firestore.googleapis.com/v1/projects/{your-project}/databases/(default)/documents"
firestore_url = os.getenv("FIRESTORE_URL")
INITIAL_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z") # IF updateTime = ISO
INITIAL_DATE_EPOCH = str(int(datetime(2024, 1, 1, tzinfo=timezone.utc).timestamp())) # IF updateTime = EPOCH

def convert_epoch_fields(record):
    if "fields" in record and "updateTime" in record["fields"]:
        epoch_value = record["fields"]["updateTime"].get("integerValue")
        if epoch_value:
            record["fields"]["updateTime_date"] = {
                "stringValue": pendulum.from_timestamp(int(epoch_value)).to_iso8601_string()
            }
    return record

def create_firestore_resource(name, write_disposition="merge", page_size=100): # Page Limit
    return rest_api_resources(
        {
            "client": {
                "base_url": firestore_url,
                "auth":{
                    "type": "bearer",
                    "token": ACCESS_TOKEN
                }
            },
            "resource_defaults": {
                "write_disposition": write_disposition, 
                "primary_key": "name"
                },
            "resources": [
                {
                    "name": name,
                    "write_disposition": write_disposition,
                    "endpoint": {
                        "method": "POST",
                        "path": ":runQuery",
                        "data_selector": "$[*].document",
                        "json": {
                            "structuredQuery": {
                                "from": [{"collectionId": name}],
                                "where": {
                                    "fieldFilter": {
                                        "field": {"fieldPath": "updateTime"},
                                        "op": "GREATER_THAN",
                                        "value": {
                                            "integerValue": "{incremental.start_value}" # integerValue if EPOCH
                                        }
                                    }
                                },
                                "limit":page_size # Page Limit
                            },
                        },
                        "incremental": {
                            "initial_value": INITIAL_DATE_EPOCH,
                            "cursor_path": "fields.updateTime.integerValue", # integerValue
                            # "convert": lambda epoch: pendulum.from_timestamp(int(epoch)).to_date_string() ## I TRY AND NOT WORK FOR FIRESTORE
                        }
                    },
                    "processing_steps": [
                        {"map": convert_epoch_fields} # add iso time in bigquery (optionall)
                    ]
                }
            ],
        }
    )

resources = [
    create_firestore_resource("convert"), # create_firestore_resource("******")
]


@dag(
    dag_id="firestore_pipeline_dags2",
    default_args=default_args,
    description="Load Firestore collections to BigQuery",
    schedule_interval="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["firestore", "bigquery", "dlt"],
)
def load_firestore_to_bigquery():
    tasks = PipelineTasksGroup(
        "firestore_collections_to_bigquery",
        use_data_folder=False,
        wipe_local_data=False,
        use_task_logger=True,
        retry_policy=Retrying(stop=stop_after_attempt(3), reraise=True),
    )
    for resource_list in resources:
        resource = resource_list[0]
        resource_name = resource.name
        @dlt.source(name="fire_store_source", section="firestore")
        def firestore_source():
            yield resource 
        pipeline = dlt.pipeline(
            pipeline_name=f"firestore_{resource_name}_pipeline",
            destination="bigquery",
            dataset_name="fire_store_data",
            progress="log",
        )

        tasks.add_run(
            pipeline=pipeline,
            data=firestore_source(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=2,
            provide_context=True,
        )

load_firestore_to_bigquery()