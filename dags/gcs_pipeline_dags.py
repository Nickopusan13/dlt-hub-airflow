from datetime import timedelta, datetime, timezone
from airflow.decorators import dag
from dlt.helpers.airflow_helper import PipelineTasksGroup
from dlt.sources.filesystem import filesystem, read_parquet
from dlt.extract import Incremental
import dlt
import pendulum
from tenacity import Retrying, stop_after_attempt

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

bucket_url = "gs://test-dlt-load/test"
INITIAL_DATE = datetime(2025, 1, 1, tzinfo=timezone.utc)

def create_resource(name, file_glob, write_disposition="append", primary_key=None):
    @dlt.resource(name=name, write_disposition=write_disposition, primary_key=primary_key)
    def resource_data():
        incr = Incremental(cursor_path="modification_date", initial_value=INITIAL_DATE)
        yield from filesystem(
            bucket_url=bucket_url,
            file_glob=file_glob,
            incremental=incr,
        ) | read_parquet()
    return resource_data

# Define resources
resources = [
    create_resource("events", "*events_v5_*.parquet"),
    create_resource("campaigns", "*campaigns_v5_*.parquet"),
    create_resource("campaign_actions", "*campaign_actions_v5_*.parquet"),
    create_resource("broadcasts", "*broadcasts_v5_*.parquet"),
    create_resource("attributes", "*attributes_v5_*.parquet", write_disposition="merge", primary_key="internal_customer_id"),
    create_resource("deliveries", "*deliveries_v5*.parquet", write_disposition="merge", primary_key="internal_customer_id"),
    create_resource("metrics", "*metrics_v5*.parquet", write_disposition="merge", primary_key="internal_customer_id"),
    create_resource("subjects", "*subjects_v5_*.parquet", write_disposition="merge", primary_key="internal_customer_id"),
    create_resource("people", "*people_v5_*.parquet", write_disposition="merge", primary_key="internal_customer_id"),
]

# Single DLT source yielding all resources
@dlt.source(section="filesystem", name="customer_io_resources")
def gcs_source():
    return resources

# DAG definition
@dag(
    dag_id='gcs_pipeline_dags_v11',
    default_args=default_args,
    description='Download files from GCS and load to BigQuery',
    schedule_interval='0 2 * * *',
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=['gcs', 'dlt', 'bigquery'],
)
def load_gcs_to_bigquery():

    # Task group for all resources
    tasks = PipelineTasksGroup(
        "customer_io_resources_to_bigquery_tasks",
        use_data_folder=False,
        wipe_local_data=False,
        use_task_logger=True,
        retry_policy=Retrying(stop=stop_after_attempt(3), reraise=True)
    )
    def create_source(resource_func):
        @dlt.source(section="filesystem", name=f"{resource_func.__name__}_source")
        def source_func():
            yield resource_func
        return source_func

    # List of sources
    sources = [create_source(r) for r in resources]

    # **Each resource has its own DLT pipeline instance**
    for source in sources:
        pipeline = dlt.pipeline(
            pipeline_name=f"{source.__name__}_pipeline",
            destination="bigquery",
            dataset_name="customer_io_data",
            progress="log"
        )

        tasks.add_run(
            pipeline=pipeline,
            data=source(),
            decompose="serialize",
            trigger_rule="all_done",
            retries=2,
            provide_context=True,
        )

load_gcs_to_bigquery()
