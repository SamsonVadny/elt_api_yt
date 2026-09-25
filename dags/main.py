import sys
sys.path.append("/opt/airflow/dags")

from airflow import DAG
import pendulum
from datetime import datetime, timedelta

from API.video_stats import (
    get_playlist_id,
    get_video_ids,
    extract_video_data,
    save_to_json
)

local_tz = pendulum.timezone("Africa/Maputo")

default_args = {
    "owner": "Samson Mambo",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "samsonvadny28@gmail.com",
}

with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to Produce Json file with raw data",
    schedule="0 14 * * *",
    start_date=datetime(2025, 1, 1, tzinfo=local_tz),
    end_date=datetime(2030, 12, 31, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

    playlist_id >> video_ids >> extract_data >> save_to_json_task