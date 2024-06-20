from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from scripts.user_profile_creation import create_user_profile
from scripts.zone_events_creation import create_zone_events
from scripts.friend_recommendation_creation import create_friend_recommendation

with DAG(
    "data_lake_update",
    start_date=datetime(2023, 11, 1),
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    # Операторы для создания витрин
    user_profile_task = PythonOperator(
        task_id="create_user_profile",
        python_callable=create_user_profile
    )

    zone_events_task = PythonOperator(
        task_id="create_zone_events",
        python_callable=create_zone_events
    )

    friend_recommendation_task = PythonOperator(
        task_id="create_friend_recommendation",
        python_callable=create_friend_recommendation
    )

    # Определение зависимостей
    [user_profile_task, zone_events_task] >> friend_recommendation_task
