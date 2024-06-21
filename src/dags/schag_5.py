from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Скрипты для создания витрин
user_profile_script = "/path/to/user_profile_creation.py"
zone_events_script = "/path/to/zone_events_creation.py"
friend_recommendation_script = "/path/to/friend_recommendation_creation.py"

with DAG(
    "data_lake_update",
    start_date=datetime(2023, 11, 1),
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    # Операторы для создания витрин
    user_profile_task = BashOperator(
        task_id="create_user_profile",
        bash_command=f"spark-submit {user_profile_script}"
    )

    zone_events_task = BashOperator(
        task_id="create_zone_events",
        bash_command=f"spark-submit {zone_events_script}"
    )

    friend_recommendation_task = BashOperator(
        task_id="create_friend_recommendation",
        bash_command=f"spark-submit {friend_recommendation_script}"
    )

    # Определение зависимостей
    [user_profile_task, zone_events_task] >> friend_recommendation_task
