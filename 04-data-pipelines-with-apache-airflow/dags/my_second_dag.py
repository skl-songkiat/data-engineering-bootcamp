from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

with DAG(
	dag_id="my_second_dag",
	start_date=timezone.datetime(2023, 8, 27),
	schedule="30 8 * * 2",
    tags=["DEB2"]
):

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")
    t3 = EmptyOperator(task_id="t3")

    t1 >> t2
    t1 >> t3