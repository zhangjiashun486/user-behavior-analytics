import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import duckdb

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator


def get_s3_folder(
    s3_bucket, s3_folder, local_folder="/opt/airflow/temp/s3folder/"
):
    # TODO: Move AWS credentials to env variables
    s3 = boto3.resource(
        service_name="s3",
        endpoint_url="http://minio:9000",
        aws_access_key_id="minio",
        aws_secret_access_key="minio123",
        region_name="us-east-1",
    )
    bucket = s3.Bucket(s3_bucket)
    stage_root = Path(local_folder).resolve()
    local_path = (stage_root / s3_folder).resolve()
    if local_path == stage_root or not local_path.is_relative_to(stage_root):
        raise ValueError("S3 staging path must stay inside its dedicated directory")
    # Delete the local folder if it exists
    if os.path.exists(local_path):
        shutil.rmtree(local_path)

    for obj in bucket.objects.filter(Prefix=s3_folder):
        if obj.key.endswith("/"):
            continue
        target = (local_path / os.path.relpath(obj.key, s3_folder)).resolve()
        if not target.is_relative_to(local_path):
            raise ValueError("S3 object path escapes its staging directory")
        target.parent.mkdir(parents=True, exist_ok=True)
        bucket.download_file(obj.key, str(target))
        print(f"Downloaded {obj.key} to {target}")


with DAG(
    "user_analytics_dag",
    description="A DAG to Pull user data and movie review data \
        to analyze their behaviour",
    schedule_interval=None,
    max_active_runs=1,
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    user_analytics_bucket = "user-analytics"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=user_analytics_bucket
    )
    movie_review_to_s3 = LocalFilesystemToS3Operator(
        task_id="movie_review_to_s3",
        filename="/opt/airflow/data/movie_review.csv",
        dest_key="raw/movie_review.csv",
        dest_bucket=user_analytics_bucket,
        replace=True,
    )

    user_purchase_to_s3 = SqlToS3Operator(
        task_id="user_purchase_to_s3",
        sql_conn_id="postgres_default",
        query=("select invoice_number, stock_code, detail, quantity, invoice_date, unit_price, "
               "CAST(customer_id AS VARCHAR) AS customer_id, country from retail.user_purchase"),
        s3_bucket=user_analytics_bucket,
        s3_key="raw/user_purchase/user_purchase.csv",
        replace=True,
    )

    movie_classifier = BashOperator(
        task_id="movie_classifier",
        bash_command='python /opt/airflow/dags/scripts/spark/random_text_classification.py --run-id "{{ run_id }}"',
    )

    get_movie_review_to_warehouse = PythonOperator(
        task_id="get_movie_review_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={
            "s3_bucket": "user-analytics",
            "s3_folder": "clean/movie_review",
        },
    )

    get_user_purchase_to_warehouse = PythonOperator(
        task_id="get_user_purchase_to_warehouse",
        python_callable=get_s3_folder,
        op_kwargs={
            "s3_bucket": "user-analytics",
            "s3_folder": "raw/user_purchase",
        },
    )

    def create_user_behaviour_metric():
        q = (Path(__file__).parent / "sql/customer_metrics.sql").read_text(encoding="utf-8")
        with duckdb.connect() as conn:
            conn.execute("CREATE VIEW purchases AS SELECT * FROM read_csv_auto("
                         "'/opt/airflow/temp/s3folder/raw/user_purchase/user_purchase.csv', "
                         "types={'unit_price': 'DECIMAL(18,4)', 'quantity': 'BIGINT', 'customer_id': 'VARCHAR'})")
            conn.execute("CREATE VIEW reviews AS SELECT * FROM read_parquet('/opt/airflow/temp/s3folder/clean/movie_review/*.parquet')")
            conn.sql(q).write_csv("/opt/airflow/data/behaviour_metrics.csv")

    get_user_behaviour_metric = PythonOperator(
        task_id="get_user_behaviour_metric",
        python_callable=create_user_behaviour_metric,
    )

    markdown_path = "/opt/airflow/dags/scripts/dashboard/"
    q_cmd = (
        f"cd {markdown_path} && quarto render {markdown_path}/dashboard.qmd"
    )
    gen_dashboard = BashOperator(
        task_id="generate_dashboard", bash_command=q_cmd
    )

    create_s3_bucket >> [user_purchase_to_s3, movie_review_to_s3]

    user_purchase_to_s3 >> get_user_purchase_to_warehouse

    movie_review_to_s3 >> movie_classifier >> get_movie_review_to_warehouse

    (
        [get_user_purchase_to_warehouse, get_movie_review_to_warehouse]
        >> get_user_behaviour_metric
        >> gen_dashboard
    )
