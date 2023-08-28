from __future__ import annotations

import os
import datetime

from airflow.decorators import dag, task
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreateSparkJobOperator,
    DataprocDeleteClusterOperator,
)

from airflow.utils.trigger_rule import TriggerRule

AVAILABILITY_ZONE_ID = "ru-central1-b"
S3_BUCKET_NAME = "dproc-wh"
DAG_ID = "yc_dp_sample1"

@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessJob():
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        zone=AVAILABILITY_ZONE_ID,
        s3_bucket=S3_BUCKET_NAME,
        service_account_id='ajegsd3ausbcap0fvli6',
        cluster_image_version='2.1',
        computenode_count=3,
        datanode_count=0,
        services=("SPARK", "YARN"),
        labels={"mylabel1": 1, "mylabel2": 2},
    )

    create_spark_job = DataprocCreateSparkJobOperator(
        cluster_id=create_cluster.cluster_id,
        task_id="create_spark_job",
        main_jar_file_uri="file:///usr/lib/spark/examples/jars/spark-examples.jar",
        main_class="org.apache.spark.examples.SparkPi",
        args=["1000"],
    )

    delete_cluster = DataprocDeleteClusterOperator(
        cluster_id=create_cluster.cluster_id,
        task_id="delete_cluster",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_cluster >> create_spark_job >> delete_cluster

dag = ProcessJob()

