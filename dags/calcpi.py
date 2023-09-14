#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Example DAG demonstrating how to apply a Kubernetes Resource from Airflow running in-cluster"""
from datetime import datetime, timedelta
from airflow import DAG
from stackable.spark_operator import SparkKubernetesOperator
from stackable.spark_kubernetes_sensor import SparkKubernetesSensor

with DAG(
    dag_id='sparkapp_dag',
    schedule_interval='0 0 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    params={"example_key": "example_value"},
) as dag:

    t1 = SparkKubernetesOperator(
        task_id='spark_pi_submit',
        namespace="default",
        application_file="pyspark-pi.yaml",
        do_xcom_push=True,
        dag=dag,
    )

    t2 = SparkKubernetesSensor(
        task_id='spark_pi_monitor',
        namespace="default",
        application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
        poke_interval=5,
        dag=dag,
    )

    t1 >> t2
