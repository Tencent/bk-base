# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.

Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.

BK-BASE 蓝鲸基础平台 is licensed under the MIT License.

License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import datetime
import json

from dataflow import pizza_settings
from dataflow.batch import settings
from dataflow.batch.custom_jobs.custom_jobs_create_args_obj import CustomJobsCreateArgsObj
from dataflow.batch.exceptions.comp_execptions import JobNaviAlreadyExitsScheduleError, OneTimeJobDataNotFoundException
from dataflow.batch.handlers.batch_one_time_execute_job import BatchOneTimeExecuteJobHandler
from dataflow.batch.utils import bksql_util, result_table_util
from dataflow.shared.datalab.datalab_helper import DataLabHelper
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.udf.functions import function_driver


def create_one_time_job(args):
    batch_logger.info("custom_jobs arg: {}".format(args))
    custom_jobs_create_args_obj = CustomJobsCreateArgsObj(args)
    jobnavi = __get_one_time_jobnavi(
        custom_jobs_create_args_obj.geog_area_code
    )  # get jobnavi first in case get jobnavi failed
    udfs = __handle_udfs(
        custom_jobs_create_args_obj.processing_logic,
        custom_jobs_create_args_obj.geog_area_code,
    )
    bksql_check_response = __sql_check(custom_jobs_create_args_obj, udfs)

    if str(custom_jobs_create_args_obj.processing_type).lower() == "queryset":
        input_list = []
        for input_item in custom_jobs_create_args_obj.inputs:
            input_list.append(input_item["result_table_id"])

        DataLabHelper.create_query_set(
            custom_jobs_create_args_obj.queryset_params["notebook_id"],
            custom_jobs_create_args_obj.queryset_params["cell_id"],
            custom_jobs_create_args_obj.outputs[0]["result_table_id"],
            bksql_check_response["fields"],
            input_list,
            custom_jobs_create_args_obj.username,
        )
    else:
        __make_data_processing(custom_jobs_create_args_obj, bksql_check_response["fields"])

        try:
            __create_storekit(custom_jobs_create_args_obj)
        except Exception as e:
            batch_logger.exception(e)
            DataProcessingHelper.delete_data_processing(custom_jobs_create_args_obj.job_id, with_data=True)
            raise e

    if jobnavi.get_schedule_info(custom_jobs_create_args_obj.job_id) is not None:
        raise JobNaviAlreadyExitsScheduleError(
            "Schedule info {} already exists".format(custom_jobs_create_args_obj.job_id)
        )

    try:
        exec_id = __register_one_time_job_jobnavi(
            jobnavi,
            custom_jobs_create_args_obj.job_id,
            custom_jobs_create_args_obj.node_label,
            custom_jobs_create_args_obj.cluster_name,
            custom_jobs_create_args_obj.cluster_group,
        )

        __save_to_one_time_db(custom_jobs_create_args_obj, udfs, exec_id, bksql_check_response)
    except Exception as e:
        batch_logger.exception(e)
        jobnavi.delete_schedule(custom_jobs_create_args_obj.job_id)
        if str(custom_jobs_create_args_obj.processing_type).lower() != "queryset":
            StorekitHelper.delete_physical_table(custom_jobs_create_args_obj.outputs[0]["result_table_id"], "hdfs")
            from common.local import _local

            _local.bk_username = custom_jobs_create_args_obj.username
            DataProcessingHelper.delete_data_processing(custom_jobs_create_args_obj.job_id, with_data=True)
        else:
            DataLabHelper.delete_query_set(
                custom_jobs_create_args_obj.outputs[0]["result_table_id"],
                custom_jobs_create_args_obj.queryset_params["notebook_id"],
                custom_jobs_create_args_obj.queryset_params["cell_id"],
                custom_jobs_create_args_obj.username,
            )
        raise e

    result = {"job_id": custom_jobs_create_args_obj.job_id, "execute_id": exec_id}
    return result


def delete_one_time_job(job_id, queryset_delete=False):
    if BatchOneTimeExecuteJobHandler.is_one_time_job_exists_by_job_id(job_id):
        processing_type = DataProcessingHelper.get_data_processing_type(job_id)
        one_time_job = BatchOneTimeExecuteJobHandler.get_one_time_job_by_job_id(job_id)
        output_item = json.loads(one_time_job.job_config)["outputs"][0]
        result_table_id = output_item["result_table_id"]

        if str(processing_type).lower() != "queryset":
            storage_response = ResultTableHelper.get_result_table_storage(result_table_id, "hdfs")
            if len(storage_response) != 0:
                StorekitHelper.clear_hdfs_data(result_table_id)
                StorekitHelper.delete_physical_table(result_table_id, "hdfs")

            data_processing = DataProcessingHelper.get_data_processing(job_id)

            if len(data_processing) != 0:
                from common.local import _local

                _local.bk_username = one_time_job.created_by
                DataProcessingHelper.delete_data_processing(job_id, with_data=True)

        else:
            if queryset_delete:
                job_config = json.loads(one_time_job.job_config)
                DataLabHelper.delete_query_set(
                    job_id,
                    job_config["queryset_params"]["notebook_id"],
                    job_config["queryset_params"]["cell_id"],
                    one_time_job.created_by,
                )

        jobserver_config = json.loads(one_time_job.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        jobnavi = __get_one_time_jobnavi(geog_area_code)

        if one_time_job.execute_id is not None:
            kill_job(one_time_job.execute_id, jobnavi)

        jobnavi.delete_schedule(job_id)

        BatchOneTimeExecuteJobHandler.delete_one_time_job(job_id)
    else:
        raise OneTimeJobDataNotFoundException("Can't find job {}".format(job_id))


def kill_job(execute_id, jobnavi):
    execute_result = jobnavi.get_execute_status(execute_id)
    status = execute_result["status"].lower()
    if status == "preparing" or status == "running":
        jobnavi.kill_execute(execute_id)


def retrieve_one_time_job_status(job_id):
    one_time_job = BatchOneTimeExecuteJobHandler.get_one_time_job_by_job_id(job_id)
    jobserver_config = json.loads(one_time_job.jobserver_config)
    execute_id = one_time_job.execute_id
    geog_area_code = jobserver_config["geog_area_code"]

    jobnavi = __get_one_time_jobnavi(geog_area_code)
    execute_result = jobnavi.get_execute_status(execute_id)
    status = execute_result["status"].lower()
    info = execute_result["info"]
    result = {"status": status, "message": info}
    return result


def delete_expire_custom_jobs_data():
    """
    删除过期数据，60天之前的

    :return: void
    """
    try:
        time_format = "%Y-%m-%d %H:%M:%S"
        expire_date = datetime.datetime.today() + datetime.timedelta(-60)
        expire_date_format = expire_date.strftime(time_format)
        one_time_jobs = BatchOneTimeExecuteJobHandler.filter_one_time_job_before_date(expire_date_format)
        for one_time_job in one_time_jobs:
            delete_one_time_job(one_time_job.job_id)
    except Exception as e:
        print(e)


def __save_to_one_time_db(custom_jobs_create_args_obj, udfs, exec_id, bksql_check_response):
    jobserver_config = {
        "geog_area_code": custom_jobs_create_args_obj.geog_area_code,
        "node_label": custom_jobs_create_args_obj.node_label,
    }

    job_config = {
        "processing_type": custom_jobs_create_args_obj.processing_type,
        "inputs": custom_jobs_create_args_obj.inputs,
        "outputs": custom_jobs_create_args_obj.outputs,
        "udfs": udfs,
    }

    deploy_config = custom_jobs_create_args_obj.deploy_config

    if custom_jobs_create_args_obj.engine_conf is not None:
        deploy_config["engine_conf"] = custom_jobs_create_args_obj.engine_conf

    if custom_jobs_create_args_obj.queryset_params is not None:
        job_config["queryset_params"] = custom_jobs_create_args_obj.queryset_params

    final_args = {
        "job_id": custom_jobs_create_args_obj.job_id,
        "job_type": custom_jobs_create_args_obj.job_type,
        "execute_id": exec_id,
        "username": custom_jobs_create_args_obj.username,
        "processing_logic": bksql_check_response["sql"],
        "cluster_group": custom_jobs_create_args_obj.cluster_group,
        "jobserver_config": json.dumps(jobserver_config),
        "job_config": json.dumps(job_config),
        "deploy_config": json.dumps(deploy_config),
        "description": custom_jobs_create_args_obj.job_id,
    }

    BatchOneTimeExecuteJobHandler.save_one_time_job(final_args)


def __make_data_processing(custom_jobs_create_args_obj, fields):
    processing_id = custom_jobs_create_args_obj.job_id
    processing_type = custom_jobs_create_args_obj.processing_type

    processing_input = []

    for input_item in custom_jobs_create_args_obj.inputs:
        if input_item["storage_type"] != "ignite":
            input_storage_cluster_config_id = ResultTableHelper.get_result_table_storage(
                input_item["result_table_id"], "hdfs"
            )["hdfs"]["storage_cluster"]["storage_cluster_config_id"]
            processing_input.append(
                {
                    "data_set_type": "result_table",
                    "data_set_id": input_item["result_table_id"],
                    "storage_cluster_config_id": input_storage_cluster_config_id,
                    "storage_type": "storage",
                    "tags": [],
                }
            )
        else:
            input_storage_cluster_config_id = ResultTableHelper.get_result_table_storage(
                input_item["result_table_id"], "ignite"
            )["ignite"]["storage_cluster"]["storage_cluster_config_id"]
            processing_input.append(
                {
                    "data_set_type": "result_table",
                    "data_set_id": input_item["result_table_id"],
                    "storage_cluster_config_id": input_storage_cluster_config_id,
                    "storage_type": "storage",
                    "tags": ["static_join"],
                }
            )

    tags = custom_jobs_create_args_obj.tags

    processings_args = {
        "bk_username": custom_jobs_create_args_obj.username,
        "project_id": custom_jobs_create_args_obj.project_id,
        "processing_id": processing_id,
        "processing_alias": processing_id,
        "processing_type": processing_type,
        "created_by": custom_jobs_create_args_obj.username,
        "description": processing_id,
        "inputs": processing_input,
        "outputs": [{"data_set_type": "result_table", "data_set_id": processing_id}],
        "tags": tags,
    }

    output_item = custom_jobs_create_args_obj.outputs[0]
    bk_biz_id = str(output_item["result_table_id"]).split("_")[0]
    result_table = {
        "bk_biz_id": bk_biz_id,
        "project_id": custom_jobs_create_args_obj.project_id,
        "result_table_id": output_item["result_table_id"],
        "result_table_name": str(output_item["result_table_id"]).split("_", 1)[1],
        "result_table_name_alias": output_item["result_table_id"],
        "processing_type": processing_type,
        "fields": fields,
        "created_by": custom_jobs_create_args_obj.username,
        "description": output_item["result_table_id"],
        "tags": custom_jobs_create_args_obj.tags,
    }

    processings_args["result_tables"] = [result_table]

    DataProcessingHelper.set_data_processing(processings_args)


def __create_storekit(custom_jobs_create_args_obj):
    output_item = custom_jobs_create_args_obj.outputs[0]
    storage = output_item["storage"]
    result_table_id = output_item["result_table_id"]
    # this cluster name is not queue this comes from storekit api for hdfs cluster
    cluster_name = storage["cluster_name"]
    cluster_type = "hdfs"
    expires = storage["expires"]
    storage_config = "{}"
    if "storage_config" in storage:
        storage_config = json.dumps(storage["storage_config"])
    StorekitHelper.create_physical_table(result_table_id, cluster_name, cluster_type, expires, storage_config)
    StorekitHelper.prepare(result_table_id, cluster_type)


def get_engine_conf(job_id):
    one_time_job = BatchOneTimeExecuteJobHandler.get_one_time_job_by_job_id(job_id)
    return __load_engine_conf(one_time_job)


def get_param_json(job_id):
    one_time_job = BatchOneTimeExecuteJobHandler.get_one_time_job_by_job_id(job_id)
    jobserver_config = json.loads(one_time_job.jobserver_config)
    geog_area_code = jobserver_config["geog_area_code"]

    job_config = json.loads(one_time_job.job_config)

    nodes = {
        "source": __parse_topo_source(job_config),
        "transform": __parse_topo_transform(one_time_job.processing_logic, job_config),
        "sink": __parse_topo_sink(job_config),
    }

    spark_conf = __load_engine_conf(one_time_job)

    result = {
        "job_type": one_time_job.job_type,
        "job_id": one_time_job.job_id,
        "job_name": one_time_job.job_id,
        "run_mode": "product",
        "geog_area_code": geog_area_code,
        "engine_conf": spark_conf,
        "udf": {"config": job_config["udfs"]},
        "nodes": nodes,
    }
    return result


def __load_engine_conf(one_time_job_data):
    deploy_config = ""
    try:
        deploy_config = json.loads(one_time_job_data.deploy_config)
    except Exception as e:
        batch_logger.warning(e)

    spark_conf = {}
    if isinstance(deploy_config, dict):
        # 为了兼容用户传resource的参数为executor.memory, executor.cores, executor.memoryOverhead, 需要加上spark前缀
        # 新参数可以使用engine_conf
        if "resource" in deploy_config:
            resource = deploy_config["resource"]
            for item in resource:
                if item in settings.DEPLOY_CONFIG_RESOURCE:
                    key = "spark.{}".format(item)
                    spark_conf[key] = str(resource[item])

        if "engine_conf" in deploy_config:
            engine_conf = deploy_config["engine_conf"]
            for item in engine_conf:
                spark_conf[item] = str(engine_conf[item])
    return spark_conf


def __parse_topo_source(job_config):
    input_rts = {}

    for input_rt in job_config["inputs"]:

        input_rt_id = input_rt["result_table_id"]
        storage_type = input_rt.get("storage_type", "hdfs")
        input_storage_params = {
            "type": storage_type,
        }
        if storage_type != "ignite":
            input_storage_params["conf"] = result_table_util.parse_hdfs_params(input_rt_id)

        fields = result_table_util.parse_source_fields(input_rt_id)

        input_rt_param = {
            "id": input_rt["result_table_id"],
            "name": input_rt["result_table_id"],
            "description": input_rt["result_table_id"],
            "partition": input_rt["partition"],
            "input": input_storage_params,
            "fields": fields,
        }
        input_rts[input_rt_id] = input_rt_param
    return input_rts


def __parse_topo_transform(sql, job_config):
    parents = []
    for input_rt in job_config["inputs"]:
        parents.append(input_rt["result_table_id"])

    output_rt_id = job_config["outputs"][0]["result_table_id"]

    transform = {
        output_rt_id: {
            "id": output_rt_id,
            "name": output_rt_id,
            "parents": parents,
            "processor": {"processor_args": sql},
        }
    }
    return transform


def __parse_topo_sink(job_config):
    sink = {}
    for output in job_config["outputs"]:
        output_rt_id = output["result_table_id"]
        result_table = ResultTableHelper.get_result_table(output_rt_id)

        mode = "overwrite"
        if "mode" in output:
            mode = output["mode"]

        sink[output_rt_id] = {
            "id": output_rt_id,
            "name": output_rt_id,
            "description": output_rt_id,
            "role": result_table["processing_type"],
            "output": {
                "type": "hdfs",
                "mode": mode,
                "conf": result_table_util.parse_hdfs_params(output_rt_id),
            },
        }

        if "partition" in output:
            sink[output_rt_id]["partition"] = output["partition"]
    return sink


def __sql_check(custom_jobs_create_args_obj, udfs):
    sql = custom_jobs_create_args_obj.processing_logic
    input_tables = []
    for item in custom_jobs_create_args_obj.inputs:
        input_tables.append(item["result_table_id"])
    output_table = custom_jobs_create_args_obj.outputs[0]["result_table_id"]
    return bksql_util.call_one_time_sparksql(sql, input_tables, output_table, udfs)


def __handle_udfs(sql, geog_area_code):
    return function_driver.parse_sql(sql, geog_area_code)


def __register_one_time_job_jobnavi(jobnavi, schedule_id, node_label, queue, cluster_group):
    extra_info = {
        "run_mode": pizza_settings.RUN_MODE,
        "makeup": False,
        "debug": False,
        "type": "one_time_sql",
        "queue": queue,
        "cluster_group_id": cluster_group,
        "service_type": "batch",
        "component_type": "spark",
        "engine_conf_path": "/dataflow/batch/custom_jobs/{job_id}/get_engine_conf/".format(job_id=schedule_id),
    }

    jobnavi_args = {
        "schedule_id": schedule_id,
        "type_id": "one_time_sql",
        "description": schedule_id,
        "node_label": node_label,
        "recovery": {
            "enable": False,
        },
        "period": {"period_unit": "o"},  # o for once
        "exec_oncreate": True,
        "extra_info": str(json.dumps(extra_info)),
        "max_running_task": 1,
    }

    batch_logger.info("Register schedule with {}".format(jobnavi_args))
    res = jobnavi.create_schedule_info(jobnavi_args)
    return res


def __get_one_time_jobnavi(geog_area_code):
    cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
    return JobNaviHelper(geog_area_code, cluster_id)
