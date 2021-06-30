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

import json
import time
from datetime import datetime, timedelta

from django.core.exceptions import ObjectDoesNotExist

import dataflow.shared.handlers.processing_udf_job as processing_udf_job
from dataflow.batch import settings
from dataflow.batch.api.api_helper import DatabusHelper
from dataflow.batch.exceptions.comp_execptions import BatchIllegalArgumentError, ExternalApiReturnException
from dataflow.batch.handlers.dataflow_control_list import DataflowControlListHandler
from dataflow.batch.handlers.processing_batch_job import ProcessingBatchJobHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.job.jobnavi_register_v1 import JobnaviRegisterV1
from dataflow.batch.processings import processings_driver
from dataflow.batch.utils import batch_db_util, resource_util, result_table_util
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper


def register_schedule(job_id, job_info, schedule_time, created_by, is_restart=False):
    prop = json.loads(json.loads(job_info.job_config)["submit_args"])
    if prop.get("batch_type", "default") in ["tdw", "tdw_jar"]:
        from dataflow.batch.extend.tdw.tdw_jobnavi_register import TdwJobnaviRegister

        jobnavi_register = TdwJobnaviRegister(job_id, job_info, created_by, is_restart)
        return jobnavi_register.register_jobnavi(schedule_time)
    else:
        jobnavi_register = JobnaviRegisterV1(job_id, job_info, created_by, is_restart)
        return jobnavi_register.register_jobnavi(schedule_time)


def execute_job(job_id, job_info, data_time):
    if job_info is not None and job_info.jobserver_config is not None:
        jobserver_config = json.loads(job_info.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_id = jobserver_config["cluster_id"]
    elif DatabusHelper.is_rt_batch_import(job_id):
        geog_area_code = TagHelper.get_geog_area_code_by_processing_id(job_id)
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        tmp_jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        try:
            job_rtn = tmp_jobnavi.get_schedule_info(job_id)
        except ObjectDoesNotExist as e:
            batch_logger.exception(e)
            job_rtn = None
        if not job_rtn:
            default_args = {
                "schedule_id": job_id,
                "description": " Project %s is a rt batch import job" % job_id,
                "type_id": "default",
            }

            batch_logger.info(default_args)
            tmp_jobnavi.create_schedule_info(default_args)
    else:
        raise BatchIllegalArgumentError("jobserver_config error: can't find valid jobserver_config")
    jobnavi = JobNaviHelper(geog_area_code, cluster_id)
    if len(data_time) == 8:
        period = "day"
    elif len(data_time) == 10:
        period = "hour"
    else:
        raise BatchIllegalArgumentError("data_time pattern error，support pattern is 'yyyyMMdd' or 'yyyyMMddhh'")

    if period == "day":
        time_format = "%Y%m%d"
        for i in range(1, 25):
            dt_time = datetime.strptime(data_time, time_format) + timedelta(hours=i)
            to_run_time = int(time.mktime(dt_time.timetuple()) * 1000)
            batch_logger.info("execute {} , schedule_time is {}".format(job_id, to_run_time))
            jobnavi.create_execute_info({"schedule_id": job_id, "schedule_time": to_run_time})
    else:
        time_format = "%Y%m%d%H"
        dt_time = datetime.strptime(data_time, time_format) + timedelta(hours=1)
        to_run_time = int(time.mktime(dt_time.timetuple()) * 1000)
        batch_logger.info("execute {} , schedule_time is {}".format(job_id, to_run_time))
        jobnavi.create_execute_info({"schedule_id": job_id, "schedule_time": to_run_time})


def fetch_top_parent_delay(job_id):
    batch_job = ProcessingBatchJobHandler.get_proc_batch_job(job_id)
    parents = json.loads(batch_job.submit_args)["result_tables"]
    for parent in parents:
        if (
            parents[parent]["type"] == "stream"
            or parents[parent]["type"] == "clean"
            or parents[parent]["type"] == "batch_model"
            or parents[parent]["type"] == "stream_model"
        ):
            return batch_job.delay
        else:
            fetch_top_parent_delay(parent)


def save_udf(job_id, udfs):
    for udf in udfs:
        processing_udf_job.save(
            job_id=job_id,
            processing_id=job_id,
            processing_type="batch",
            udf_name=udf.udf_name,
            udf_info=udf.udf_info,
        )


def delete_udf(job_id):
    processing_udf_job.delete(job_id=job_id)


def process_parquet_white_list(job_info, job_id):
    if (
        job_info
        and DataflowControlListHandler.is_in_parquet_white_list(job_id)
        and DataflowControlListHandler.is_in_bksql_spark_sql_black_list(job_id) is False
    ):
        # 1、存在job信息
        # 2、在灰度的parquet_white_list白名单中
        # 3、不在bksql_spark_sql_black_list黑名单中
        # 4、同时满足下列条件（有output_schema校验信息），则更新存储关联关系为parquet
        job_config = job_info.job_config
        submit_args = json.loads(json.loads(job_config)["submit_args"])
        output_schema = submit_args["output_schema"] if "output_schema" in submit_args else None
        if output_schema:
            for result_table_id in output_schema:
                physical_table = StorekitHelper.get_physical_table(result_table_id, cluster_type="hdfs")
                if (
                    physical_table
                    and "data_type" in physical_table
                    and (physical_table["data_type"] == "json" or physical_table["data_type"] == "")
                ):
                    physical_table_args = {
                        "result_table_id": result_table_id,
                        "cluster_name": physical_table["cluster_name"],
                        "cluster_type": physical_table["cluster_type"],
                        "expires": physical_table["expires"],
                        "storage_config": physical_table["storage_config"],
                        "description": physical_table["description"],
                        "priority": physical_table["priority"],
                        "generate_type": "system",
                        "data_type": "parquet",
                    }
                    # 更新存储格式为parquet
                    StorekitHelper.update_physical_table(**physical_table_args)


def set_self_dependency_rt(submit_args, output_rt_id, parent_rt):
    for rt in parent_rt:
        parent_rt[rt]["self_dependency_mode"] = ""
        if str(parent_rt[rt]["type"]).lower() == "batch":
            try:
                rt_batch_job = ProcessingBatchJobHandler.get_proc_batch_job(rt)
            except ObjectDoesNotExist:
                batch_logger.info("Can't find %s in ProcessingBatchJob" % rt)
                rt_batch_job = None
            if rt_batch_job and rt_batch_job.submit_args is not None:
                parent_submit_args = json.loads(rt_batch_job.submit_args)
                if "advanced" in parent_submit_args:
                    if parent_submit_args["advanced"]["self_dependency"]:
                        parent_rt[rt]["self_dependency_mode"] = "upstream"
    if "advanced" in submit_args:
        if submit_args["advanced"]["self_dependency"]:
            parent_rt[output_rt_id] = {}
            parent_rt[output_rt_id]["window_size"] = submit_args["count_freq"]
            parent_rt[output_rt_id]["window_delay"] = submit_args["count_freq"]
            parent_rt[output_rt_id]["window_size_period"] = submit_args["schedule_period"]
            parent_rt[output_rt_id]["self_dependency_mode"] = "current"
            parent_rt[output_rt_id]["storage_type"] = "hdfs"
            parent_rt[output_rt_id]["dependency_rule"] = submit_args["advanced"]["self_dependency_config"][
                "dependency_rule"
            ]
            parent_rt[output_rt_id]["type"] = "batch"
            return parent_rt
        else:
            return parent_rt
    else:
        return parent_rt


def format_parent_rt_type(parent_rts):
    for rt in parent_rts:
        parent_rts[rt]["type"] = convert_rt_type(rt, parent_rts[rt]["type"])


# Convert rt type to either batch or stream, because we have two different way read data in uc-sparksql code
def convert_rt_type(rt_id, origin_type):
    if origin_type.lower() in settings.RESULT_TABLE_TYPE_MAP:
        return settings.RESULT_TABLE_TYPE_MAP[origin_type.lower()]
    elif result_table_util.is_model_serve_mode_offline(rt_id):
        return "batch"
    else:
        raise BatchIllegalArgumentError("unsupported rt type")


def update_batch_node_label(processing_id, args):
    node_label = args["node_label"]

    if ProcessingJobInfoHandler.is_proc_job_info(processing_id):
        job_info = ProcessingJobInfoHandler.get_proc_job_info(processing_id)
        jobserver_config = json.loads(job_info.jobserver_config)
        geog_area_code = jobserver_config["geog_area_code"]
        cluster_id = jobserver_config["cluster_id"]
        jobnavi = JobNaviHelper(geog_area_code, cluster_id)
        rtn = jobnavi.get_schedule_info(processing_id)
        if rtn is not None:
            jobnavi_args = {"schedule_id": processing_id, "node_label": node_label}
            jobnavi.update_schedule_info(jobnavi_args)

    batch_db_util.update_batch_deploy_config(processing_id, "node_label", node_label)


def update_batch_engine_conf(processing_id, args):
    engine_conf = args["engine_conf"]
    # 转换配置项为string
    if isinstance(engine_conf, dict):
        for key in engine_conf:
            engine_conf[key] = str(engine_conf[key])

    # 下面暂时不用，只需刷新deploy_config即可生效，未来开放前端配置engine_conf时手工刷新可考虑放开这行代码
    # batch_db_util.update_batch_advance_config(processing_id, "engine_conf", engine_conf)
    batch_db_util.update_batch_deploy_config(processing_id, "engine_conf", engine_conf)


def get_engine_conf(processing_id):
    batch_job = ProcessingBatchJobHandler.get_proc_batch_job(processing_id)
    engine_conf = {}
    try:
        deploy_config = json.loads(batch_job.deploy_config)
        if "user_engine_conf" in deploy_config and len(deploy_config["user_engine_conf"]) > 0:
            engine_conf = deploy_config["user_engine_conf"]
        if "engine_conf" in deploy_config and len(deploy_config["engine_conf"]) > 0:
            for item in deploy_config["engine_conf"]:
                engine_conf[item] = deploy_config["engine_conf"][item]
    except Exception as e:
        batch_logger.exception(e)
    return engine_conf


def parse_code_job_info_params(args):
    submit_args = args["job_config"]["submit_args"]
    args["job_config"]["count_freq"] = submit_args["count_freq"]
    args["job_config"]["schedule_period"] = submit_args["schedule_period"]
    args["job_config"]["delay"] = submit_args["delay"]
    args["job_config"]["submit_args"] = json.dumps(args["job_config"]["submit_args"])
    args["job_config"]["processor_logic"] = json.dumps(args["job_config"]["processor_logic"])
    args["job_config"] = json.dumps(args["job_config"])
    return set_cluster_group(args)


def set_cluster_group(args):
    geog_area_code = args["jobserver_config"]["geog_area_code"]
    args["cluster_name"], resource_group_id = resource_util.get_yarn_queue_name(
        geog_area_code, args["cluster_group"], "batch"
    )
    job_config = json.loads(args["job_config"])
    job_config["resource_group_id"] = resource_group_id
    args["job_config"] = json.dumps(job_config)
    return args


def set_deploy_config_engine_conf(args):
    # 将engine_conf付给deploy_config
    submit_args = json.loads(json.loads(args["job_config"])["submit_args"])
    engine_conf = None
    if "advanced" in submit_args and "engine_conf" in submit_args["advanced"]:
        engine_conf = submit_args["advanced"]["engine_conf"]

    deploy_config = {}
    if args["deploy_config"] is not None:
        try:
            deploy_config = json.loads(args["deploy_config"])
        except Exception as e:
            batch_logger.exception(e)

    if engine_conf is not None:
        deploy_config["engine_conf"] = engine_conf
        args["deploy_config"] = json.dumps(deploy_config)


def create_topo_response(batch_job):
    jobserver_config = json.loads(batch_job.jobserver_config)
    geog_area_code = jobserver_config["geog_area_code"]
    nodes = {
        "source": __parse_topo_source(batch_job),
        "transform": __parse_topo_transform(batch_job),
        "sink": __parse_topo_sink(batch_job),
    }

    processor_logic = json.loads(batch_job.processor_logic)

    submit_args = json.loads(batch_job.submit_args)
    spark_conf = {}
    if "advanced" in submit_args and "engine_conf" in submit_args["advanced"]:
        spark_conf = submit_args["advanced"]["engine_conf"]

    queue_name = batch_job.cluster_name
    spark_conf["spark.yarn.queue"] = queue_name

    deploy_config = ""
    try:
        deploy_config = json.loads(batch_job.deploy_config)
    except Exception as e:
        batch_logger.warning(e)

    if isinstance(deploy_config, dict) and "resource" in deploy_config:
        resource = deploy_config["resource"]
        for item in resource:
            if item in settings.DEPLOY_CONFIG_RESOURCE:
                key = "spark.{}".format(item)
                spark_conf[key] = resource[item]

    result = {
        "job_type": "spark_python_code",
        "job_id": batch_job.batch_id,
        "job_name": batch_job.batch_id,
        "run_mode": "product",
        "user_args": str(processor_logic["user_args"]).strip().split(),
        "geog_area_code": geog_area_code,
        "user_package_path": processor_logic["user_package_path"],
        "user_main_class": processor_logic["user_main_class"],
        "engine_conf": spark_conf,
        "nodes": nodes,
    }
    return result


def __parse_topo_source(batch_job):
    submit_args = json.loads(batch_job.submit_args)
    parent_rts = {}
    result_tables = processings_driver.handle_static_data_and_storage_type(submit_args)
    for parent_rt in result_tables:
        parent_rt_args = result_tables[parent_rt]

        storage_type = parent_rt_args["storage_type"]
        input_params = {
            "type": storage_type,
        }
        if storage_type != "ignite":
            input_params = {
                "type": "hdfs",
                "conf": __parse_input_hdfs_params(parent_rt),
            }

        fields = __parse_source_fields(parent_rt)
        self_dependency_mode = (
            "upstream" if __check_upstream_self_dependency(parent_rt, result_tables[parent_rt]) else ""
        )

        parent_rt_period = ""
        count_freq = -1
        if parent_rt_args["type"].lower() == "batch":
            parent_job_info = ProcessingJobInfoHandler.get_proc_job_info(parent_rt)
            parent_submit_args = json.loads(json.loads(parent_job_info.job_config)["submit_args"])
            parent_rt_period = parent_submit_args["schedule_period"].lower()
            count_freq = parent_submit_args["count_freq"]

        role = convert_rt_type(parent_rt, parent_rt_args["type"])

        parent_rt_param = {
            "id": parent_rt,
            "name": parent_rt,
            "description": parent_rt,
            "role": role.upper(),
            "is_managed": parent_rt_args["is_managed"],
            "window": __parse_source_input_window(parent_rt, parent_rt_args, submit_args),
            "input": input_params,
            "self_dependency_mode": self_dependency_mode,
            "fields": fields,
            "schedule_period": parent_rt_period,
            "count_freq": count_freq,
        }
        parent_rts[parent_rt] = parent_rt_param

    advance = submit_args["advanced"] if "advanced" in submit_args else None
    if advance and "self_dependency" in advance and advance["self_dependency"]:
        for output in submit_args["outputs"]:
            id = output["data_set_id"]
            fields = __parse_source_fields(id)
            input_params = {"type": "hdfs", "conf": __parse_input_hdfs_params(id)}
            self_dependency_rt_param = {
                "id": id,
                "name": id,
                "description": id,
                "role": "BATCH",
                "is_managed": 1,
                "window": __create_self_dependency_window(submit_args, id),
                "input": input_params,
                "self_dependency_mode": "current",
                "fields": fields,
            }
            parent_rts[id] = self_dependency_rt_param

    return parent_rts


def __parse_topo_transform(batch_job):
    submit_args = json.loads(batch_job.submit_args)
    is_accumulation = submit_args["accumulate"]
    window_type = "accumulate" if is_accumulation else "tumbling"
    window = {
        "count_freq": submit_args["count_freq"],
        "period_unit": submit_args["schedule_period"],
        "type": window_type,
    }

    result_tables = submit_args["result_tables"]
    parent_rts = []
    for parent_rt in result_tables:
        parent_rts.append(parent_rt)

    transform = {"window": window, "parents": parent_rts}
    return transform


def __parse_topo_sink(batch_job):
    storage_args = json.loads(batch_job.storage_args)
    submit_args = json.loads(batch_job.submit_args)

    sink = {}
    for output in submit_args["outputs"]:
        id = output["data_set_id"]
        sink[id] = {
            "id": id,
            "name": id,
            "description": id,
            "role": "BATCH",
            "output": __parse_sink_storage_args(storage_args, id),
        }

    return sink


def __check_upstream_self_dependency(rt_id, rt_params):
    if str(rt_params["type"]).lower() == "batch":
        try:
            rt_batch_job = ProcessingBatchJobHandler.get_proc_batch_job(rt_id)
        except ObjectDoesNotExist:
            batch_logger.info("Can't find %s in ProcessingBatchJob" % rt_id)
            rt_batch_job = None
        if rt_batch_job and rt_batch_job.submit_args is not None:
            parent_submit_args = json.loads(rt_batch_job.submit_args)
            if parent_submit_args["advanced"]["self_dependency"]:
                return True
    return False


def __parse_sdk_params(batch_job):
    processor_logic = json.loads(batch_job.processor_logic)

    sdk_params = {
        "language": "python",
        "user_package_path": processor_logic["user_package_path"],
        "user_main_class": processor_logic["user_main_class"],
        "user_args": processor_logic["user_args"],
    }
    return sdk_params


def __parse_source_fields(rt_id):
    fields_response = ResultTableHelper.get_result_table_fields(rt_id)
    batch_logger.info("Try to get %s fields" % rt_id)
    batch_logger.info(fields_response)
    fields = []
    for field_response in fields_response:
        if field_response["field_name"] != "timestamp" and field_response["field_name"] != "offset":
            field = {
                "field": field_response["field_name"],
                "type": field_response["field_type"],
                "origin": rt_id,
                "description": field_response["field_name"],
            }
            fields.append(field)
    return fields


def __create_self_dependency_window(submit_args, result_table_id):
    segment = {
        "start": int(submit_args["count_freq"]),
        "end": int(submit_args["count_freq"]) + int(submit_args["count_freq"]),
        "unit": submit_args["schedule_period"],
    }

    batch_window_offset, batch_window_offset_unit = result_table_util.get_batch_min_window_size(
        submit_args, result_table_id
    )

    window_params = {
        "segment": segment,
        "length": 1,
        "batch_window_offset": batch_window_offset,
        "batch_window_offset_unit": batch_window_offset_unit,
    }
    return window_params


def __parse_source_input_window(parent_rt_id, parent_rt_args, submit_args):
    is_accumulation = submit_args["accumulate"]
    if is_accumulation:
        segment = {
            "start": int(submit_args["data_start"]),
            "end": int(submit_args["data_end"]),
            "unit": "hour",
        }
        window_params = {"segment": segment, "length": 1}
    else:
        parent_window_size = int(parent_rt_args["window_size"])
        parent_window_period = parent_rt_args["window_size_period"]
        parent_window_delay = int(parent_rt_args["window_delay"])
        segment = {
            "start": parent_window_delay,
            "end": parent_window_size + parent_window_delay,
            "unit": parent_window_period,
        }

        batch_window_offset = 0
        batch_window_offset_unit = ""
        if parent_rt_args["type"].lower() == "batch":
            parent_job_info = ProcessingJobInfoHandler.get_proc_job_info(parent_rt_id)
            parent_submit_args = json.loads(json.loads(parent_job_info.job_config)["submit_args"])
            batch_window_offset, batch_window_offset_unit = result_table_util.get_batch_min_window_size(
                parent_submit_args, parent_rt_id
            )

        window_params = {
            "segment": segment,
            "length": parent_window_size,
            "batch_window_offset": batch_window_offset,
            "batch_window_offset_unit": batch_window_offset_unit,
        }
    return window_params


def __parse_input_hdfs_params(rt_id):
    storage_params = {}
    storage_response = ResultTableHelper.get_result_table_storage(rt_id, "hdfs")
    batch_logger.info("Try to get %s hdfs storages" % rt_id)
    batch_logger.info(storage_response)
    storage_connection_info = json.loads(storage_response["hdfs"]["storage_cluster"]["connection_info"])
    storage_params["physical_table_name"] = storage_response["hdfs"]["physical_table_name"]
    storage_params["cluster_name"] = storage_response["hdfs"]["storage_cluster"]["cluster_name"]
    storage_params["cluster_group"] = storage_response["hdfs"]["storage_cluster"]["cluster_group"]
    storage_params["name_service"] = storage_connection_info["hdfs_url"]
    storage_params["data_type"] = storage_response["hdfs"]["data_type"]
    return storage_params


def __parse_sink_storage_args(storage_args, rt_id):
    if "multi_outputs" in storage_args:
        storage_args = storage_args["multi_outputs"]
    has_hdfs_storage = False
    extra_storage = False
    conf = {}
    for storage_name in storage_args[rt_id]:
        if storage_name == "hdfs":
            hdfs_storage_params = __parse_input_hdfs_params(rt_id)
            conf = {
                "rt_id": rt_id,
                "data_type": storage_args[rt_id]["hdfs"]["data_type"],
                "physical_table_name": hdfs_storage_params["physical_table_name"],
                "cluster_name": hdfs_storage_params["cluster_name"],
                "cluster_group": hdfs_storage_params["cluster_group"],
                "name_service": hdfs_storage_params["name_service"],
            }
            has_hdfs_storage = True
        else:
            extra_storage = True

    if not has_hdfs_storage:
        raise ExternalApiReturnException("Can't find hdfs storage for sink node %s" % rt_id)

    storage_params = {"type": "hdfs", "conf": conf, "extra_storage": extra_storage}

    return storage_params


def get_jobnavi_data_dependency_param(job_id):
    batch_job = ProcessingBatchJobHandler.get_proc_batch_job(job_id)
    submit_args = json.loads(batch_job.submit_args)
    result_tables = submit_args["result_tables"]

    parent_result_tables = {}

    for parent_rt_id in result_tables:
        result_table = ResultTableHelper.get_result_table(parent_rt_id, related="data_processing")

        parent_rt_type = result_table["processing_type"]
        if "data_processing" in result_table and "processing_id" in result_table["data_processing"]:
            parent_processing_id = result_table["data_processing"]["processing_id"]
        else:
            parent_processing_id = parent_rt_id

        check_is_tdw_data_source = False
        if submit_args.get("batch_type", "default") in ["tdw", "tdw_jar"]:
            from dataflow.batch.extend.tdw.utils import tdw_utils

            check_is_tdw_data_source = tdw_utils.is_tdw_data_source(result_table, submit_args)

        if (
            parent_rt_type == "batch" and not check_is_tdw_data_source
        ) or result_table_util.is_model_serve_mode_offline(parent_processing_id):
            # mod parent_id 应该为 parent_processing_id
            if not submit_args["accumulate"]:
                parent_submit_args = result_table_util.get_rt_job_submit_args_from_db(parent_processing_id)
                parent_schedule_period = parent_submit_args["schedule_period"]
                (
                    parent_min_window_size,
                    parent_min_window_size_unit,
                ) = result_table_util.get_batch_min_window_size(parent_submit_args, parent_processing_id)
                window_size = submit_args["result_tables"][parent_rt_id]["window_size"]
                window_delay = submit_args["result_tables"][parent_rt_id]["window_delay"]

                if parent_schedule_period == "hour" and submit_args["schedule_period"] == "hour":
                    window_size_str = str(window_size) + "H"
                elif parent_schedule_period == "hour" and submit_args["schedule_period"] == "day":
                    window_size_str = str(window_size * 24) + "H"
                elif parent_schedule_period == "day" and submit_args["schedule_period"] == "day":
                    window_size_str = str(window_size) + "d"
                elif submit_args["schedule_period"] == "week":
                    window_size_str = str(window_size) + "W"
                elif submit_args["schedule_period"] == "month":
                    window_size_str = str(window_size) + "M"

                window_offset_str = JobnaviRegisterV1.calculate_jobnavi_window_offset(
                    window_delay,
                    submit_args["schedule_period"],  # 由于flow限制调度时间单位与窗口单位一致
                    parent_min_window_size,
                    parent_min_window_size_unit,
                    parent_schedule_period,
                )

                parent_result_tables[parent_rt_id] = {
                    "window_size": window_size_str,
                    "window_offset": window_offset_str,
                }

    data_time_offset = JobnaviRegisterV1.get_jobnavi_data_offset(submit_args, job_id)

    is_self_dependency = False
    if (
        "advanced" in submit_args
        and "self_dependency" in submit_args["advanced"]
        and submit_args["advanced"]["self_dependency"]
    ):
        is_self_dependency = True

    result = {
        "parent_result_tables": parent_result_tables,
        "data_time_offset": data_time_offset,
        "is_accumulate": submit_args["accumulate"],
        "is_self_dependency": is_self_dependency,
    }
    return result
