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

from common.local import get_request_username
from django.utils.translation import ugettext_lazy as _

from dataflow.shared.handlers import processing_udf_info
from dataflow.shared.log import stream_logger as logger
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.stream.api.api_helper import BksqlHelper, MetaApiHelper
from dataflow.stream.api.udf import parse_sql_function
from dataflow.stream.exceptions.comp_execptions import (
    OutputsIllegalError,
    ResultTableExistsError,
    SourceIllegal,
    UpstreamError,
)
from dataflow.stream.handlers import processing_job_info, processing_stream_info
from dataflow.stream.job.job_driver import delete_flink_checkpoint_redis_info
from dataflow.stream.processing.processing_for_common import (
    delete_processings,
    generate_outputs,
    generate_result_table_input,
    get_dimension_fields,
)
from dataflow.stream.settings import STREAM_TYPE, SYSTEM_GENERATE_TYPE, USER_GENERATE_TYPE


def add_result_table_extra_info(parsed_table):
    # bksql 解析结果 parents 不包括静态关联，在此加上该信息
    static_parents = []
    if parsed_table.get("processor", {}).get("processor_type") == "static_join_transform":
        processor_args = json.loads(parsed_table["processor"].get("processor_args"))
        static_parents.append(processor_args["storage_info"]["data_id"])
    parsed_table["static_parents"] = static_parents
    return parsed_table


def create_flink_processing(args):
    tags = args["tags"]
    source_data = args["source_data"]
    project_id = args["project_id"]
    parent_result_tables = args["input_result_tables"]
    processing_id = args["processing_id"]
    window_type = args["dict"]["window_type"]
    static_data = args["static_data"]
    count_freq = args["dict"].get("count_freq") or 0
    waiting_time = args["dict"].get("waiting_time") or 0
    window_length = args["dict"].get("window_time") or 0
    session_gap = args["dict"].get("session_gap") or 0
    expired_time = args["dict"].get("expired_time") or 0
    # 延迟数据计算
    allowed_lateness = args["dict"].get("allowed_lateness") or False
    lateness_time = args["dict"].get("lateness_time") or 0
    lateness_count_freq = args["dict"].get("lateness_count_freq") or 0

    window_length *= 60
    # 过期时间，官网单位为分钟，需要转化为秒
    expired_time *= 60

    sql = args["sql"]
    description = args["dict"]["description"]
    outputs = args["outputs"]
    if len(outputs) != 1:
        raise OutputsIllegalError()

    bk_biz_id = outputs[0]["bk_biz_id"]
    result_table_name = outputs[0]["table_name"]

    real_processing_id = processing_id
    # 根据 real_processing_id 判断 rt 是否存在，若存在，则不允许创建
    if MetaApiHelper.is_result_table_exist(real_processing_id):
        raise ResultTableExistsError()

    checkpoint_type, parsed_table_list, stream_result = fetch_parsed_table_list(
        allowed_lateness,
        bk_biz_id,
        count_freq,
        expired_time,
        lateness_count_freq,
        lateness_time,
        parent_result_tables,
        processing_id,
        project_id,
        real_processing_id,
        result_table_name,
        session_gap,
        sql,
        static_data,
        waiting_time,
        window_length,
        window_type,
    )

    # 统一获取 sql 中的维度字段
    dimensions_fields = get_dimension_fields(sql)
    # 保存processing data和result table配置到元数据库
    save_processings = []
    try:
        for parsed_table in parsed_table_list:
            parsed_table["bk_biz_id"] = bk_biz_id
            parsed_table["project_id"] = project_id
            parsed_table["description"] = description
            save_processings.append({"processing_id": parsed_table["id"], "with_data": True})
            save_flink_processing(
                tags,
                parsed_table,
                real_processing_id,
                checkpoint_type,
                source_data,
                dimensions_fields,
            )
    except Exception as e:
        # 创建processing异常时，删除已经保存过的processing
        logger.exception(e)
        logger.info("save processing failed and backup " + str(save_processings))
        MetaApiHelper.bulk_delete_data_processing(save_processings)
        raise e
    return stream_result


def fetch_parsed_table_list(
    allowed_lateness,
    bk_biz_id,
    count_freq,
    expired_time,
    lateness_count_freq,
    lateness_time,
    parent_result_tables,
    processing_id,
    project_id,
    real_processing_id,
    result_table_name,
    session_gap,
    sql,
    static_data,
    waiting_time,
    window_length,
    window_type,
):
    # 根据传入的window_type更改
    window_type = get_real_window_type(window_type)
    # 获取增加系统字段 timestamp, offset
    system_fields, checkpoint_type = get_stream_system_field(parent_result_tables, window_type)
    # save udf info
    function_names = _save_udf_info(sql, project_id, processing_id)
    # 用于sql校验数据源是否正确
    check_inputs = parent_result_tables + static_data
    is_current_rt_new = "true"
    is_reuse_time_field = "true"
    # 用于bksql处理单个上游非窗口的情形时，决定是否需要复用上游节点的_startTime_/endTime_ 时间字段
    # 即从父类直接继承(select 父表的start/end time)
    # 或是直接获取dtEventTime来AS为新的_startTime/endTime
    if len(parent_result_tables) == 1:
        is_reuse_time_field = is_new_rt(parent_result_tables[0])
    # 调用bksql接口
    sql_parse_args = {
        "sql": sql,
        "properties": {
            "flink.result_table_name": result_table_name,
            "flink.bk_biz_id": bk_biz_id,
            "flink.system_fields": system_fields,
            "flink.window_type": window_type,
            "flink.static_data": static_data,
            "flink.window_length": window_length,
            "flink.waiting_time": waiting_time,
            "flink.count_freq": count_freq,
            "flink.expired_time": expired_time,
            "flink.session_gap": session_gap,
            "flink.env": "product",
            "flink.function_name": ",".join(function_names),
            "flink.source_data": check_inputs,
            "flink.allowed_lateness": allowed_lateness,
            "flink.lateness_time": lateness_time,
            "flink.lateness_count_freq": lateness_count_freq,
            "flink.is_current_rt_new": is_current_rt_new,
            "flink.is_reuse_time_field": is_reuse_time_field,
        },
    }
    parsed_table_list = BksqlHelper.list_result_tables_by_sql_parser(sql_parse_args)
    # 获取sql解析后的rt ids
    stream_result = {}
    parents = []
    processing_ids = []
    heads = []
    head_parent = []
    # 获取rt所有的parents
    for value in parent_result_tables:
        parents.append(value)
    # 如果rt parent在parents里面则判定为head（主要针对虚拟rt） tail为保存rt本身
    for parsed_table in parsed_table_list:
        processing_ids.append(parsed_table["id"])
        for rt_parent in parsed_table["parents"]:
            if rt_parent in parents:
                if parsed_table["id"] not in heads:
                    heads.append(parsed_table["id"])
                head_parent.append(rt_parent)
        add_result_table_extra_info(parsed_table)
    # 校验数据源是否正确
    validate_source(head_parent, parent_result_tables)
    tails = [real_processing_id]
    (
        stream_result["heads"],
        stream_result["tails"],
        stream_result["processing_id"],
        stream_result["result_table_ids"],
    ) = (heads, tails, real_processing_id, processing_ids)
    return checkpoint_type, parsed_table_list, stream_result


def delete_processing_redis_checkpoint(processing_id):
    """
    删除processing时清理redis checkpoint信息
    :param processing_id:
    :return:
    """
    try:
        stream_info = processing_stream_info.get(processing_id)
        job_id = stream_info.stream_id
        job_info = processing_job_info.get(job_id)
        job_config = json.loads(job_info.job_config)
        heads = job_config.get("heads")
        tails = job_config.get("tails")
        geog_area_code = TagHelper.get_geog_area_code_by_cluster_group(job_info.cluster_group)
        delete_flink_checkpoint_redis_info(processing_id, stream_info.checkpoint_type, heads, tails, geog_area_code)
    except Exception as e:
        logger.exception(e)
        logger.info("delete processing (%s) redis checkpoint failed " % processing_id)


def update_flink_processings(args, processing_id):
    tags = args["tags"]
    project_id = args["project_id"]
    parent_result_tables = args["input_result_tables"]
    window_type = args["dict"]["window_type"]
    static_data = args["static_data"]
    source_data = args["source_data"]
    count_freq = args["dict"].get("count_freq") or 0
    waiting_time = args["dict"].get("waiting_time") or 0
    window_length = args["dict"].get("window_time") or 0
    session_gap = args["dict"].get("session_gap") or 0
    expired_time = args["dict"].get("expired_time") or 0
    allowed_lateness = args["dict"].get("allowed_lateness") or False
    lateness_time = args["dict"].get("lateness_time") or 0
    lateness_count_freq = args["dict"].get("lateness_count_freq") or 0
    window_length *= 60

    # 过期时间，官网单位为分钟，需要转化为秒
    expired_time *= 60

    sql = args["sql"]
    description = args["dict"]["description"]
    outputs = args["outputs"]
    if len(outputs) != 1:
        raise OutputsIllegalError()

    bk_biz_id = outputs[0]["bk_biz_id"]
    result_table_name = outputs[0]["table_name"]

    real_processing_id = processing_id

    checkpoint_type, parsed_table_list, stream_result = fetch_parsed_table_list(
        allowed_lateness,
        bk_biz_id,
        count_freq,
        expired_time,
        lateness_count_freq,
        lateness_time,
        parent_result_tables,
        processing_id,
        project_id,
        real_processing_id,
        result_table_name,
        session_gap,
        sql,
        static_data,
        waiting_time,
        window_length,
        window_type,
    )

    # 统一获取 sql 中的维度字段
    dimension_fields = get_dimension_fields(sql)

    # 删除虚拟表
    delete_processings(real_processing_id, False, contain_real_processing=False, is_direct_delete=False)
    # 保存processing data和result table配置到元数据库，以防回退
    save_processings = []
    # 保存processing data和result table配置到元数据库
    try:
        for parsed_table in parsed_table_list:
            parsed_table["bk_biz_id"] = bk_biz_id
            parsed_table["project_id"] = project_id
            parsed_table["description"] = description
            update_flink_single_processing(
                tags,
                parsed_table,
                real_processing_id,
                checkpoint_type,
                source_data,
                dimension_fields,
            )
            if parsed_table["id"] != real_processing_id:
                save_processings.append({"processing_id": parsed_table["id"], "with_data": True})
    except Exception as e:
        # 更新processing异常时，删除已经保存过的processing
        logger.exception(e)
        logger.info("update flink processing failed and backup " + str(save_processings))
        MetaApiHelper.bulk_delete_data_processing(save_processings)
        raise e
    return stream_result


def validate_source(head_parent, parent_result_tables):
    """
    校验数据源是否正确

    :param head_parent:
    :param parent_result_tables:
    """
    for tmp_parent in parent_result_tables:
        if tmp_parent not in head_parent:
            raise UpstreamError(_("上游节点%s没有使用，请检查sql配置" % tmp_parent))


def is_new_rt(result_table):
    """
    判断rt是新老rt，即上游节点meta有无(_startTime_/_endTime_)字段

    :param result_table:
    :return: true 新rt，false 老rt
    """
    # 静态表
    fields = MetaApiHelper.get_result_table(result_table, related=["fields"])["fields"]
    field_name_list = []
    for field in fields:
        field_name_list.append(field["field_name"])
    if "_startTime_" in field_name_list and "_endTime_" in field_name_list:
        return "true"
    return "false"


def save_flink_processing(tags, processing, real_processing_id, checkpoint_type, source_data, dimension_fields):
    """
    将sql配置保存至元数据配置表

    :param processing:
    :return:
    """
    logger.info("save the flink processing %s" % processing)

    generate_type, inputs, outputs, processing_id, processing_name, project_id, table_params = build_table_params(
        processing, real_processing_id, source_data, tags
    )
    field_index = 0
    for field in processing["fields"]:
        tmp_field = {}
        if field["field"] == "timestamp" or field["field"] == "bkdata_par_offset":
            continue
        if field["field"] == "dtEventTime":
            tmp_field["field_name"] = "timestamp"
            tmp_field["field_type"] = "timestamp"
        else:
            tmp_field["field_name"] = field["field"]
            tmp_field["field_type"] = field["type"]
        tmp_field["field_alias"] = field["field"]
        tmp_field["description"] = field.get("description") or field["field"]
        tmp_field["is_dimension"] = 1 if field["field"] in dimension_fields else 0
        tmp_field["origins"] = field.get("origin") or ""
        field_index += 1
        tmp_field["field_index"] = field_index
        table_params["fields"].append(tmp_field)

    # save data processing
    processing_params = {
        "project_id": project_id,
        "processing_id": processing_id,
        "processing_alias": processing_name,
        "processing_type": STREAM_TYPE,
        "generate_type": generate_type,
        "bk_username": get_request_username(),
        "description": processing["description"],
        "result_tables": [table_params],
        "inputs": inputs,
        "outputs": outputs,
        "tags": tags,
    }

    MetaApiHelper.set_data_processing(processing_params)

    # save processing_stream_info
    window = processing.get("window")
    if "type" not in window:
        window["type"] = None

    # save processing stream info
    processing_stream_info.save(
        processing_id=processing_id,
        checkpoint_type=checkpoint_type,
        window=json.dumps(window),
        processor_type=processing.get("processor").get("processor_type"),
        processor_logic=processing.get("processor").get("processor_args"),
        component_type="flink",
        created_by=get_request_username(),
        description=processing.get("description"),
    )


def build_table_params(processing, real_processing_id, source_data, tags):
    project_id = processing.get("project_id")
    processing_id = processing["id"]
    # check_processing_exists(processing_id) todo
    processing_name = processing["name"]
    inputs = []
    parents = processing.get("parents")
    parents = parents if isinstance(parents, (list, tuple)) else [parents]
    for parent in parents:
        if parent in source_data:
            parent_table_info = MetaApiHelper.get_result_table(parent, related=["storages"])
            if "kafka" not in parent_table_info["storages"]:
                raise SourceIllegal()
            channel_cluster_config_id = parent_table_info["storages"]["kafka"]["storage_channel"][
                "channel_cluster_config_id"
            ]
            storage_type = "channel"
        else:
            storage_type = None
            channel_cluster_config_id = None
        tmp_dict = generate_result_table_input(parent, storage_type, channel_cluster_config_id)
        inputs.append(tmp_dict)
    # 增加静态关联类型的 input
    for static_parent in processing.get("static_parents", []):
        tmp_dict = generate_result_table_input(static_parent, None, None, ["static_join"])
        inputs.append(tmp_dict)
    outputs = generate_outputs(processing_id)
    if processing_id == real_processing_id:
        generate_type = USER_GENERATE_TYPE  # 实时表
    else:
        generate_type = SYSTEM_GENERATE_TYPE  # 实时虚拟表
    # save result table
    table_params = {
        "bk_biz_id": processing["bk_biz_id"],
        "project_id": project_id,
        "result_table_id": processing["id"],
        "result_table_name": processing["name"],
        "result_table_name_alias": processing["description"],
        "generate_type": generate_type,
        "bk_username": get_request_username(),
        "description": processing["description"],
        "count_freq": processing.get("window").get("count_freq") or 0,
        "fields": [],
        "tags": tags,
    }
    return generate_type, inputs, outputs, processing_id, processing_name, project_id, table_params


def update_flink_single_processing(
    tags, processing, real_processing_id, checkpoint_type, source_data, dimension_fields
):
    """
    将sql配置更新至元数据配置表

    :param processing:
    :return:
    """
    logger.info("update the flink processing %s" % processing)

    generate_type, inputs, outputs, processing_id, processing_name, project_id, table_params = build_table_params(
        processing, real_processing_id, source_data, tags
    )
    field_index = 0
    for field in processing["fields"]:
        tmp_field = {}
        if field["field"] == "timestamp" or field["field"] == "bkdata_par_offset":
            continue
        if field["field"] == "dtEventTime":
            tmp_field["field_name"] = "timestamp"
            tmp_field["field_type"] = "timestamp"
        else:
            tmp_field["field_name"] = field["field"]
            tmp_field["field_type"] = field["type"]
        tmp_field["field_alias"] = tmp_field["field_name"]
        tmp_field["description"] = field.get("description") or tmp_field["field_name"]
        tmp_field["is_dimension"] = 1 if field["field"] in dimension_fields else 0
        tmp_field["origins"] = field.get("origin") or ""
        field_index += 1
        tmp_field["field_index"] = field_index
        table_params["fields"].append(tmp_field)

    # save data processing
    processing_params = {
        "project_id": project_id,
        "processing_id": processing_id,
        "processing_alias": processing_name,
        "processing_type": STREAM_TYPE,
        "generate_type": generate_type,
        "bk_username": get_request_username(),
        "description": processing["description"],
        "result_tables": [table_params],
        "inputs": inputs,
        "outputs": outputs,
        "tags": tags,
    }

    # save processing_stream_info
    window = processing.get("window")
    if "type" not in window:
        window["type"] = None

    # 在更新processing时，虚拟rt会删掉，重新保存，非虚拟rt直接更新
    if processing_id == real_processing_id:
        # 更新data processing 不需要更新outputs
        processing_params.pop("outputs", None)
        MetaApiHelper.update_data_processing(processing_params)
        # save processing stream info
        processing_stream_info.update(
            processing_id,
            checkpoint_type=checkpoint_type,
            window=json.dumps(window),
            processor_type=processing.get("processor").get("processor_type"),
            processor_logic=processing.get("processor").get("processor_args"),
            component_type="flink",
            created_by=get_request_username(),
            description=processing.get("description"),
        )
    else:
        MetaApiHelper.set_data_processing(processing_params)
        # save processing stream info
        processing_stream_info.save(
            processing_id=processing_id,
            checkpoint_type=checkpoint_type,
            window=json.dumps(window),
            processor_type=processing.get("processor").get("processor_type"),
            processor_logic=processing.get("processor").get("processor_args"),
            component_type="flink",
            created_by=get_request_username(),
            description=processing.get("description"),
        )


def get_real_window_type(window_type):
    """
    由官网窗口配置转换为后台窗口配置
    """
    window_type_map = {
        "scroll": "tumbling",
        "accumulate": "accumulate",
        "slide": "sliding",
        "session": "session",
    }
    if window_type in window_type_map:
        return window_type_map.get(window_type)
    else:
        return None


def get_stream_system_field(parent_result_tables, window_type):
    """
    当window类型为窗口时，仅有时间字段
    当上游节点是清洗表，并且非窗口，则有timestamp和offset两个字段
    当上游节点是计算节点，并且上游节点有offset字段，本节点为非窗口节点，则有timestamp和offset两个字段

    :param parent_result_tables:
    :param window_type:
    :return:
    """
    parents = []
    for value in parent_result_tables:
        parents.append(value)
    if len(parents) == 1:
        parent = parents[0]

        rt_type = MetaApiHelper.get_result_table(parent)["processing_type"]
        if not rt_type:
            raise Exception("The result table's type %s not found." % parent)
        # 当前节点为窗口节点 没有offset
        if not is_none(window_type):
            checkpoint_type = "timestamp"
        # 上游为实时节点 需要判断上游节点有没有offset
        elif rt_type == STREAM_TYPE:
            #  parent是rt，根据rt找到父processing，然后看processing的checkpoint type
            parent_result_table_info = MetaApiHelper.get_result_table(parent, related=["data_processing"])
            parent_processing = parent_result_table_info["data_processing"]["processing_id"]

            stream_info = processing_stream_info.get(parent_processing)

            checkpoint_type = "offset" if stream_info.checkpoint_type == "offset" else "timestamp"
        # 上游节点不是实时节点 而且当前节点没有窗口 则当前节点有offset
        else:
            checkpoint_type = "offset"
    elif len(parents) > 1:
        checkpoint_type = "timestamp"
    else:
        raise Exception("Must have parent result table.")
    timestamp = {
        "field": "dtEventTime",
        "type": "string",
        "origins": "",
        "description": "time",
    }

    bkdata_par_offset = {
        "field": "bkdata_par_offset",
        "type": "string",
        "origin": "",
        "description": "bkdata par offset",
    }

    if checkpoint_type == "offset":
        system_fields = [timestamp, bkdata_par_offset]
    else:
        system_fields = [timestamp]
    return system_fields, checkpoint_type


def is_none(abc):
    return abc == "none" or abc is None or abc == ""


def _save_udf_info(sql, project_id, processing_id):
    geog_area_code = TagHelper.get_geog_area_code_by_project(project_id)
    functions = parse_sql_function(sql, geog_area_code)
    processing_udf_info.delete(processing_id=processing_id, processing_type="stream")
    function_names = []
    for function in functions:
        function_names.append(function["name"])
        processing_udf_info.save(
            processing_id=processing_id,
            processing_type="stream",
            udf_name=function["name"],
            udf_info=json.dumps(function),
        )
    return function_names
