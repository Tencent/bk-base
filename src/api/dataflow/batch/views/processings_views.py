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

from common.base_utils import model_to_dict
from common.decorators import detail_route, list_route, params_valid
from common.local import get_request_username
from common.views import APIViewSet
from django.utils.translation import ugettext as _
from rest_framework.response import Response

import dataflow.batch.job.job_driver as job_driver
import dataflow.batch.processings.processings_driver as processings_driver
import dataflow.shared.meta.processing.data_processing_helper as data_processing_driver
import dataflow.shared.meta.result_table.result_table_helper as result_table_driver
from dataflow.batch.exceptions.comp_execptions import BatchIllegalStatusError
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.periodic.processings_sql_v2_driver import ProcessingsSqlV2Driver
from dataflow.batch.serializer.serializers import (
    ProcessingCheckBatchParamSerializer,
    ProcessingsBatchSerializer,
    ProcessingUpdateBatchAdvanceJobConfigSerializer,
)
from dataflow.shared.log import batch_logger
from dataflow.shared.meta.tag.tag_helper import TagHelper


class ProcessingsViewSet(APIViewSet):
    lookup_field = "processing_id"
    lookup_value_regex = r"\w+"

    @params_valid(serializer=ProcessingsBatchSerializer)
    def create(self, request, params):
        """
        @api {post} /dataflow/batch/processings 新增v2离线processing
        @apiName create_processing
        @apiGroup Batch
        @apiParam {string} api_version api版本
        @apiParam {string} bk_username 提交人
        @apiParam {int} project_id 项目id
        @apiParam {list} outputs 输出配置
        @apiParam {list} tags 地域标签
        @apiParam {json} dedicated_config 离线计算配置
        @apiParam {list} window_info 上游节点窗口配置
        @apiParamExample {json} 参数样例:
        {
            "api_version":"v2",
            "outputs":[
                {
                    "bk_biz_id":591,
                    "table_name":"input_table",
                }
            ],
            "bk_username":"xxx",
            "project_id":4,
            "tags":[
                "inland"
            ],
            "dedicated_config":{
                "batch_type":"batch_sql_v2",
                "sql":"select dtEventTime1, udf_py_udf2(city) as c, province, country from 591_input_table",
                "self_dependence":{
                    "self_dependency":false,
                    "self_dependency_config":{

                    }
                },
                "schedule_config":{
                    "count_freq":1,
                    "schedule_period":"hour",
                    "start_time":"2021-03-11 00:00:00"
                },
                "recovery_config":{
                    "recovery_enable":false,
                    "recovery_times":"1",
                    "recovery_interval":"60m"
                },
                "output_config":{
                    "enable_customize_output":false,
                    "output_baseline_type": "upstream_result_table/schedule_time",
                    "output_baseline":"591_output_table",
                    "output_baseline_location":"start",
                    "output_offset":"1",
                    "output_offset_unit":"hour"
                }
            },
            "window_info":[
                {
                    "result_table_id":"591_input_table",
                    "window_type":"scroll",
                    "window_offset":"0",
                    "window_offset_unit":"hour",
                    "dependency_rule":"at_least_one_finished",
                    "window_size":"2",
                    "window_size_unit":"hour",
                    "window_start_offset":"1",
                    "window_start_offset_unit":"hour",
                    "window_end_offset":"2",
                    "window_end_offset_unit":"hour",
                    "accumulate_start_time":"2021-03-11 00:00:00"
                }
            ]
        }
        @apiSuccessExample {json} 成功返回processing_id
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200"
                "data":  {
                    'result_table_ids': [processing_id],
                    "processing_id": processing_id
                },
                "result": true
            }
        """
        """
         @api {post} /dataflow/batch/processings 新增离线processing
         @apiName add_processing
         @apiGroup Batch
         @apiParam {string} bk_username 提交人
         @apiParam {string} sql 处理内容，一般是一条sql
         @apiParam {json} dict 配置字典
         @apiParam {json} result_tables 父表信息
         @apiParam {string} window_size 窗口长度
         @apiParam {string} window_size_period 窗口长度周期
         @apiParam {string} type 表类型
         @apiParam {boolean} accumulate 累加
         @apiParam {int} count_freq 统计频率
         @apiParam {string} description 描述信息
         @apiParam {int} data_start 数据起始时间
         @apiParam {int} data_end 数据截止时间
         @apiParam {int} delay 延迟
         @apiParam {string} table_name 表名
         @apiParam {string} scheduler_period 调度周期
         @apiParam {string} cluster 集群
         @apiParam {string} project_id 工程标识
         @apiParamExample {json} 参数样例:
             {
               "sql": "select * from 104_XXX",
               "bk_username": "xxx",
               "static_data": []
               "result_tables": {
                 "104_XXX": {
                  "window_size": 0,
                  "window_size_period": hour,
                  "window_delay":1,
                  "dependency_rule": "all_finished"
                 }
                },
               "processing_id": "xxx",
               "outputs": [{bk_biz_id: xxx,table_name: xxx},{bk_biz_id:xxx,table_name: xxx}]
               "dict": {
                "count_freq": 1,
                "description": "xxx",
                "data_start": 0,
                "data_end": 23,
                "accumulate": false,
                "delay": 0,
                "schedule_period": "hour",
                "cluster": "xx",
                "advanced":{
                    "start_time": "",
                    "exec_oncreate": False,
                    "self_dependency": False,
                    "recovery_enable":False,
                    "recovery_times":1,
                    "recovery_interval": 5M,
                    "max_running_task": -1,
                    "active":False
                }
               },
               "project_id": 3249
             }
         @apiSuccessExample {json} 成功返回processing_id
             HTTP/1.1 200 OK
                 {
                     "message": "ok",
                     "code": "1500200"
                     "data": "xxx",
                     "result": true
                 }
         """
        if "api_version" in params and params["api_version"] == "v2":
            res = ProcessingsSqlV2Driver.create_processings(request.data)
            return Response(res)

        args = request.data
        processing_id = args["processing_id"]

        # if data_processing_driver.DataProcessingHelper.check_data_processing(processing_id):  todo
        #     raise BatchException(message=_("名称(%s)已存在" % processing_id),
        #                          code=DataapiBatchCode.SELECT_EX)
        processings_driver.valid_current_param(args)
        processings_driver.valid_parent_param(args)
        # 保存result_table信息
        udfs = None
        args["bk_username"] = get_request_username()
        if args["dict"]["batch_type"] == "tdw_jar":
            from dataflow.batch.extend.tdw import tdw_processings_driver

            result_tables = tdw_processings_driver.make_jar_result_tables(args)
        else:
            if args.get("sql"):
                udfs = processings_driver.parse_udf(args.get("sql"), args["project_id"])
            __result_table = processings_driver.make_result_table(args, udfs)
            result_tables = [__result_table]
        for rt in result_tables:
            rt["tags"] = args["tags"]
        # 保存processing信息
        processing = processings_driver.make_mapping_data_processing(args)
        if args["dict"]["batch_type"] == "tdw_jar" or args["dict"]["batch_type"] == "tdw":
            from dataflow.batch.extend.tdw import tdw_processings_driver

            processing["platform"] = "tdw"
            tdw_processings_driver.add_tdw_extra_info_for_tdw(args, result_tables)

        processing["result_tables"] = result_tables
        # 校验字段信息变更是否合法（从job表获取运行中的schema，然后校验）
        result_tables_schema = processings_driver.get_valid_result_table_schema(processing["result_tables"], None)
        # TODO 是否需要验证RT存在的问题（特别是多输出场景）？？
        data_processing_driver.DataProcessingHelper.set_data_processing(processing)
        # 保存离线信息
        try:
            if udfs:
                processings_driver.save_udf(args, udfs)
            processings_driver.save_batch_processing_info(
                args,
                outputs=processing["outputs"],
                result_tables_schema=result_tables_schema,
            )
        except Exception as e:
            batch_logger.exception(e)
            data_processing_driver.DataProcessingHelper.delete_data_processing(processing_id, with_data=True)
            raise e
        rtn = {
            "processing_id": processing_id,
            "result_table_ids": [rt["result_table_id"] for rt in result_tables],
        }
        return Response(rtn)

    @params_valid(serializer=ProcessingsBatchSerializer)
    def update(self, request, processing_id, params):
        """
        @api {put} /dataflow/batch/processings/:processing_id 更新v2离线processing
        @apiName update_processing
        @apiGroup Batch
        @apiParam {string} api_version api版本
        @apiParam {string} bk_username 提交人
        @apiParam {int} project_id 项目id
        @apiParam {list} outputs 输出配置
        @apiParam {list} tags 地域标签
        @apiParam {json} dedicated_config 离线计算配置
        @apiParam {list} window_info 上游节点窗口配置
        @apiParamExample {json} 参数样例:
        {
            "api_version":"v2",
            "outputs":[
                {
                    "bk_biz_id":591,
                    "table_name":"input_table",
                }
            ],
            "bk_username":"xxx",
            "project_id":4,
            "tags":[
                "inland"
            ],
            "dedicated_config":{
                "batch_type":"batch_sql_v2",
                "sql":"select dtEventTime1, udf_py_udf2(city) as c, province, country from 591_input_table",
                "self_dependence":{
                    "self_dependency":false,
                    "self_dependency_config":{

                    }
                },
                "schedule_config":{
                    "count_freq":1,
                    "schedule_period":"hour",
                    "start_time":"2021-03-11 00:00:00"
                },
                "recovery_config":{
                    "recovery_enable":false,
                    "recovery_times":"1",
                    "recovery_interval":"60m"
                },
                "output_config":{
                    "enable_customize_output":false,
                    "output_baseline_type": "upstream_result_table/schedule_time",
                    "output_baseline":"591_output_table",
                    "output_baseline_location":"start",
                    "output_offset":"1",
                    "output_offset_unit":"hour"
                }
            },
            "window_info":[
                {
                    "result_table_id":"591_input_table",
                    "window_type":"scroll",
                    "window_offset":"0",
                    "window_offset_unit":"hour",
                    "dependency_rule":"at_least_one_finished",
                    "window_size":"2",
                    "window_size_unit":"hour",
                    "window_start_offset":"1",
                    "window_start_offset_unit":"hour",
                    "window_end_offset":"2",
                    "window_end_offset_unit":"hour",
                    "accumulate_start_time":"2021-03-11 00:00:00"
                }
            ]
        }
        @apiSuccessExample {json} 成功返回processing_id
            HTTP/1.1 200 OK
            {
                "message": "ok",
                "code": "1500200"
                "data":  {
                    'result_table_ids': [processing_id],
                    "processing_id": processing_id
                },
                "result": true
            }
        """
        """
        @api {put} /dataflow/batch/processings/:processing_id 更新离线processing
        @apiName update_processing
        @apiGroup Batch
        @apiParam {string} processing_id processing的id
        @apiParam {string} bk_username 提交人
        @apiParam {string} sql 处理内容，一般是一条sql
        @apiParam {json} dict 配置字典
        @apiParam {json} result_tables 父表信息
        @apiParam {string} window_size 窗口长度
        @apiParam {string} window_size_period 窗口长度周期
        @apiParam {string} type 表类型
        @apiParam {boolean} accumulate 累加
        @apiParam {int} count_freq 统计频率
        @apiParam {string} description 描述信息
        @apiParam {int} data_start 数据起始时间
        @apiParam {int} data_end 数据截止时间
        @apiParam {int} delay 延迟
        @apiParam {string} table_name 表名
        @apiParam {string} scheduler_period 调度周期
        @apiParam {string} cluster 集群
        @apiParam {string} project_id 工程标识
        @apiParamExample {json} 参数样例:
            {
              "processing_id": "xxx",
              "bk_username": "xxx",
              "sql": "select * from xxx",
              "static_data": []
              "result_tables": {
                "P_XXX": {
                 "window_size": 0,
                 "window_size_period": hour,
                 "window_delay":1,
                 "dependency_rule": "ALL_FINISHED"
                }
               },
              "processing_id": "xxx",
              "outputs": [{bk_biz_id: xxx,table_name: xxx},{bk_biz_id:xxx,table_name: xxx}],
              "dict": {
               "count_freq": 1,
               "description": "xxx",
               "data_start": 0,
               "data_end": 23,
               "accumulate": false,
               "delay": 0,
               "scheduler_period": "hour",
               "cluster": "xx",
               "advanced":{
                   "start_time": "",
                   "exec_oncreate": False,
                   "self_dependency": False,
                   "recovery_enable":False,
                   "recovery_times":1,
                   "recovery_interval": 5M,
                   "max_running_task": -1,
                   "active":False
               }
              },
              "project_id": 3249
            }
        @apiSuccessExample {json} 成功返回processing_id
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "xxx",
                    "result": true
                }
        """
        if "api_version" in params and params["api_version"] == "v2":
            res = ProcessingsSqlV2Driver.update_processings(request.data)
            return Response(res)

        args = request.data
        args["bk_username"] = get_request_username()
        args["processing_id"] = processing_id

        processings_driver.valid_current_param(args)
        processings_driver.valid_parent_param(args)
        processings_driver.valid_child_param(args)
        # 更新processing信息
        # if not data_processing_driver.DataProcessingHelper.check_data_processing(processing_id): todo
        #     raise BatchException(message=_("不存在的data_processing_id(%s)" % processing_id),
        #                          code=DataapiBatchCode.SELECT_EX)
        udfs = None
        processing = processings_driver.make_mapping_data_processing(args)
        if args["dict"]["batch_type"] == "tdw_jar":
            from dataflow.batch.extend.tdw import tdw_processings_driver

            result_tables = tdw_processings_driver.make_jar_result_tables(args)
        else:
            if args.get("sql"):
                udfs = processings_driver.parse_udf(args.get("sql"), args["project_id"])
            __result_table = processings_driver.make_result_table(args, udfs)
            result_table_driver.ResultTableHelper.get_result_table(
                __result_table["result_table_id"], not_found_raise_exception=True
            )
            result_tables = [__result_table]
        for rt in result_tables:
            rt["tags"] = args["tags"]
        if args["dict"]["batch_type"] == "tdw_jar" or args["dict"]["batch_type"] == "tdw":
            from dataflow.batch.extend.tdw import tdw_processings_driver

            processing["platform"] = "tdw"
            tdw_processings_driver.add_tdw_extra_info_for_tdw(args, result_tables)

        # 校验字段信息变更是否合法（从job表获取运行中的schema，然后校验）
        result_tables_schema = processings_driver.get_valid_result_table_schema(
            result_tables=result_tables, processing_id=processing_id
        )

        # ------更新-------- #
        # TODO 是否需要验证RT存在的问题（特别是多输出场景）？？
        # 1、更新meta data_processing
        if args["dict"]["batch_type"] == "tdw_jar":
            from dataflow.batch.extend.tdw import tdw_processings_driver

            # 多输出可能有增加、删除RT的过程。
            tdw_processings_driver.update_data_processing_for_tdw_jar(
                args=args,
                processing=processing,
                processing_id=processing_id,
                result_tables=result_tables,
            )
        else:
            processing["result_tables"] = result_tables
            data_processing_driver.DataProcessingHelper.update_data_processing(processing)

        if udfs:
            processings_driver.update_udf(args, udfs)
        # 2、 更新batch processing
        processings_driver.update_processing_batch_info(
            args,
            processing_id,
            outputs=processing["outputs"],
            result_tables_schema=result_tables_schema,
        )
        rtn = {"result_table_ids": [rt["result_table_id"] for rt in result_tables]}
        return Response(rtn)

    def destroy(self, request, processing_id):
        """
        @api {delete} /dataflow/batch/processings/:processing_id 删除离线processing
        @apiName delete_processing
        @apiGroup Batch
        @apiParam {string} api_version api版本
        @apiParam {string} processing_id processing的id
        @apiParamExample {json} 参数样例:
            {
                "api_version", "v2"
                "processing_id": processing_id
                "with_data": False
            }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {"processing_id": processing_id},
                    "result": true
                }
        """
        args = request.data
        if "api_version" in args and args["api_version"] == "v2":
            ProcessingsSqlV2Driver.delete_processings(processing_id, args)
            return Response({"processing_id": processing_id})

        # 删除data_processing
        with_data = request.data.get("with_data", False)
        geog_area_code = TagHelper.get_geog_area_code_by_processing_id(processing_id)
        cluster_id = job_driver.JobNaviHelper.get_jobnavi_cluster("batch")
        jobnavi = job_driver.JobNaviHelper(geog_area_code, cluster_id)
        if not processings_driver.ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(processing_id):
            data_processing_driver.DataProcessingHelper.delete_data_processing(processing_id, with_data=with_data)
            ProcessingJobInfoHandler.delete_proc_job_info(processing_id)
            # result_table_driver.ResultTableHelper.delete_result_table(processing_id)
            jobnavi.delete_schedule(processing_id)
        else:
            processing_info = processings_driver.ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(
                processing_id
            )
            if job_driver.ProcessingBatchJobHandler.is_proc_batch_job_active(processing_info.batch_id):
                raise BatchIllegalStatusError(message=_("离线(%s)作业正在运行") % processing_info.batch_id)

            submit_args = json.loads(processing_info.submit_args)
            # tdw jar 多输出的需要按需删除 rt。
            if submit_args.get("batch_type", "default") in ["tdw_jar"]:
                from dataflow.batch.extend.tdw import tdw_processings_driver

                to_delete_result_tables = request.data.get("delete_result_tables", [])
                tdw_processings_driver.delete_data_processing_for_tdw_jar(
                    processing_id=processing_id,
                    to_delete_result_tables=to_delete_result_tables,
                )
            else:
                data_processing_driver.DataProcessingHelper.delete_data_processing(processing_id, with_data=with_data)
            jobnavi.delete_schedule(processing_id)
            # 删除离线作业processing维度配置
            ProcessingJobInfoHandler.delete_proc_job_info(processing_id)
            # result_table_driver.ResultTableHelper.delete_result_table(processing_id)
            processings_driver.ProcessingBatchInfoHandler.delete_proc_batch_info(processing_id)

        # 删除udf processing信息和job信息
        processings_driver.delete_udf_related_info(processing_id)

        return Response({"processing_id": processing_id})

    def retrieve(self, request, processing_id):
        """
        @api {get} /dataflow/batch/processings/:processing_id 获取离线processing
        @apiName update_processing
        @apiGroup Batch
        @apiParam {string} processing_id processing的id
        @apiSuccessExample {json}
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": {
                        "count_freq": 1,
                        "processor_type": "sql",
                        "component_type": "spark",
                        "schedule_period": "hour",
                        "description": "xxx",
                        "updated_at": "2018-12-19T14:43:29",
                        "created_by": "xxx",
                        "delay": 0,
                        "processor_logic": "SELECT xxx FROM xxx",
                        "updated_by": "xxx",
                        "batch_id": "591_xxx",
                        "created_at": "2018-12-19T14:43:29",
                        "processing_id": "591_xxx",
                        "submit_args": "..."
                    },
                    "result": true
                }
        """
        processing_info = processings_driver.get_processing_batch_info(processing_id)
        return Response(model_to_dict(processing_info))

    @detail_route(methods=["get"], url_path="get_udf")
    def get_udf(self, request, processing_id):
        """
        @api {get} /dataflow/batch/processings/:processing_id/get_udf 获取离线processing
        @apiName get_udf
        @apiGroup Batch
        @apiParam {string} processing_id processing的id
        @apiSuccessExample {json}
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": [{
                            "udf_info": "{
                                 \"hdfs_path\": \"hdfs://xxxx/app/udf/udf_java_udf2/v6/udf_java_udf2.jar\",
                                 \"version\": \"v6\",
                                 \"language\": \"java\",
                                 \"local_path\": \"/data/ieg/bkdata/dataflow/udf_jar\",
                                 \"type\": \"udf\",
                                 \"name\": \"udf_java_udf2\"
                            }",
                            "description": null,
                            "udf_name": "udf_java_udf2",
                            "processing_type": "batch",
                            "id": 769,
                            "processing_id": "591_sz_udf_test"
                        }],
                    "result": true
                }
        """
        udf_list = []
        udfs = processings_driver.get_udf(processing_id)
        for udf in udfs:
            udf_list.append(model_to_dict(udf))
        return Response(udf_list)

    @detail_route(methods=["post"], url_path="update_batch_job_advance_config")
    @params_valid(serializer=ProcessingUpdateBatchAdvanceJobConfigSerializer)
    def update_batch_job_advance_config(self, request, processing_id, params):
        """
        @api {post} /dataflow/batch/processings/:jid/update_batch_job_advance_config 注册清理过期数据
        @apiName update_engine_conf
        @apiGroup Batch
        @apiParam {int} start_time start_time
        @apiParam {boolean} force_execute force_execute
        @apiParam {boolean} self_dependency self_dependency
        @apiParam {boolean} recovery_enable recovery_enable
        @apiParam {int} recovery_times recovery_times
        @apiParam {string} recovery_interval recovery_interval
        @apiParamExample {json} 参数样例:
        {
            "start_time": "",
            "force_execute": False,
            "self_dependency": False,
            "self_dependency_config" {},
            "recovery_enable":False,
            "recovery_times":1,
            "recovery_interval": 5M,
            "engine_conf": {}
        }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        return Response(processings_driver.update_batch_job_advance_config(processing_id, params))

    @list_route(methods=["post"], url_path="check_batch_param")
    @params_valid(serializer=ProcessingCheckBatchParamSerializer)
    def check_batch_param(self, request, params):
        """
        @api {post} /dataflow/batch/processings/check_batch_param 离线计算检测参数
        @apiName check_batch_param
        @apiGroup Batch
        @apiParam {string} scheduling_type scheduling_type
        @apiParam {json} scheduling_content current node info
        @apiParam {json} from_nodes parents node info
        @apiParam {json} to_nodes child node info
        @apiParamExample {json} 参数样例:
        {
            "scheduling_type":"batch",
            "scheduling_content":{
                "window_type":"fixed",
                "count_freq":1,
                "schedule_period":"day",
                "fixed_delay":0,
                "delay_period": "hour",
                "delay": 1,
                "dependency_config_type":"unified",
                "unified_config":{
                    "window_size":1,
                    "window_size_period":"day",
                    "dependency_rule":"all_finished"
                },
                "advanced":{
                    "recovery_times":3,
                    "recovery_enable":false,
                    "recovery_interval":"60m"
                }
            },
            "from_nodes":[
                {
                    "scheduling_type":"batch",
                    "scheduling_content":{

                    }
                }
            ],
            "to_nodes":[
                {
                    "scheduling_type":"batch",
                    "scheduling_content":{

                    }
                }
            ]
        }
        @apiSuccessExample {json} 成功返回ok
            HTTP/1.1 200 OK
                {
                    "message": "ok",
                    "code": "1500200"
                    "data": "",
                    "result": true
                }
        """
        return Response(processings_driver.check_batch_param(params))
