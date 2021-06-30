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

from dataflow.shared.api.modules.batch import BatchApi
from dataflow.shared.api.util.api_driver import APIResponseUtil as res_util


class BatchHelper(object):
    @staticmethod
    def create_processing(project_id, processing_id, result_tables, sql, dict, **kwargs):
        kwargs.update(
            {
                "project_id": project_id,
                "processing_id": processing_id,
                "result_tables": result_tables,
                "sql": sql,
                "dict": dict,
            }
        )
        res = BatchApi.processings.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_processingv2(**kwargs):
        res = BatchApi.processings.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_processing(project_id, processing_id, result_tables, sql, dict, **kwargs):
        kwargs.update(
            {
                "project_id": project_id,
                "processing_id": processing_id,
                "result_tables": result_tables,
                "sql": sql,
                "dict": dict,
            }
        )
        res = BatchApi.processings.update(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_processingv2(**kwargs):
        res = BatchApi.processings.update(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_processing(processing_id, with_data=False, delete_result_tables=[]):
        """
        删除 DP
        @param processing_id:
        @param with_data: 是否删除关联所有 RT
        @param delete_result_tables: 指定删除 DP 时应该删除的结果表列表
        @return:
        """
        res = BatchApi.processings.delete(
            {
                "processing_id": processing_id,
                "with_data": with_data,
                "delete_result_tables": delete_result_tables,
            }
        )
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_processingv2(processing_id, with_data=False, delete_result_tables=[]):
        """
        删除 DP
        @param processing_id:
        @param with_data: 是否删除关联所有 RT
        @param delete_result_tables: 指定删除 DP 时应该删除的结果表列表
        @return:
        """
        res = BatchApi.processings.delete(
            {
                "processing_id": processing_id,
                "with_data": with_data,
                "delete_result_tables": delete_result_tables,
            }
        )
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def check_batch_param(params):
        res = BatchApi.processings.check_batch_param(params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_job(
        project_id, processing_id, code_version, cluster_group, deploy_mode, deploy_config, job_config, **kwargs
    ):
        # example = {
        #     "processing_id": "591_xxx_58",
        #     "code_version": "0.1.0",
        #     "cluster_group": "gem",         # 集群类型
        #     "cluster_name": "root.default", # 队列
        #     "deploy_mode": "yarn",
        #     "deploy_config": "{executor_memory:1024m}",
        #     "job_config": {},
        #     "project_id": 24
        # }
        kwargs.update(
            {
                "project_id": project_id,
                "code_version": code_version,
                "cluster_group": cluster_group,
                "job_config": job_config,
                "deploy_mode": deploy_mode,
                "deploy_config": deploy_config,
                "processing_id": processing_id,
            }
        )
        res = BatchApi.jobs.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def start_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = BatchApi.jobs.start(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def update_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = BatchApi.jobs.update(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def stop_job(job_id, **kwargs):
        kwargs.update({"job_id": job_id})
        res = BatchApi.jobs.stop(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_result_table_data_tail(result_table_id):
        """
        获取rt最近的几条数据
        @return:
        """
        request_params = {"result_table_id": result_table_id}
        res = BatchApi.hdfs_result_tables.new_line(request_params)
        res_util.check_response(res)
        return res.data

    @staticmethod
    def create_debug(heads, tails, **kwargs):
        kwargs.update({"heads": heads, "tails": tails})
        res = BatchApi.debugs.create(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_debug_basic_info(debug_id, **kwargs):
        kwargs.update({"debug_id": debug_id})
        res = BatchApi.debugs.basic_info(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_debug_node_info(debug_id, result_table_id, **kwargs):
        kwargs.update({"debug_id": debug_id, "result_table_id": result_table_id})
        res = BatchApi.debugs.node_info(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def stop_debug(debug_id, **kwargs):
        kwargs.update({"debug_id": debug_id})
        res = BatchApi.debugs.stop(kwargs)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_custom_calculate(
        custom_calculate_id,
        rerun_processings,
        data_start,
        data_end,
        geog_area_code,
        type="makeup",
    ):
        api_params = {
            "custom_calculate_id": custom_calculate_id,
            "rerun_processings": rerun_processings,
            "data_start": data_start,
            "data_end": data_end,
            "type": type,
            "geog_area_code": geog_area_code,
        }
        res = BatchApi.custom_calculates.create(api_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def analyze_custom_calculate(rerun_processings, data_start, data_end, geog_area_code, type="makeup"):
        api_params = {
            "rerun_processings": rerun_processings,
            "data_start": data_start,
            "data_end": data_end,
            "type": type,
            "geog_area_code": geog_area_code,
        }
        res = BatchApi.custom_calculates.analyze(api_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def hdfs_upload(hdfs_ns_id, file_path, file):
        request_params = {"hdfs_ns_id": hdfs_ns_id, "path": file_path}
        res = BatchApi.hdfs.upload(request_params, files={"file": file})
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def get_custom_calculate_basic_info(custom_calculate_id):
        """
        @param custom_calculate_id:
        @return:
            {
                "custom_calculate_id": xxx,
                "status": running,
                "execute": [{
                    "processing_id": xxx,
                    "schedule_time": xxx,
                    "status": running,
                    "info": xxx
                },{
                    "processing_id": xxx,
                    "schedule_time": xxx,
                    "status": finished,
                    "info": xxx
                }]
            }
        """
        api_params = {"custom_calculate_id": custom_calculate_id}
        res = BatchApi.custom_calculates.basic_info(api_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def hdfs_move(hdfs_ns_id, from_path, to_path, is_overwrite):
        request_params = {
            "hdfs_ns_id": hdfs_ns_id,
            "from_path": from_path,
            "to_path": to_path,
            "is_overwrite": is_overwrite,
        }
        res = BatchApi.hdfs.move(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def stop_custom_calculate(custom_calculate_id, geog_area_code):
        api_params = {
            "custom_calculate_id": custom_calculate_id,
            "geog_area_code": geog_area_code,
        }
        res = BatchApi.custom_calculates.stop(api_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def hdfs_list_status(hdfs_ns_id, path):
        request_params = {"hdfs_ns_id": hdfs_ns_id, "path": path}
        res = BatchApi.hdfs.list_status(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def hdfs_clean(hdfs_ns_id, paths):
        request_params = {"hdfs_ns_id": hdfs_ns_id, "paths": paths}
        res = BatchApi.hdfs.clean(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def stop_source(result_table_id):
        """
        停止 TDW 数据源检查任务
        @param result_table_id: 存量表对应 result_table_id
        @return:
        """
        request_params = {"job_id": result_table_id}
        res = BatchApi.jobs.stop_source(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def status_list(processing_id, data_start, data_end, geog_area_code):
        """
        {
            "data_start": 1537152075939,
            "data_end": 1537152075939,
            "processing_id": "xxxx",
            "geog_area_code": "inland"
        }
        @param processing_id:
        @param data_start:
        @param data_end:
        @param geog_area_code:
        @return:
            [{
                "schedule_time": "xxx",
                "status": "running",
                "created_at": "",
                "updated_at": ""
            }]
        """
        request_params = {
            "processing_id": processing_id,
            "data_start": data_start,
            "data_end": data_end,
            "geog_area_code": geog_area_code,
        }
        res = BatchApi.data_makeup.status_list(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def check_execution(processing_id, schedule_time, geog_area_code):
        request_params = {
            "processing_id": processing_id,
            "schedule_time": schedule_time,
            "geog_area_code": geog_area_code,
        }
        res = BatchApi.data_makeup.check_execution(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def create_data_makeup(
        processing_id,
        rerun_processings,
        source_schedule_time,
        target_schedule_time,
        with_storage,
        geog_area_code,
    ):
        """
        @param processing_id:
        @param rerun_processings:
        @param source_schedule_time:
        @param target_schedule_time:
        @param with_storage:
        @param geog_area_code:
        @return:
        """
        request_params = {
            "processing_id": processing_id,
            "rerun_processings": rerun_processings,
            "source_schedule_time": source_schedule_time,
            "target_schedule_time": target_schedule_time,
            "with_storage": with_storage,
            "geog_area_code": geog_area_code,
        }
        res = BatchApi.data_makeup.create(request_params)
        res_util.check_response(res, self_message=False)
        return res.data

    @staticmethod
    def delete_job(job_id):
        request_params = {"job_id": job_id}
        res = BatchApi.jobs.delete(request_params)
        res_util.check_response(res, self_message=False)
        return res.data
