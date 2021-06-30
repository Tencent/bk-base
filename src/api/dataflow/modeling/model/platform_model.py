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
from django.utils.translation import ugettext as _

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.api.api_helper import ModelingApiHelper
from dataflow.modeling.exceptions.comp_exceptions import ModelReleaseInspectionError
from dataflow.modeling.handler.algorithm import AlgorithmHandler
from dataflow.modeling.handler.algorithm_version import AlgorithmVersionHandler
from dataflow.modeling.handler.mlsql_execute_log import MLSqlExecuteLogHandler
from dataflow.modeling.handler.model_experiment import ModelExperimentHandler
from dataflow.modeling.handler.model_info import ModelInfoHandler
from dataflow.modeling.handler.model_release import ModelReleaseHandler
from dataflow.modeling.job.job_config_driver import generate_complete_config
from dataflow.modeling.job.job_driver import get_input_info, get_output_info, get_window_info, update_process_and_job
from dataflow.modeling.models import MLSqlExecuteLog
from dataflow.modeling.settings import MLSQL_IDENTIFIER, MLSQL_SEPARATOR, ProcessorType
from dataflow.modeling.tasks import create_model, update_model
from dataflow.shared.handlers import debug_metric_log
from dataflow.shared.log import modeling_logger as logger


class PlatformModel(object):

    ALGORITHM_TYPE_MAP = {
        "default": "其它",
        "regression": "分类与回归",
        "recommend": "推荐",
        "transformer": "特征转换",
        "cluster": "聚类",
        "classify": "分类与回归",
        "recommendation": "推荐",
        "segmentation": "图像分割",
        "classification": "分类与回归",
        "evaluation": "评估",
        "transform": "特征转换",
    }

    @classmethod
    def inspection_before_release(cls, sql):
        """
        Inspection before release, check whether the model meets the release standards.

        :param sql: sql block in one cell.
        :return: If the inspection passes, return ok.
        :raises ModelReleaseInspectionError: If the inspection fails.
        """
        # 首先去除内容里的magic_code
        sql = sql.strip(MLSQL_IDENTIFIER)
        sql_list = [one_sql.strip() for one_sql in sql.split(MLSQL_SEPARATOR) if one_sql.strip()]
        last_sql = sql_list[-1]

        sql_parse_args = {
            "sql": last_sql,
            "properties": {"mlsql.only_parse_table": True},
        }
        parse_result = ModelingApiHelper.list_result_tables_by_mlsql_parser(sql_parse_args)
        result_table_list = parse_result["content"]["write"]["result_table"]
        if not result_table_list:
            raise ModelReleaseInspectionError(_("最后一条MLSQL必须为模型应用语句，请检查。"))

        processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(result_table_list[0])
        processor_logic = json.loads(processing_info.processor_logic)
        if processor_logic["logic"]["processor"]["type"] not in [
            ProcessorType.UNTRAINED_RUN.value,
            ProcessorType.TRAINED_RUN.value,
        ]:
            raise ModelReleaseInspectionError(_("最后一条MLSQL必须为模型应用语句，请检查。"))
        model_info = processor_logic["model"]
        model_name = model_info["name"]
        return {"last_sql": last_sql, "model_name": model_name}

    @classmethod
    def create(
        cls,
        last_sql,
        project_id,
        model_name,
        model_alias,
        is_public,
        result_table_ids,
        description,
        experiment_name,
        evaluation_result,
        notebook_id,
    ):
        context = {
            "last_sql": last_sql,
            "project_id": project_id,
            "model_name": model_name,
            "model_alias": model_alias,
            "is_public": is_public,
            "result_table_ids": result_table_ids,
            "description": description,
            "experiment_name": experiment_name,
            "notebook_id": notebook_id,
        }
        if evaluation_result:
            context["evaluation_result"] = cls.simplify_evaluate_result(evaluation_result)
        task_id = create_model(get_request_username(), context)
        return task_id

    def update(
        self,
        model_name,
        last_sql,
        project_id,
        result_table_ids,
        description,
        experiment_name,
        experiment_id,
        evaluation_result={},
    ):
        context = {
            "model_name": model_name,
            "last_sql": last_sql,
            "project_id": project_id,
            "result_table_ids": result_table_ids,
            "description": description,
            "experiment_name": experiment_name,
            "experiment_id": experiment_id,
        }
        if evaluation_result:
            logger.info("evaluation result:" + json.dumps(evaluation_result))
            context["evaluation_result"] = PlatformModel.simplify_evaluate_result(evaluation_result)
            logger.info("simple evaluation result:" + json.dumps(context["evaluation_result"]))
        task_id = update_model(get_request_username(), context)
        return task_id

    @classmethod
    def simplify_evaluate_result(cls, evaluation_result):
        """
        数据探索生成的评估结果与最终发布时需要的评估结果在结构上是有差异的，这里进行简单的转换
        @param evaluation_result 输入
                {
                    "train": {
                        "recall": {
                            "name": "召回率",
                            "value": 0.0
                        },
                        "f1score": {
                            "name": "f1score",
                            "value": 0.0
                        },
                        "precision": {
                            "name": "精确率",
                            "value": 0.0
                        }
                    }
                }
        @return 简化后的评估结果
                {
                    "train": {
                        "recall": 0.0,
                        "f1score": 0.0,
                        "precision": 0.0
                    }
                }
        """
        simple_evaluate_result = {}
        for data_set in evaluation_result:
            simple_evaluate_result[data_set] = {}
            for metric_name in evaluation_result[data_set]:
                simple_evaluate_result[data_set].update(
                    {metric_name: evaluation_result[data_set][metric_name]["value"]}
                )
        return simple_evaluate_result

    def get_release_result(self, task_id):
        merged_logs = []
        task = MLSqlExecuteLogHandler.get(task_id)
        logs = task.get_logs()
        # 需要对日志进行适当的合并
        logger.info(logs)
        index = 0
        release_message = "ok"
        while index < len(logs):
            start_log = logs[index]
            stage_log = {
                "status": start_log["status"],
                "time": start_log["time"],
                "message": start_log["message"],
            }
            index = index + 1
            if index < len(logs):
                end_log = logs[index]
                stage_log["status"] = end_log["status"]
                if stage_log["status"] == MLSqlExecuteLog.STATUS.FAILURE:
                    stage_log["message"] = end_log["detail"]
                    release_message = end_log["detail"]
            index = index + 1
            merged_logs.append(stage_log)
        release_data = {
            "status": task.status,
            "logs": merged_logs,
            "message": release_message,
        }
        return {"data": release_data, "message": release_message}

    def generate_release_debug_config(self, debug_id):
        return json.loads(debug_metric_log.get(debug_id=debug_id).job_config)

    def get_release_model_by_project(self, project_id):
        release_list = []
        model_info_result = ModelInfoHandler.get_model_by_project(project_id)
        for model_info in model_info_result:
            try:
                model_id = model_info.model_id
                release_model_info = {
                    "model_id": model_id,
                    "model_name": model_info.model_name,
                    "input_info": json.loads(model_info.input_standard_config),
                    "output_info": json.loads(model_info.output_standard_config),
                    "model_alias": model_info.model_alias,
                }
                params = {"model_id": model_id, "active": 1, "publish_status": "latest"}
                release_result = ModelReleaseHandler.where(**params)
                for release_info in release_result:
                    release_model_info["model_config_template"] = json.loads(release_info.model_config_template)
                    release_model_info["model_version"] = release_info.version_index
                release_list.append(release_model_info)
            except Exception:
                pass
        return release_list

    @classmethod
    def get_update_model_by_project(cls, project_id):
        release_list = []
        model_info_result = ModelInfoHandler.get_update_model_by_project(project_id)
        for model_info in model_info_result:
            release_list.append(
                {
                    "model_id": model_info.model_id,
                    "model_name": model_info.model_name,
                    "model_alias": model_info.model_alias,
                }
            )
        return release_list

    def get_model_by_name(self, model_name):
        return ModelInfoHandler.get_model_info_by_name(model_name, active=1)

    def check_model_update(self, model_id, processing_id):
        params = {"model_id": model_id, "active": 1, "publish_status": "latest"}
        release_result = ModelReleaseHandler.where(**params)[0]
        version_index = release_result.version_index

        processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
        submit_args = json.loads(processing_info.submit_args)
        logger.info(submit_args["node"])
        deployed_model_info = submit_args["node"]["deployed_model_info"]
        current_version_index = int(deployed_model_info["model_version"])
        if version_index != current_version_index:
            return True
        else:
            return False

    def update_model_info(self, model_id, processing_id):
        params = {"model_id": model_id, "active": 1, "publish_status": "latest"}
        release_result = ModelReleaseHandler.where(**params)[0]
        new_model_config_template = release_result.model_config_template
        processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
        submit_args = json.loads(processing_info.submit_args)
        node_info = submit_args["node"]
        dependence_info = submit_args["dependence"]
        deploy_model_info = node_info["deployed_model_info"]
        output_info = get_output_info(node_info)
        input_info = get_input_info(dependence_info)
        window_info = get_window_info(input_info["name"], dependence_info, node_info)
        model_params = deploy_model_info["model_params"]
        new_model_config = generate_complete_config(
            json.loads(new_model_config_template),
            input_info,
            output_info,
            window_info,
            model_params,
        )
        deploy_model_info["model_version"] = release_result.version_index
        update_process_and_job(output_info["name"], new_model_config, submit_args)

    @classmethod
    def get_algrithm_by_user(cls, framework=None):
        return AlgorithmHandler.get_algorithm(get_request_username(), framework=framework)

    @classmethod
    def get_algorithm_by_name(cls, name, version=1):
        return AlgorithmVersionHandler.get_alorithm_by_name(name, version=version)

    @classmethod
    def get_model_release_by_version(cls, model_id, version_index):
        params = {"model_id": model_id, "active": 1, "version_index": version_index}
        return ModelReleaseHandler.where(**params)[0]

    def is_model_experiment_exists(self, model_id, model_experiment_name):
        return ModelExperimentHandler.is_model_experiment_exists_by_name(model_experiment_name)
