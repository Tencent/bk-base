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
import subprocess
import time
import uuid
from functools import wraps

from common.local import get_request_username
from django.db.models import Max
from django.utils.decorators import available_attrs
from django.utils.translation import ugettext_lazy as _

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.modeling.api.ai_ops_helper import AIOpsHelper
from dataflow.modeling.api.api_helper import ModelingApiHelper
from dataflow.modeling.exceptions.comp_exceptions import (
    ModelExistsError,
    ModelReleaseInspectionError,
    NotSupportJoinModelPublishError,
)
from dataflow.modeling.handler.algorithm_version import AlgorithmVersionHandler
from dataflow.modeling.handler.basic_model import BasicModelHandler
from dataflow.modeling.handler.experiment_instance import ExperimentInstanceHandler
from dataflow.modeling.handler.mlsql_execute_log import MLSqlExecuteLogHandler
from dataflow.modeling.handler.mlsql_model_info import MLSqlModelInfoHandler
from dataflow.modeling.handler.model_experiment import ModelExperimentHandler
from dataflow.modeling.handler.model_experiment_node import ModelExperimentNodeHandler
from dataflow.modeling.handler.model_info import ModelInfoHandler
from dataflow.modeling.handler.model_release import ModelReleaseHandler
from dataflow.modeling.handler.user_operation_log import UserOperationLogHandler
from dataflow.modeling.job.job_config_controller import ModelingJobConfigController
from dataflow.modeling.models import (
    ExperimentInstance,
    MLSqlExecuteLog,
    ModelExperiment,
    ModelExperimentNode,
    ModelInfo,
)
from dataflow.modeling.settings import (
    DATALAB,
    DEBUG_SUBMIT_JOB_TIMEOUT,
    PARSED_TASK_TYPE,
    LogLevel,
    MLComponentType,
    ProcessorType,
    RunMode,
)
from dataflow.pizza_settings import (
    BASE_DATAFLOW_URL,
    MLSQL_HDFS_SERVER_DIR,
    MLSQL_MODEL_INFO_HDFS_USER,
    MLSQL_MODEL_INFO_PATH,
    MLSQL_NODE_LABEL,
    UC_TIME_ZONE,
)
from dataflow.shared.component.component_helper import ComponentHelper
from dataflow.shared.handlers import debug_metric_log
from dataflow.shared.jobnavi.jobnavi_helper import JobNaviHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper
from dataflow.stream.api.api_helper import MetaApiHelper


def wrap_stage(func):
    @wraps(func, assigned=available_attrs(func))
    def _wrap(self, message, stage, *args, **kwargs):
        level = LogLevel.INFO.value
        detail = ""
        status = MLSqlExecuteLog.STATUS.RUNNING
        self.log(message, level=level, stage=stage, status=status)
        try:
            return func(self, message, stage, *args, **kwargs)
        except Exception as e:
            level = LogLevel.ERROR.value
            detail = "{}".format(e)
            status = MLSqlExecuteLog.STATUS.FAILURE
            self.task.set_status(status)
            logger.exception("Release model error. %s" % e)
            raise e
        finally:
            self.log(message, level=level, stage=stage, status=status, detail=detail)

    return _wrap


class ReleaseModelTask(object):
    def __init__(self, task_id):
        self.task_id = task_id
        self.task = MLSqlExecuteLogHandler.get(task_id)

    @property
    def context(self):
        _context = self.task.get_context()
        return {} if _context is None else _context

    @property
    def geog_area_code(self):
        return TagHelper.get_geog_area_code_by_project(self.context["project_id"])

    def save_context(self, context):
        self.task.save_context(context)

    @classmethod
    def create(cls, operator, context):
        kwargs = {"context": json.dumps(context), "created_by": operator}
        task = MLSqlExecuteLogHandler.create(**kwargs)
        return cls(task.id)

    def create_model(self):
        self.task.set_status(MLSqlExecuteLog.STATUS.RUNNING)
        # 1 ready to debug
        self.__before_debug(_("准备发布模型所需信息..."), "1/3", is_create=True)
        # 2 debugging
        self.__debug(_("检查模型是否符合发布条件..."), "2/3")
        # 3 release new model
        self.__release(_("正在进行模型发布..."), "3/3")
        self.task.set_status(MLSqlExecuteLog.STATUS.SUCCESS)
        logger.info("The model %s release and create succeed." % self.context["model_name"])

    def update_model(self):
        self.task.set_status(MLSqlExecuteLog.STATUS.RUNNING)
        # 1 ready to debug
        self.__before_debug(_("准备发布模型所需信息..."), "1/3")
        # 2 debugging
        self.__debug(_("检查模型是否符合发布条件..."), "2/3")
        # 3 release new model
        self.__release_and_update(_("正在进行模型发布..."), "3/3")
        self.task.set_status(MLSqlExecuteLog.STATUS.SUCCESS)
        logger.info("The model %s release and update succeed." % self.context["model_name"])

    @wrap_stage
    def __before_debug(self, message, stage, is_create=False):
        # 检查 model_name 是否重名
        if is_create and ModelInfoHandler.exists(model_id=self.context["model_name"]):
            raise ModelExistsError(self.context["model_name"])
        # Get the processing id corresponding to the last sql
        sql_parse_args = {
            "sql": self.context["last_sql"],
            "properties": {"mlsql.only_parse_table": True},
        }
        parse_result = ModelingApiHelper.list_result_tables_by_mlsql_parser(sql_parse_args)
        processing_id = parse_result["content"]["write"]["result_table"][0]
        model_name = parse_result["content"]["read"]["model"][0]
        mlsql_link = []
        mlsql_processing_ids = []
        result_table_ids = []
        for result_table in self.context["result_table_ids"]:
            result_table_ids.append(result_table["name"])
        self.__set_processing_id_mlsql_link(processing_id, result_table_ids, mlsql_link, mlsql_processing_ids)
        logger.info("The mlsql link is %s" % (str(mlsql_link)))
        # collect all basic model
        basic_model_ids = self.__collect_basic_model(mlsql_processing_ids)
        context = self.context
        context["debug_id"] = "{}-{}".format(
            "mlsql-debug",
            str(uuid.uuid4()).replace("-", ""),
        )
        context["basic_model_ids"] = basic_model_ids
        context["last_model_name"] = model_name
        context["last_table_name"] = processing_id
        context["first_table_name"] = mlsql_processing_ids[0]
        context["node_config"] = ModelingJobConfigController.get_mlsql_config(
            mlsql_processing_ids, project_id=self.context["project_id"]
        )[0]
        self.save_context(context)
        self.__prepare_debug_job_config()

    def __collect_basic_model(self, mlsql_processing_ids):
        basic_model_ids = []
        for processing_id in mlsql_processing_ids:
            processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
            processor_logic = json.loads(processing_info.processor_logic)
            if (
                processor_logic["logic"]["processor"]["type"] == ProcessorType.TRAINED_RUN.value
                and processor_logic["logic"]["task_type"] == PARSED_TASK_TYPE.MLSQL_QUERY.value
            ):
                # 仅关注mlsql语句内的模型信息
                basic_model_ids.append(processor_logic["model"]["name"])
        logger.info("The basic model is %s" % (str(basic_model_ids)))
        return basic_model_ids

    def __prepare_debug_job_config(self):
        node_config = self.context["node_config"]
        del node_config["sink"]
        url = BASE_DATAFLOW_URL
        url = url.rstrip("/").rstrip("dataflow").rstrip("/")
        job_config = {
            "job_id": self.context["debug_id"],
            "job_name": self.context["debug_id"],
            "job_type": MLComponentType.SPARK_MLLIB.value,
            "run_mode": RunMode.RELEASE_DEBUG.value,
            "time_zone": UC_TIME_ZONE,
            "resource": {
                "cluster_group": "debug",
                "queue_name": "root.dataflow.batch.debug",
            },
            "nodes": node_config,
            "data_flow_url": url,
        }
        debug_metric_log.save(
            debug_id=self.context["debug_id"],
            job_id=self.context["debug_id"],
            processing_id=self.context["debug_id"],
            operator=get_request_username(),
            job_type=MLComponentType.SPARK_MLLIB.value,
            job_config=json.dumps(job_config),
        )

    @wrap_stage
    def __debug(self, message, stage):
        cluster_id = JobNaviHelper.get_jobnavi_cluster("batch")
        extra_info = {
            "job_id": self.context["debug_id"],
            "job_type": MLComponentType.SPARK_MLLIB.value,
            "run_mode": RunMode.RELEASE_DEBUG.value,
            "api_url": {"base_dataflow_url": BASE_DATAFLOW_URL},
        }
        params = {
            "schedule_id": self.context["debug_id"],
            "type_id": MLComponentType.SPARK_MLLIB.value,
            "active": True,
            "exec_oncreate": True,
            "extra_info": json.dumps(extra_info),
            "cluster_id": cluster_id,
            "node_label": MLSQL_NODE_LABEL,
        }
        jobnavi = JobNaviHelper(self.geog_area_code, cluster_id)
        execute_id = jobnavi.create_schedule_info(params)
        logger.info("Submit spark mllib debug job, and execute id is " + execute_id)
        # 轮训提交任务
        for i in range(DEBUG_SUBMIT_JOB_TIMEOUT):
            data = jobnavi.get_execute_status(execute_id)
            if not data or data["status"] == "running" or data["status"] == "preparing":
                time.sleep(1)
            elif data["status"] == "failed":
                raise Exception("Debug failed.")
            elif data["status"] == "finished":
                logger.info("The execute id %s, job finished." % execute_id)
                break
            else:
                logger.warning(
                    "The execute id {} status is unexpected, status is {}".format(execute_id, data["status"])
                )
                time.sleep(1)

    @wrap_stage
    def __release_and_update(self, message, stage):
        version = (
            ModelReleaseHandler.where(model_id=self.context["model_name"]).aggregate(Max("version_index"))[
                "version_index__max"
            ]
            + 1
        )
        # collect all basic model
        base_model_new_path = {}
        try:
            for basic_model_id in self.context["basic_model_ids"]:
                to_path = self.__upload_release_basic_model(basic_model_id, self.context["model_name"], version)
                base_model_new_path[basic_model_id] = to_path
            new_basic_model_id = "{}_{}_{}".format(
                self.context["model_name"],
                version,
                str(uuid.uuid4()).replace("-", ""),
            )
            if self.context["experiment_id"]:
                logger.info("use existing experiment...")
                experiment_id = self.context["experiment_id"]
                logger.info("existing experiment id:" + str(experiment_id))
                experiment_node = ModelExperimentNodeHandler.get_model_experiment_node(
                    self.context["model_name"], "sample_preparation"
                )
                experiment_node_id = experiment_node.node_id
                logger.info("existing node id:" + experiment_node_id)
            else:
                # 创建新实验
                logger.info("create new experiment...")
                experiment_id, experiment_node_id = self.__save_model_experiment(new_basic_model_id)
                logger.info("new experiment id:" + str(experiment_id))
            experiment_instance_id = self.__save_model_experiment_instance(experiment_id, new_basic_model_id)
            new_basic_model_id = self.__save_basic_model(
                new_basic_model_id,
                experiment_id,
                experiment_instance_id,
                experiment_node_id,
            )
            self.__save_model_release(version, base_model_new_path, new_basic_model_id, experiment_id)
        except Exception as e:
            logger.exception(e)
            self._clean_basic_model_hdfs_path(base_model_new_path)
            raise e

    def _clean_basic_model_hdfs_path(self, base_model_new_path):
        for basic_model_id in base_model_new_path:
            try:
                logger.info("Clean model hdfs when exception:%s" % base_model_new_path[basic_model_id])
                hdfs_cluster = StorekitHelper.get_default_storage("hdfs", self.geog_area_code)["cluster_group"]
                logger.info("start cleaning..")
                total_path = base_model_new_path[basic_model_id]
                logger.info("path:" + total_path)
                # 抽取出路径
                index_array = [i for i, ltr in enumerate(total_path) if ltr == "/"]
                path = total_path[index_array[2] :]
                if path.startswith("//"):
                    path = path[1:]
                logger.info("delete path:" + path)
                clean_data = ComponentHelper.hdfs_clean(hdfs_cluster, [path], True, MLSQL_MODEL_INFO_HDFS_USER)
                logger.info(clean_data)
            except Exception as e:
                logger.error("Clean model hdfs failed:{}".format(e))

    @wrap_stage
    def __release(self, message, stage):
        # 第一次创建，版本为1
        version = 1
        # collect all basic model
        base_model_new_path = {}
        for basic_model_id in self.context["basic_model_ids"]:
            to_path = self.__upload_release_basic_model(basic_model_id, self.context["model_name"], version)
            base_model_new_path[basic_model_id] = to_path
        try:
            new_basic_model_id = "{}_{}_{}".format(
                self.context["model_name"],
                version,
                str(uuid.uuid4()).replace("-", ""),
            )
            # todo:生成虚拟的样本集信息
            sample_set_id = self.__save_sample_set()
            # 生成模型信息
            self.__save_model_info(sample_set_id)
            # 生成实验信息
            experiment_id, experiment_node_id = self.__save_model_experiment(new_basic_model_id)
            # 生成实验实例
            experiment_instance_id = self.__save_model_experiment_instance(experiment_id, new_basic_model_id)
            # 生成basic_model
            new_basic_model_id = self.__save_basic_model(
                new_basic_model_id,
                experiment_id,
                experiment_instance_id,
                experiment_node_id,
            )
            # 生成model_release
            self.__save_model_release(version, base_model_new_path, new_basic_model_id, experiment_id)
        except Exception as e:
            logger.exception(e)
            self._clean_basic_model_hdfs_path(base_model_new_path)
            raise e

    def __save_model_experiment(self, basic_model_id):
        if ModelExperimentHandler.is_model_experiment_exists(self.context["model_name"]):
            # 当前模型已经有实验
            experiment_index = ModelExperimentHandler.get_max_experiment_index(self.context["model_name"]) + 1
        else:
            experiment_index = 1
        experiment_name = "{}_{}".format("实验", experiment_index)
        msql_model_info = MLSqlModelInfoHandler.get(model_name=self.context["basic_model_ids"][-1])
        algrithm_name = msql_model_info.algorithm_name
        experiment_properties = {
            "basic_model_id": basic_model_id,
            "basic_model_name": self.context["basic_model_ids"][-1],
            "algorithm_name": algrithm_name,
        }
        experiment_id = ModelExperimentHandler.create_model_experiment(
            experiment_name=experiment_name,
            experiment_alias=self.context["experiment_name"],
            description=self.context["experiment_name"],
            project_id=self.context["project_id"],
            status=ModelExperiment.STATUS.FINISHED,
            pass_type=ModelExperiment.PASS_TYPE.PASSED,
            active=1,
            experiment_config="{}",
            model_file_config="{}",
            execute_config="{}",
            properties=json.dumps(experiment_properties),
            model_id=self.context["model_name"],
            template_id=7,
            experiment_index=experiment_index,
            created_by=self.task.created_by,
            updated_by=self.task.created_by,
            protocol_version="1.1",
        )
        logger.info("create experiment id:" + str(experiment_id))
        self.add_user_operation_log("update", self.context["model_name"], experiment_name, "添加实验")
        # todo:新增实验的时候，新增实验对应的node
        node_id = self.__save_model_experiment_node(self.context["model_name"], experiment_id)
        return experiment_id, node_id

    @classmethod
    def format_step_name(cls, basic_model_id, step_name):
        node_name = "{}_{}".format(step_name, uuid.uuid4().hex[0:8])
        total_step_name = "{}_{}".format(node_name, str(uuid.uuid4()).replace("-", ""))
        node_info = {
            node_name: {
                total_step_name: {
                    "task_id": total_step_name,
                    "task_status": "finished",
                    "basic_model_id": basic_model_id,
                    "task_result": {},
                    "task_error_message": "",
                }
            }
        }
        return node_info

    def __save_model_experiment_instance(self, experiment_id, basic_model_id):
        evaluate_node_info = ReleaseModelTask.format_step_name(basic_model_id, "algorithm_evaluation")
        sample_node_info = ReleaseModelTask.format_step_name(basic_model_id, "sample_preparation")
        train_node_info = ReleaseModelTask.format_step_name(basic_model_id, "algorithm_training")
        if not ExperimentInstanceHandler.is_model_experiment_instance_exists(experiment_id):
            runtime_info = {
                "model_evaluation": {
                    "nodes": evaluate_node_info,
                    "pipeline_task_status": "finished",
                },
                "sample_preparation": {
                    "nodes": sample_node_info,
                    "pipeline_task_status": "finished",
                },
                "model_train": {
                    "nodes": train_node_info,
                    "pipeline_task_status": "finished",
                },
            }
            experiment_instance_id = ExperimentInstanceHandler.create_experiment_instance(
                experiment_id=experiment_id,
                model_id=self.context["model_name"],
                generate_type=ExperimentInstance.GENERATE_TYPE.USER,
                training_method=ExperimentInstance.TRAINING_METHOD.MANUAL,
                config="{}",
                execute_config="{}",
                properties="{}",
                status=ExperimentInstance.STATUS.FINISHED,
                runtime_info=json.dumps(runtime_info),
                run_status=ExperimentInstance.RUN_STATUS.FINISHED,
                active=1,
                created_by=self.task.created_by,
                updated_by=self.task.created_by,
            )
        else:
            experiment_instance = ExperimentInstanceHandler.get_latest_experiment_instance(experiment_id)
            runtime_info = json.loads(experiment_instance.runtime_info)
            runtime_info["model_evaluation"]["nodes"].update(evaluate_node_info)
            # runtime_info['sample_preparation']['nodes'].update(sample_node_info)
            runtime_info["model_train"]["nodes"].update(train_node_info)
            logger.info("runtime info:" + json.dumps(runtime_info))
            experiment_instance_id = ExperimentInstanceHandler.create_experiment_instance(
                id=experiment_instance.id,
                experiment_id=experiment_instance.experiment_id,
                model_id=experiment_instance.model_id,
                generate_type=experiment_instance.generate_type,
                training_method=experiment_instance.training_method,
                config=experiment_instance.config,
                execute_config=experiment_instance.execute_config,
                properties=experiment_instance.properties,
                status=experiment_instance.status,
                runtime_info=json.dumps(runtime_info),
                run_status=experiment_instance.run_status,
                active=experiment_instance.active,
                created_by=experiment_instance.created_by,
                updated_by=self.task.created_by,
            )

        return experiment_instance_id

    def __save_sample_set(self):
        result_table_storage = ResultTableHelper.get_result_table_storage(self.context["last_table_name"], "hdfs")[
            "hdfs"
        ]
        storage_cluster_id = result_table_storage["storage_cluster_config_id"]
        processing_cluster = AIOpsHelper.get_model_project_storage_cluster(
            self.context["project_id"], self.task.created_by
        )
        logger.info("processing cluster:" + json.dumps(processing_cluster))
        if processing_cluster:
            processing_cluster_id = processing_cluster["cluster_id"]
        else:
            processing_cluster_id = 0
        sample_set_params = {
            "scene_name": "custom",
            "sample_type": "timeseries",
            "project_id": self.context["project_id"],
            "sample_set_name": self.context["model_name"],
            "description": self.context["model_name"],
            "sensitivity": "private",
            "bk_username": self.task.created_by,
            "ts_freq": 0,  # 单位是秒
            "processing_cluster_id": processing_cluster_id,
            "storage_cluster_id": storage_cluster_id,
            "modeling_type": "mlsql",
        }
        sample_set = AIOpsHelper.create_sample_set(sample_set_params)
        self.__save_sample_features(sample_set["id"])
        return sample_set["id"]

    def __save_sample_features(self, sample_set_id):
        sample_features_params = {
            "project_id": self.context["project_id"],
            "sample_set_id": sample_set_id,
            "bk_username": self.task.created_by,
            "pipeline_config": {},
            "sample_set_configs": {"sample_features": {"added": [], "removed": []}},
        }

        model_input_config = ModelingJobConfigController.get_config_input(self.__search_head_transform())
        feature_columns = model_input_config["feature_columns"]
        field_index = 0
        for feature in feature_columns:
            field_alias = feature["field_alias"] if feature["field_alias"] else feature["field_name"]
            sample_features_params["sample_set_configs"]["sample_features"]["added"].append(
                {
                    "id": str(uuid.uuid4()).replace("-", ""),
                    "sample_set_id": sample_set_id,
                    "field_name": feature["field_name"],
                    "field_alias": field_alias,
                    "description": field_alias,
                    "field_index": field_index,
                    "origin": [feature["field_name"]],
                    "is_dimension": 0,
                    "field_type": feature["field_type"],
                    "attr_type": "feature",
                    "generate_type": "origin",
                    "feature_transform_node_id": 0,
                    "created_by": self.task.created_by,
                    "updated_by": self.task.created_by,
                }
            )
            field_index = field_index + 1
        logger.info("sample features:" + json.dumps(sample_features_params))
        AIOpsHelper.create_sample_features(sample_features_params)

    def __save_model_info(self, sample_set_id=-1):
        ModelInfoHandler.save(
            model_id=self.context["model_name"],
            model_name=self.context["model_name"],
            model_alias=self.context["model_alias"],
            project_id=self.context["project_id"],
            model_type=DATALAB,
            status=ModelInfo.STATUS.FINISHED,
            sensitivity="public" if self.context["is_public"] else "private",
            scene_name="custom",
            input_standard_config=json.dumps(
                ModelingJobConfigController.get_config_input(self.__search_head_transform())
            ),
            output_standard_config=json.dumps(
                ModelingJobConfigController.get_config_output(self.context["node_config"]["sink"])
            ),
            created_by=self.task.created_by,
            updated_by=self.task.created_by,
            sample_set_id=sample_set_id,
            run_env="spark_cluster",
            description=self.context["description"],
            modeling_type="mlsql",
            sample_type="custom",
            properties=json.dumps({"notebook_id": self.context["notebook_id"]}),
        )
        # 保存模型存储信息
        storage_cluster = AIOpsHelper.get_model_project_storage_cluster(
            self.context["project_id"], self.task.created_by, is_processing=False
        )
        logger.info("storage cluster:" + json.dumps(storage_cluster))
        storage_params = {
            "physical_table_name": "{bash_path}/{model_id}".format(
                bash_path=MLSQL_MODEL_INFO_PATH, model_id=self.context["model_name"]
            ),
            "storage_cluster_config_id": storage_cluster["cluster_id"],
            "storage_cluster_name": storage_cluster["cluster_name"],
            "storage_cluster_type": "hdfs",
            "model_id": self.context["model_name"],
            "created_by": self.task.created_by,
            "updated_by": self.task.created_by,
        }
        ModelInfoHandler.save_storage_model(storage_params)
        self.add_user_operation_log("create", self.context["model_name"], "带参数模型", "创建模型")

    def __get_basic_model_properties(self):
        """
        获取basic_model的properties属性
        @return:
        """
        properties = {
            "prediction_status": {"status": "finished"},
            "evaluation_status": {"status": "finished"},
            "training_result": {"saved_model": ""},
        }
        if "evaluation_result" in self.context:
            properties["evaluation_result"] = self.context["evaluation_result"]
        return properties

    def __get_basic_model_config(self):
        """
        获取basic_model的config属性
        @return:
        """
        config = {}
        # 首先获取评估输出信息
        processing_batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(
            processing_id=self.context["last_table_name"]
        )
        processor_logic = json.loads(processing_batch_info.processor_logic)
        task_config = processor_logic["task_config"]
        if "evaluate_map" in task_config and task_config["evaluate_map"]:
            # 如果有配置对应的评估过程，获取评估算法的相关信息
            evaluate_function = task_config["evaluate_map"]["evaluate_function"]
            algorithm_version = AlgorithmVersionHandler.get_alorithm_by_name(evaluate_function)
            evaluate_output = json.loads(algorithm_version.config)["evaluate_output"]
            config["evaluate_output"] = evaluate_output

        # 获取training_args
        # step_1:从算法中得到所有训练参数
        msql_model_info = MLSqlModelInfoHandler.get(model_name=self.context["basic_model_ids"][-1])
        algrithm_name = msql_model_info.algorithm_name
        framework = msql_model_info.model_framework
        model_algrithm_version = AlgorithmVersionHandler.get_alorithm_by_name("{}_{}".format(framework, algrithm_name))
        model_algorithm_config = json.loads(model_algrithm_version.config)
        training_args = model_algorithm_config["training_args"]

        # 从模型训练过程中获取所有用户输入参数
        training_processing_batch_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(
            processing_id=self.context["basic_model_ids"][-1]
        )
        training_processor_logic = json.loads(training_processing_batch_info.processor_logic)
        user_input_training_args = training_processor_logic["logic"]["processor"]["args"]

        # 合并用户输入与算法参数，如果用户未输入但有默认值，value=默认值
        for arg_item in training_args:
            arg_name = arg_item["field_name"]
            if arg_name in user_input_training_args:
                arg_item["value"] = user_input_training_args[arg_name]
            elif arg_item["default_value"]:
                arg_item["value"] = arg_item["default_value"]
        config["training_args"] = training_args
        # 填充特征属性
        model_info = ModelInfoHandler.get(model_id=self.context["model_name"])
        model_input_config = json.loads(model_info.input_standard_config)
        config["feature_columns"] = model_input_config["feature_columns"]
        #
        output_standard_config = json.loads(model_info.output_standard_config)
        predict_output = []
        for output_item in output_standard_config:
            predict_output.append(
                {
                    "field_name": output_item["field"],
                    "field_type": output_item["type"],
                    "field_alias": output_item["field"],
                    "value": None,
                    "default_value": None,
                    "sample_value": None,
                    "comparison": None,
                }
            )
        config["predict_output"] = predict_output
        config["predict_args"] = []

        algorithm_properties = {
            "algorithm_name": algrithm_name,
            "logic": model_algrithm_version.logic,
            "algorithm_framework": framework,
            "algorithm_version": model_algrithm_version.version,
            "load_mode": "bundle",
        }
        config["algorithm_properties"] = algorithm_properties
        return config

    def __save_basic_model(self, basic_model_id, experiment_id, experiment_instance_id, experiment_node_id):
        msql_model_info = MLSqlModelInfoHandler.get(model_name=self.context["basic_model_ids"][-1])
        algrithm_name = msql_model_info.algorithm_name
        # todo 后续algorithm_name里会支持框架格式，即framework.name的格式，但如果是spark的，则可以不用输入framework
        # 为了与算法库对应，对于未输入framework的情况，需要补齐
        if algrithm_name.find(".") == -1:
            algrithm_name = "{framework}_{name}".format(framework="spark", name=algrithm_name)
        # basic model save
        # basic model里需要写入properties，记录预测与评估的状态
        properties = self.__get_basic_model_properties()
        config = self.__get_basic_model_config()
        BasicModelHandler.save(
            basic_model_id=basic_model_id,
            basic_model_name=self.context["basic_model_ids"][-1],
            basic_model_alias=self.context["basic_model_ids"][-1],
            algorithm_name=algrithm_name,
            algorithm_version=1,  # todo 当使用自定义算法时，需获取算法版本
            status="released",
            node_id=experiment_node_id,
            source="online",
            config=json.dumps(config),
            execute_config="{}",
            properties=json.dumps(properties),
            created_by=self.task.created_by,
            updated_by=self.task.created_by,
            model_id=self.context["model_name"],
            experiment_id=experiment_id,
            experiment_instance_id=experiment_instance_id,
        )
        return basic_model_id

    def __save_model_release(self, version, base_model_new_path, new_basic_model_id, experiment_id):
        # update existing version to standby
        model_release_list = ModelReleaseHandler.where(model_id=self.context["model_name"], publish_status="latest")
        if model_release_list:
            for model_release in model_release_list:
                # model_release.update(publish_status='standby')
                ModelReleaseHandler.save(
                    id=model_release.id,
                    model_experiment_id=model_release.model_experiment_id,
                    model_id=model_release.model_id,
                    active=model_release.active,
                    description=model_release.description,
                    publish_status="standby",
                    model_config_template=model_release.model_config_template,
                    created_by=model_release.created_by,
                    created_at=model_release.created_at,
                    updated_by=model_release.updated_by,
                    version_index=model_release.version_index,
                    protocol_version=model_release.protocol_version,
                    basic_model_id=model_release.basic_model_id,
                )
        # model release info to save
        ModelReleaseHandler.save(
            model_id=self.context["model_name"],
            description=self.context["description"],
            created_by=self.task.created_by,
            updated_by=self.task.created_by,
            version_index=version,
            protocol_version="1.0",
            basic_model_id=new_basic_model_id,
            model_experiment_id=experiment_id,
            model_config_template=json.dumps(
                self.__config_standardization(self.context["node_config"], base_model_new_path)
            ),
        )

    def __save_model_experiment_node(self, model_id, experiment_id):
        node_id = "{}_{}".format("sample_preparation", uuid.uuid4().hex[0:8])
        ModelExperimentNodeHandler.create_model_experiment_node(
            node_id=node_id,
            model_id=model_id,
            model_experiment_id=experiment_id,
            node_name="sample_preparation",
            node_alias="样本准备",
            node_config="{}",
            properties="{}",
            active=1,
            step_name="sample_preparation",
            node_index=1,
            action_name="sample_preparation",
            node_role="{}",
            input_config="{}",
            output_config="{}",
            execute_config="{}",
            algorithm_config="{}",
            run_status=ModelExperimentNode.ExperimentNodeRunStatus.SUCCESS,
            operate_status=ModelExperimentNode.ExperimentNodeOperateStatus.FINISHED,
            created_by=self.task.created_by,
            updated_by=self.task.created_by,
        )
        return node_id

    def add_user_operation_log(
        self,
        operation,
        object_id,
        object_name,
        operation_name,
        object="model",
        module="experiment",
    ):
        UserOperationLogHandler.add_log(
            operation=operation,
            module=module,
            operator=self.task.created_by,
            description="{} {}".format(operation_name, object_name),
            operation_result="success",
            object=object,
            object_id=object_id,
            created_by=self.task.created_by,
            updated_by=self.task.created_by,
            object_name=object_name,
            operation_name=operation_name,
        )

    def __search_head_transform(self):
        node_config = self.context["node_config"]
        data_source_id = None
        head_source_info = None
        for source_id, source_info in list(node_config["source"].items()):
            if source_info["type"] == "data":
                data_source_id = source_id
                head_source_info = node_config["source"][data_source_id]
                break
        head_transform_info = None
        for transform_id, transform_info in list(node_config["transform"].items()):
            if data_source_id in transform_info["parents"]:
                head_transform_info = transform_info
        if head_transform_info is None:
            raise Exception("Failed to search head transform info.")
        logger.info("The head transform is %s" % str(head_transform_info))
        return {"source": head_source_info, "transform": head_transform_info}

    def __config_standardization(self, original_config, base_model_new_path):
        """
        生成发布模型对应的任务配置模版

        :param original_config: 配置模版来源
        :return:
        """
        # search tail transform
        node_ids = []
        for source_id in list(original_config["source"].keys()):
            node_ids.append(source_id)
        for transform_id in list(original_config["transform"].keys()):
            node_ids.append(transform_id)
        for transform_info in list(original_config["transform"].values()):
            for parent_id in transform_info["parents"]:
                if parent_id in node_ids:
                    node_ids.remove(parent_id)
        if len(node_ids) != 1:
            raise Exception("The tail transform must be one, actual is %s" % node_ids)
        tail_transform_id = node_ids[0]

        # Collect the node id that needs to be replaced
        replace_map = {}
        for source_id, source_info in list(original_config["source"].items()):
            if source_info["type"] == "data":
                replace_map[source_id] = "__DATA_SOURCE_ID__"
        for transform_id in list(original_config["transform"].keys()):
            transform_id_template = "__TRANSFORM_ID__"
            if transform_id != tail_transform_id:
                replace_map[transform_id] = "{}_{}".format(
                    transform_id_template,
                    str(uuid.uuid4()).replace("-", "")[0:6],
                )
            else:
                replace_map[transform_id] = transform_id_template

        # config standard
        template_config = {"source": {}, "transform": {}}
        for source_id, source_info in list(original_config["source"].items()):
            if source_info["type"] == "data":
                source_info["id"] = replace_map[source_id]
                source_info["name"] = replace_map[source_id]
                source_info["fields"] = "__DATA_SOURCE_FIELDS__"
                source_info["input"] = "__DATA_SOURCE_INPUT__"
                template_config["source"][replace_map[source_id]] = source_info
            else:
                # Replace the storage path of the model
                source_info["input"]["path"] = base_model_new_path[source_id]
                template_config["source"][source_id] = source_info
        for transform_id, transform_info in list(original_config["transform"].items()):
            transform_info["id"] = replace_map[transform_id]
            transform_info["name"] = replace_map[transform_id]
            transform_info["parents"] = [
                replace_map[parent_id] if parent_id in replace_map else parent_id
                for parent_id in transform_info["parents"]
            ]
            template_config["transform"][transform_id] = transform_info
            # 如果是子查询，则将原来的sql替换为模型板sql
            if transform_info["task_type"] == PARSED_TASK_TYPE.SUB_QUERY.value:
                format_sql = transform_info["processor"]["args"]["sql"]
                transform_info["processor"]["args"]["sql"] = format_sql
        return template_config

    def __upload_release_basic_model(self, basic_model_id, model_name, version):
        basic_model_storage_config = json.loads(
            MLSqlModelInfoHandler.get_storage_info(model_name=basic_model_id).storage_config
        )
        from_path = basic_model_storage_config["path"]

        hdfs_url = json.loads(StorekitHelper.get_default_storage("hdfs", self.geog_area_code)["connection_info"])[
            "hdfs_url"
        ]
        to_path = "{}/{}/{}/{}/{}".format(
            hdfs_url,
            MLSQL_MODEL_INFO_PATH,
            model_name,
            version,
            basic_model_id,
        )
        copy_hdfs_dir_command = "{} fs -mkdir -p {} && {} fs -cp {}/* {}".format(
            MLSQL_HDFS_SERVER_DIR,
            to_path,
            MLSQL_HDFS_SERVER_DIR,
            from_path,
            to_path,
        )
        logger.info(
            "During the release model process, copy the model file from %s to %s, and the command is %s"
            % (from_path, to_path, copy_hdfs_dir_command)
        )
        code, output = subprocess.getstatusoutput(copy_hdfs_dir_command)
        if code != 0:
            raise ModelReleaseInspectionError(output)
        logger.info(
            "When model {} is released, basic model {} is successfully copied.".format(model_name, basic_model_id)
        )
        return to_path

    def __set_processing_id_mlsql_link(self, processing_id, result_table_ids, mlsql_link, mlsql_processing_ids):
        processor_logic = json.loads(
            ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id).processor_logic
        )
        if "sub_query_has_join" in processor_logic and processor_logic["sub_query_has_join"]:
            raise NotSupportJoinModelPublishError(message_kv={"processing_id": processing_id})
        mlsql = processor_logic["sql"]
        mlsql_link.insert(0, mlsql)
        mlsql_processing_ids.insert(0, processing_id)
        # model 应用的 processing 只会有一个 input
        processing = MetaApiHelper.get_data_processing(processing_id)
        logger.info("link processing id:" + processing_id)
        logger.info(result_table_ids)
        if len(processing["inputs"]) != 1:
            raise ModelReleaseInspectionError("The processing %s can only have one input." % processing_id)
        parent_processing_id = processing["inputs"][0]["data_set_id"]
        logger.info("parent processing id:" + parent_processing_id)
        # 如果 parent_processing_id 在查找范围内，并且是由 mlsql 产生的，则其mlsql在查找链路之中。并且继续向上查找
        if parent_processing_id in result_table_ids and ProcessingBatchInfoHandler.is_proc_batch_info_exist_by_proc_id(
            parent_processing_id
        ):
            self.__set_processing_id_mlsql_link(
                parent_processing_id, result_table_ids, mlsql_link, mlsql_processing_ids
            )

    def log(self, message, level, stage, status, log_time=None, detail=""):
        self.task.add_log(
            message,
            status=status,
            stage=stage,
            level=level,
            time=log_time,
            detail=detail,
        )
