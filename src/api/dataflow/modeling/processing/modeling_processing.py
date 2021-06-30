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

from common.exceptions import ApiRequestError, ApiResultError
from common.local import get_request_username
from conf.dataapi_settings import MLSQL_HDFS_CLUSTER_GROUP, MLSQL_MODEL_PATH
from django.utils.translation import ugettext_lazy as _

from dataflow.batch.handlers.processing_batch_info import ProcessingBatchInfoHandler
from dataflow.batch.handlers.processing_job_info import ProcessingJobInfoHandler
from dataflow.batch.models.bkdata_flow import ProcessingBatchInfo
from dataflow.modeling.api.api_helper import ModelingApiHelper
from dataflow.modeling.exceptions.comp_exceptions import (
    CreateModelProcessingException,
    DPAlreadyExistsError,
    EntityAlreadyExistsError,
    EvaluateFunctionNotFoundError,
    EvaluateLabelNotFoundError,
    HDFSStorageNotExistsError,
    ModelAlreadyExistsError,
    ModelCreateError,
    RTCreateError,
    SQLForbiddenError,
    SqlParseError,
    TableAlreadyExistsError,
    TableNotQuerySetError,
)
from dataflow.modeling.handler.algorithm import AlgorithmHandler
from dataflow.modeling.handler.mlsql_model_info import MLSqlModelInfoHandler
from dataflow.modeling.handler.model_instance import ModelInstanceHandler
from dataflow.modeling.handler.model_release import ModelReleaseHandler
from dataflow.modeling.job import job_driver
from dataflow.modeling.job.job_config_driver import generate_complete_config
from dataflow.modeling.job.job_driver import get_input_info, get_output_info, get_window_info
from dataflow.modeling.settings import PARSED_TASK_TYPE, USE_SCENARIO
from dataflow.modeling.utils.modeling_utils import ModelingUtils
from dataflow.shared.datalab.datalab_helper import DataLabHelper
from dataflow.shared.log import modeling_logger as logger
from dataflow.shared.meta.processing.data_processing_helper import DataProcessingHelper
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper
from dataflow.shared.meta.tag.tag_helper import TagHelper
from dataflow.shared.storekit.storekit_helper import StorekitHelper

USER_GENERATE_TYPE = "user"


class ModelingProcessing(object):
    def __init__(self, processing_id):
        self.processing_id = processing_id

    @classmethod
    def get_table_operator(cls, bulk_params):
        if bulk_params.type == "datalab":
            operator = DatalabTableOperator(bulk_params)
        elif bulk_params.type == "dataflow":
            operator = DataflowTableOperator(bulk_params)
        return operator

    @classmethod
    def parse_sql_table(cls, sql_list):
        TableOperator.parse_sql_table(sql_list)

    @classmethod
    def create_processing(cls, bulk_params):
        cls.get_table_operator(bulk_params).create_processing()


class TableOperator(object):
    def __init__(self, bulk_params):
        self.bulk_params = bulk_params
        if self.bulk_params.type == "datalab":
            self.operator = DatalabTableOperator(self.bulk_params)
        elif self.bulk_params.type == "dataflow":
            self.operator = DataflowTableOperator(self.bulk_params)

    def create_processing(self):
        pass

    def update_processing(self):
        pass

    @staticmethod
    def delete_processing(process_id, with_data):
        processing_list = ProcessingBatchInfoHandler.get_proc_batch_info_by_prefix(process_id)
        for processing in processing_list:
            if with_data:
                # 先删除storage
                storage_response = ResultTableHelper.get_result_table_storage(processing.processing_id, "hdfs")
                logger.info(json.dumps(storage_response))
                if len(storage_response) > 0:
                    logger.info("delete:" + processing.processing_id)
                    StorekitHelper.delete_physical_table(processing.processing_id, "hdfs")
                # 再删除DP
                DataProcessingHelper.delete_data_processing(processing.processing_id)
                # 再删除ret
                ResultTableHelper.delete_result_table(processing.processing_id)
            processing.delete()

    def get_task_operate_type(self, task):
        operate_type = ""
        if "table_name" in task:
            if task["table_need_create"]:
                operate_type = "create"
            else:
                operate_type = "insert"
        else:
            operate_type = "train"
        return operate_type

    @classmethod
    def parse_sql_table(cls, sql_list):
        heads = set()
        tails = set()
        sql_index = 0
        for sql in sql_list:
            sql_index = sql_index + 1
            sql_parse_args = {
                "sql": sql,
                "properties": {"mlsql.only_parse_table": True},
            }
            mlsql_parse_result = ModelingApiHelper.list_result_tables_by_mlsql_parser(sql_parse_args)
            if "result" in mlsql_parse_result:
                if mlsql_parse_result["result"]:
                    parse_content = mlsql_parse_result["content"]
                    logger.info(json.dumps(parse_content))
                    heads.update(parse_content["read"]["result_table"])
                    heads.update(parse_content["read"]["model"])
                    tails.update(parse_content["write"]["result_table"])
                    tails.update(parse_content["write"]["model"])
                else:
                    raise SqlParseError(
                        message_kv={
                            "number": sql_index,
                            "content": mlsql_parse_result["content"],
                        }
                    )
        origin_table = []
        for head_table in heads:
            if head_table not in tails:
                origin_table.append(head_table)
        last_table = []
        for tail_table in tails:
            if tail_table not in heads:
                last_table.append(tail_table)
        read_table = []
        write_table = []
        read_model = []
        write_model = []
        for table in origin_table:
            if cls.check_table_exists(table):
                read_table.append(table)
            else:
                read_model.append(table)
        for table in last_table:
            if cls.check_table_exists(table):
                write_table.append(table)
            else:
                write_model.append(table)
        return read_table, write_table, read_model, write_model

    @classmethod
    def parse_ddl_table(cls, sql_list):
        drop_model = []
        drop_table = []
        # truncate
        write_table = []
        sql_index = 0
        for sql in sql_list:
            sql_index = sql_index + 1
            sql_parse_args = {
                "sql": sql,
                "properties": {"mlsql.only_parse_table": True},
            }
            mlsql_parse_result = ModelingApiHelper.list_result_tables_by_mlsql_parser(sql_parse_args)
            parse_content = mlsql_parse_result["content"]
            operate_type = parse_content["type"]
            if "result" in mlsql_parse_result and mlsql_parse_result["result"]:
                if operate_type in ["drop_model", "drop_table", "truncate"]:
                    logger.info(parse_content)
                    logger.info(json.dumps(parse_content))
                    operate_data = parse_content["data"]
                    if operate_type == "drop_model":
                        model_info = MLSqlModelInfoHandler.fetch_model_by_name(operate_data)
                        if model_info:
                            # 模型存在的情况下才加入列表
                            drop_model.append(operate_data)
                    elif operate_type == "drop_table":
                        table_info = ResultTableHelper.get_result_table(operate_data, not_found_raise_exception=False)
                        if table_info:
                            drop_table.append(operate_data)
                    elif operate_type == "truncate":
                        table_info = ResultTableHelper.get_result_table(operate_data, not_found_raise_exception=False)
                        if table_info:
                            write_table.append(operate_data)
            else:
                raise SqlParseError(
                    message_kv={
                        "number": sql_index,
                        "content": mlsql_parse_result["content"],
                    }
                )
        return drop_table, drop_model, write_table

    @classmethod
    def check_table_exists(cls, table_name):
        try:
            result = ResultTableHelper.get_result_table(table_name)
            if not result:
                return False
            # 表存在
            return True
        except Exception:
            return False


class DatalabTableOperator(TableOperator):
    def __init__(self, bulk_params):
        self.notebook_id = bulk_params.notebook_id
        self.cell_id = bulk_params.cell_id
        self.project_id = bulk_params.project_id
        self.sql_list = bulk_params.sql_list
        self.component_type = bulk_params.component_type
        self.bk_username = bulk_params.bk_username
        self.bk_biz_id = bulk_params.bk_biz_id

        # 其它运行需要的全局变量
        self.heads = []  # head信息
        self.tails = []  # tail信息
        self.processing_ids = []  # 整个过程所有的processing_id信息
        self.result_table_ids = []  # 整个过程生成的所有rt信息
        self.contains_create_run_sql = False  # 语句是否包括run语句
        self.processing_need_create_map = {}  # 标识是否需要创建表，主要用于排队同时有create及insert的情况

    def create_processing(self):
        sql_index = 0
        # sub_query_has_join是一个标记，用于标识当前次执行是否有包含join的子查询
        sub_query_has_join = False
        # sub_query_processing_id用于记录有join子查询的processing_id，用于一些异常消息的反馈
        sub_query_processing_id = None

        # 本次执行的evaluate信息
        evaluate_map = {}
        for sql in self.sql_list:
            try:
                geog_area_code = TagHelper.get_geog_area_code_by_project(self.project_id)

                sql_index = sql_index + 1
                # 检查是否有子查询
                sql_properties = {"sql": sql}
                sub_query_result = ModelingApiHelper.get_sub_query_by_mlsql_parser(sql_properties)
                logger.info("sub query result:" + json.dumps(sub_query_result))
                has_sub_query = False
                sub_query_fields = []
                sub_table_name = None
                if sub_query_result["has_sub_query"]:
                    has_sub_query = True
                    # 当前语句有子查询
                    sub_query = sub_query_result["sql"]
                    operate_type = sub_query_result["operate_type"]
                    target_entity_name = sub_query_result["target_name"]
                    sub_query_has_join = sub_query_result["sub_query_has_join"]
                    sub_query_processing_id = target_entity_name
                    tmp_sub_query_task = self.parse_subquery_result(
                        sql,
                        sub_query,
                        operate_type,
                        target_entity_name,
                        geog_area_code,
                        sub_query_has_join,
                        sql_index,
                    )
                    sub_table_name = tmp_sub_query_task["table_name"]
                    sub_query_fields = tmp_sub_query_task["fields"]

                sql_parse_args = {
                    "sql": sql,
                    "properties": {
                        "mlsql.only_parse_table": False,
                        "mlsql.geog_area_code": geog_area_code,
                        "mlsql.has_sub_query": has_sub_query,
                    },
                }
                if has_sub_query:
                    sql_parse_args["properties"]["mlsql.sub_table_name"] = sub_table_name
                    sql_parse_args["properties"]["mlsql.sub_table_fields"] = sub_query_fields

                mlsql_parse_result = ModelingApiHelper.list_result_tables_by_mlsql_parser(sql_parse_args)
                logger.info("sql parse result:" + json.dumps(mlsql_parse_result))
                if "result" in mlsql_parse_result:
                    if mlsql_parse_result["result"]:
                        # sql中间的DDL语句，忽略
                        # continue
                        return {"content": mlsql_parse_result["content"]}
                    else:
                        raise SqlParseError(
                            message_kv={
                                "number": sql_index,
                                "content": mlsql_parse_result["content"],
                            }
                        )
                for parsed_task in mlsql_parse_result:
                    self.parse_mlsql_result(parsed_task, sql, sql_index)
                    if "evaluate_map" in parsed_task and parsed_task["evaluate_map"]:
                        evaluate_map = parsed_task["evaluate_map"]
                    else:
                        evaluate_map = {}
            except ApiRequestError as e:
                logger.error("bksql api error:%s" % e)
                raise SqlParseError(message_kv={"number": sql_index, "content": e.message})
            except Exception as e:
                logger.exception(e)
                self.clear(self.processing_ids)
                raise e

        heads, tails = self.find_last_heads_and_tails()
        processing_result = {
            "heads": heads,
            "tails": tails,
            "processing_ids": self.processing_ids,
            "result_table_ids": self.result_table_ids,
            "contains_create_run_sql": self.contains_create_run_sql,
            # 返回包含有join子查询的相关信息
            "disable": sub_query_has_join,
            "sub_query_processing_id": sub_query_processing_id,
            "evaluate_map": evaluate_map,
        }
        return processing_result

    def parse_subquery_result(
        self,
        sql,
        sub_sql,
        operate_type,
        target_entity_name,
        geog_area_code,
        sub_query_has_join,
        sql_index=0,
    ):
        """
        此方法通过调用各种外部的服务，完成对子查询相关信息的解析，最终生成子查询对应的RT与DP
        @param sql: 原始sql,即train,create等
        @param sub_sql: 子查询语句
        @param operate_type: 操作类型，用于存储在processing中
        @param target_entity_name 原sql中生成的表名模型名称
        @param geog_area_code: code
        @param sub_query_has_join 子查询中是否包含join
        @param sql_index: 在整个查询中当前语句的索引
        @return:
        """
        processing_id = None
        try:
            tmp_subquery_task = job_driver.get_sub_query_task(sql, sub_sql, target_entity_name, geog_area_code)
            processing_id = tmp_subquery_task["processing_id"]
            self.tails.append(processing_id)
            self.heads.extend(tmp_subquery_task["parents"])
            # 生成目标表与dp信息
            target_result_table = self.make_target_result_table(processing_id, tmp_subquery_task)
            logger.info("target result table:" + json.dumps(target_result_table))
            data_processing = self.make_data_processing(target_result_table, tmp_subquery_task, processing_id)
            # 子查询生成的dp, generate_type的值为system
            data_processing["generate_type"] = "system"
            logger.info("data processing:" + json.dumps(data_processing))
            task_logic = {
                "type": "data",
                "interpreter": [],
                "processor": tmp_subquery_task["processor"],
                "task_type": PARSED_TASK_TYPE.SUB_QUERY.value,
            }
            processor_logic = {
                "sql": sql,
                "logic": task_logic,
                "mode": "overwrite",
                "sub_query_has_join": sub_query_has_join,
                "operate_type": operate_type,
                "udfs": tmp_subquery_task["udfs"],
                "task_config": tmp_subquery_task,
            }

            # 生成processing信息
            mlsql_processing = self.make_mlsql_processing(processing_id, processor_logic)
            logger.info("mlsql processing:" + json.dumps(mlsql_processing))
            self.save_or_update_processings(mlsql_processing, data_processing, True)
            self.processing_ids.append(processing_id)
            return tmp_subquery_task
        except Exception as e:
            logger.exception(e)
            self.processing_ids.append(processing_id)
            error_msg = _("第{index}个sql执行异常:{error}".format(index=sql_index, error=e))
            logger.error(error_msg)
            raise CreateModelProcessingException(message=error_msg)

    @staticmethod
    def refine_evaluate_info(evaluate_map, algorithm_name, target_fields):
        """
        从参数信息判断用户当前任务是否需要进行评估过程
        注意：这里会变更evaluate_map的内容，比如用户未输入评估信息，而我们又能找到合理的默认的评估信息，就会填充进去
            如果用户输入的评估信息有误则会直接报错
        @param evaluate_map 用户输入的评估信息
        @param algorithm_name 使用算法的名称，需要根据预测算法的框架来获取对应评估算法的框架，如spark和pyspark的算法
                              对应的评估算法都是pyspark，而sklearn算法对应的评估算法就是sklearn
        @param target_fields 当前语句生成的目标表列信息
        @return:
        """
        evaluate_type = evaluate_map["evaluate_type"]
        table_field_map = {item["field"]: item for item in target_fields}
        if algorithm_name.find(".") > 0:
            algorithm_framework = algorithm_name[0 : algorithm_name.find(".")]
        else:
            # 注意：算法默认的框架是spark，但是spark的算法使用的评估算法为pyspark的
            algorithm_framework = "pyspark"
        if "evaluate_function" in evaluate_map and "evaluate_label" in evaluate_map:
            # 用户输入了函数与评估列
            evaluate_function = evaluate_map["evaluate_function"]
            evaluate_label = evaluate_map["evaluate_label"]
            algorithm = AlgorithmHandler.get_algorithm(evaluate_function)
            if not algorithm:
                # 评估算法不存在
                raise EvaluateFunctionNotFoundError(message_kv={"message": evaluate_function})
            if evaluate_label not in table_field_map:
                raise EvaluateLabelNotFoundError(message_kv={"message": evaluate_label})
        elif "evaluate_function" not in evaluate_map and "evaluate_label" not in evaluate_map:
            # 用户两个都没有输入，则取默认值，如果没有默认值则不用进行评估
            defalut_evaluate_function = "{}_{}_evaluation".format(algorithm_framework, evaluate_type)
            algorithm = AlgorithmHandler.get_algorithm(defalut_evaluate_function)
            if not algorithm:
                return False
            evaluate_map["evaluate_function"] = defalut_evaluate_function
            if "label" not in table_field_map:
                return False
            evaluate_map["evaluate_label"] = "label"
        else:
            # 用户仅输入了function 或是 label，则需要校验，校验不通过报错，通过后另外一个值取默认值
            if "evaluate_label" in evaluate_map:
                evaluate_label = evaluate_map["evaluate_label"]
                if evaluate_label not in table_field_map:
                    raise EvaluateLabelNotFoundError(message_kv={"message": evaluate_label})
                defalut_evaluate_function = "{}_{}_evaluation".format(algorithm_framework, evaluate_type)
                algorithm = AlgorithmHandler.get_algorithm(defalut_evaluate_function)
                if not algorithm:
                    raise EvaluateFunctionNotFoundError(message_kv={"message": defalut_evaluate_function})
                evaluate_map["evaluate_function"] = defalut_evaluate_function
            else:
                evaluate_function = evaluate_map["evaluate_function"]
                algorithm = AlgorithmHandler.get_algorithm(evaluate_function)
                if not algorithm:
                    # 评估算法不存在
                    raise EvaluateFunctionNotFoundError(message_kv={"message": evaluate_function})
                if "label" not in table_field_map:
                    raise EvaluateLabelNotFoundError(message_kv={"message": "label"})
                evaluate_map["evaluate_label"] = "label"
        return True

    def parse_mlsql_result(self, parsed_task, sql, sql_index):
        """
        此方法用于解析mlsql语句的主查询, 这个方法有以下几点要说明：
           1. 目前我们不支持同一段sql里创建表，然后再insert；但由于要支持多次insert；
              为了区分这两种情况，引入了processing_id与是否创建表的map:processing_need_create_map
              如果一个processing_id，已经在此map内存在，且值为true，那如果同一个processing_id再出现就会raise error
              如果同一个processing_id，存在且值为false，表示为多次insert，是允许的
           2. 当前的情况下，目前不支持train,create与drop,show等混合使用
           3. 本方法解析的结果不包括mlsql中子查询中的任何信息
        """
        model_need_create = False
        if "table_name" in parsed_task:
            model_name = parsed_task["model_info"]["model_name"]
            # 有生成表，为insert或是create,即run
            """
            parsed_task中的 model_info，可能为实际的模型，也可能为用于特征提取的虚拟模型，
            通过是否有实际的存储(model_storage)来判断，只有实际存在的模型才可加入head并参与血缘分析
            """
            if "model_storage" in parsed_task["model_info"]:
                # head中加入模型
                if model_name not in self.heads:
                    self.heads.append(model_name)
            # tail中加入结果表
            if parsed_task["table_name"] not in self.tails:
                self.tails.append(parsed_task["table_name"])
            processing_id = parsed_task["table_name"]
            model_info = self.format_model_info(model_name)
            if "model_storage" not in parsed_task["model_info"]:
                # 对于虚拟模型，移队其中的Path属性
                del model_info["path"]
            self.result_table_ids.append(parsed_task["table_name"])
            if parsed_task["table_need_create"]:
                self.contains_create_run_sql = True
        else:
            # 没有生成表，说明为train
            # tail中加入模型
            model_name = parsed_task["model_name"]
            self.tails.append(model_name)
            processing_id = model_name
            model_info = self.format_model_info(model_name)
            algorithm_name = parsed_task["processor"]["name"]
            # train操作下，需要生成模型
            model_need_create = True
        try:
            # 以下即为本方法特别说明的第1点
            if processing_id not in self.processing_need_create_map:
                self.processing_need_create_map[processing_id] = parsed_task["table_need_create"]
            else:
                # 表在此次sql段中已经处理过,可能是create或是insert
                if self.processing_need_create_map[processing_id]:
                    # create，目前不支持两次create或是create+insert,所以报错
                    raise SQLForbiddenError(message_kv={"table": processing_id})
            if model_need_create:
                # 这里生成创建模型需要的所有信息
                try:
                    self.create_model_info(self.project_id, model_info, algorithm_name, self.bk_username)
                except EntityAlreadyExistsError as e:
                    raise e
                except Exception as e:
                    raise ModelCreateError(message_kv={"name": model_name, "content": "{}".format(e)})

            # parent中的所有内容加入到head
            for parent_table in parsed_task["parents"]:
                if parent_table not in self.heads:
                    self.check_is_query_set(parent_table)
                    self.heads.append(parent_table)

            # 如果有目标表，则根据解析结果生成目标表的表结构
            target_result_table = {}
            if "table_name" in parsed_task:
                # 有目标表生成，需要生成result_tables
                target_result_table = self.make_target_result_table(parsed_task["table_name"], parsed_task)
                logger.info("target result table:" + json.dumps(target_result_table))

            # 生成data_processing的信息，这里用到了目标表的信息
            data_processing = self.make_data_processing(target_result_table, parsed_task, processing_id)
            logger.info("data processing:" + json.dumps(data_processing))
            # 生成processing_batch_info 需要的信息
            # parsed_task_logics.append(self.get_task_logic(parsed_task))
            task_logic = {
                "type": parsed_task["type"],
                "interpreter": parsed_task["interpreter"],
                "processor": parsed_task["processor"],
                "task_type": parsed_task["task_type"],
            }
            operate_type = self.get_task_operate_type(parsed_task)
            evaluate_map = parsed_task["evaluate_map"]
            if evaluate_map:
                # evaluate_map非空，表示语句可能需要进行评估，且肯定为run语句，需要根据用户输入内容进行判断
                need_evaluate = DatalabTableOperator.refine_evaluate_info(
                    evaluate_map,
                    parsed_task["processor"]["name"],
                    parsed_task["fields"],
                )
            else:
                # 比如train语句，即不需要评估
                need_evaluate = False
            if not need_evaluate:
                del parsed_task["evaluate_map"]

            processor_logic = {
                "sql": sql,
                "logic": task_logic,
                "model": model_info,
                "mode": parsed_task["write_mode"],
                "operate_type": operate_type,
                "task_config": parsed_task,
            }
            mlsql_processing = self.make_mlsql_processing(processing_id, processor_logic)
            logger.info("mlsql batch processing:" + json.dumps(mlsql_processing))

            # 最后实际执行对db的操作
            self.save_or_update_processings(mlsql_processing, data_processing, parsed_task["table_need_create"])
            self.processing_ids.append(processing_id)
        except EntityAlreadyExistsError as e:
            # 如果创建过程中出现的是实体已经存在异常，这种情况下当前的Processing是不可以进行清理的
            error_msg = _("第{index}个个sql执行异常:{error}".format(index=sql_index, error=e.message))
            logger.error(error_msg)
            raise EntityAlreadyExistsError(message=error_msg, code=e.code, errors=e.errors)
        except (SqlParseError, ModelCreateError, RTCreateError, SQLForbiddenError, TableNotQuerySetError) as e:
            self.processing_ids.append(processing_id)
            error_msg = _("第{index}个个sql执行异常:{error}".format(index=sql_index, error=e.message))
            logger.error(error_msg)
            logger.exception(e)
            raise e
        except Exception as e:
            logger.exception(e)
            # 如果是其它异常则需要清理，记录入processing_ids
            self.processing_ids.append(processing_id)
            error_msg = _("第{index}个个sql执行异常:{error}".format(index=sql_index, error=e))
            logger.error(error_msg)
            raise CreateModelProcessingException(message=error_msg)

    def check_is_query_set(self, table_name):
        try:
            data_processing = DataProcessingHelper.get_data_processing(table_name)
            if data_processing:
                processing_type = data_processing["processing_type"]
                if processing_type != "queryset":
                    raise TableNotQuerySetError(message_kv={"name": table_name})
            table_storage = ResultTableHelper.get_result_table_storage(table_name, "hdfs")
            if "hdfs" not in table_storage:
                raise HDFSStorageNotExistsError(message_kv={"name": table_name})
        except (TableNotQuerySetError, HDFSStorageNotExistsError) as e:
            raise e
        except Exception as e:
            logger.error("check query set error:%s" % e)

    def find_last_heads_and_tails(self):
        final_heads = []
        final_tails = []
        for head in self.heads:
            if head not in self.tails:
                final_heads.append(head)
        for tail in self.tails:
            if tail not in self.heads:
                final_tails.append(tail)
        return final_heads, final_tails

    def make_data_processing(self, result_table, parse_task, processing_id):
        """
        根据目标表，解析结果生成data_processing里的所有内容，包括inputs,outputs等，
        如果为train操作，生成的dp内outputs为空
        """
        inputs = []
        parents = parse_task["parents"] or []
        for parent in parents:
            inputs.append(parent)

        outputs = []
        if result_table:
            outputs.append(result_table["result_table_id"])
        data_processings = [
            {
                "processing_id": processing_id,
                "inputs": inputs,
                "outputs": outputs,
            }
        ]
        processing_params = {
            "bkdata_authentication_method": "user",
            "bk_username": self.bk_username,
            "result_tables": [],
            "data_processings": data_processings,
        }

        if result_table:
            processing_params["result_tables"] = [result_table]
        return processing_params

    def make_target_result_table(self, table_name, parse_task):
        """
        根据解析结果中的fields，生成rt的具体结构
        """
        logger.info("make target result table information...")

        table_params = {
            "result_table_id": table_name,
            "fields": [],
            "storages": {"hdfs": {"data_type": "parquet"}},
        }

        """
        table_params = {
            'result_table_id': table_name,
            'fields': []
        }
        """
        field_index = 0
        for field in parse_task["fields"]:
            tmp_field = {}
            field_index += 1
            origins = field.get("origins", [])
            origins = origins if isinstance(origins, (list, tuple)) else [origins]
            origins = ",".join(origins)

            tmp_field["field_type"] = field["type"]
            tmp_field["field_name"] = field["field"]
            tmp_field["field_alias"] = field["field"]
            tmp_field["description"] = field.get("description") or ""
            tmp_field["origins"] = origins
            tmp_field["field_index"] = field_index
            table_params["fields"].append(tmp_field)
        return table_params

    @staticmethod
    def format_model_info(model_name, model_alias=None):
        """
        生成模型的相关信息，model_info同样来自解析结果
        """
        model = {}
        config_id, hdfs_url = ModelingUtils.get_hdfs_cluster_group("hdfs", MLSQL_HDFS_CLUSTER_GROUP)
        model_path = "{hdfs_url}{model_path}{model_name}".format(
            hdfs_url=hdfs_url, model_path=MLSQL_MODEL_PATH, model_name=model_name
        )
        model["name"] = model_name
        if model_alias:
            model["alias"] = model_alias
        model["type"] = "file"
        model["path"] = model_path
        model["config_id"] = config_id
        return model

    @staticmethod
    def create_model_info(project_id, model_info, algorithm_name, bk_username):
        """
        创建模型实体
        """
        # 表与模型名称不可重复
        logger.info("check create model:" + model_info["name"])
        duplicate_table_info = ResultTableHelper.get_result_table(model_info["name"])
        logger.info("check create model result:" + json.dumps(duplicate_table_info))
        if duplicate_table_info:
            raise TableAlreadyExistsError(
                message_kv={"name": model_info["name"]},
                errors={"name": model_info["name"]},
            )
        result_data = MLSqlModelInfoHandler.fetch_model_by_name(model_info["name"])
        if result_data:
            raise ModelAlreadyExistsError(
                message_kv={"name": model_info["name"]},
                errors={"name": model_info["name"]},
            )
        logger.info("model path:" + model_info["path"])
        # create storage
        storage_params = {
            "expires": 5,
            "active": 1,
            "priority": 0,
            "generate_type": "user",
            "physical_table_name": model_info["name"],
            "storage_config": {"path": model_info["path"]},
            "storage_cluster_config_id": model_info["config_id"],
            "created_by": bk_username,
            "updated_by": bk_username,
            "description": model_info["name"],
            "data_type": "parquet",
        }
        model_storage = MLSqlModelInfoHandler.create_model_storage(storage_params)
        # create model
        create_model_args = {
            "model_name": model_info["name"],
            "model_alias": model_info["alias"] if "alias" in model_info else model_info["name"],
            "model_framework": "spark",
            "model_type": "process",
            "project_id": project_id,
            "algorithm_name": algorithm_name,
            "train_mode": "supervised",
            "sensitivity": "private",
            "active": 0,
            "status": "developing",
            "created_by": bk_username,
            "modified_by": bk_username,
            "model_storage_id": model_storage.id,
            "description": model_info["name"],
            "input_standard_config": "{}",
            "output_standard_config": "{}",
        }
        return MLSqlModelInfoHandler.create_mlsql_model_info(create_model_args)

    def get_task_logic(self, task):
        task_logic = {
            "type": task["type"],
            "interpreter": task["interpreter"],
            "processor": task["processor"],
        }
        if "table_name" in task:
            logic_name = task["table_name"]
        else:
            logic_name = task["model_name"]
        return {logic_name: task_logic}

    def create_rt_and_dp(self, data_processing):
        try:
            DataLabHelper.create_queryset_by_dp(self.notebook_id, self.cell_id, data_processing)
        except (ApiRequestError, ApiResultError) as e:
            raise RTCreateError(message_kv={"content": e.message})
        except Exception as e:
            raise RTCreateError(message_kv={"content": "unknown error:%s" % e})

    def clear(self, processing_ids):
        notebook_info = {
            "notebook_id": self.notebook_id,
            "cell_id": self.cell_id,
            "bk_username": self.bk_username,
        }
        ModelingUtils.clear(processing_ids, notebook_info)

    def make_mlsql_processing(self, processing_id, processor_logic, job_id=None, submit_args={}):
        """
        生成datalab processing_batch_info内需要的信息
        """
        mlsql_processing_info = {
            "processing_id": processing_id,
            "batch_id": job_id,
            "processor_type": "sql",
            "processor_logic": json.dumps(processor_logic),
            "schedule_period": "once",
            "count_freq": 0,
            "delay": 0,
            "submit_args": submit_args,
            "component_type": self.component_type,
            "created_by": self.bk_username,
            "updated_by": self.bk_username,
            "description": processing_id,
        }
        return mlsql_processing_info

    # 这里返回标识出是创建还是更新操作，如果是更新操作，后续是不应该进行清理操作的
    def save_or_update_processings(self, mlsql_processing_info, dp_processing, table_need_create):
        processing = ProcessingBatchInfo.objects.filter(processing_id=mlsql_processing_info["processing_id"])
        if not processing.exists():
            ProcessingBatchInfo.objects.create(**mlsql_processing_info)
            if dp_processing:
                self.create_or_update_dp(dp_processing, table_need_create)
        else:
            if table_need_create:
                raise TableAlreadyExistsError(
                    message_kv={"name": mlsql_processing_info["processing_id"]},
                    errors={"name": mlsql_processing_info["processing_id"]},
                )
            else:
                # todo:这里逻辑需要优化，insert操作的时候processing_batch表的信息要如何存在
                ProcessingBatchInfo.objects.filter(processing_id=mlsql_processing_info["processing_id"]).update(
                    processor_logic=mlsql_processing_info["processor_logic"]
                )
                if dp_processing:
                    self.create_or_update_dp(dp_processing, table_need_create)
                return True
        return False

    def create_or_update_dp(self, data_processing, table_need_create):
        data_processing_only = data_processing["data_processings"][0]
        existing_data_processing = DataProcessingHelper.get_data_processing(data_processing_only["processing_id"])
        if existing_data_processing:
            if table_need_create:
                raise DPAlreadyExistsError(
                    message_kv={"name": data_processing_only["processing_id"]},
                    errors={"name": data_processing_only["processing_id"]},
                )
            else:
                existing_inputs = existing_data_processing["inputs"]
                inputs = data_processing_only["inputs"]
                existing_input_ids = [str(existing_dp_input["data_set_id"]) for existing_dp_input in existing_inputs]
                dp_need_update = False
                for dp_input in inputs:
                    if dp_input not in existing_input_ids:
                        dp_need_update = True
                        dp_input_info = existing_inputs[0].copy()
                        dp_input_info["data_set_id"] = dp_input
                        existing_inputs.append(dp_input_info)
                if dp_need_update:
                    DataProcessingHelper.update_data_processing(existing_data_processing)
        else:
            self.create_rt_and_dp(data_processing)

    def delete_processing(self, process_id, with_data):
        TableOperator.delete_processing(process_id, with_data)


class DataflowTableOperator(TableOperator):
    def __init__(self, bulk_params):
        self.model_config = bulk_params.model_config
        self.submit_args = bulk_params.submit_args
        self.component_type = bulk_params.component_type
        schedule_info = self.submit_args["node"]["schedule_info"]
        self.schedule_period = schedule_info["schedule_period"]
        self.count_freq = schedule_info["count_freq"]
        self.delay = schedule_info["delay"]
        self.project_id = bulk_params.project_id
        self.bk_biz_id = bulk_params.bk_biz_id
        self.tags = bulk_params.tags

    def deal_with_processing(self, is_update):
        node_info = self.submit_args["node"]
        dependence_info = self.submit_args["dependence"]
        deployed_model_info = node_info["deployed_model_info"]

        # 获取最终输出表的信息
        output_info = get_output_info(node_info)
        # 从前端传递的内容中解析出输入表的信息
        input_info = get_input_info(dependence_info)
        # 得到窗口信息
        window_info = get_window_info(input_info["name"], dependence_info, node_info)
        # 获取参数信息
        model_params = node_info["deployed_model_info"]["model_params"]
        # 获取当前真实的配置配置
        # 由于模型的配置有自己的更新策略(自动或手动)，所以用户在`更新`节点的信息的时候，有些情况下是不需要变更模型配置的
        # 因此在节点更新的操作中，使用的model_config需要结合情况重新获取
        real_model_config = None
        processing_id = output_info["name"]
        if is_update:
            processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
            # 获取当前使用的模型版本号
            current_submit_args = json.loads(processing_info.submit_args)
            current_deployed_model_info = current_submit_args["node"]["deployed_model_info"]
            if deployed_model_info["input_model"] != current_deployed_model_info["input_model"]:
                # 模型本身发生变化，使用最新值
                real_model_config = self.model_config
            elif deployed_model_info["model_version"] == current_deployed_model_info["model_version"]:
                # 模型未变化，且版本未发生变化，可以直接应用前端传入的model_config（因为此前后台是一致的）
                real_model_config = self.model_config
            else:
                # 模型没变，但版本变化，此时需要取原有的模型配置:包括配置文件，使用的版本信息都要从原有Processing中取
                model_id = current_deployed_model_info["input_model"]
                version_index = current_deployed_model_info["model_version"]
                current_release = self.get_model_release_by_version(model_id, version_index)
                real_model_config = json.loads(current_release.model_config_template)
                # 更新用户传入的submit_args中的版本信息(目前机制下用户传入的肯定是最新的版本号，但用户实际用的不一定是最新版本)
                deployed_model_info["model_version"] = current_deployed_model_info["model_version"]
        else:
            # 新创建的时候需要计算新的配置
            real_model_config = self.model_config

        # 得到最新的模型配置信息，因为model_params有可能发生变化，所以不管保存还是更新，最新配置都要重新计算
        new_model_config = generate_complete_config(
            real_model_config, input_info, output_info, window_info, model_params
        )
        target_rt_info = {
            "description": output_info["alias"],
            "fields": output_info["fields"],
        }
        # 得到创建rt时的parent信息，用于生成DP里的Input信息
        parent_tables = {"parents": []}
        for source_id in new_model_config["source"]:
            source_object_info = new_model_config["source"][source_id]
            if source_object_info["type"] == "data":
                parent_tables["parents"].append(source_id)
        # 生成rt的信息
        result_table_info = self.make_target_result_table(output_info["name"], target_rt_info)
        logger.info("result table info:" + json.dumps(result_table_info))
        # 生成DP的信息
        data_processing_info = self.make_data_processing(result_table_info, parent_tables, output_info["name"])
        logger.info("data processing info:" + json.dumps(data_processing_info))
        # 生成processing_info的信息
        processing_info = self.make_mlsql_processing(output_info["name"], new_model_config)
        logger.info("batch processing info:" + json.dumps(processing_info))
        model_instance_info = self.make_model_instance(
            processing_id, deployed_model_info, input_info, output_info, model_params
        )
        logger.info("model instance info:" + json.dumps(model_instance_info))
        # 保存所有信息（创建表、BP以及processing）
        self.save_or_update_processings(processing_info, data_processing_info, is_update)

        # 向meta同步模型的使用情况
        logger.info("save model instance...")
        self.save_or_update_model_instance(model_instance_info, is_update)
        logger.info("create mlsql info successfully")
        # 返回结果，其中processing_id，即为最终输出的表名称
        processing_result = {
            "processing_id": output_info["name"],
            "result_table_ids": [output_info["name"]],
        }
        return processing_result

    def get_model_release_by_version(self, model_id, version_index):
        params = {"model_id": model_id, "active": 1, "version_index": version_index}
        return ModelReleaseHandler.where(**params)[0]

    def create_processing(self):
        return self.deal_with_processing(False)

    def update_processing(self):
        update_result = self.deal_with_processing(True)
        processing_id = update_result["processing_id"]
        batch_processing_info = ProcessingBatchInfoHandler.get_proc_batch_info_by_proc_id(processing_id)
        processing_job_info_list = ProcessingJobInfoHandler.get_proc_job_info_by_prefix(processing_id)
        for processing_job_info in processing_job_info_list:
            new_job_config = {
                "count_freq": batch_processing_info.count_freq,
                "processor_type": batch_processing_info.processor_type,
                "component_type": processing_job_info.component_type,
                "schedule_period": batch_processing_info.schedule_period,
                "delay": batch_processing_info.delay,
                "use_type": USE_SCENARIO.DATAFLOW.value,
                "submit_args": batch_processing_info.submit_args,
                "processor_logic": batch_processing_info.processor_logic,
            }
            params = {
                "bk_username": get_request_username(),
                "job_id": processing_job_info.job_id,
                "job_config": json.dumps(new_job_config),
                "jobserver_config": json.loads(processing_job_info.jobserver_config),
            }
            ProcessingJobInfoHandler.update_proc_job_info(params)
        return update_result

    def make_mlsql_processing(self, processing_id, processor_logic):
        """
        生成processing_batch_info内需要的信息
        """
        mlsql_processing_info = {
            "processing_id": processing_id,
            "batch_id": None,
            "processor_type": "sql",
            "processor_logic": json.dumps(processor_logic),
            "schedule_period": self.schedule_period,
            "count_freq": self.count_freq,
            "delay": self.delay,
            "submit_args": json.dumps(self.submit_args),
            "component_type": self.component_type,
            "created_by": get_request_username(),
            "updated_by": get_request_username(),
            "description": processing_id,
        }
        return mlsql_processing_info

    def make_model_instance(self, processing_id, deployed_model_info, input_info, output_info, model_params):
        """
        生成model_instance相关信息
        @param processing_id: processing id
        @param deployed_model_info: 应用的模型信息
        @param input_info: 输入表信息
        @param output_info: 输出表信息
        @param model_params: 模型参数信息
        @return:
        """
        current_release = self.get_model_release_by_version(
            deployed_model_info["input_model"], deployed_model_info["model_version"]
        )
        model_instance = {
            "model_release_id": current_release.id,
            "model_id": deployed_model_info["input_model"],
            "project_id": self.project_id,
            "instance_config": json.dumps({"input_params": model_params, "output_params": output_info["fields"]}),
            "upgrade_config": "{}",
            "sample_feedback_config": "{}",
            "serving_mode": "batch_model",
            "execute_config": "{}",
            "data_processing_id": processing_id,
            "input_result_table_ids": json.dumps([input_info["name"]]),
            "output_result_table_ids": json.dumps([output_info["name"]]),
            "created_by": get_request_username(),
            "updated_by": get_request_username(),
            "protocol_version": current_release.protocol_version,
            "basic_model_id": current_release.basic_model_id,
        }
        return model_instance

    def make_data_processing(self, result_table, parse_task, processing_id, args=[]):
        inputs = []
        parents = parse_task["parents"] or []
        for parent in parents:
            parent_table_info = ResultTableHelper.get_result_table(parent, related=["storages"])
            parent_storage_cluster_config_id = parent_table_info["storages"]["hdfs"]["storage_cluster"][
                "storage_cluster_config_id"
            ]
            tmp_dict = {
                "data_set_type": "result_table",
                "data_set_id": parent,
                "storage_cluster_config_id": parent_storage_cluster_config_id,
                "storage_type": "storage",
                "tags": self.tags,
            }
            inputs.append(tmp_dict)
        outputs = []
        if result_table:
            outputs.append(
                {
                    "data_set_type": "result_table",
                    "data_set_id": result_table["result_table_id"],
                }
            )
        processing_params = {
            "project_id": self.project_id,
            "processing_id": processing_id,
            "processing_alias": processing_id,
            "processing_type": "batch",
            "generate_type": USER_GENERATE_TYPE,
            "bk_username": get_request_username(),
            "description": processing_id,
            "inputs": inputs,
            "outputs": outputs,
            "tags": self.tags,
        }
        if result_table:
            processing_params["result_tables"] = [result_table]
        return processing_params

    def make_mlsql_processigg(self, processing_id, processor_logic, job_id):
        mlsql_processing_info = {
            "processing_id": processing_id,
            "batch_id": job_id,
            "processor_type": "sql",
            "processor_logic": json.dumps(processor_logic),
            "schedule_period": "once",
            "count_freq": 0,
            "delay": 0,
            "submit_args": "",
            "component_type": "spark_mllib",
            "created_by": get_request_username(),
            "updated_by": get_request_username(),
            "description": processing_id,
        }
        return mlsql_processing_info

    def make_target_result_table(self, table_name, parse_task, args=[]):
        logger.info("make target result table information...")
        table_params = {
            "bk_biz_id": self.bk_biz_id,
            "project_id": self.project_id,
            "result_table_id": table_name,
            "result_table_name": table_name[(len(str(self.bk_biz_id)) + 1) :],
            "result_table_name_alias": parse_task["description"],
            "generate_type": USER_GENERATE_TYPE,
            "bk_username": get_request_username(),
            "description": parse_task["description"],
            "count_freq": 0,
            "fields": [],
            "tags": self.tags,
        }
        """
        table_params['storages'] = {
            'hdfs': {
                'data_type': 'iceberg'
            }
        }
        """
        field_index = 0
        for field in parse_task["fields"]:
            tmp_field = {}
            field_index += 1
            origins = field.get("origin", [])
            origins = origins if isinstance(origins, (list, tuple)) else [origins]
            origins = ",".join(origins)
            tmp_field["field_type"] = field["type"]
            tmp_field["field_name"] = field["field"]
            tmp_field["field_alias"] = field["description"]
            tmp_field["description"] = field["description"]
            tmp_field["origins"] = origins
            tmp_field["field_index"] = field_index
            table_params["fields"].append(tmp_field)
        return table_params

    def create_or_update_dp(self, data_processing):
        """
        注意：这里与datalab的方式是不一样的，在flow的场景下：
        1. 在创建Process的时候不需要关联hdfs存储，hdfs存储是用户在拖存储节点的时候创建的
        2. 由1导致，我们在创建Process的时候是不知道某个RT的具体存储位置的，因此这个函数的返回没有任何意义
        """
        processing_id = data_processing["processing_id"]
        exist_data_processing = DataProcessingHelper.get_data_processing(processing_id)
        if exist_data_processing:
            DataProcessingHelper.update_data_processing(data_processing)
        else:
            DataProcessingHelper.set_data_processing(data_processing)

    def create_hdfs_storage(self, result_table_id):
        config_id, hdfs_url = ModelingUtils.get_hdfs_cluster_group("hdfs", MLSQL_HDFS_CLUSTER_GROUP)
        params = {
            "cluster_name": MLSQL_HDFS_CLUSTER_GROUP,
            "cluster_type": "hdfs",
            "expires": "7d",
            "result_table_id": result_table_id,
            "storage_config": "{}",
            "generate_type": "system",
        }
        result = StorekitHelper.create_physical_table(**params)
        StorekitHelper.prepare(result_table_id, "hdfs")
        return "{hdfs_url}{hdfs_path}".format(hdfs_url=hdfs_url, hdfs_path=result["physical_table_name"])

    def save_processings(self, mlsql_processing_info, dp_processing):
        # 创建DP及RT
        logger.info("create data processing...")
        self.create_or_update_dp(dp_processing)
        # 创建DP
        logger.info("create batch processing...")
        ProcessingBatchInfo.objects.create(**mlsql_processing_info)

    def update_processings(self, mlsql_processing_info, dp_processing):
        ProcessingBatchInfo.objects.filter(processing_id=mlsql_processing_info["processing_id"]).update(
            processor_logic=mlsql_processing_info["processor_logic"],
            submit_args=mlsql_processing_info["submit_args"],
        )

    def save_or_update_processings(self, mlsql_processing_info, dp_processing, is_update=True):
        if not is_update:
            self.save_processings(mlsql_processing_info, dp_processing)
        else:
            self.update_processings(mlsql_processing_info, dp_processing)

    def save_or_update_model_instance(self, model_instance_info, is_update):
        model_release_id = model_instance_info["model_release_id"]
        data_processing_id = model_instance_info["data_processing_id"]
        if is_update:
            # 更新节点信息应用模型也有可能发生变化，根据data_processing_id检查是否有变化
            model_instance_list = ModelInstanceHandler.get_model_instance_by_processing_id(data_processing_id)
            if not model_instance_list.exists():
                # 没有查询到
                logger.info(json.dumps(model_instance_info))
                ModelInstanceHandler.save(**model_instance_info)
            else:
                model_instance = model_instance_list[0]
                logger.info(model_instance.model_release_id)
                logger.info(model_release_id)
                logger.info(json.dumps(model_instance_info))
                if model_instance.model_release_id != model_release_id:
                    # 模型版本发生了变更，需要更新
                    model_instance_info["id"] = model_instance.id
                    ModelInstanceHandler.save(**model_instance_info)
                    # ModelInstanceHandler.update_model_instance_by_id(model_instance.id, **model_instance_info)
        else:
            ModelInstanceHandler.save(**model_instance_info)

    def delete_processing(self, process_id, with_data):
        TableOperator.delete_processing(process_id, with_data)
