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
import traceback

import numpy
import pandas
from bkdata_datalake import tables
from command.api.auth import (
    check_project_object_auth,
    check_user_object_auth,
    retrieve_cluster_group,
)
from command.api.datahub import (
    destroy_rt_storage_relation,
    destroy_storage,
    get_hdfs_conf,
    get_transport_result,
    retrieve_cluster_info,
    transport_data,
)
from command.api.datalab import (
    cancel_relate_output,
    create_rt,
    destroy_rt,
    get_notebook_id,
    relate_output,
    retrieve_outputs,
)
from command.api.meta import get_result_table_info
from command.base_logger import BaseLogger
from command.constants import (
    AFFECTED_RECORDS,
    ALL,
    AND,
    BIZ_COMMON_ACCESS,
    CLICKHOUSE,
    CLUSTER_GROUP,
    CLUSTER_NAME,
    COLUMN_NAME,
    COMMITTER,
    CONDITION,
    CREATE,
    DATA_TYPE,
    DELETE,
    DROP,
    FIELD_ALIAS,
    FIELD_INDEX,
    FIELD_NAME,
    FIELD_TYPE,
    FIELDS,
    HDFS,
    ICEBERG,
    IGNITE,
    INFO,
    INPUT_RT,
    INSERT,
    NAME,
    NOTEBOOK_CREATE,
    NOTEBOOK_INSERT,
    OPERATOR,
    OR,
    OUTPUT_RT,
    PANDAS_TYPE_CONVERT,
    PHYSICAL_TABLE_NAME,
    PROCESSING_TYPE,
    QUERYSET,
    RESULT_DICT,
    RESULT_TABLE_ID,
    RESULT_TABLES,
    RIGHT_VALUE,
    SAVE,
    SCHEMA,
    SINGLE_WRITE_NUMS,
    SNAPSHOT,
    SQL,
    STORAGE_CONFIG,
    STORAGES,
    STRING,
    SUPPORT_SAVE_STORAGES,
    TYPE,
    UPDATE,
)
from command.exceptions import AuthException, ExecuteException
from command.settings import JUPYTERHUB_USER
from command.utils import get_bk_username, get_cell_id


class ResultTable(object):
    @staticmethod
    @BaseLogger.save_log_deco(content_type=SAVE)
    def save(src_rt, dst_rt, storage=HDFS, storage_config={}, cluster_name=None):
        """
        create normal result table with storage

        :param src_rt: source result table
        :param dst_rt: destination result table
        :param storage: storage type
        :param storage_config: storage config
        :param cluster_name: cluster name

        :return: execute result
        """
        if JUPYTERHUB_USER.isdigit():
            project_id = JUPYTERHUB_USER
            bk_username = get_bk_username()
            check_user_object_auth(bk_username, [project_id])
        else:
            raise ExecuteException('save操作失败：暂时不支持在"个人"笔记内使用save方法，请在"项目"笔记内使用')
        ResultTableHelper.check_result_table_save_auth(project_id, src_rt, dst_rt, storage, cluster_name)
        storages = {storage: {CLUSTER_NAME: cluster_name}}
        if storage in [CLICKHOUSE, IGNITE]:
            storages[storage][STORAGE_CONFIG] = json.dumps(storage_config)
        result_table_conf = {
            INPUT_RT: src_rt,
            OUTPUT_RT: dst_rt,
            FIELDS: [],
            STORAGES: storages,
            PROCESSING_TYPE: SNAPSHOT,
        }
        create_rt(result_table_conf, bk_username, get_notebook_id())

        # 迁移数据
        transport_id = transport_data(src_rt, HDFS, dst_rt, storage)
        transport_result = get_transport_result(transport_id)
        return dict(message=transport_result, result_table_ids=[src_rt])

    @staticmethod
    @BaseLogger.save_log_deco(content_type=CREATE)
    def create(dataframe, result_table_id):
        """
        基于dataframe数据创建结果表

        :param dataframe: pandas dataframe
        :param result_table_id: result_table_id
        :return: create result
        """
        ResultTableHelper.validate_result_table_id(result_table_id)
        bk_username = get_bk_username()

        # 校验用户是否有相应业务下创建结果表的权限
        check_user_object_auth(bk_username, [result_table_id.split("_", 1)[0]], BIZ_COMMON_ACCESS)
        # 目前只支持数据格式是dataframe
        if isinstance(dataframe, pandas.DataFrame) and not dataframe.empty:
            notebook_id = get_notebook_id()
            result_table_conf = ResultTableHelper.generate_result_table_conf(result_table_id, dataframe)
            create_rt(result_table_conf, bk_username, notebook_id)

            table = IcebergHelper.load_iceberg_table(result_table_id)
            IcebergHelper.write_data_to_iceberg(table, dataframe, result_table_id, notebook_id, bk_username)
            result_tables = [{NAME: result_table_id, SQL: "ResultTable.create"}]
            relate_output(notebook_id, get_cell_id(), [], result_tables, bk_username)
            return dict(message="创建成功", result_table_ids=[result_table_id])
        else:
            raise ExecuteException("数据格式不正确，需要为dataframe格式且不为空")

    @staticmethod
    @BaseLogger.save_log_deco(content_type=DROP)
    def drop(result_table_id, storage):
        """
        删除结果表，或者单删结果表关联的某种存储

        :param result_table_id: 结果表id
        :param storage: 存储类型，可选 all
        :return: 删除结果
        """
        ResultTableHelper.check_result_table_update_auth(result_table_id)
        notebook_id = get_notebook_id()
        if storage == ALL:
            destroy_rt(result_table_id, notebook_id, get_bk_username())
            cancel_relate_output(notebook_id, [], [result_table_id])
        elif storage in get_result_table_info(result_table_id).get(STORAGES):
            destroy_storage(storage, result_table_id)
            destroy_rt_storage_relation(result_table_id, storage)
        else:
            raise ExecuteException("删除失败，结果表没有关联%s存储" % storage)
        return dict(message="删除成功", result_table_ids=[result_table_id])

    @staticmethod
    @BaseLogger.save_log_deco(content_type=DELETE)
    def delete(result_table_id, conditions):
        """
        删除数据，目前只支持删除hdfs存储的数据

        :param result_table_id: 结果表id
        :param conditions: 删除条件
        :return: 删除结果
        """
        ResultTableHelper.check_result_table_update_auth(result_table_id)
        table = IcebergHelper.load_iceberg_table(result_table_id)
        expr = IcebergHelper.build_field_condition_expr(table, conditions)
        metric = tables.delete_table_records(table, expr)
        return dict(message="删除成功，删除 %s 条数据" % metric[AFFECTED_RECORDS], result_table_ids=[result_table_id])

    @staticmethod
    @BaseLogger.save_log_deco(content_type=UPDATE)
    def update(result_table_id, conditions, values):
        """
        更新数据，目前只支持更新hdfs存储的数据

        :param result_table_id: 结果表id
        :param conditions: 更新条件
        :param values: 更新内容
        :return: 更新结果
        """
        ResultTableHelper.check_result_table_update_auth(result_table_id)
        table = IcebergHelper.load_iceberg_table(result_table_id)
        expr = IcebergHelper.build_field_condition_expr(table, conditions)
        metric = tables.update_table(table, expr, values)
        return dict(message="修改成功，修改 %s 条数据" % metric[AFFECTED_RECORDS], result_table_ids=[result_table_id])

    @staticmethod
    @BaseLogger.save_log_deco(content_type=INSERT)
    def insert(result_table_id, column_names, column_values):
        """
        插入数据，目前只支持对hdfs存储执行插入操作

        :param result_table_id: 结果表id
        :param column_names: 字段名
        :param column_values: 字段类型
        :return: 插入结果
        """
        ResultTableHelper.check_result_table_update_auth(result_table_id)
        table = IcebergHelper.load_iceberg_table(result_table_id)
        tables.append_table(table, column_names, column_values, {COMMITTER: get_bk_username(), INFO: NOTEBOOK_INSERT})
        return dict(message="数据插入成功", result_table_ids=[result_table_id])


class IcebergHelper(object):
    @staticmethod
    def load_iceberg_table(result_table_id):
        """
        加载数据湖中的表对象

        :param result_table_id: 结果表id
        :return: iceberg表
        """
        hdfs_conf = get_hdfs_conf(result_table_id)
        physical_table_name = hdfs_conf[PHYSICAL_TABLE_NAME]
        table = tables.load_table(physical_table_name.split(".")[0], physical_table_name.split(".")[1], hdfs_conf)
        return table

    @staticmethod
    def write_data_to_iceberg(table, df, result_table_id, notebook_id, bk_username):
        """
        往iceberg表中写数据

        :param table: iceberg表
        :param df: dataframe
        :param result_table_id: 结果表id
        :param notebook_id: 笔记id
        :param bk_username: 用户名
        :return: 写数据结果
        """
        try:
            IcebergHelper.convert_unexpected_value(df)
            write_current_num = 0
            for index in range(SINGLE_WRITE_NUMS, len(df.values), SINGLE_WRITE_NUMS):
                tables.append_table(
                    table,
                    df.columns,
                    df.values[index - SINGLE_WRITE_NUMS : index],
                    {COMMITTER: bk_username, INFO: NOTEBOOK_CREATE},
                )
                write_current_num = index
            tables.append_table(
                table, df.columns, df.values[write_current_num:], {COMMITTER: bk_username, INFO: NOTEBOOK_CREATE}
            )
        except Exception as e:
            print(traceback.format_exc())
            destroy_rt(result_table_id, notebook_id, bk_username)
            raise ExecuteException("数据写入异常，异常信息：%s" % str(e))

    @staticmethod
    def convert_unexpected_value(df):
        """
        转换规则：
        规则1：df中两列，一列为整型，一列为浮点型，dtypes分别显示为int64和float64类型，但是df.values显示的内容都为float型，造成类型不匹配，因此需转换
        规则2：df中的bool类型需转换为平台的string类型
        规则3：pandas的nan值实质为numpy.float64类型
              如果字符型列中存在nan值，dtypes显示仍是object，但是此值的判断类型是float型，造成类型不匹配，因此需转换
              如果整型列或浮点型列中存在nan值，dtypes显示是float64，无需做转换

        :param df: dataframe数据
        """
        for column in df.columns:
            df[column] = (
                df[column].astype(pandas.Int64Dtype())
                if df[column].dtypes == numpy.int64
                else df[column].map(RESULT_DICT)
                if df[column].dtypes == numpy.bool8
                else df[column].replace({numpy.nan: ""})
                if df[column].dtypes == numpy.object0 and df[column].isnull().any()
                else df[column]
            )

    @staticmethod
    def build_field_condition_expr(table, conditions):
        """
        构造字段条件表达式

        :param table: iceberg表
        :param conditions: 构建条件
        :return: 字段的表达式
        """
        table_info = json.loads(str(table.info()))
        column_with_type = {filed_info[NAME]: filed_info[TYPE] for filed_info in table_info[SCHEMA]}

        condition, expr = None, None
        for index, condition_info in enumerate(conditions):
            if condition_info[COLUMN_NAME] not in column_with_type:
                raise ExecuteException("%s字段不存在" % condition_info[COLUMN_NAME])
            new_expr = tables.build_field_condition_expr(
                condition_info[COLUMN_NAME],
                column_with_type.get(condition_info[COLUMN_NAME]),
                condition_info[OPERATOR],
                condition_info[RIGHT_VALUE],
            )
            expr = (
                new_expr
                if index == 0
                else tables.build_and_condition(expr, new_expr)
                if condition == AND
                else tables.build_or_condition(expr, new_expr)
                if condition == OR
                else expr
            )
            condition = condition_info.get(CONDITION)
        return expr


class ResultTableHelper(object):
    @staticmethod
    def check_result_table_save_auth(project_id, src_rt, dst_rt, storage, cluster_name):
        """
        校验queryset上线相关权限

        :param project_id: 项目id
        :param src_rt: 源rt
        :param dst_rt: 目标rt
        :param storage: 存储类型
        :param cluster_name: 集群名
        :return: 校验结果
        """
        # 校验源表是不是queryset类型
        result_table_info = get_result_table_info(src_rt)
        if result_table_info[PROCESSING_TYPE] != QUERYSET:
            raise ExecuteException("save操作失败，当前仅支持上线queryset类型结果表")

        # 校验项目有没有源rt的查询权限
        check_project_object_auth(project_id, [src_rt])

        # 校验目标表命名是否符合规范
        ResultTableHelper.validate_result_table_id(dst_rt)
        if src_rt.split("_", 1)[0] != dst_rt.split("_", 1)[0]:
            raise ExecuteException("save操作失败：源表和目标表必须属于同一个业务")

        # 校验是否有存储和集群的权限
        if storage not in SUPPORT_SAVE_STORAGES:
            raise ExecuteException("save操作失败：存储类型暂时只支持%s" % SUPPORT_SAVE_STORAGES)
        if cluster_name:
            cluster_groups = retrieve_cluster_group(project_id)
            cluster_info = retrieve_cluster_info(storage, cluster_name)
            if cluster_info.get(CLUSTER_GROUP) not in cluster_groups:
                raise AuthException("save操作失败：该项目没有此集群存储权限，请联系助手开通权限")

    @staticmethod
    def check_result_table_update_auth(result_table_id):
        """
        校验笔记有没有结果表修改权限

        :param result_table_id: 结果表id
        """
        result_table_info = get_result_table_info(result_table_id)
        if result_table_info[PROCESSING_TYPE] != QUERYSET:
            raise ExecuteException("结果表操作失败，当前仅支持更改queryset类型结果表")
        outputs_info = retrieve_outputs(get_notebook_id())
        result_tables = [result_table[NAME] for result_table in outputs_info[RESULT_TABLES]]
        if result_table_info[RESULT_TABLE_ID] not in result_tables:
            raise AuthException("此笔记没有结果表 %s 的修改权限" % result_table_info[RESULT_TABLE_ID])

    @staticmethod
    def validate_result_table_id(result_table_id):
        """
        校验结果表id是否符合要求

        :param result_table_id: 结果表id
        """
        biz_id_and_rt_name = result_table_id.split("_", 1)
        if len(biz_id_and_rt_name) < 2:
            raise ExecuteException('结果表定义不符合规范，请使用 "业务id_表名" 的命名规则')
        if not biz_id_and_rt_name[0].isdigit():
            raise ExecuteException("业务id定义不符合规范，请填写合法的整数值")

    @staticmethod
    def generate_result_table_conf(result_table_id, df=None):
        """
        生成结果表配置

        :param result_table_id: 结果表id
        :param df: dataframe数据
        :return: 结果表配置信息
        """
        fields = [
            {
                FIELD_TYPE: PANDAS_TYPE_CONVERT.get(str(df[column].dtypes), STRING),
                FIELD_NAME: column,
                FIELD_ALIAS: column,
                FIELD_INDEX: index,
            }
            for index, column in enumerate(df.columns)
        ]
        return {
            OUTPUT_RT: result_table_id,
            FIELDS: fields,
            STORAGES: {HDFS: {DATA_TYPE: ICEBERG}},
            PROCESSING_TYPE: QUERYSET,
        }
