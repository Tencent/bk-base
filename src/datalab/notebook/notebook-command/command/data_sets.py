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

import os

from command.api.auth import check_project_object_auth, check_user_object_auth
from command.api.datahub import get_file_config
from command.api.datalab import get_query_tasks, get_user_projects
from command.api.queryengine import get_query_result
from command.base_logger import BaseLogger
from command.constants import (
    COMMON,
    DATASETS,
    DIR_PATH,
    DOWNLOAD_FILE,
    FILE_NAME,
    FILE_PATH,
    HDFS_HOST,
    HDFS_PATH,
    HDFS_PORT,
    PERSONAL,
    PROJECT_ID,
    QUERY_LIST,
    QUERY_NAME,
    QUERY_TASK_ID,
    RAW_DATA,
    RAW_DATA_ID,
    RAW_DATA_INFO_DICT,
    RAW_DATA_QUERY,
    SHOW_NOTEBOOK_FILE,
    SQL_TEXT,
)
from command.exceptions import ExecuteException
from command.settings import JUPYTERHUB_USER
from command.util.result_set import ResultSet
from command.utils import get_bk_username, transform_column_name


@BaseLogger.save_log_deco(content_type=DATASETS)
def datasets(query_name):
    if JUPYTERHUB_USER.isdigit():
        project_id = JUPYTERHUB_USER
        project_type = COMMON
    else:
        bk_username = JUPYTERHUB_USER
        projects = get_user_projects(bk_username)
        project_id = projects[0][PROJECT_ID]
        project_type = PERSONAL

    query_tasks = get_query_tasks(project_id, project_type)
    query_task = next((query_task for query_task in query_tasks if query_task[QUERY_NAME] == query_name), None)
    if not query_task:
        raise ExecuteException("查询任务 %s 不存在" % query_name)

    query_task_id = query_task[QUERY_TASK_ID]
    if not query_task_id:
        raise ExecuteException("请确保查询任务 %s 存在结果集" % query_name)
    sql = query_task[SQL_TEXT]
    query_result = get_query_result(query_task_id, sql)
    return dict(data=query_result)


class DataSet(object):
    @staticmethod
    @BaseLogger.save_log_deco(content_type=SHOW_NOTEBOOK_FILE)
    def show_notebook_file(raw_data_id=None):
        file_list = get_dir_files(raw_data_id)
        file_set = ResultSet(file_list, list(RAW_DATA_INFO_DICT.values()), len(file_list), "", QUERY_LIST)
        return dict(data=file_set)

    @staticmethod
    @BaseLogger.save_log_deco(content_type=DOWNLOAD_FILE)
    def download_file(raw_data_id, file_name):
        import pyhdfs

        file_config = get_file_config(raw_data_id, file_name)

        object_auth_func = check_project_object_auth if JUPYTERHUB_USER.isdigit() else check_user_object_auth
        object_auth_func(JUPYTERHUB_USER, [file_config[RAW_DATA_ID]], RAW_DATA_QUERY)

        hosts = ["{}:{}".format(host, file_config[HDFS_PORT]) for host in file_config[HDFS_HOST].split(",")]
        fs = pyhdfs.HdfsClient(hosts=hosts, user_name=get_bk_username())

        # 判断业务目录是否存在，不存在即创建
        biz_dir_path = "{}/{}".format(DIR_PATH, raw_data_id)
        if not os.path.exists(biz_dir_path):
            os.makedirs(biz_dir_path)
        # 加载文件
        file_path = "{}/{}".format(biz_dir_path, file_name)
        fs.copy_to_local(file_config[HDFS_PATH], file_path)
        return dict(message="文件加载成功，文件路径：%s" % file_path)


def get_dir_files(raw_data_id):
    """
    获取笔记已加载的文件列表

    :param raw_data_id: 数据源id
    :return: 文件列表
    """
    notebook_files = []
    if raw_data_id:
        if not os.path.exists("{}/{}".format(DIR_PATH, raw_data_id)):
            raise ExecuteException("数据源文件未加载进笔记，请先执行 DataSet.download_file 方法加载文件")
        notebook_files = [
            {RAW_DATA_ID: raw_data_id, FILE_NAME: file, FILE_PATH: "{}/{}/{}".format(DIR_PATH, raw_data_id, file)}
            for file in os.listdir("{}/{}".format(DIR_PATH, raw_data_id))
        ]
    else:
        raw_data_ids = []
        for dirnum, (subdir, dirs, files) in enumerate(os.walk(DIR_PATH)):
            if dirnum == 0:
                raw_data_ids = dirs
            else:
                raw_data_files = [
                    {
                        RAW_DATA_ID: raw_data_ids[dirnum - 1],
                        FILE_NAME: file,
                        FILE_PATH: "{}/{}/{}".format(DIR_PATH, raw_data_ids[dirnum - 1], file),
                    }
                    for file in files
                ]
                notebook_files.extend(raw_data_files)
    file_list = transform_column_name(notebook_files, RAW_DATA)
    return file_list
