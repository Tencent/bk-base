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

from celery import shared_task
from common.bklanguage import BkLanguage
from common.local import set_local_param
from django.utils import translation
from django.utils.translation import ugettext as _

from dataflow.modeling.exceptions.comp_exceptions import SQLSubmitError
from dataflow.modeling.handler.mlsql_execute_log import MLSqlExecuteLogHandler
from dataflow.modeling.job.task_handler import ModelingDatalabTaskHandler
from dataflow.modeling.tasks.model_task import ReleaseModelTask
from dataflow.shared.log import modeling_logger as logger


def submit_task(task, task_id, delay=True):
    """
    若调用失败，优化抛出的异常信息
    @param task:
    @param task_id:
    @param delay:
    @return:
    """
    try:
        if delay:
            return task.delay(task_id, BkLanguage.current_language())
        else:
            return task(task_id, BkLanguage.current_language())
    except Exception as e:
        logger.error(e)
        raise EnvironmentError(_("系统环境出现异常，请联系管理员处理."))


def create_model(operator, context):
    task = ReleaseModelTask.create(operator=operator, context=context)
    task_id = task.task_id
    submit_task(execute_model_create, task_id)
    return task_id


def update_model(operator, context):
    task = ReleaseModelTask.create(operator=operator, context=context)
    task_id = task.task_id
    submit_task(execute_model_update, task_id)
    return task_id


@shared_task(queue="flow")
def execute_model_create(task_id, language="en"):
    BkLanguage.set_language(language)
    translation.activate(language)
    task = ReleaseModelTask(task_id)
    task.create_model()


@shared_task(queue="flow")
def execute_model_update(task_id, language="en"):
    BkLanguage.set_language(language)
    translation.activate(language)
    task = ReleaseModelTask(task_id)
    task.update_model()


def start_modeling_job(task):
    task_id = task.flow_task.id
    submit_task(execute_model_task, task_id)
    return task_id


def stop_model_job(job):
    # 暂时未应用
    submit_model_task(stop_model_task, job)


def submit_model_task(task, job_entity):
    # 暂时未应用
    try:
        return task(job_entity, BkLanguage.current_language())
    except Exception as e:
        logger.error(e)
        raise SQLSubmitError()


@shared_task(queue="flow")
def execute_model_task(task_id, language="en"):
    """
    celery 任务部署日志有中英两种，language为默认语言
    目前默认用英语，在该过程若调用其它 API 失败，直接返回英文
    @param task_id:
    @param language:
    @return:
    """
    # request_language_code 用于保存用户当前通过界面请求的语言环境
    # 任务部署后，pizza common获取不到内部版用户的个人信息而返回默认的语言——中文，会导致内部版用户发送的邮件都是中文
    # 增加线程变量记录用户的接口访问语言，以此作为通知语言
    set_local_param("request_language_code", language)
    # 设置API调用 header 语言，若调用异常，返回信息应该是英文
    BkLanguage.set_language("en")
    # 默认django翻译用英文，因为存在字符串拼接的方式(format)获取完整的报错信息并双语存入DB，无法保证双语准确，因此统一用英语
    translation.activate("en")
    # task = MLSqlTaskHandler(task)
    exec_task = MLSqlExecuteLogHandler.get(task_id)
    task = ModelingDatalabTaskHandler(exec_task)
    task.execute()


@shared_task(queue="flow")
def stop_model_task(job, language="en"):
    """
    celery 任务部署日志有中英两种，language为默认语言
    目前默认用英语，在该过程若调用其它 API 失败，直接返回英文
    @param task_id:
    @param language:
    @return:
    """
    # request_language_code 用于保存用户当前通过界面请求的语言环境
    # 任务部署后，pizza common获取不到内部版用户的个人信息而返回默认的语言——中文，会导致内部版用户发送的邮件都是中文
    # 增加线程变量记录用户的接口访问语言，以此作为通知语言
    set_local_param("request_language_code", language)
    # 设置API调用 header 语言，若调用异常，返回信息应该是英文
    BkLanguage.set_language("en")
    # 默认django翻译用英文，因为存在字符串拼接的方式(format)获取完整的报错信息并双语存入DB，无法保证双语准确，因此统一用英语
    translation.activate("en")
    job.stop_job()
