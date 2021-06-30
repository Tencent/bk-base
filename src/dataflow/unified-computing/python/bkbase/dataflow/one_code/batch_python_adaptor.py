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
import os

from jobnavi.jobnavi_logging import get_logger
from jobnavi.JobNaviTask import EventListener, JobNaviTask

logger = get_logger()


class SparkPythonCodeJobNaviTask(JobNaviTask):
    def __init__(self):
        logger.info("init jobnavi_task")

    def get_event_listener(self, event_name):
        if "runJob" == event_name:
            return RunJobEventListener()

    def run(self, task_event):
        logger.info("run test jobnavi task. event info is:")
        logger.info(task_event)
        if task_event.context.taskInfo.extraInfo is not None:
            extra_info = json.loads(task_event.context.taskInfo.extraInfo)
        else:
            extra_info = {}
        schedule_id = task_event.context.taskInfo.scheduleId
        schedule_time = task_event.context.taskInfo.scheduleTime
        execute_id = task_event.context.executeInfo.id

        env_name = task_event.context.taskInfo.type.env

        main_args = {
            "job_type": "spark_python_code",
            "schedule_info": {
                "schedule_id": schedule_id,
                "schedule_time": schedule_time,
                "exec_id": execute_id,
                "extra_info": extra_info,
            },
        }

        logger.info("Main args: {}".format(main_args))

        env_dist = os.environ
        logger.info("JOBNAVI_HOME: {}".format(env_dist.get("JOBNAVI_HOME")))

        path_prefix = env_dist.get("JOBNAVI_HOME")
        spark_home_path = path_prefix + "/env/" + env_name
        hadoop_conf_dir = spark_home_path + "/conf"

        logger.info("SPARK_HOME: {}, HADOOP_CONF_DIR: {}".format(spark_home_path, hadoop_conf_dir))

        # set env
        os.environ["SPARK_HOME"] = spark_home_path
        os.environ["HADOOP_CONF_DIR"] = hadoop_conf_dir

        from bkbase.dataflow.one_code.main import do_main

        do_main(main_args)


class RunJobEventListener(EventListener):
    def do_event(self, task_event):
        logger.info("run job")
