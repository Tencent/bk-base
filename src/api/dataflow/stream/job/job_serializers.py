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

from django.utils.translation import ugettext as _
from rest_framework import serializers


class JobSerializer(serializers.Serializer):
    class JobConfigSerializer(serializers.Serializer):
        class ProcessorLogic(serializers.Serializer):
            class PackageSerializer(serializers.Serializer):
                path = serializers.CharField(label="path", allow_blank=True, required=False)
                id = serializers.CharField(label="id", allow_blank=True, required=False)

            class AdvancedSerializer(serializers.Serializer):
                use_savepoint = serializers.BooleanField(label="enable savepoint or not", required=False)
                engine_conf = serializers.DictField(label="engine conf", required=False)

                def validate(self, attrs):
                    if "use_savepoint" not in attrs:
                        attrs["use_savepoint"] = True
                    return attrs

            programming_language = serializers.ChoiceField(label="module", choices=(("python", "python"),))
            code = serializers.CharField(required=False, allow_blank=True, label="user_args")
            user_main_class = serializers.CharField(required=False, label="user_main_class")
            user_args = serializers.CharField(required=False, allow_blank=True, label="user_args")
            package = PackageSerializer(label="package", required=False)
            advanced = AdvancedSerializer(required=False, label="advanced")

            def validate(self, attrs):
                if "user_args" not in attrs:
                    attrs["user_args"] = ""
                return attrs

        heads = serializers.CharField(label=_("heads"))
        tails = serializers.CharField(label=_("tails"))
        concurrency = serializers.IntegerField(required=False, label=_("资源数"), allow_null=True)
        offset = serializers.ChoiceField(label=_("offset"), choices=((0, _("最新")), (1, _("继续")), (-1, _("最早"))))
        processor_type = serializers.CharField(required=False, label="processor_type")
        processor_logic = ProcessorLogic(required=False, label="processor_logic")

    class JobserverConfigSerializer(serializers.Serializer):
        geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
        cluster_id = serializers.CharField(required=True, label=_("jobnavi集群ID"))

    project_id = serializers.IntegerField(label=_("项目id"))
    code_version = serializers.CharField(label=_("代码版本"), allow_null=True)
    cluster_group = serializers.CharField(label=_("集群组"), required=False, allow_null=True)

    job_config = JobConfigSerializer(required=True, many=False, label=_("配置"))
    jobserver_config = JobserverConfigSerializer(required=True, label=_("作业服务配置"))

    deploy_config = serializers.DictField(label=_("部署参数"), allow_null=True)
    processings = serializers.ListField(label=_("包含的processing"))

    def validate_name(self, value):
        return True


class CreateJobSerializer(JobSerializer):
    component_type = serializers.CharField(label=_("组件"))


class GetJobSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "related": "processings" (可选)
        "extra": "" (可选)
    }
    """

    related = serializers.ListField(label="related", required=False)


class GetCheckpointSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "related": "storm" (可选)
    }
    """

    related = serializers.CharField(label="related", required=False)
    extra = serializers.CharField(label="extra", required=False)


class CodeVersionSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "code_version": "master"
    }
    """

    code_version = serializers.CharField(label=_("代码号"), allow_null=True, required=False)

    def validate_name(self, value):
        return True


class RegisterSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "heads": "102_clean",
        "tails": "102_test",
        "offset": 0,
        "jar_name": "master-0.0.1.jar",
        "cluster_group": "default",
        "cluster_name": "default_advanced1",
        "concurrency": null,
        "deploy_mode": "yarn-session"
    }
    """

    geog_area_code = serializers.CharField(required=True, label=_("地域标签信息"))
    jar_name = serializers.CharField(label=_("运行具体版本"))

    def validate_name(self, value):
        return True


class SyncStatusSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
        "cluster_name": "default_standard1"
    }
    """

    cluster_name = serializers.CharField(label=_("集群名称"))

    def validate_name(self, value):
        return True


class SubmitSerializer(serializers.Serializer):
    """
    序列化器，需要校验的参数

    满足当前校验器的请求参数如下:
    {
      "conf": {
        "info": {
          "job_id": "102_xxx",
          "job_name": "102_xxx",
          "project_version": 1,
          "job_type": "flink",
          "run_mode": "product"
        },
        "checkpoint": {
          "from_tail": 1,
          "manager": "redis",
          "is_replay": 1
        },
        "params": {
          "cluster_name": "default_advanced1",
          "resources": 1,
          "jar_name": "master-0.0.1.jar"
        },
        "result_tables": {
          "source": {
            "2_clean": {
              "input": {
                "kafka_info": "host:port"
              },
              "description": "xxx",
              "id": "2_clean",
              "fields": [
                {
                  "origin": "",
                  "field": "dtEventTime",
                  "type": "string",
                  "description": "event time"
                },
                {
                  "origin": "",
                  "field": "par_offset",
                  "type": "string",
                  "description": "offset"
                },
                {
                  "origin": "",
                  "field": "path",
                  "type": "string",
                  "description": "日志路径"
                }
              ],
              "name": "clean"
            }
          },
          "transform": {
            "2_test": {
              "description": "xxx",
              "fields": [
                {
                  "origin": "",
                  "field": "dtEventTime",
                  "type": "string",
                  "description": "event time"
                },
                {
                  "origin": "",
                  "field": "par_offset",
                  "type": "string",
                  "description": "offset"
                },
                {
                  "origin": "",
                  "field": "path",
                  "type": "string",
                  "description": "日志路径"
                }
              ],
              "id": "2_test",
              "window": {
                "count_freq": 0,
                "length": 0,
                "type": null,
                "waiting_time": 0
              },
              "parents": [
                "2_clean"
              ],
              "processor": {
                "processor_type": "common_transform",
                "processor_args": "SELECT * FROM 2_test42_clean"
              },
              "name": "test"
            }
          },
          "sink": {
            "2_test": {
              "description": "xxx",
              "fields": [
                {
                  "origin": "",
                  "field": "dtEventTime",
                  "type": "string",
                  "description": "event time"
                },
                {
                  "origin": "",
                  "field": "par_offset",
                  "type": "string",
                  "description": "offset"
                },
                {
                  "origin": "",
                  "field": "path",
                  "type": "string",
                  "description": "日志路径"
                }
              ],
              "id": "2_test",
              "window": {
                "count_freq": 0,
                "length": 0,
                "type": null,
                "waiting_time": 0
              },
              "output": {
                "kafka_info": "host:port"
              },
              "name": "test"
            }
          }
        }
      }
    }
    """

    conf = serializers.DictField(label=_("conf"))

    def validate_name(self, value):
        return True


class UnlockJobSerializer(serializers.Serializer):
    entire = serializers.BooleanField(label=_("是否对作业所有信息解锁"), required=False)


class StreamConfSerializer(serializers.Serializer):
    deploy_mode = serializers.ChoiceField(
        label=_("deploy_mode"),
        choices=(("yarn-session", "yarn-session"), ("yarn-cluster", "yarn-cluster")),
        required=False,
    )
    state_backend = serializers.ChoiceField(
        label=_("state_backend"),
        choices=(("filesystem", "filesystem"), ("rocksdb", "rocksdb")),
        required=False,
    )
    checkpoint_interval = serializers.IntegerField(label=_("checkpoint_interval"), required=False)
    concurrency = serializers.IntegerField(label=_("concurrency"), required=False)
    task_manager_memory = serializers.IntegerField(label=_("task_manager_memory"), required=False)
    slots = serializers.IntegerField(label=_("slots"), required=False)
    sink = serializers.DictField(label=_("sink"), required=False)
    other = serializers.DictField(label=_("other"), required=False)

    def validate_name(self, value):
        return True
