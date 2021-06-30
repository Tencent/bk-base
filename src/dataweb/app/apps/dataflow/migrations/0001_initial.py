# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础计算平台 available.
Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and limitations under the License.
"""


import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="BeginnerTask",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("user", models.CharField(max_length=256, verbose_name="\u7528\u6237\u540d")),
                ("build_project", models.BooleanField(default=False, verbose_name="\u65b0\u5efa\u9879\u76ee")),
                (
                    "apply_biz",
                    models.BooleanField(default=False, verbose_name="\u63a5\u5165\u4e1a\u52a1, \u5df2\u5e9f\u5f03"),
                ),
                (
                    "build_dataid",
                    models.BooleanField(default=False, verbose_name="\u63a5\u5165\u539f\u59cb\u6570\u636e"),
                ),
                ("build_dataflow", models.BooleanField(default=False, verbose_name="\u65b0\u5efa\u4efb\u52a1")),
                ("config_dataflow", models.BooleanField(default=False, verbose_name="\u914d\u7f6e\u4efb\u52a1")),
                ("run_dataflow", models.BooleanField(default=False, verbose_name="\u8fd0\u884c\u4efb\u52a1")),
                (
                    "query_result_table",
                    models.BooleanField(default=False, verbose_name="\u67e5\u8be2\u7ed3\u679c\u6570\u636e"),
                ),
                ("apply_data", models.BooleanField(default=False, verbose_name="\u7533\u8bf7\u6570\u636e\u6743\u9650")),
            ],
        ),
        migrations.CreateModel(
            name="DataApply",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("applicant", models.CharField(max_length=256, verbose_name="\u7533\u8bf7\u4eba")),
                ("apply_time", models.DateTimeField(auto_now_add=True, verbose_name="\u7533\u8bf7\u65f6\u95f4")),
                (
                    "apply_result",
                    models.CharField(
                        default=b"unprocess",
                        max_length=32,
                        verbose_name="\u7533\u8bf7\u7ed3\u679c",
                        choices=[
                            (b"accept", "\u5df2\u901a\u8fc7"),
                            (b"refuse", "\u5df2\u62d2\u7edd"),
                            (b"unprocess", "\u672a\u5904\u7406"),
                        ],
                    ),
                ),
                ("apply_reason", models.CharField(max_length=256, verbose_name="\u7533\u8bf7\u7406\u7531")),
                ("app_code", models.CharField(max_length=255, verbose_name="\u7533\u8bf7APP")),
                ("project_id", models.IntegerField(verbose_name="\u9879\u76eeID")),
                ("biz_id", models.IntegerField(verbose_name="\u4e1a\u52a1ID")),
                (
                    "is_all",
                    models.BooleanField(
                        default=False, verbose_name="\u662f\u5426\u7533\u8bf7\u5168\u90e8\u7ed3\u679c\u8868"
                    ),
                ),
                (
                    "result_table_ids",
                    models.TextField(
                        verbose_name="\u7ed3\u679cID\u5217\u8868\uff0c\u9017\u53f7\u9694\u5f00", blank=True
                    ),
                ),
                ("processor", models.CharField(max_length=256, verbose_name="\u5ba1\u6279\u4eba", blank=True)),
                ("process_time", models.DateTimeField(null=True, verbose_name="\u5ba1\u6279\u65f6\u95f4")),
                ("process_msg", models.CharField(max_length=256, verbose_name="\u5ba1\u6279\u5907\u6ce8", blank=True)),
            ],
        ),
        migrations.CreateModel(
            name="DataIdDeployRecord",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("created_by", models.CharField(max_length=128, verbose_name="\u521b\u5efa\u4eba")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="\u521b\u5efa\u65f6\u95f4")),
                ("data_id", models.IntegerField(db_index=True)),
                ("task_id", models.IntegerField(null=True, verbose_name="\u540e\u53f0\u4efb\u52a1ID", db_index=True)),
                ("config", models.TextField(verbose_name="\u90e8\u7f72\u4fe1\u606f")),
                (
                    "exc_status",
                    models.CharField(
                        default=b"RUNNING",
                        max_length=32,
                        verbose_name="\u90e8\u7f72\u72b6\u6001",
                        choices=[(b"RUNNING", "\u6267\u884c\u4e2d"), (b"FINISHED", "\u6267\u884c\u5b8c\u6210")],
                    ),
                ),
                ("exc_result", models.TextField(null=True, verbose_name="\u90e8\u7f72\u8be6\u7ec6\u7ed3\u679c")),
                (
                    "operate_type",
                    models.CharField(
                        default=b"DEPLOY",
                        max_length=32,
                        verbose_name="\u64cd\u4f5c\u7c7b\u578b",
                        choices=[(b"DEPLOY", "\u90e8\u7f72"), (b"STOP", "\u5220\u9664(\u505c\u6b62)")],
                    ),
                ),
            ],
            options={
                "verbose_name": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011DataID \u90e8\u7f72\u8bb0\u5f55",
                "verbose_name_plural": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011DataID \u90e8\u7f72\u8bb0\u5f55",
            },
        ),
        migrations.CreateModel(
            name="DataIdHostRecord",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("ip", models.CharField(max_length=32, verbose_name="\u4e3b\u673aIP")),
                ("plat_id", models.IntegerField(default=1, verbose_name="\u5b50\u7f51")),
                (
                    "errormsg",
                    models.CharField(
                        default=None, max_length=512, null=True, verbose_name="\u9519\u8bef\u4fe1\u606f", blank=True
                    ),
                ),
                (
                    "result",
                    models.CharField(
                        default=b"pending",
                        max_length=16,
                        verbose_name="\u4e3b\u673a\u90e8\u7f72\u72b6\u6001",
                        choices=[
                            (b"success", "\u6210\u529f"),
                            (b"failed", "\u5931\u8d25"),
                            (b"pending", "\u90e8\u7f72\u4e2d"),
                        ],
                    ),
                ),
                (
                    "deploy_record",
                    models.ForeignKey(
                        to="dataflow.DataIdDeployRecord",
                        on_delete=django.db.models.deletion.CASCADE,
                    ),
                ),
            ],
            options={
                "verbose_name": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011DataID \u5355\u4e2ahost\u90e8\u7f72\u8bb0\u5f55",
                "verbose_name_plural": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011DataID "
                "\u5355\u4e2ahost\u90e8\u7f72\u8bb0\u5f55",
            },
        ),
        migrations.CreateModel(
            name="Project",
            fields=[
                ("is_deleted", models.BooleanField(default=False, verbose_name="\u662f\u5426\u5220\u9664")),
                ("project_id", models.AutoField(serialize=False, primary_key=True)),
                ("name", models.CharField(unique=True, max_length=255, verbose_name="\u9879\u76ee\u540d\u79f0")),
                (
                    "biz_id",
                    models.IntegerField(
                        null=True, verbose_name="\u76ee\u524d\u9879\u76ee\u4e0d\u7ed1\u5b9a\u4e1a\u52a1", blank=True
                    ),
                ),
                ("is_public", models.IntegerField(default=0, verbose_name="\u5b57\u6bb5\u5e9f\u5f03")),
                ("description", models.TextField(verbose_name="\u9879\u76ee\u63cf\u8ff0")),
                ("created_by", models.CharField(max_length=255, verbose_name="\u521b\u5efa\u4eba")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="\u521b\u5efa\u65f6\u95f4")),
                (
                    "updated_by",
                    models.CharField(max_length=255, null=True, verbose_name="\u66f4\u65b0\u4eba", blank=True),
                ),
                ("updated_at", models.DateTimeField(auto_now=True, verbose_name="\u66f4\u65b0\u65f6\u95f4", null=True)),
                (
                    "deleted_by",
                    models.CharField(max_length=255, null=True, verbose_name="\u5220\u9664\u4eba", blank=True),
                ),
                ("deleted_at", models.DateTimeField(null=True, verbose_name="\u5220\u9664\u65f6\u95f4", blank=True)),
                ("is_valid", models.IntegerField(default=1, verbose_name="\u5b57\u6bb5\u5e9f\u5f03")),
                ("is_open", models.CharField(default=b"N", max_length=1, verbose_name="\u5b57\u6bb5\u5e9f\u5f03")),
                ("public_id", models.IntegerField(null=True, verbose_name="\u5b57\u6bb5\u5e9f\u5f03", blank=True)),
                ("status", models.IntegerField(default=0, verbose_name="\u5b57\u6bb5\u5e9f\u5f03")),
                (
                    "tag_name",
                    models.CharField(default=b"common", max_length=255, verbose_name="\u9879\u76ee\u6807\u7b7e"),
                ),
                (
                    "app_code",
                    models.CharField(
                        default=b"data",
                        max_length=255,
                        verbose_name="\u901a\u8fc7\u54ea\u4e2aAPP\u521b\u5efa\u8bb0\u5f55",
                    ),
                ),
            ],
            options={
                "db_table": "config_project",
                "verbose_name": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u8868",
                "verbose_name_plural": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u8868",
            },
        ),
        migrations.CreateModel(
            name="ProjectBizData",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("project_id", models.IntegerField()),
                ("biz_id", models.IntegerField()),
                ("is_sensitive", models.IntegerField(null=True, blank=True)),
                ("data_id", models.IntegerField(null=True, blank=True)),
                ("created_by", models.CharField(max_length=128, verbose_name="\u521b\u5efa\u4eba")),
                ("created_at", models.DateTimeField(auto_now_add=True, verbose_name="\u521b\u5efa\u65f6\u95f4")),
            ],
            options={
                "db_table": "project_biz_data",
                "verbose_name": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u6709\u6743\u9650\u4e1a\u52a1\u6570"
                "\u636e",
                "verbose_name_plural": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u6709\u6743\u9650\u4e1a"
                "\u52a1\u6570\u636e",
            },
        ),
        migrations.CreateModel(
            name="ProjectDeleteRecord",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("project_id", models.IntegerField(verbose_name="\u9879\u76eeid")),
                ("operator", models.CharField(max_length=128, verbose_name="\u64cd\u4f5c\u4eba")),
                ("time", models.DateTimeField(auto_now_add=True, verbose_name="\u5220\u9664\u65f6\u95f4")),
                (
                    "status",
                    models.CharField(
                        default=b"DELETING",
                        max_length=32,
                        verbose_name="\u5220\u9664\u72b6\u6001",
                        choices=[(b"DELETING", "\u5220\u9664\u4e2d"), (b"FINISHED", "\u5220\u9664\u5b8c\u6210")],
                    ),
                ),
            ],
            options={
                "verbose_name": "\u3010\u5220\u9664\u8bb0\u5f55\u3011\u9879\u76ee\u5220\u9664",
                "verbose_name_plural": "\u3010\u5220\u9664\u8bb0\u5f55\u3011\u9879\u76ee\u5220\u9664",
            },
        ),
        migrations.CreateModel(
            name="ProjectTag",
            fields=[
                (
                    "tag_name",
                    models.CharField(
                        max_length=255, serialize=False, verbose_name="\u6807\u7b7e\u540d", primary_key=True
                    ),
                ),
                ("description", models.CharField(max_length=255, verbose_name="\u6807\u7b7e\u63cf\u8ff0")),
                (
                    "is_show",
                    models.IntegerField(
                        default=1, verbose_name="\u662f\u5426\u5f00\u653e\u666e\u901a\u7528\u6237\u9009\u62e9"
                    ),
                ),
                ("level", models.IntegerField(default=1, verbose_name="\u6807\u7b7e\u7ea7\u522b\u3001\u6b21\u5e8f")),
                (
                    "storage_types",
                    models.CharField(max_length=255, null=True, verbose_name="\u5b57\u6bb5\u5e9f\u5f03", blank=True),
                ),
                (
                    "remark",
                    models.CharField(max_length=255, null=True, verbose_name="\u5b57\u6bb5\u5e9f\u5f03", blank=True),
                ),
                (
                    "extra",
                    models.CharField(max_length=255, null=True, verbose_name="\u5b57\u6bb5\u5e9f\u5f03", blank=True),
                ),
            ],
            options={
                "db_table": "config_project_tag",
                "verbose_name": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u6807\u7b7e\u8868",
                "verbose_name_plural": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u6807\u7b7e\u8868",
            },
        ),
        migrations.CreateModel(
            name="ResultTableQueryRecord",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("project_id", models.IntegerField(verbose_name="\u9879\u76eeid")),
                ("result_table_ids", models.TextField(verbose_name="\u7ed3\u679c\u8868")),
                ("operator", models.CharField(max_length=128, verbose_name="\u67e5\u8be2\u4eba")),
                ("time", models.DateTimeField(auto_now=True, verbose_name="\u67e5\u8be2\u65f6\u95f4")),
                ("sql", models.TextField(null=True, verbose_name="\u67e5\u8be2SQL")),
                (
                    "storage_type",
                    models.CharField(default=b"mysql", max_length=128, verbose_name="\u5b58\u50a8\u7c7b\u578b"),
                ),
                (
                    "search_range_start_time",
                    models.DateTimeField(null=True, verbose_name="ES\u67e5\u8be2\u5f00\u59cb\u65f6\u95f4"),
                ),
                (
                    "search_range_end_time",
                    models.DateTimeField(null=True, verbose_name="ES\u67e5\u8be2\u7ed3\u675f\u65f6\u95f4"),
                ),
                ("keyword", models.TextField(null=True, verbose_name="\u641c\u7d22\u5173\u952e\u5b57")),
                ("time_taken", models.CharField(default=0, max_length=32, verbose_name="\u8017\u65f6")),
                ("total", models.IntegerField(default=0, verbose_name="\u8fd4\u56de\u6761\u6570")),
                ("result", models.BooleanField(default=True, verbose_name="\u67e5\u8be2\u7ed3\u679c")),
                ("err_msg", models.TextField(null=True, verbose_name="\u9519\u8bef\u4fe1\u606f")),
            ],
            options={
                "verbose_name": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011DB\u67e5\u8be2",
                "verbose_name_plural": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011DB\u67e5\u8be2",
            },
        ),
        migrations.CreateModel(
            name="ResultTableSelectedRecord",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("project_id", models.IntegerField(verbose_name="\u9879\u76eeid")),
                ("result_table_id", models.TextField(verbose_name="\u7ed3\u679c\u8868")),
                ("operator", models.CharField(max_length=128, verbose_name="\u67e5\u8be2\u4eba")),
                ("time", models.DateTimeField(auto_now=True, verbose_name="\u67e5\u8be2\u65f6\u95f4")),
            ],
            options={
                "verbose_name": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011\u7ed3\u679c\u8868\u9009\u62e9\u5386\u53f2",
                "verbose_name_plural": "\u3010\u67e5\u8be2\u8bb0\u5f55\u3011\u7ed3\u679c\u8868\u9009\u62e9\u5386\u53f2",
            },
        ),
        migrations.CreateModel(
            name="TagStorageClusterConfig",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("tag_name", models.CharField(max_length=255, verbose_name="\u6807\u7b7e(Tag)\u540d")),
                ("cluster", models.CharField(max_length=32, verbose_name="\u96c6\u7fa4(Cluster)\u540d")),
                ("cluster_type", models.CharField(max_length=32, verbose_name="\u5b58\u50a8\u7c7b\u578b")),
            ],
            options={
                "db_table": "config_tag_storage_cluster",
                "verbose_name": "\u3010Dataflow\u3011\u6807\u7b7e(Tag)\u548c\u5b58\u653e\u96c6\u7fa4("
                "Cluster)\u5bf9\u5e94\u5173\u7cfb",
                "verbose_name_plural": "\u3010Dataflow\u3011\u6807\u7b7e(Tag)\u548c\u5b58\u653e\u96c6\u7fa4("
                "Cluster)\u5bf9\u5e94\u5173\u7cfb",
            },
        ),
        migrations.CreateModel(
            name="TagStormClusterSet",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("tag_name", models.CharField(max_length=32, verbose_name="\u9879\u76ee\u6807\u7b7e\u540d\u79f0")),
                ("cluster_set", models.CharField(max_length=32, verbose_name="\u96c6\u7fa4 SET")),
                ("description", models.CharField(max_length=256, verbose_name="\u96c6\u7fa4 SET \u63cf\u8ff0")),
            ],
            options={
                "db_table": "config_tag_storm_cluster_set",
                "verbose_name": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u6807\u7b7e\u4e0e Storm "
                "\u96c6\u7fa4\u5bf9\u5e94\u5173\u7cfb",
                "verbose_name_plural": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u6807\u7b7e\u4e0e Storm "
                "\u96c6\u7fa4\u5bf9\u5e94\u5173\u7cfb",
            },
        ),
        migrations.CreateModel(
            name="UserOperateRecord",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("request_id", models.CharField(max_length=64, verbose_name="\u8bf7\u6c42ID", db_index=True)),
                ("operator", models.CharField(max_length=32, verbose_name="\u64cd\u4f5c\u8005", db_index=True)),
                ("operate_time", models.DateTimeField(auto_now=True, verbose_name="\u64cd\u4f5c\u65f6\u95f4")),
                (
                    "operate_type",
                    models.CharField(
                        db_index=True,
                        max_length=64,
                        verbose_name="\u64cd\u4f5c\u7c7b\u578b",
                        choices=[
                            (b"ProjectViewSet.create", "\u521b\u5efa\u9879\u76ee"),
                            (b"ProjectViewSet.partial_update", "\u7f16\u8f91\u66f4\u65b0\u9879\u76ee"),
                            (b"ProjectViewSet.destroy", "\u5220\u9664\u9879\u76ee"),
                            (b"ProjectViewSet.update_project_user", "\u7f16\u8f91\u9879\u76ee\u6210\u5458"),
                            (b"ProjectViewSet.apply_biz", "\u7533\u8bf7\u4e1a\u52a1"),
                            (b"ProjectViewSet.query_rt", "\u7ed3\u679c\u8868\u67e5\u8be2"),
                            (b"ProjectViewSet.restore", "\u6062\u590d\u5220\u9664\u7684\u9879\u76ee"),
                            (b"FlowViewSet.create", "\u521b\u5efa\u4efb\u52a1"),
                            (b"FlowViewSet.partial_update", "\u7f16\u8f91\u66f4\u65b0\u4efb\u52a1"),
                            (b"FlowViewSet.destroy", "\u5220\u9664\u4efb\u52a1"),
                            (b"FlowViewSet.start", "\u542f\u52a8\u4efb\u52a1"),
                            (b"FlowViewSet.stop", "\u505c\u6b62\u4efb\u52a1"),
                            (b"FlowViewSet.restart", "\u91cd\u542f\u4efb\u52a1"),
                            (b"FlowViewSet.start_debug", "\u5f00\u59cb\u8c03\u8bd5"),
                            (b"FlowViewSet.stop_debug", "\u505c\u6b62\u8c03\u8bd5"),
                            (b"NodeViewSet.create", "\u521b\u5efa\u8282\u70b9"),
                            (b"NodeViewSet.update", "\u7f16\u8f91\u66f4\u65b0\u8282\u70b9"),
                            (b"NodeViewSet.destroy", "\u5220\u9664\u8282\u70b9"),
                            (b"ProjectDataIdSet.create", "\u521b\u5efa\u539f\u59cb\u6570\u636e"),
                            (b"ProjectDataIdSet.deploy_dataid_collector", "\u91cd\u65b0\u4e0b\u53d1\u91c7\u96c6\u5668"),
                            (b"ProjectDataIdSet.delete_dataid_collector", "\u5220\u9664\u91c7\u96c6\u5668"),
                            (
                                b"ProjectApplySet.create_data_apply",
                                "\u521b\u5efaAPP\u7533\u8bf7\u6570\u636e\u7684\u5355\u636e",
                            ),
                            (b"ApplyViewSet.process", "\u5ba1\u6279\u4e2d\u5fc3\u5355\u636e\u5ba1\u6279"),
                        ],
                    ),
                ),
                ("url", models.CharField(max_length=256, null=True, verbose_name="\u63a5\u53e3url", blank=True)),
                (
                    "method",
                    models.CharField(max_length=1024, null=True, verbose_name="\u8bf7\u6c42\u65b9\u6cd5", blank=True),
                ),
                ("operate_params", models.TextField(null=True, verbose_name="\u64cd\u4f5c\u53c2\u6570", blank=True)),
                ("operate_result", models.TextField(null=True, verbose_name="\u64cd\u4f5c\u8fd4\u56de", blank=True)),
                (
                    "operate_final",
                    models.CharField(max_length=64, null=True, verbose_name="\u64cd\u4f5c\u7ed3\u679c", blank=True),
                ),
                (
                    "project_id",
                    models.IntegerField(db_index=True, null=True, verbose_name="\u9879\u76eeID", blank=True),
                ),
                (
                    "operate_object",
                    models.CharField(
                        db_index=True, max_length=64, null=True, verbose_name="\u64cd\u4f5c\u5bf9\u8c61", blank=True
                    ),
                ),
                (
                    "operate_object_id",
                    models.CharField(max_length=64, null=True, verbose_name="\u64cd\u4f5c\u5bf9\u8c61id", blank=True),
                ),
                ("extra_data", models.TextField(null=True, verbose_name="\u5176\u5b83", blank=True)),
            ],
            options={
                "verbose_name": "\u3010\u7528\u6237\u64cd\u4f5c\u3011\u6d41\u6c34\u65e5\u5fd7",
                "verbose_name_plural": "\u3010\u7528\u6237\u64cd\u4f5c\u3011\u6d41\u6c34\u65e5\u5fd7",
            },
        ),
        migrations.CreateModel(
            name="UserProject",
            fields=[
                ("id", models.AutoField(verbose_name="ID", serialize=False, auto_created=True, primary_key=True)),
                ("user_name", models.CharField(max_length=255, verbose_name="\u7528\u6237\u540d")),
                ("project_id", models.IntegerField(verbose_name="\u9879\u76eeID")),
                (
                    "project_role",
                    models.CharField(
                        max_length=255,
                        verbose_name="\u9879\u76ee\u89d2\u8272",
                        choices=[
                            (b"member", "\u4f5c\u4e1a\u7ba1\u7406\u5458"),
                            (b"admin", "\u9879\u76ee\u7ba1\u7406\u5458"),
                        ],
                    ),
                ),
            ],
            options={
                "db_table": "user_project",
                "verbose_name": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u4e0e\u7528\u6237\u5173\u7cfb\u8868",
                "verbose_name_plural": "\u3010\u9879\u76ee\u914d\u7f6e\u3011\u9879\u76ee\u4e0e\u7528\u6237\u5173"
                "\u7cfb\u8868",
            },
        ),
        migrations.AlterUniqueTogether(
            name="tagstormclusterset",
            unique_together={("tag_name", "cluster_set")},
        ),
        migrations.AlterUniqueTogether(
            name="tagstorageclusterconfig",
            unique_together={("tag_name", "cluster")},
        ),
        migrations.AlterUniqueTogether(
            name="projectbizdata",
            unique_together={("project_id", "biz_id", "is_sensitive", "data_id")},
        ),
    ]
