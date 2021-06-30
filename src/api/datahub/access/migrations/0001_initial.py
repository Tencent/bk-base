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
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="AccessDbTypeConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("db_type_name", models.CharField(max_length=128)),
                ("db_type_alias", models.CharField(max_length=128)),
                ("active", models.IntegerField(null=True, blank=True)),
                ("created_by", models.CharField(max_length=128, null=True, blank=True)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=128, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "access_db_type_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="AccessScenarioConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("data_scenario_name", models.CharField(max_length=128)),
                ("data_scenario_alias", models.CharField(max_length=128)),
                ("active", models.IntegerField(null=True, blank=True)),
                ("created_by", models.CharField(max_length=128, null=True, blank=True)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=128, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "access_scenario_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="AccessScenarioStorageChannel",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("data_scenario", models.CharField(max_length=128)),
                ("storage_channel_id", models.IntegerField()),
                ("priority", models.IntegerField(null=True, blank=True)),
                ("created_by", models.CharField(max_length=128, null=True, blank=True)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=128, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "access_scenario_storage_channel",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="AccessSourceConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("data_source_name", models.CharField(max_length=128)),
                ("data_source_alias", models.CharField(max_length=128)),
                ("created_by", models.CharField(max_length=128, null=True, blank=True)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=128, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "access_source_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="ConfigField",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("data_id", models.IntegerField()),
                ("orig_index", models.IntegerField(null=True, blank=True)),
                ("report_index", models.IntegerField(null=True, blank=True)),
                ("name", models.CharField(max_length=255)),
                ("alias", models.CharField(max_length=255, null=True, blank=True)),
                ("description", models.CharField(max_length=255)),
                ("type", models.CharField(max_length=255)),
                ("created_by", models.CharField(max_length=128)),
                ("funcs", models.CharField(max_length=256, null=True, blank=True)),
                ("is_key", models.IntegerField(null=True, blank=True)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=128, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
            ],
            options={
                "db_table": "config_field",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="DatabusChannelClusterConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("cluster_name", models.CharField(unique=True, max_length=32)),
                ("cluster_type", models.CharField(max_length=32)),
                ("cluster_role", models.CharField(max_length=32)),
                ("cluster_domain", models.CharField(max_length=128)),
                ("cluster_backup_ips", models.CharField(max_length=128)),
                ("cluster_port", models.IntegerField()),
                ("zk_domain", models.CharField(max_length=128)),
                ("zk_port", models.IntegerField()),
                ("zk_root_path", models.CharField(max_length=128)),
                ("active", models.IntegerField(null=True, blank=True)),
                ("priority", models.IntegerField(null=True, blank=True)),
                ("attribute", models.CharField(max_length=128, null=True, blank=True)),
                ("created_by", models.CharField(max_length=128, null=True, blank=True)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=128, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "databus_channel_cluster_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="DatabusOperationLog",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("operation_type", models.CharField(max_length=255)),
                ("item", models.CharField(max_length=255)),
                ("target", models.CharField(max_length=510)),
                ("request", models.TextField()),
                ("response", models.TextField()),
                ("created_by", models.CharField(max_length=128, null=True, blank=True)),
                ("created_at", models.DateTimeField()),
            ],
            options={
                "db_table": "databus_operation_log",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="DataCategoryConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("data_category_name", models.CharField(max_length=128)),
                ("data_category_alias", models.CharField(max_length=128)),
                ("active", models.IntegerField()),
                ("created_by", models.CharField(max_length=50)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=50, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField(null=True, blank=True)),
            ],
            options={
                "db_table": "data_category_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="EncodingConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("encoding_name", models.CharField(max_length=128)),
                ("encoding_alias", models.CharField(max_length=128)),
                ("active", models.IntegerField()),
                ("created_by", models.CharField(max_length=50)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=50, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "encoding_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="FieldDelimiterConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("en_display", models.CharField(max_length=128)),
                ("display", models.CharField(max_length=128)),
                ("value", models.CharField(max_length=128)),
                ("disabled", models.IntegerField()),
                ("created_by", models.CharField(max_length=50)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=50, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField(null=True, blank=True)),
            ],
            options={
                "db_table": "field_delimiter_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="FieldTypeConfig",
            fields=[
                (
                    "field_type",
                    models.CharField(max_length=128, serialize=False, primary_key=True),
                ),
                ("type_name", models.CharField(max_length=128)),
                ("active", models.IntegerField()),
                ("created_by", models.CharField(max_length=50)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=50, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "field_type_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="FileFrequencyConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("en_display", models.CharField(max_length=128)),
                ("display", models.CharField(max_length=128)),
                ("value", models.CharField(max_length=128)),
                ("disabled", models.IntegerField()),
                ("created_by", models.CharField(max_length=50)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=50, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField(null=True, blank=True)),
            ],
            options={
                "db_table": "file_frequency_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="TimeFormatConfig",
            fields=[
                (
                    "format",
                    models.CharField(max_length=128, serialize=False, primary_key=True),
                ),
                ("active", models.IntegerField(null=True, blank=True)),
                ("created_by", models.CharField(max_length=50)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=50, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
            ],
            options={
                "db_table": "time_format_config",
                "managed": False,
            },
        ),
        migrations.CreateModel(
            name="AccessRawData",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
                ("bk_biz_id", models.IntegerField()),
                ("raw_data_name", models.CharField(max_length=128)),
                ("raw_data_alias", models.CharField(max_length=128)),
                ("sensitivity", models.CharField(max_length=32, null=True, blank=True)),
                ("data_source", models.CharField(max_length=32)),
                (
                    "data_encoding",
                    models.CharField(max_length=32, null=True, blank=True),
                ),
                (
                    "data_category",
                    models.CharField(max_length=32, null=True, blank=True),
                ),
                ("data_scenario", models.CharField(max_length=128)),
                ("bk_app_code", models.CharField(max_length=128)),
                ("storage_channel_id", models.IntegerField(null=True, blank=True)),
                ("created_by", models.CharField(max_length=128, null=True, blank=True)),
                ("created_at", models.DateTimeField()),
                ("updated_by", models.CharField(max_length=128, null=True, blank=True)),
                ("updated_at", models.DateTimeField(null=True, blank=True)),
                ("description", models.TextField()),
                ("maintainer", models.CharField(max_length=255, null=True, blank=True)),
            ],
            options={
                "db_table": "access_raw_data",
            },
        ),
        migrations.CreateModel(
            name="DataCoupleinConfig",
            fields=[
                (
                    "id",
                    models.AutoField(
                        verbose_name="ID",
                        serialize=False,
                        auto_created=True,
                        primary_key=True,
                    ),
                ),
            ],
        ),
    ]
