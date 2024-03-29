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
# Generated by Django 1.11.2 on 2019-04-30 08:07


from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("dataflow", "0007_auto_20190325_1036"),
    ]

    operations = [
        migrations.CreateModel(
            name="QQRTXMapping",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("rtx", models.CharField(max_length=64, verbose_name="RTX")),
                ("qq", models.CharField(max_length=64, verbose_name="QQ")),
            ],
            options={
                "verbose_name": "\u3010\u914d\u7f6e\u4fe1\u606f\u3011QQ-RTX\u6620\u5c04",
                "verbose_name_plural": "\u3010\u914d\u7f6e\u4fe1\u606f\u3011QQ-RTX\u6620\u5c04",
            },
        ),
    ]
