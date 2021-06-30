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

from sqlalchemy import Column, ForeignKey, String
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declared_attr

from metadata.backend.mysql.replica_base import ReplicaMixIn


class AccessRawDatumMixIn(ReplicaMixIn):
    @declared_attr
    def storage_channel_id(cls):
        return Column(
            INTEGER(11),
            ForeignKey(
                'databus_channel_cluster_config.id',
                onupdate='CASCADE',
                ondelete='CASCADE',
            ),
        )


class ResultTableMixIn(ReplicaMixIn):
    @declared_attr
    def project_id(cls):
        return Column(
            INTEGER(11),
            ForeignKey(
                'project_info.project_id',
                onupdate='CASCADE',
                ondelete='CASCADE',
            ),
        )

    @declared_attr
    def processing_type(cls):
        return Column(
            String(32),
            ForeignKey('processing_type_config.processing_type_name', onupdate='CASCADE'),
            nullable=False,
        )


class DataProcessingMixIn(ReplicaMixIn):
    @declared_attr
    def project_id(cls):
        return Column(
            INTEGER(11),
            ForeignKey(
                'project_info.project_id',
                onupdate='CASCADE',
                ondelete='CASCADE',
            ),
            primary_key=True,
        )

    @declared_attr
    def processing_type(cls):
        return Column(
            String(32),
            ForeignKey('processing_type_config.processing_type_name', onupdate='CASCADE'),
            nullable=False,
        )


class DataTransferringMixIn(ReplicaMixIn):
    @declared_attr
    def project_id(cls):
        return Column(
            INTEGER(11),
            ForeignKey(
                'project_info.project_id',
                onupdate='CASCADE',
                ondelete='CASCADE',
            ),
            nullable=False,
        )

    @declared_attr
    def transferring_type(cls):
        return Column(
            String(32),
            ForeignKey('transferring_type_config.transferring_type_name', onupdate='CASCADE'),
            nullable=False,
        )


class DataProcessingRelationMixIn(ReplicaMixIn):
    @declared_attr
    def processing_id(cls):
        return Column(
            String(32),
            ForeignKey('data_processing.processing_id', ondelete='CASCADE', onupdate='CASCADE'),
            nullable=False,
        )

    @declared_attr
    def storage_cluster_config_id(cls):
        return Column(INTEGER(11), ForeignKey('storage_cluster_config.id', ondelete='CASCADE', onupdate='CASCADE'))

    @declared_attr
    def channel_cluster_config_id(cls):
        return Column(
            INTEGER(11), ForeignKey('databus_channel_cluster_config.id', ondelete='CASCADE', onupdate='CASCADE')
        )


class DataTransferringRelationMixIn(ReplicaMixIn):
    @declared_attr
    def transferring_id(cls):
        return Column(
            String(32),
            ForeignKey('data_transferring.transferring_id', ondelete='CASCADE', onupdate='CASCADE'),
            nullable=False,
        )

    @declared_attr
    def storage_cluster_config_id(cls):
        return Column(INTEGER(11), ForeignKey('storage_cluster_config.id', ondelete='CASCADE', onupdate='CASCADE'))

    @declared_attr
    def channel_cluster_config_id(cls):
        return Column(
            INTEGER(11), ForeignKey('databus_channel_cluster_config.id', ondelete='CASCADE', onupdate='CASCADE')
        )


class ResultTableFieldMixIn(ReplicaMixIn):
    @declared_attr
    def result_table_id(cls):
        return Column(
            String(255),
            ForeignKey('result_table.result_table_id', ondelete='CASCADE', onupdate='CASCADE'),
            nullable=False,
        )

    @declared_attr
    def field_type(self):
        return Column(String(255), ForeignKey('field_type_config.field_type', onupdate='CASCADE'), nullable=False)


class StorageResultTableMixIn(ReplicaMixIn):
    @declared_attr
    def result_table_id(cls):
        return Column(
            String(128),
            ForeignKey('result_table.result_table_id', ondelete='CASCADE', onupdate='CASCADE'),
            nullable=False,
        )

    @declared_attr
    def storage_cluster_config_id(cls):
        return Column(INTEGER(11), ForeignKey('storage_cluster_config.id', ondelete='CASCADE', onupdate='CASCADE'))

    @declared_attr
    def storage_channel_id(cls):
        return Column(
            INTEGER(11), ForeignKey('databus_channel_cluster_config.id', ondelete='CASCADE', onupdate='CASCADE')
        )
