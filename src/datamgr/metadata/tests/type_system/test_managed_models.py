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

from metadata.db_models.meta_service import replica_models_collection
from metadata_biz.db_models.bkdata import managed_models_collection
from metadata_biz.types import default_registry as dgraph_models_collection


def test_show_managed_models():
    managed_models = dict(same=list(), diff=dict(replica=list(), dgraph=list()))
    biz_md_names = {md_name: None for md_name in managed_models_collection.keys()}
    replica_md_names = {md_name: None for md_name in replica_models_collection.keys()}
    dgraph_md_names = {md_name: None for md_name in dgraph_models_collection.keys()}
    for md_name in biz_md_names:
        if md_name in replica_md_names and md_name in dgraph_md_names:
            managed_models['same'].append(md_name)
            del replica_md_names[md_name]
            del dgraph_md_names[md_name]
            continue
        if md_name not in replica_md_names:
            managed_models['diff']['replica'].append('{} {}'.format('-', md_name))
        else:
            del replica_md_names[md_name]
        if md_name not in dgraph_md_names:
            managed_models['diff']['dgraph'].append('{} {}'.format('-', md_name))
        else:
            del dgraph_md_names[md_name]
    if replica_md_names:
        for md_name in replica_md_names:
            managed_models['diff']['replica'].append('{} {}'.format('+', md_name))
    if dgraph_md_names:
        for md_name in dgraph_md_names:
            managed_models['diff']['dgraph'].append('{} {}'.format('+', md_name))
    # print(managed_models)
    # diff 的内容应该保持一致
    assert {item for item in managed_models['diff']['replica']} == {'- Base', '- StorageTask', '+ ReplicaBase'}
    assert {item for item in managed_models['diff']['dgraph']} == {
        "- Base",
        "- DataProcessingDel",
        "- DataTransferringDel",
        "- ProjectDel",
        "- ResultTableDel",
        "- StorageClusterExpiresConfig",
        "- StorageScenarioConfig",
        "- StorageTask",
        "- TagAttribute",
        "- TagAttributeSchema",
        "- ModelingTask",
        "- ModelingTaskExecuteLog",
        "- UserOperationLog",
        "+ Asset",
        "+ Entity",
        "+ Relation",
        "+ AddOn",
        "+ Classification",
        "+ Internal",
        "+ Staff",
        "+ Biz",
        "+ BKBiz",
        "+ BizV1",
        "+ BizV3",
        "+ BizCloud",
        "+ Infrastructure",
        "+ DataSet",
        "+ DMCategoryConfig",
        "+ DMLayerConfig",
        "+ DataSetFeature",
        "+ DataSetIndex",
        "+ DataSetSummary",
        "+ Audit",
        "+ WhoQueryRT",
        "+ Procedure",
        "+ Range",
        "+ Heat",
        "+ Importance",
        "+ AssetValue",
        "+ StorageCapacity",
        "+ Cost",
        "+ LifeCycle",
        "+ Preference",
        "+ Storage",
        "+ CustomTagTarget",
    }
