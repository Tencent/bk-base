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

from inspect import isclass

from attr import fields_dict

from metadata.type_system.core import default_metadata_type_registry as default_registry
from metadata_biz.type_system.converters import (
    BKBizInstanceConverter,
    CommonJsonInstanceConverter,
    DataModelConverter,
    DataProcedureRelationInstanceConverter,
    DefaultTargetInstanceConverter,
    ModelSpecialConverter,
    ReferredAttrFromIdAttrForDataModelConverter,
    ReferredAttrFromIdAttrForModelingConverter,
    TagInstanceConverter,
    TagTargetInstanceConverter,
)
from metadata_biz.types.entities.by_biz import data_model, modeling

md_converters = {
    default_registry['TagTarget']: TagTargetInstanceConverter,
    default_registry['LifeCycle']: DefaultTargetInstanceConverter,
    default_registry['DataSetFeature']: DefaultTargetInstanceConverter,
    default_registry['AssetValue']: DefaultTargetInstanceConverter,
    default_registry['DataProcessingRelation']: DataProcedureRelationInstanceConverter,
    default_registry['DataTransferringRelation']: DataProcedureRelationInstanceConverter,
    default_registry['Tag']: TagInstanceConverter,
    default_registry['ResultTableField']: CommonJsonInstanceConverter,
    default_registry['BKBiz']: BKBizInstanceConverter,
    default_registry['ActionInfo']: ModelSpecialConverter,
    default_registry['SampleSetStoreDataset']: DataProcedureRelationInstanceConverter,
}
if 'TdwTable' in default_registry:
    md_converters[default_registry['TdwTable']] = CommonJsonInstanceConverter

for item in vars(modeling).values():
    if isclass(item):
        for (
            model_special,
            model_special_id,
        ) in ReferredAttrFromIdAttrForModelingConverter.attr_name_with_id_attr_name.items():
            try:
                fields = fields_dict(item)
                if model_special in fields and model_special_id in fields:
                    md_converters[item] = ModelSpecialConverter
            except Exception:
                pass

for item in list(vars(data_model).values()):
    if isclass(item):
        for (
            model_special,
            model_special_id,
        ) in list(ReferredAttrFromIdAttrForDataModelConverter.attr_name_with_id_attr_name.items()):
            try:
                fields = fields_dict(item)
                if model_special in fields and model_special_id in fields:
                    md_converters[item] = DataModelConverter
            except Exception:
                pass
