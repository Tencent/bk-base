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


from transitions import Machine

from common.transaction import auto_meta_sync

from datamanage.pro.datamodel.models.model_dict import DataModelStep, DataModelPublishStatus
from datamanage.pro.datamodel.models.datamodel import DmmModelInfo
from datamanage.pro.datamodel.dmm.object import get_object

# step_id的状态转移矩阵
STEP_ID_TRANSITIONS = [
    {'trigger': 'create_data_model', 'source': DataModelStep.INIT, 'dest': DataModelStep.MODEL_BASIC_INFO_DONE},
    {
        'trigger': 'update_master_table',
        'source': DataModelStep.MODEL_BASIC_INFO_DONE,
        'dest': DataModelStep.MASTER_TABLE_DONE,
    },
    {'trigger': 'confirm_indicators', 'source': DataModelStep.INDICATOR_DONE, 'dest': DataModelStep.INDICATOR_DONE},
    {'trigger': 'confirm_indicators', 'source': DataModelStep.MASTER_TABLE_DONE, 'dest': DataModelStep.INDICATOR_DONE},
    {
        'trigger': 'confirm_data_model_overview',
        'source': DataModelStep.INDICATOR_DONE,
        'dest': DataModelStep.MODLE_OVERVIEW,
    },
    {'trigger': 'release_data_model', 'source': DataModelStep.MODLE_OVERVIEW, 'dest': DataModelStep.MODLE_RELEASE},
]

# publish_status的状态转移矩阵
PUBLISH_STATUS_TRANSITIONS = [
    {
        'trigger': 'update_data_model',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'update_master_table',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'create_calculation_atom',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'update_calculation_atom',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'quote_calculation_atoms',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'delete_calculation_atom',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'create_indicator',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'update_indicator',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'delete_indicator',
        'source': DataModelPublishStatus.PUBLISHED,
        'dest': DataModelPublishStatus.REDEVELOPING,
    },
    {
        'trigger': 'release_data_model',
        'source': DataModelPublishStatus.DEVELOPING,
        'dest': DataModelPublishStatus.PUBLISHED,
    },
    {
        'trigger': 'release_data_model',
        'source': DataModelPublishStatus.REDEVELOPING,
        'dest': DataModelPublishStatus.PUBLISHED,
    },
]


class DataModelStepIdFSM(object):
    # step_id状态机
    def __init__(self, model_id, source_state, bk_username):
        self.model_id = model_id
        self.source_state = DataModelStep(source_state)
        self.bk_username = bk_username
        self.machine = Machine(
            model=self,
            states=list(DataModelStep.__members__.values()),
            transitions=STEP_ID_TRANSITIONS,
            initial=self.source_state,
            ignore_invalid_triggers=True,
            after_state_change=self.after_change,
        )

    def __getattr__(self, item):
        if hasattr(self.machine, item):
            return getattr(self.machine, item)
        raise KeyError(("step_id state machine has no {} method".format(item)))

    @property
    def step_id(self):
        return self.state.value

    @auto_meta_sync(using='bkdata_basic')
    def after_change(self):
        # 数据库变更step_id
        datamodel_obj = get_object(DmmModelInfo, model_id=self.model_id)
        datamodel_obj.step_id = self.step_id
        datamodel_obj.updated_by = self.bk_username
        datamodel_obj.save()


class DataModelPublishStatusFSM(object):
    # publish_status的状态机
    def __init__(self, model_id, source_state, bk_username):
        self.model_id = model_id
        self.source_state = DataModelPublishStatus(source_state)
        self.bk_username = bk_username
        self.machine = Machine(
            model=self,
            states=list(DataModelPublishStatus.__members__.values()),
            transitions=PUBLISH_STATUS_TRANSITIONS,
            initial=self.source_state,
            ignore_invalid_triggers=True,
            after_state_change=self.after_change,
        )

    def __getattr__(self, item):
        if hasattr(self.machine, item):
            return getattr(self.machine, item)
        raise KeyError(("publish_status state machine has no {} method".format(item)))

    @property
    def publish_status(self):
        return self.state.value

    @auto_meta_sync(using='bkdata_basic')
    def after_change(self):
        # 数据库变更publish_status
        datamodel_obj = get_object(DmmModelInfo, model_id=self.model_id)
        datamodel_obj.publish_status = self.publish_status
        datamodel_obj.updated_by = self.bk_username
        datamodel_obj.save()


def update_data_model_state(model_id, event_name, bk_username):
    """
    变更数据模型的step_id和publish_status状态
    :param model_id: {Int} 模型ID
    :param event_name: {String} 事件名称
    :param bk_username: {String} 用户名
    :return:
    """
    datamodel_obj = get_object(DmmModelInfo, model_id=model_id)
    step_id = datamodel_obj.step_id
    publish_status = datamodel_obj.publish_status

    # 1)更新step_id
    step_id_fsm_instance = DataModelStepIdFSM(model_id, step_id, bk_username)
    if hasattr(step_id_fsm_instance, event_name):
        getattr(step_id_fsm_instance, event_name)()

    # 2)更新publish_status
    publish_status_fsm_instance = DataModelPublishStatusFSM(model_id, publish_status, bk_username)
    if hasattr(publish_status_fsm_instance, event_name):
        getattr(publish_status_fsm_instance, event_name)()
    return step_id_fsm_instance.step_id, publish_status_fsm_instance.publish_status
