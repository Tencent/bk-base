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

import logging
from contextlib import contextmanager

from sqlalchemy.orm import sessionmaker

from metadata.runtime import rt_context, rt_g, rt_local
from metadata.state.state import State, StateMode, state_lock


class Support(object):
    """
    支持功能集合。
    """

    def __init__(self):
        self.class_logger = logging.getLogger(__name__ + '.' + self.__class__.__name__)

    @staticmethod
    def create_db_session(db_name, in_local=None, resource_module=None):
        if not resource_module:
            resource_module = rt_context.m_resource
        if in_local:
            if not getattr(rt_local, in_local, None):
                setattr(rt_local, in_local, sessionmaker(bind=getattr(resource_module, db_name + '_engine'))())
            transaction = getattr(rt_local, in_local)
        else:
            transaction = sessionmaker(bind=getattr(resource_module, db_name + '_engine'))()
        try:
            yield transaction
            transaction.expunge_all()
        except Exception:
            transaction.rollback()
            raise
        finally:
            transaction.close()

    @contextmanager
    def bkdata_basic_session(self, in_local=None):
        return self.create_db_session('bkdata_basic', in_local, rt_context.m_biz_resource)

    @contextmanager
    def bkdata_meta_session(self, in_local=None):
        return self.create_db_session('bkdata_meta', in_local)

    @contextmanager
    def bkdata_log_session(self, in_local=None):
        return self.create_db_session('bkdata_log', in_local)


quick_support = Support()


def join_service_party(service_ha, init=True, rejoin=False):
    with state_lock:
        if getattr(rt_g, 'system_state', None):
            if not rejoin:
                return
            with rt_g.system_state:
                pass
        rt_g.system_state = State('/metadata', StateMode.ONLINE if service_ha else StateMode.LOCAL)
        rt_g.system_state.syncing()
        if init:
            rt_g.system_state.init()
