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

summary:
    Settings and configuration for DM_ENGINE.

    Values will be read from the module specified by the DM_ENGINE_SETTINGS_MODULE
    environment variable, and then from dm_engine.config.global_settings; see the global
    settings file for a list of all possible variables.
"""

import importlib
import os
import sys

from dm_engine.config import default_settings as default_mod
from dm_engine.runtime import g_context
from dm_engine.utils.functional import LazyObject

SETTING_PREFIX = "DM_ENGINE_"


class Settings(object):
    """
    DM_ENGINE settings manager, maybe it is better that it's lazyobject.
    """

    def __init__(self):

        for setting in dir(default_mod):
            setattr(self, setting, getattr(default_mod, setting))

        settings_module = g_context.settings
        sys.path.insert(0, os.getcwd())
        try:
            mod = importlib.import_module(settings_module)

            for setting in dir(mod):
                if setting.startswith(SETTING_PREFIX):
                    md_engine_setting = setting[len(SETTING_PREFIX) :]
                    setattr(self, md_engine_setting, getattr(mod, setting))
        except ModuleNotFoundError as err:
            print(f"{settings_module} not exist, give the right one. {err}")


class LazySettings(LazyObject):
    def _setup(self):
        self._wrapped = Settings()


settings = LazySettings()
