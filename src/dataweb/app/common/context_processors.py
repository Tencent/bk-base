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

import datetime
import os

from blueapps.account.conf import ConfFixture
from django.conf import settings
from django.utils import translation
from django.utils.translation import ugettext_lazy as _

WEB_TITLE_MAP = {
    "ieod": _("IEG数据平台"),
    "openpaas": _("数据平台|蓝鲸智云企业版"),
}

SUPERSET_URL = None
MODEL_URL = None


def get_superset_url():
    try:
        from apps.api import DatacubeApi

        result = DatacubeApi.healthz()
        return result.get("superset_url", getattr(settings, "SUPERSET_URL", None))
    except Exception:
        return getattr(settings, "SUPERSET_URL", None)


def get_model_url():
    try:
        from apps.api import ModelApi

        result = ModelApi.healthz()
        return result.get("model_url", getattr(settings, "MODEL_URL", ""))
    except Exception:
        return getattr(settings, "MODEL_URL", "")


def mysetting(request):
    help_doc_url = getattr(settings, "HELP_DOC_URL_EN", "")

    if translation.get_language() in ["zh-hans"] and hasattr(settings, "HELP_DOC_URL_ZH"):
        help_doc_url = settings.HELP_DOC_URL_ZH

    global SUPERSET_URL
    if SUPERSET_URL is None:
        SUPERSET_URL = get_superset_url()

    global MODEL_URL
    if MODEL_URL is None:
        MODEL_URL = get_model_url()

    return {
        "gettext": _,
        "LANGUAGES": settings.LANGUAGES,
        # 基础信息
        "RUN_MODE": settings.RUN_MODE,
        "APP_ID": settings.APP_CODE,
        "SITE_URL": settings.SITE_URL,
        # 静态资源
        "STATIC_URL": settings.STATIC_URL,
        "STATIC_VERSION": settings.STATIC_VERSION,
        # 登录跳转链接
        "LOGIN_URL": ConfFixture.LOGIN_URL,
        # 'LOGOUT_URL': settings.LOGOUT_URL,
        "BK_PAAS_HOST": "%s/app/list/" % settings.BK_PAAS_HOST,
        "BK_PLAT_HOST": settings.BK_PAAS_HOST,
        "BK_CC_HOST": settings.BK_CC_HOST,
        "MODELFLOW_URL": getattr(settings, "MODELFLOW_URL", ""),
        "MODEL_URL": MODEL_URL,
        "SUPERSET_URL": SUPERSET_URL,
        "GRAFANA_URL": getattr(settings, "GRAFANA_URL", ""),
        "HELP_DOC_URL": help_doc_url,
        "DOCUMENT_DOMAIN": getattr(settings, "DOCUMENT_DOMAIN", ""),
        "TDW_ACCOUNT_URL": getattr(settings, "TDW_ACCOUNT_URL", ""),
        # 当前页面，主要为了login_required做跳转用
        "APP_PATH": request.get_full_path(),
        "NOW": datetime.datetime.now(),
        # @todo 此变量后期由后台API功能开关代替
        "DATA_VERSION": os.environ.get("BKAPP_DATA_VERSION", "integration"),
        "RUN_VER": settings.RUN_VER,
        "WEB_TITLE": WEB_TITLE_MAP.get(settings.RUN_VER, _("数据平台")),
        # 适配不同环境的地址
        "LOGIN_OA_URL": getattr(settings, "LOGIN_OA_URL", ""),
        "LANG_SERVER_PROD_WS": getattr(settings, "LANG_SERVER_PROD_WS", ""),
        "LANG_SERVER_DEV_WS": getattr(settings, "LANG_SERVER_DEV_WS", ""),
        "TDW_SITE_URL": getattr(settings, "TDW_SITE_URL", ""),
        "TDBANK_SITE_URL": getattr(settings, "TDBANK_SITE_URL", ""),
        "CE_SITE_URL": getattr(settings, "CE_SITE_URL", ""),
    }
