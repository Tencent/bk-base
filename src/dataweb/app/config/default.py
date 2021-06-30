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
import os

from blueapps.conf.default_settings import *  # noqa
from blueapps.conf.log import get_logging_config_dict

# 这里是默认的 INSTALLED_APPS，大部分情况下，不需要改动
# 如果你已经了解每个默认 APP 的作用，确实需要去掉某些 APP，请去掉下面的注释，然后修改
# INSTALLED_APPS = (
#     'bkoauth',
#     # 框架自定义命令
#     'blueapps.contrib.bk_commands',
#     'django.contrib.admin',
#     'django.contrib.auth',
#     'django.contrib.contenttypes',
#     'django.contrib.sessions',
#     'django.contrib.sites',
#     'django.contrib.messages',
#     'django.contrib.staticfiles',
#     # account app
#     'blueapps.account',
# )

# 请在这里加入你的自定义 APP
INSTALLED_APPS += ("app_control", "apps.api", "apps.dataflow", "rest_framework", "django_filters")  # noqa

# 这里是默认的中间件，大部分情况下，不需要改动
# 如果你已经了解每个默认 MIDDLEWARE 的作用，确实需要去掉某些 MIDDLEWARE，或者改动先后顺序，请去掉下面的注释，然后修改
# MIDDLEWARE = (
#     # request instance provider
#     'blueapps.middleware.request_provider.RequestProvider',
#     'django.contrib.sessions.middleware.SessionMiddleware',
#     'django.middleware.common.CommonMiddleware',
#     'django.middleware.csrf.CsrfViewMiddleware',
#     'django.contrib.auth.middleware.AuthenticationMiddleware',
#     'django.contrib.messages.middleware.MessageMiddleware',
#     # 跨域检测中间件， 默认关闭
#     # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
#     'django.middleware.security.SecurityMiddleware',
#     # 蓝鲸静态资源服务
#     'whitenoise.middleware.WhiteNoiseMiddleware',
#     # Auth middleware
#     'blueapps.account.middlewares.RioLoginRequiredMiddleware',
#     'blueapps.account.middlewares.WeixinLoginRequiredMiddleware',
#     'blueapps.account.middlewares.LoginRequiredMiddleware',
#     # exception middleware
#     'blueapps.core.exceptions.middleware.AppExceptionMiddleware',
#     # django国际化中间件
#     'django.middleware.locale.LocaleMiddleware',
# )

# 自定义中间件
MIDDLEWARE += (
    "apps.middlewares.record_middleware.RecordMiddleware",
    "apps.middlewares.common.CommonMid",
    "apps.middlewares.i18n_middleware.I18NMiddleware",
)

# 所有环境的日志级别可以在这里配置
# LOG_LEVEL = 'INFO'

# {% verbatim %}STATIC_VERSION_BEGIN
# 静态资源文件(js,css等）在APP上线更新后, 由于浏览器有缓存,
# 可能会造成没更新的情况. 所以在引用静态资源的地方，都把这个加上
# Django 模板中：<script src="/a.js?v={{ STATIC_VERSION }}"></script>
# mako 模板中：<script src="/a.js?v=${ STATIC_VERSION }"></script>
# 如果静态资源修改了以后，上线前改这个版本号即可
# STATIC_VERSION_END{% endverbatim %}
STATIC_VERSION = "1.0"

STATICFILES_DIRS = [os.path.join(BASE_DIR, "static")]  # noqa

# CELERY 开关，使用时请改为 True，修改项目目录下的 Procfile 文件，添加以下两行命令：
# worker: python manage.py celery worker -l info
# beat: python manage.py celery beat -l info
# 不使用时，请修改为 False，并删除项目目录下的 Procfile 文件中 celery 配置
IS_USE_CELERY = False

# 前后端分离开发配置开关，设置为True时dev和stag环境会自动加载允许跨域的相关选项
FRONTEND_BACKEND_SEPARATION = True

# CELERY 并发数，默认为 2，可以通过环境变量或者 Procfile 设置
CELERYD_CONCURRENCY = os.getenv("BK_CELERYD_CONCURRENCY", 2)  # noqa

# CELERY 配置，申明任务的文件路径，即包含有 @task 装饰器的函数文件
CELERY_IMPORTS = ("apps.dataflow.celery_tasks",)

# log level setting
LOG_LEVEL = "INFO"

# load logging settings
LOGGING = get_logging_config_dict(locals())

# 初始化管理员列表，列表中的人员将拥有预发布环境和正式环境的管理员权限
# 注意：请在首次提测和上线前修改，之后的修改将不会生效
INIT_SUPERUSER = []


# 使用mako模板时，默认打开的过滤器：h(过滤html)
MAKO_DEFAULT_FILTERS = ["h"]

# BKUI是否使用了history模式
IS_BKUI_HISTORY_MODE = False

# 是否需要对AJAX弹窗登录强行打开
IS_AJAX_PLAIN_MODE = False

# 国际化配置
LOCALE_PATHS = (os.path.join(BASE_DIR, "locale"),)  # noqa

USE_TZ = True
TIME_ZONE = "Asia/Shanghai"
LANGUAGE_CODE = "zh-hans"

LANGUAGES = (
    ("en", u"English"),
    ("zh-hans", u"简体中文"),
)


# 框架代码 请勿修改
# celery settings
if IS_USE_CELERY:
    INSTALLED_APPS = locals().get("INSTALLED_APPS", [])
    INSTALLED_APPS += ("django_celery_beat", "django_celery_results")
    CELERY_ENABLE_UTC = False
    CELERYBEAT_SCHEDULER = "django_celery_beat.schedulers.DatabaseScheduler"

# remove disabled apps
if locals().get("DISABLED_APPS"):
    INSTALLED_APPS = locals().get("INSTALLED_APPS", [])
    DISABLED_APPS = locals().get("DISABLED_APPS", [])

    INSTALLED_APPS = [_app for _app in INSTALLED_APPS if _app not in DISABLED_APPS]

    _keys = (
        "AUTHENTICATION_BACKENDS",
        "DATABASE_ROUTERS",
        "FILE_UPLOAD_HANDLERS",
        "MIDDLEWARE",
        "PASSWORD_HASHERS",
        "TEMPLATE_LOADERS",
        "STATICFILES_FINDERS",
        "TEMPLATE_CONTEXT_PROCESSORS",
    )

    import itertools

    for _app, _key in itertools.product(DISABLED_APPS, _keys):
        if locals().get(_key) is None:
            continue
        locals()[_key] = tuple(_item for _item in locals()[_key] if not _item.startswith(_app + "."))

# 项目自定义
# 第三方报错是否直接显示
SHOW_EXCEPTION_DETAIL = True


# 修改日志默认配置
def patch_logging_settings(logging_dict):
    for name, content in logging_dict["handlers"].items():
        # 涉及到日志写入文件都需要申明编码
        if content["class"] == "logging.handlers.RotatingFileHandler":
            content["encoding"] = "utf8"


# 修改模板检索目录
TEMPLATES[0]["DIRS"] = TEMPLATES[0]["DIRS"] + (os.path.join(BASE_DIR, "static/dist"),)
TEMPLATES[0]["OPTIONS"]["context_processors"].append("common.context_processors.mysetting")
TEMPLATES[1]["OPTIONS"]["context_processors"].append("common.context_processors.mysetting")

# 敏感参数
SENSITIVE_PARAMS = ["app_code", "app_secret", "bk_app_code", "bk_app_secret", "auth_info"]

# resf_framework
REST_FRAMEWORK = {
    "DATETIME_FORMAT": "%Y-%m-%d %H:%M:%S",
    "EXCEPTION_HANDLER": "apps.generic.custom_exception_handler",
}

# PaaS 平台地址
BK_PAAS_HOST = os.getenv("BK_PAAS_HOST", BK_URL)
BK_PAAS_INNER_HOST = os.getenv("BK_PAAS_INNER_HOST", BK_PAAS_HOST)

# 数据平台后台时区
DATAAPI_TIME_ZONE = "Etc/GMT-8"

# AIOPS 地址
MODEL_URL = os.getenv("BKAPP_MODEL_URL", "")
MODELFLOW_URL = os.getenv("BKAPP_MODELFLOW_URL", "")

# BI 地址
SUPERSET_URL = os.getenv("BKAPP_SUPERSET_URL", "")
GRAFANA_URL = os.getenv("BKAPP_GRAFANA_URL", "")

# CC 地址
BK_CC_HOST = os.getenv("BKAPP_BK_CC_HOST", "")

# 文档地址
DOCUMENT_DOMAIN = os.getenv("BKAPP_DOCUMENT_DOMAIN", "")
HELP_DOC_URL_CH = os.getenv("BKAPP_HELP_DOC_URL_CH", "")
HELP_DOC_URL_EN = os.getenv("BKAPP_HELP_DOC_URL_EN", "")

# TDW 地址
TDW_ACCOUNT_URL = os.getenv("BKAPP_TDW_ACCOUNT_URL", "")
TDW_SITE_URL = os.getenv("BKAPP_TDW_SITE_URL", "")
TDBANK_SITE_URL = os.getenv("BKAPP_TDBANK_SITE_URL", "")

# OA 登录地址
LOGIN_OA_URL = os.getenv("BKAPP_LOGIN_OA_URL", "")

# LANG_SERVER 交互式语法校验地址
LANG_SERVER_PROD_WS = os.getenv("BKAPP_LANG_SERVER_PROD_WS", "")
LANG_SERVER_DEV_WS = os.getenv("BKAPP_LANG_SERVER_DEV_WS", "")

# 平台反馈地址
CE_SITE_URL = os.getenv("BKAPP_CE_SITE_URL", "")

# 数据平台接口地址
BKDATA_APIGW = os.getenv("BKAPP_BKDATA_APIGW", "")
BKDATA_APIGW_DIRECT = os.getenv("BKAPP_BKDATA_APIGW_DIRECT", "")

# PaaS 地址
PAAS_HOST = os.getenv("BKAPP_PAAS_HOST", BK_PAAS_INNER_HOST)
PAAS_API_HOST = os.getenv("BKAPP_PAAS_API_HOST", BK_PAAS_INNER_HOST)
