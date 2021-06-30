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
import os
import time

from environs import Env

env = Env()

# 从环境变量中获取
APP_ID = env.str("APP_ID")
APP_TOKEN = env.str("APP_TOKEN")
APP_NAME = env.str("APP_NAME")
RUN_VERSION = env.str("RUN_VERSION")
RUN_MODE = env.str("RUN_MODE")
CRYPT_INSTANCE_KEY = env.str("CRYPT_INSTANCE_KEY")
CRYPT_ROOT_KEY = env.str("CRYPT_ROOT_KEY")
CRYPT_ROOT_IV = env.str("CRYPT_ROOT_IV")
CONFIG_DB_USER = env.str("CONFIG_DB_USER", "root")
CONFIG_DB_PASSWORD = env.str("CONFIG_DB_PASSWORD", "")
CONFIG_DB_HOST = env.str("CONFIG_DB_HOST", "127.0.0.1")
CONFIG_DB_PORT = env.int("CONFIG_DB_PORT", 3306)
LOGGER_LEVEL = env.str("LOGGER_LEVEL", "INFO")
LOG_MAX_BYTES = env.int("LOG_MAX_BYTES", 524288000)  # 500M
LOG_BACKUP_COUNT = env.int("LOG_BACKUP_COUNT", 5)
AUTH_API_HOST = env.str("AUTH_API_HOST")
AUTH_API_PORT = env.int("AUTH_API_PORT")
META_API_HOST = env.str("META_API_HOST")
META_API_PORT = env.int("META_API_PORT")
PAAS_HOST = env.str("PAAS_HOST")
PAAS_HTTP_PORT = env.str("PAAS_HTTP_PORT")

AUTH_API_URL = f"http://{AUTH_API_HOST}:{AUTH_API_PORT}/v3/auth/"
META_API_URL = f"http://{META_API_HOST}:{META_API_PORT}/v3/meta/"

ESBV2_API_URL = f"http://{PAAS_HOST}:{PAAS_HTTP_PORT}/api/c/compapi/v2/"
CMSI_API_URL = f"{ESBV2_API_URL}/cmsi/"
SMCS_API_URL = env.str("SMCS_API_URL", "")


SECRET_KEY = APP_TOKEN

# 初始化支持多进程的FileLogger, 仅需import
import concurrent_log_handler  # noqa

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.8/howto/deployment/checklist/

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = False

ALLOWED_HOSTS = ["*"]

# Application definition

INSTALLED_APPS = (
    # 'django.contrib.admin',
    # 'django.contrib.auth',
    # 'django.contrib.contenttypes',
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
)

MIDDLEWARE = (
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    # 'django.middleware.csrf.CsrfViewMiddleware',
    # 'django.contrib.auth.middleware.AuthenticationMiddleware',
    # 'django.contrib.auth.middleware.SessionAuthenticationMiddleware',
    "django.contrib.messages.middleware.MessageMiddleware",
    # 'django.middleware.clickjacking.XFrameOptionsMiddleware',
    "django.middleware.security.SecurityMiddleware",
    "common.middlewares.BkLocaleMiddleware",
    "common.middlewares.PizzaOpenTracingMiddleware",
    "common.middlewares.PizzaCommonMiddleware",
    "common.auth.middlewares.AuthenticationMiddleware",
)

REST_FRAMEWORK = {
    "EXCEPTION_HANDLER": "common.base_utils.custom_exception_handler",
    "DEFAULT_THROTTLE_CLASSES": ("rest_framework.throttling.ScopedRateThrottle",),
    "DEFAULT_RENDERER_CLASSES": (
        "common.renderers.BKDataJSONRenderer",
        # 'rest_framework.renderers.JSONRenderer',
        # No BrowsableAPIRenderer, avoid 'get_serializer' error
        # 'rest_framework.renderers.BrowsableAPIRenderer',
    ),
    "DATETIME_FORMAT": "%Y-%m-%d %H:%M:%S",
    "UNICODE_JSON": False,
    "URL_FORMAT_OVERRIDE": "url_format",
}

ROOT_URLCONF = "pizza.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "pizza.wsgi.application"

# Database
# https://docs.djangoproject.com/en/1.8/ref/settings/#databases

# Use pymysql to replace MySQL-python
try:
    import pymysql

    pymysql.install_as_MySQLdb()
    # Patch version info to forcely pass Django client check
    setattr(pymysql, "version_info", (1, 3, 13, "final", 0))
except ImportError as e:
    raise ImportError("PyMySQL is not installed: %s" % e)

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.mysql",
        "NAME": "bkdata_basic",
        "USER": CONFIG_DB_USER,
        "PASSWORD": CONFIG_DB_PASSWORD,
        "HOST": CONFIG_DB_HOST,
        "PORT": CONFIG_DB_PORT,
        "ENCRYPTED": False,
        "TEST": {
            "NAME": "bkdata_test",
        },
    }
}

# Internationalization
# https://docs.djangoproject.com/en/1.8/topics/i18n/

LANGUAGE_CODE = "zh-hans"
USE_I18N = True
USE_L10N = True
BK_LANGUAGE_HEADER = "HTTP_BLUEKING_LANGUAGE"
BK_TIMEZONE_HEADER = "HTTP_BLUEKING_TIMEZONE"

BK_LANGUAGE_EN = "en"
BK_LANGUAGE_CN = "zh-cn"
BK_LANGUAGE_ALL = "all"
BK_SUPPORTED_LANGUAGES = [BK_LANGUAGE_EN, BK_LANGUAGE_CN, BK_LANGUAGE_ALL]
LANGUAGES = (
    ("zh-hans", "中文"),
    ("en", "English"),
)
PIZZA_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOCALE_PATHS = [
    os.path.join(PIZZA_ROOT, APP_NAME, "locale"),
    os.path.join(PIZZA_ROOT, "locale"),
]

TIME_ZONE = "Etc/GMT%+d" % ((time.altzone if time.daylight else time.timezone) / 3600)

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.8/howto/static-files/
STATIC_URL = "/static/"

# logging
LOG_DIR = os.path.join(BASE_DIR, "logs", "bkdata", "{}api".format(APP_NAME))
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

LOG_CLASS = "logging.handlers.ConcurrentRotatingFileHandler"

try:
    import socket

    hostname = socket.gethostname()
except Exception:
    hostname = ""


def get_loggings(log_level):
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "verbose": {
                "format": (
                    "%(asctime)s.%(msecs)03d|{hostname}|%(pathname)s|" "%(lineno)d|%(levelname)s||||||%(message)s\n"
                ).format(hostname=hostname),
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
            "simple": {"format": "%(message)s"},
        },
        "handlers": {
            "null": {
                "level": "DEBUG",
                "class": "logging.NullHandler",
            },
            "console": {"level": "DEBUG", "class": "logging.StreamHandler", "formatter": "simple"},
            "root": {
                "class": LOG_CLASS,
                "formatter": "verbose",
                "filename": os.path.join(LOG_DIR, "sys.log"),
                "maxBytes": LOG_MAX_BYTES,
                "backupCount": LOG_BACKUP_COUNT,
                "encoding": "utf8",
            },
            "sys": {
                "class": LOG_CLASS,
                "formatter": "verbose",
                "filename": os.path.join(LOG_DIR, "sys.log"),
                "maxBytes": LOG_MAX_BYTES,
                "backupCount": LOG_BACKUP_COUNT,
                "encoding": "utf8",
            },
            "api": {
                "class": LOG_CLASS,
                "formatter": "simple",
                "filename": os.path.join(LOG_DIR, "api.log"),
                "maxBytes": LOG_MAX_BYTES,
                "backupCount": LOG_BACKUP_COUNT,
                "encoding": "utf8",
            },
            "jaeger_tracing": {
                "class": LOG_CLASS,
                "formatter": "simple",
                "filename": os.path.join(LOG_DIR, "tracing.log"),
                "maxBytes": LOG_MAX_BYTES,
                "backupCount": LOG_BACKUP_COUNT,
                "encoding": "utf8",
            },
        },
        "loggers": {
            "django": {
                "handlers": ["null"],
                "level": "INFO",
                "propagate": True,
            },
            "django.request": {
                "handlers": ["root"],
                "level": "ERROR",
                "propagate": True,
            },
            # the root logger, for all the project
            "root": {
                "handlers": ["root"],
                "level": log_level,
                "propagate": False,
            },
            # Logging config for api
            "api": {
                "handlers": ["api"],
                "level": log_level,
                "propagate": False,
            },
            "sys": {
                "handlers": ["sys"],
                "level": log_level,
                "propagate": False,
            },
            # For ESB-SDK component LOGGER
            "component": {"handlers": ["root"], "level": log_level, "propagate": False},
            "jaeger_tracing": {"handlers": ["jaeger_tracing"], "level": log_level, "propagate": False},
        },
    }


LOGGING = get_loggings(LOGGER_LEVEL)

# Cache

CACHES = {
    "db": {
        "BACKEND": "django.core.cache.backends.db.DatabaseCache",
        "LOCATION": "django_cache",
    },
    "locmem": {
        "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    },
}

CACHES["default"] = CACHES["locmem"]

# Authentication 认证配置
# 是否开启对来自 ESB 用户身份的强校验模式，开启后 jwt.verify=False 的请求均返回失败
FORCE_USER_VERIFY = False

# TRACING 配置
DO_TRACE = False
TRACING_SAMPLE_RATE = 1
