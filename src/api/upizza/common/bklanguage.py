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
from threading import RLock

from common.api import BkLoginApi
from common.local import get_local_param, set_local_param
from django.conf import settings
from django.utils import translation
from django.utils.translation.trans_real import get_supported_language_variant


class BkLanguage:
    """
    当前请求需要的语言环境
    在中间件BkLocaleMiddleware中根据http header初始化
    """

    default_language_code = settings.LANGUAGE_CODE

    EN = settings.BK_LANGUAGE_EN
    CN = settings.BK_LANGUAGE_CN
    ALL = settings.BK_LANGUAGE_ALL

    @staticmethod
    def current_language():
        """
        获取当前请求的语言环境
        :return:
        """
        return get_local_param("language", BkLanguage.default_language_code)

    @staticmethod
    def set_language(language_code, request=False):
        """
        设置当前请求的语言环境
        :param language_code: 取值只能在setting中定义的语言列表中选择
        :return:
        """
        input_language_code = language_code
        if input_language_code == "zh-hans":
            # 统一
            input_language_code = BkLanguage.CN

        from pizza.settings_default import BK_SUPPORTED_LANGUAGES

        if input_language_code not in BK_SUPPORTED_LANGUAGES:
            return False

        set_local_param("language", input_language_code)

        # 如果是双语， 则默认语言为英文
        if language_code == BkLanguage.ALL:
            language_key = BkLanguage.EN
        else:
            language_key = language_code

        # 设置Django国际化语言
        if language_key == "zh-cn":
            # zh-cn在Django中不建议用， 统一使用zh-hans
            language_key = "zh-hans"

        language = get_supported_language_variant(language_key)
        translation.activate(language)
        if request:
            request.LANGUAGE_CODE = translation.get_language()
        return True

    @staticmethod
    def get_user_language(bk_username, default=None):
        language = BkLanguage.current_language()

        try:
            res = BkLoginApi.get_user_info(
                {
                    "bk_username": bk_username,
                }
            )
            user_info = res.data if res.is_success() else {}
            if "language" in user_info:
                language = user_info["language"]
        except Exception:
            if default:
                language = default

        return language

    @staticmethod
    def translate(origin_string, language_code):
        """
        把当前origin_string翻译为指定语言
        :param origin_string:
        :param language_code:
        :return:
        """
        from pizza.settings_default import BK_SUPPORTED_LANGUAGES

        if language_code not in BK_SUPPORTED_LANGUAGES:
            return False

        language_key = language_code
        if language_key == "zh-cn":
            # zh-cn在Django中不建议用， 统一使用zh-hans
            language_key = "zh-hans"

        # 获取国际化翻译对象
        translator = translation._trans.translation(language_key)
        # 翻译
        target_message = translator.ugettext(origin_string)
        return target_message

    @staticmethod
    def to_en(origin_string):
        """
        把当前origin_string翻译为英文
        :param origin_string:
        :return:
        """
        language_key = BkLanguage.EN

        if language_key == "zh-cn":
            # zh-cn在Django中不建议用， 统一使用zh-hans
            language_key = "zh-hans"

        # 获取国际化翻译对象
        translator = translation._trans.translation(language_key)
        # 翻译
        target_message = translator.ugettext(origin_string)
        return target_message

    @staticmethod
    def to_cn(origin_string):
        """
        把当前origin_string翻译为中文
        :param origin_string:
        :return:
        """
        language_key = BkLanguage.CN
        if language_key == "zh-cn":
            # zh-cn在Django中不建议用， 统一使用zh-hans
            language_key = "zh-hans"

        # 获取国际化翻译对象
        translator = translation._trans.translation(language_key)
        # 翻译
        target_message = translator.ugettext(origin_string)
        return target_message

    translates_cache = None
    cache_lock = RLock()

    @classmethod
    def translate_multiply_by_config_db(cls, content_key):
        """
        根据配置库语言对照配置表进行翻译
        """
        from common.api.modules.meta import MetaApi

        target_language = BkLanguage.current_language()
        translates = cls.translates_cache
        if translates is None:
            with cls.cache_lock:
                if translates is None:
                    response = MetaApi.content_language_configs.list()
                    content_language_configs = response.data if response.is_success() else []
                    translates = {}
                    for config in content_language_configs:
                        if config["content_key"] not in translates:
                            translates[config["content_key"]] = {}
                        translates[config["content_key"]][config["language"]] = config["content_value"]
                    cls.translates_cache = translates
        return translates.get(content_key, {}).get(target_language, content_key)

    @staticmethod
    def translate_singly_by_config_db(content_key):
        """
        根据配置库语言对照配置表进行翻译
        """
        from common.api.modules.meta import MetaApi

        target_language = BkLanguage.current_language()
        translates = get_local_param("translates", {})
        if content_key not in translates or target_language not in translates[content_key]:
            response = MetaApi.content_language_configs.translate(
                {
                    "content_key": content_key,
                    "language": target_language,
                }
            )
            if response.is_success():
                translates.update({content_key: {target_language: response.data}})
        set_local_param("translates", translates)
        return translates.get(content_key, {}).get(target_language, content_key)


bktranslate = BkLanguage.translate_singly_by_config_db
bktranslates = BkLanguage.translate_multiply_by_config_db
