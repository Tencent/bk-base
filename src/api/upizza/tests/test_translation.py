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
import unittest
from unittest import mock

from common.api.base import DataResponse
from common.bklanguage import BkLanguage, bktranslate, bktranslates
from common.local import del_local_param, get_local_param


class TestTranslation(unittest.TestCase):
    def setUp(self):
        self.language = "zh-cn"
        self.translate_result = DataResponse(
            {"data": "测试", "result": True, "message": "", "errors": None, "code": "1500200"}
        )

        self.translates_result = DataResponse(
            {
                "data": [
                    {"content_key": "haha", "language": "zh-cn", "content_value": "哈哈"},
                    {"content_key": "hehe", "language": "zh-cn", "content_value": "呵呵"},
                ],
                "result": True,
                "message": "",
                "errors": None,
                "code": "1500200",
            }
        )
        del_local_param("translates")

    @mock.patch("common.bklanguage.BkLanguage.current_language")
    @mock.patch("common.api.modules.meta.MetaApi.content_language_configs.translate")
    def test_translate(self, patch_translate, patch_language):
        patch_language.return_value = self.language
        patch_translate.return_value = self.translate_result

        assert get_local_param("translates", None) is None
        content_key = "test"
        content_value = bktranslate(content_key)
        assert get_local_param("translates", None) is not None
        assert content_value == "测试"

    @mock.patch("common.bklanguage.BkLanguage.current_language")
    @mock.patch("common.api.modules.meta.MetaApi.content_language_configs.list")
    def test_translates(self, patch_translates, patch_language):
        patch_language.return_value = self.language
        patch_translates.return_value = self.translates_result

        assert BkLanguage.translates_cache is None
        assert bktranslates("haha") == "哈哈"
        assert bktranslates("hehe") == "呵呵"
        assert BkLanguage.translates_cache is not None
