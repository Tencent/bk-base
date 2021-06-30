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

import json

from django import forms
from django.forms import BooleanField, CharField, ChoiceField, Field, FileField, Form, ValidationError
from django.utils.encoding import smart_text
from django.utils.translation import ugettext as _
from django.utils.translation import ugettext_lazy as ugettext


def html_escape(s):
    """Replace special characters "&", "<" and ">" to HTML-safe sequences.
    If the optional flag quote is true, the quotation mark character (")
    is also translated.
    """
    s = s.replace("&", "&amp;")
    s = s.replace("<", "&lt;")
    s = s.replace(">", "&gt;")
    s = s.replace(" ", "&nbsp;")
    s = s.replace('"', "&quot;")
    s = s.replace("'", "&#39;")

    return s


def html_exempt(s):
    s = s.replace("&amp;", "&")
    s = s.replace("&lt;", "<")
    s = s.replace("&gt;", ">")
    s = s.replace("&nbsp;", " ")
    s = s.replace("&quot;", '"')
    s = s.replace("&#39;", "'")

    return s


class BaseForm(Form):
    def __init__(self, *args, **kwargs):
        if "is_create" in kwargs:
            if not kwargs["is_create"] and "table_name" in self.base_fields:
                required = self.base_fields["table_name"].required
                self.base_fields["table_name"] = forms.RegexField(
                    required=required,
                    regex=r"^[a-zA-Z][_a-zA-Z0-9]*$",
                    error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头")},
                    max_length=255,
                    label=_("计算标识"),
                )
            del kwargs["is_create"]
        super(BaseForm, self).__init__(*args, **kwargs)

    # strip leading or trailing whitespace
    def _clean_fields(self):
        for name, field in list(self.fields.items()):
            # value_from_datadict() gets the data from the data dictionaries.
            # Each widget type knows how to retrieve its own data, because some
            # widgets split data over several HTML fields.
            value = field.widget.value_from_datadict(self.data, self.files, self.add_prefix(name))
            try:
                if isinstance(field, FileField):
                    initial = self.initial.get(name, field.initial)
                    value = field.clean(value, initial)
                else:
                    if isinstance(value, str):
                        value = field.clean(value.strip())
                    else:
                        value = field.clean(value)
                self.cleaned_data[name] = value
                if hasattr(self, "clean_%s" % name):
                    value = getattr(self, "clean_%s" % name)()
                    self.cleaned_data[name] = value
            except ValidationError as e:
                self.add_error(name, e)

    def format_errmsg(self):
        errors = self.errors
        # django 1.8，取declared_fields
        declared_fields = self.declared_fields
        errmsg_arr = []
        for _key in errors:
            _keymsg_arr = errors[_key]
            if _key in declared_fields:
                _label = declared_fields[_key].label
                label = _label if _label else _key
                _errmsg = []
                for _err in _keymsg_arr.data:
                    if _err.code in ["max_length", "min_length"]:
                        # 为了兼容django1.8 zh-hans表单中文翻译bug的问题
                        _msg = ugettext(_err.message % _err.params.get("limit_value", 0)) % _err.params
                    else:
                        _msg = _err.message
                    _errmsg.append("{}，{}".format(label, _msg))

                errmsg_arr.extend(_errmsg)
            if _key == "__all__":
                errmsg_arr.extend(_keymsg_arr)
        return errmsg_arr[0]


class MultiStrSplitByCommaField(Field):
    """
    多个字段，使用逗号隔开
    """

    def to_python(self, value):
        if not value:
            return []

        return [_v for _v in value.split(",") if _v != ""]

    def validate(self, value):
        super(MultiStrSplitByCommaField, self).validate(value)


class MultiStrSplitBySemiconField(Field):
    """
    多个字段，使用分号隔开
    """

    def to_python(self, value):
        if not value:
            return []

        return [_v for _v in value.split(";") if _v != ""]

    def validate(self, value):
        super(MultiStrSplitBySemiconField, self).validate(value)


class MulChoiceFieldByComma(ChoiceField):
    """
    多选字段，使用逗号隔开
    """

    default_error_messages = {
        "invalid_choice": _("Select a valid choice. %(value)s is not one of the available choices."),
        "invalid_list": _("Enter a list of values."),
    }

    def to_python(self, value):
        if not value:
            return []
        else:
            value = value.split(",")
        return [smart_text(val) for val in value]

    def validate(self, value):
        """
        Validates that the input is a list or tuple.
        """
        if self.required and not value:
            raise ValidationError(self.error_messages["required"])
        # Validate that each value in the value list is in self.choices.
        for val in value:
            if not self.valid_value(val):
                raise ValidationError(self.error_messages["invalid_choice"] % {"value": val})


class ListField(Field):
    """
    列表字段
    """

    def to_python(self, value):
        if value is None:
            return []

        if not (isinstance(value, list)):
            raise ValidationError(_("字段类型必须为数组"))
        return value

    def validate(self, value):
        super(ListField, self).validate(value)


class DictField(Field):
    """
    字典字段
    """

    def to_python(self, value):
        if value is None:
            return None
        if not (isinstance(value, dict)):
            raise ValidationError(_("字段类型必须为字典类型"))
        return value

    def validate(self, value):
        super(DictField, self).validate(value)


class JSONField(Field):
    def to_python(self, value):
        if value is None:
            return None
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            try:
                # 若本身就是json格式，直接返回，为了兼容自更新情况
                json.dumps(value)
            except (TypeError, ValueError):
                raise ValidationError(_("字段格式不对，必须符合json格式"))
            else:
                return value
        return value


class HtmlEscapeCharField(CharField):
    """
    转义 html 相关字符，其他使用方式与 CharField 一致
    """

    def to_python(self, value):
        raw_value = super(HtmlEscapeCharField, self).to_python(value)
        return html_exempt(raw_value)

    def clean(self, value):
        cleaned_value = super(HtmlEscapeCharField, self).clean(value)
        return html_escape(cleaned_value)


def check_ip_rule(ipaddr):
    ipaddr = str(ipaddr)
    addr = ipaddr.strip().split(".")  # 切割IP地址为一个列表
    # print addr
    if len(addr) != 4:  # 切割后列表必须有4个参数
        return False
    for i in range(4):
        try:
            addr[i] = int(addr[i])  # 每个参数必须为数字，否则校验失败
        except BaseException:
            return False
        if addr[i] <= 255 and addr[i] >= 0:  # 每个参数值必须在0-255之间
            pass
        else:
            return False
        i += 1
    else:
        return True


class BooleanFieldsRequiredMixin(Form):
    """
    当BooleanField通过设置required为True时要求用户必须传入该参数，但允许该字段值为False，原表单认证会不通过
    为解决这种情况，表单类继承该类，嗯，就是这么坑
    1. https://stackoverflow.com/questions/10440937/what-is-wrong-with-the-django-forms-booleanfield-unit-test-case
    2. https://code.djangoproject.com/ticket/5957
    """

    def clean(self):
        for field_name, field in list(self.fields.items()):
            # Only BooleanField not subclasses of it.
            if not isinstance(field, BooleanField):
                continue

            if field_name not in self.data:
                self._errors[field_name] = self.error_class([ValidationError(_("该参数为必需项"))])

        return super(BooleanFieldsRequiredMixin, self).clean()


class CheckboxInput(forms.CheckboxInput):
    """
    为 BooleanField 设置默认值共用类
    """

    def __init__(self, default=False, *args, **kwargs):
        super(CheckboxInput, self).__init__(*args, **kwargs)
        self.default = default

    def value_from_datadict(self, data, files, name):
        if name not in data:
            return self.default
        return super(CheckboxInput, self).value_from_datadict(data, files, name)
