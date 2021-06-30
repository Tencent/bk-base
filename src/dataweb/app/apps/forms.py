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
import json

from django.forms import (
    BooleanField,
    CharField,
    ChoiceField,
    Field,
    FileField,
    Form,
    GenericIPAddressField,
    IntegerField,
    ValidationError,
)
from django.utils.encoding import smart_text
from django.utils.translation import ugettext_lazy as _

from apps.dataflow.handlers.result_table import ResultTable


def html_escape(s):
    """
    Replace special characters "&", "<" and ">" to HTML-safe sequences.
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
                errmsg_arr.extend(["{} | {}".format(label, _msg) for _msg in _keymsg_arr])
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
            return value

        if not (type(value) == list):
            raise ValidationError(_("字段类型必须为数组"))
        return value

    def validate(self, value):
        super(ListField, self).validate(value)


class DictField(Field):
    """
    字典字段
    """

    def to_python(self, value):
        if not (type(value) == dict):
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
        except Exception:
            raise ValidationError(_("字段格式不对，必须符合json格式"))
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


class SortForm(BaseForm):
    ordering = CharField(required=False)
    order = CharField(required=False)
    is_reverse = BooleanField(required=False)

    # 排序
    def sort(self, data, default_order):
        ordering = self.data.get("ordering", "")
        if ordering != "":
            if ordering[0] == "-":
                ordering = ordering[1:]
                is_reverse = True
            else:
                is_reverse = False
        else:
            ordering = default_order
            is_reverse = True
        data = sorted(data, key=lambda _d: _d[ordering], reverse=is_reverse)
        return data


class HostsField(ListField):
    class HostForm(BaseForm):
        ip = GenericIPAddressField(protocol="IPv4")
        plat_id = IntegerField()

    def clean(self, value):
        cleaned_value = super(HostsField, self).clean(value)
        cleaned_hosts = []
        for _h in cleaned_value:
            _hf = self.HostForm(_h)
            if not _hf.is_valid():
                raise ValidationError(_hf.format_errmsg())
            cleaned_hosts.append(_h)
        return cleaned_hosts


class StorageField(CharField):
    def to_python(self, value):
        if value in ResultTable.get_supported_storage_query() or value is None:
            return value
        else:
            raise ValidationError(_("暂不支持存储类型为{}的结果表").format(value))

    def validate(self, value):
        super(StorageField, self).validate(value)


def check_ip_rule(ipaddr):
    ipaddr = str(ipaddr)
    addr = ipaddr.strip().split(".")  # 切割IP地址为一个列表
    # print addr
    if len(addr) != 4:  # 切割后列表必须有4个参数
        return False
    for i in range(4):
        try:
            addr[i] = int(addr[i])  # 每个参数必须为数字，否则校验失败
        except Exception:
            return False
        if addr[i] <= 255 and addr[i] >= 0:  # 每个参数值必须在0-255之间
            pass
        else:
            return False
        i += 1
    else:
        return True
