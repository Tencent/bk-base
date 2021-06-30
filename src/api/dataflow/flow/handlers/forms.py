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

from django import forms
from django.utils.translation import ugettext_lazy as _

from dataflow.flow.api_models import ResultTable
from dataflow.flow.handlers.node_utils import NodeUtils
from dataflow.flow.utils.forms import BaseForm, CheckboxInput, DictField, JSONField, ListField
from dataflow.shared.log import flow_logger as logger


class UnifiedProcessingNodeForm(BaseForm):
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    name = forms.CharField(max_length=255, label=_("节点名称"))
    # # 给计算模块使用，用户并不关心
    # processing_name = forms.CharField(required=False, max_length=255, label=_(u"计算名称"))
    # 上游节点信息 [UnifiedInputsForm]
    inputs = ListField(required=False, label=_("上游节点信息"))
    # 输出信息 [UnifiedOutputsForm]
    outputs = ListField(required=False, label=_("输出信息列表"))
    # 每个计算节点的专用配置各有区别
    dedicated_config = DictField(required=False, label=_("节点专用配置"))
    # 窗口配置
    window_info = ListField(required=False, label=_("窗口配置"))
    # 冗余字段
    node_type = forms.CharField(required=False, max_length=255, label=_("节点类型"))
    api_version = forms.CharField(required=False, max_length=255, label=_("api版本"))

    class UnifiedInputsForm(BaseForm):
        id = forms.IntegerField(label=_("上游节点ID"))
        from_result_table_ids = ListField(label=_("上游节点ID所选结果表列表"))

    class UnifiedOutputsForm(BaseForm):
        bk_biz_id = forms.IntegerField()
        table_name = forms.RegexField(
            regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
            error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
            max_length=255,
            label=_("结果数据表"),
        )
        output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
        fields = ListField(label=_("字段信息列表"), required=False)

        class UnifiedOutputFieldForm(BaseForm):
            """
            输出字段格式配置
            """

            field_name = forms.CharField(label="字段名称")
            field_type = forms.CharField(label="字段类型")
            field_alias = forms.CharField(label="字段中文名")
            event_time = forms.BooleanField(
                required=False,
                widget=CheckboxInput(default=False),
                label=_("是否标记为数据时间"),
            )

        def clean_fields(self):
            fields = self.cleaned_data["fields"]
            event_time_fields = []
            for _field in fields:
                _sub_form = self.UnifiedOutputFieldForm(_field)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
                    _whole_err_msg = "字段信息列表格式错误: {err_msg}".format(err_msg=_err_msg)
                    logger.error(_(_whole_err_msg))
                    self._errors["fields"] = self.error_class([forms.ValidationError(_whole_err_msg)])
                    break
                if _sub_form.data.get("event_time", False):
                    event_time_fields.append(_sub_form.data.get("field_name"))
            if len(event_time_fields) > 1:
                self._errors["fields"] = self.error_class(
                    [forms.ValidationError(_("一个结果表不可配置多个数据时间字段(%s)") % ", ".join(event_time_fields))]
                )
            return fields

    class StreamWindowInfoForm(BaseForm):
        window_type = forms.CharField(required=False, max_length=50, label=_("窗口类型"))
        window_size = forms.CharField(required=False, max_length=50, label=_("窗口大小"))
        window_size_unit = forms.CharField(required=False, max_length=50, label=_("窗口大小单位"))
        window_count_freq = forms.CharField(required=False, max_length=50, label=_("窗口统计频率"))
        window_count_freq_unit = forms.CharField(required=False, max_length=50, label=_("窗口统计频率单位"))
        window_wait = forms.CharField(required=False, max_length=50, label=_("窗口等待时间"))
        window_wait_unit = forms.CharField(required=False, max_length=50, label=_("窗口等待时间单位"))
        window_delay = forms.CharField(required=False, max_length=50, label=_("延迟时间"))
        window_delay_unit = forms.CharField(required=False, max_length=50, label=_("延迟时间单位"))
        window_delay_count_freq = forms.CharField(required=False, max_length=50, label=_("统计频率"))
        window_delay_count_freq_unit = forms.CharField(required=False, max_length=50, label=_("统计频率单位"))
        window_gap = forms.CharField(required=False, max_length=50, label=_("间隔时间"))
        window_gap_unit = forms.CharField(required=False, max_length=50, label=_("间隔时间单位"))
        window_expire = forms.CharField(required=False, max_length=50, label=_("过期时间"))
        window_expire_unit = forms.CharField(required=False, max_length=50, label=_("过期时间单位"))

        def clean_window_type(self):
            window_type = self.cleaned_data["window_type"]
            if window_type is None:
                return ""
            return window_type

        def clean_window_size(self):
            window_size = self.cleaned_data["window_size"]
            if window_size is None:
                return ""
            return window_size

        def clean_window_size_unit(self):
            window_size_unit = self.cleaned_data["window_size_unit"]
            if window_size_unit is None:
                return ""
            return window_size_unit

        def clean_window_count_freq(self):
            window_count_freq = self.cleaned_data["window_count_freq"]
            if window_count_freq is None:
                return ""
            return window_count_freq

        def clean_window_count_freq_unit(self):
            window_count_freq_unit = self.cleaned_data["window_count_freq_unit"]
            if window_count_freq_unit is None:
                return ""
            return window_count_freq_unit

        def clean_window_wait(self):
            window_wait = self.cleaned_data["window_wait"]
            if window_wait is None:
                return ""
            return window_wait

        def clean_window_wait_unit(self):
            window_wait_unit = self.cleaned_data["window_wait_unit"]
            if window_wait_unit is None:
                return ""
            return window_wait_unit

        def clean_window_delay(self):
            window_delay = self.cleaned_data["window_delay"]
            if window_delay is None:
                return ""
            return window_delay

        def clean_window_delay_unit(self):
            window_delay_unit = self.cleaned_data["window_delay_unit"]
            if window_delay_unit is None:
                return ""
            return window_delay_unit

        def clean_window_delay_count_freq(self):
            window_delay_count_freq = self.cleaned_data["window_delay_count_freq"]
            if window_delay_count_freq is None:
                return ""
            return window_delay_count_freq

        def clean_window_delay_count_freq_unit(self):
            window_delay_count_freq_unit = self.cleaned_data["window_delay_count_freq_unit"]
            if window_delay_count_freq_unit is None:
                return ""
            return window_delay_count_freq_unit

        def clean_window_gap(self):
            window_gap = self.cleaned_data["window_gap"]
            if window_gap is None:
                return ""
            return window_gap

        def clean_window_gap_unit(self):
            window_gap_unit = self.cleaned_data["window_gap_unit"]
            if window_gap_unit is None:
                return ""
            return window_gap_unit

        def clean_window_expire(self):
            window_expire = self.cleaned_data["window_expire"]
            if window_expire is None:
                return ""
            return window_expire

        def clean_window_expire_unit(self):
            window_expire_unit = self.cleaned_data["window_expire_unit"]
            if window_expire_unit is None:
                return ""
            return window_expire_unit

    class BatchWindowInfoForm(BaseForm):
        result_table_id = forms.CharField(required=False, max_length=50, label=_("rt id"))
        window_type = forms.CharField(required=False, max_length=50, label=_("窗口类型"))
        window_size = forms.CharField(required=False, max_length=50, label=_("窗口大小"))
        window_size_unit = forms.CharField(required=False, max_length=50, label=_("窗口大小单位"))
        window_offset = forms.CharField(required=False, max_length=50, label=_("窗口偏移值"))
        window_offset_unit = forms.CharField(required=False, max_length=50, label=_("窗口偏移值单位"))
        dependency_rule = forms.CharField(required=False, max_length=50, label=_("调度中的依赖策略"))
        window_start_offset = forms.CharField(required=False, max_length=50, label=_("窗口起始偏移"))
        window_start_offset_unit = forms.CharField(required=False, max_length=50, label=_("窗口起始偏移单位"))
        window_end_offset = forms.CharField(required=False, max_length=50, label=_("窗口结束偏移"))
        window_end_offset_unit = forms.CharField(required=False, max_length=50, label=_("窗口结束偏移单位"))
        accumulate_start_time = forms.CharField(required=False, max_length=50, label=_("累加起始基准值"))

        def clean_result_table_id(self):
            result_table_id = self.cleaned_data["result_table_id"]
            if result_table_id is None:
                return ""
            return result_table_id

        def clean_window_type(self):
            window_type = self.cleaned_data["window_type"]
            if window_type is None:
                return ""
            return window_type

        def clean_window_size(self):
            window_size = self.cleaned_data["window_size"]
            if window_size is None:
                return ""
            return window_size

        def clean_window_size_unit(self):
            window_size_unit = self.cleaned_data["window_size_unit"]
            if window_size_unit is None:
                return ""
            return window_size_unit

        def clean_window_offset(self):
            window_offset = self.cleaned_data["window_offset"]
            if window_offset is None:
                return ""
            return window_offset

        def clean_window_offset_unit(self):
            window_offset_unit = self.cleaned_data["window_offset_unit"]
            if window_offset_unit is None:
                return ""
            return window_offset_unit

        def clean_dependency_rule(self):
            dependency_rule = self.cleaned_data["dependency_rule"]
            if dependency_rule is None:
                return ""
            return dependency_rule

        def clean_window_start_offset(self):
            window_start_offset = self.cleaned_data["window_start_offset"]
            if window_start_offset is None:
                return ""
            return window_start_offset

        def clean_window_start_offset_unit(self):
            window_start_offset_unit = self.cleaned_data["window_start_offset_unit"]
            if window_start_offset_unit is None:
                return ""
            return window_start_offset_unit

        def clean_window_end_offset(self):
            window_end_offset = self.cleaned_data["window_end_offset"]
            if window_end_offset is None:
                return ""
            return window_end_offset

        def clean_window_end_offset_unit(self):
            window_end_offset_unit = self.cleaned_data["window_end_offset_unit"]
            if window_end_offset_unit is None:
                return ""
            return window_end_offset_unit

        def clean_accumulate_start_time(self):
            accumulate_start_time = self.cleaned_data["accumulate_start_time"]
            if accumulate_start_time is None:
                return ""
            return accumulate_start_time

    def clean_inputs(self):
        """
        [
            {
                "id": 1,
                "from_result_table_ids": ["x1", "x2"]
            },
            {
                "id": 1,
                "from_result_table_ids": ["x1", "x2"]
            }
        ]
        """
        inputs = self.cleaned_data["inputs"]
        for from_node in inputs:
            _err_msg = None
            if not isinstance(from_node, dict):
                _err_msg = _("格式错误，非 JSON 类型")
            else:
                _sub_form = self.UnifiedInputsForm(from_node)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
            if _err_msg:
                _whole_err_msg = _("上游节点结果表信息格式错误: %(err_msg)s") % {"err_msg": _err_msg}
                logger.error(_whole_err_msg)
                self._errors["inputs"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return inputs

    def clean_outputs(self):
        """
        [
            {
                "bk_biz_id": 591,
                "fields": [],
                "output_name": "离线计算节点",
                "table_name": "min_price_test13_fixed_window_1428_3h1",
                "validate": {
                    "table_name": {
                        "status": false,
                        "errorMsg": ""
                    },
                    "output_name": {
                        "status": false,
                        "errorMsg": ""
                    },
                    "field_config": {
                        "status": false,
                        "errorMsg": "必填项不可为空"
                    }
                }
            }
        ]
        """
        outputs = self.cleaned_data["outputs"]
        for one_output in outputs:
            _err_msg = None
            if not isinstance(one_output, dict):
                _err_msg = _("格式错误，非 JSON 类型")
            else:
                _sub_form = self.UnifiedOutputsForm(one_output)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
            if _err_msg:
                _whole_err_msg = _("输出信息格式错误: %(err_msg)s") % {"err_msg": _err_msg}
                logger.error(_whole_err_msg)
                self._errors["outputs"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return outputs

    def clean_window_info(self):
        """
        [
            {
                //实时窗口
                "window_type": "accumulate", //窗口类型，无窗口/滚动窗口/滑动窗口/累加窗口/会话窗口
                "window_size": "3", //窗口大小，在滑动窗口/累加窗口中使用
                "window_size_unit": "hour", //窗口大小单位
                "window_count_freq": "3", //窗口统计频率，在滚动窗口/滑动窗口/累加窗口中使用
                "window_count_freq_unit": "hour", //窗口统计频率单位，在滚动窗口/滑动窗口/累加窗口中使用
                "window_wait": "3", //窗口等待时间，
                "window_wait_unit": "hour", //窗口等待时间单位，在所有窗口类型都会使用到
                "window_delay": "3", //延迟时间 在是否计算延迟数据配置下，在所有窗口类型都会使用到
                "window_delay_unit": "hour", //延迟时间单位 在是否计算延迟数据配置下，在所有窗口类型都会使用到
                "window_delay_count_freq": "3", //统计频率 在是否计算延迟数据配置下，在所有窗口类型都会使用到
                "window_delay_count_freq_unit": "hour", //统计频率单位 在是否计算延迟数据配置下，在所有窗口类型都会使用到
                "window_gap": "3", //间隔时间 在会话窗口中使用
                "window_gap_unit": "hour", //间隔时间单位 在会话窗口中使用
                "window_expire": "3", //过期时间 在会话窗口中使用
                "window_expire_unit": "hour" //过期时间单位 在会话窗口中使用
            },
            {
                //离线窗口
                "result_table_id": "xxx", //上游rt id
                "window_type": "accumulate/scroll/slide/whole", //窗口类型，滚动窗口/滑动窗口/累加窗口/全量数据
                "window_offset": "1", //窗口偏移值，在滚动窗口/滑动窗口/累加窗口中使用
                "window_offset_unit": "hour", //窗口偏移值单位，在滚动窗口/滑动窗口/累加窗口中使用
                "dependency_rule": "at_least_one_finished", //调度中的依赖策略，所有窗口类型都会使用到
                "window_size": "3", //窗口大小，在滑动窗口/累加窗口中使用
                "window_size_unit": "hour", //窗口大小单位，在滑动窗口/累加窗口中使用
                "window_start_offset": "1", //窗口起始偏移，在累加窗口中使用
                "window_start_offset_unit": "hour", //窗口起始偏移单位，在累加窗口中使用
                "window_end_offset": "2", //窗口结束偏移，在累加窗口中使用
                "window_end_offset_unit": "hour", //窗口结束偏移单位，在累加窗口中使用
                "accumulate_start_time": 1584414000000 //累加起始基准值，在累加窗口中使用
            }
        ]
        """
        window_infos = self.cleaned_data["window_info"]
        # node_type 是外层冗余传递到node_config中到
        node_type = self.data["node_type"]
        for one_window_info in window_infos:
            _err_msg = None
            if not isinstance(one_window_info, dict):
                _err_msg = _("格式错误，非 JSON 类型")
            else:
                _sub_form = None
                if node_type == "batchv2":
                    _sub_form = self.BatchWindowInfoForm(one_window_info)
                elif node_type == "streamv2":
                    _sub_form = self.StreamWindowInfoForm(one_window_info)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
            if _err_msg:
                _whole_err_msg = _("窗口信息格式错误: %(err_msg)s") % {"err_msg": _err_msg}
                logger.error(_whole_err_msg)
                self._errors["window_infos"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return window_infos

    def clean(self):
        form_data = super(UnifiedProcessingNodeForm, self).clean()
        return form_data


class StandardBaseForm(BaseForm):
    """
    标准表单，所有即将改造为适应多输出的节点表单需要继承该类
    """

    from_nodes = ListField(label=_("上游节点信息"), required=False)

    class FromResultTableForm(BaseForm):
        id = forms.IntegerField(label=_("上游节点ID"))
        from_result_table_ids = ListField(label=_("上游节点ID所选结果表列表"))

    def clean_from_nodes(self):
        """
            [
                {
                    "id": 1,
                    "from_result_table_ids": ["x1", "x2"]
                },
                {
                    "id": 1,
                    "from_result_table_ids": ["x1", "x2"]
                }
            ]
        @return:
        """
        from_nodes = self.cleaned_data["from_nodes"]
        for from_node in from_nodes:
            _err_msg = None
            if not isinstance(from_node, dict):
                _err_msg = _("格式错误，非 JSON 类型")
            else:
                _sub_form = self.FromResultTableForm(from_node)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
            if _err_msg:
                _whole_err_msg = _("上游节点结果表信息格式错误: %(err_msg)s") % {"err_msg": _err_msg}
                logger.error(_whole_err_msg)
                self._errors["from_nodes"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return from_nodes


class FlowRTsourceNodeForm(BaseForm):
    result_table_id = forms.CharField(label=_("结果表"))
    name = forms.CharField(label=_("结果表名称"))
    from_result_table_ids = ListField(label=_("父rt列表"), required=False)

    def clean(self):
        cleaned_data = self.cleaned_data
        if not self.errors:
            o_rt = ResultTable(result_table_id=cleaned_data["result_table_id"])
            if not o_rt.is_exist():
                raise forms.ValidationError(_("结果表不存在"))

            cleaned_data["bk_biz_id"] = o_rt.bk_biz_id

        return cleaned_data

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids


class StandardSourceForm(StandardBaseForm):
    result_table_id = forms.CharField(label=_("结果表"))
    name = forms.CharField(label=_("结果表名称"))

    def clean(self):
        cleaned_data = self.cleaned_data
        if not self.errors:
            o_rt = ResultTable(result_table_id=cleaned_data["result_table_id"])
            if not o_rt.is_exist():
                raise forms.ValidationError(_("结果表不存在"))

            cleaned_data["bk_biz_id"] = o_rt.bk_biz_id
            cleaned_data["from_nodes"] = []

        return cleaned_data


# 计算/模型节点统一表单，继承 StandardBaseForm，里面有 from_nodes 参数
# 目前先试用节点：模型实例，模型实例指标(实时/节点)
class FlowProcessingNodeStandardForm(StandardBaseForm):
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    name = forms.CharField(max_length=50, label=_("节点名称"))
    # 给计算模块使用，用户并不关心
    processing_name = forms.CharField(max_length=50, label=_("计算名称"), required=False)
    outputs = ListField(label=_("输出信息列表"), required=False)
    window_config = DictField(required=False, label=_("窗口配置"))
    # 每个计算节点的专用配置各有区别
    dedicated_config = DictField(required=False, label=_("节点专用配置"))

    def clean(self):
        form_data = super(FlowProcessingNodeStandardForm, self).clean()
        return form_data


def FlowRealtimeNodeForm(cls, params, is_create):
    REALTIME_NODE_FORM_MAP = {
        "none": FlowRealtimeNodeNoneForm,
        "scroll": FlowRealtimeNodeScrollForm,
        "slide": FlowRealtimeNodeSlideForm,
        "accumulate": FlowRealtimeNodeAccumulateForm,
        "session": FlowRealtimeNodeSessionForm,
    }

    if "window_type" in params and params["window_type"] in REALTIME_NODE_FORM_MAP:
        return REALTIME_NODE_FORM_MAP[params["window_type"]](params, is_create=is_create)
    else:
        # 默认表单
        return FlowRealtimeNodeNoneForm(params, is_create=is_create)


class FlowRealtimeNodeBaseForm(BaseForm):
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    name = forms.CharField(max_length=50, label=_("计算描述"))
    sql = forms.CharField(label="SQL")
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
    window_lateness = DictField(required=False, label=_("计算延迟数据配置"))
    from_result_table_ids = ListField(label=_("父rt列表"), required=False)
    # 增加数据修正 correct_config_id,is_open_correct
    correct_config_id = forms.IntegerField(label=_("修正配置ID"), required=False)
    is_open_correct = forms.BooleanField(label=_("是否开启修正配置"), required=False)

    def clean(self):
        form_data = super(FlowRealtimeNodeBaseForm, self).clean()
        if not self.errors:
            # output_name 输出中文名为空时，与节点名称保持一致
            if not form_data["output_name"]:
                form_data["output_name"] = form_data["name"]

            form_data = self._clean_window_lateness(form_data)

        return form_data

    class WindowLatenessForm(BaseForm):
        allowed_lateness = forms.BooleanField(required=False, widget=CheckboxInput(default=False), label=_("是否计算延迟数据"))
        lateness_time = forms.IntegerField(required=False, min_value=0, label=_("延迟时间"))
        lateness_count_freq = forms.IntegerField(required=False, min_value=0, label=_("统计频率"))

    def _clean_window_lateness(self, form_data):
        window_lateness = form_data["window_lateness"]
        if not window_lateness:
            return form_data
        _sub_form = self.WindowLatenessForm(window_lateness)
        if not _sub_form.is_valid():
            _err_msg = _sub_form.format_errmsg()
            _whole_err_msg = "解析计算延迟数据配置参数错误: %s" % _err_msg
            logger.error("实时表单检验计算延迟数据配置，参数解析失败，e={e}，param={param}".format(e=_whole_err_msg, param=window_lateness))
            self._errors["window_lateness"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        else:
            form_data["window_lateness"] = _sub_form.cleaned_data

        return form_data

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids


class FlowRealtimeNodeNoneForm(FlowRealtimeNodeBaseForm):
    def clean(self):
        form_data = super(FlowRealtimeNodeNoneForm, self).clean()
        if not self.errors:
            form_data["window_type"] = "none"
        return form_data


class FlowRealtimeNodeScrollForm(FlowRealtimeNodeBaseForm):
    count_freq = forms.IntegerField(required=True, label=_("统计频率"))
    waiting_time = forms.IntegerField(required=True, label=_("等待时间"))

    def clean(self):
        form_data = super(FlowRealtimeNodeScrollForm, self).clean()
        if not self.errors:
            form_data["window_type"] = "scroll"
        return form_data


class FlowRealtimeNodeSlideForm(FlowRealtimeNodeBaseForm):
    count_freq = forms.IntegerField(required=True, label=_("统计频率"))
    waiting_time = forms.IntegerField(required=True, label=_("等待时间"))
    window_time = forms.IntegerField(required=True, label=_("窗口长度"))

    def clean(self):
        form_data = super(FlowRealtimeNodeSlideForm, self).clean()
        if not self.errors:
            form_data["window_type"] = "slide"
            count_freq = form_data["count_freq"]
            window_time = form_data["window_time"]
            form_data["counter"] = int(window_time * 60 / count_freq)
        return form_data


class FlowRealtimeNodeAccumulateForm(FlowRealtimeNodeBaseForm):
    count_freq = forms.IntegerField(required=True, label=_("统计频率"))
    waiting_time = forms.IntegerField(required=True, label=_("等待时间"))
    window_time = forms.IntegerField(required=True, label=_("窗口长度"))

    def clean(self):
        form_data = super(FlowRealtimeNodeAccumulateForm, self).clean()
        if not self.errors:
            form_data["window_type"] = "accumulate"
            count_freq = form_data["count_freq"]
            window_time = form_data["window_time"]
            form_data["counter"] = int(window_time * 60 / count_freq)
        return form_data


class FlowRealtimeNodeSessionForm(FlowRealtimeNodeBaseForm):
    session_gap = forms.IntegerField(required=True, label=_("间隔时间"))
    waiting_time = forms.IntegerField(required=True, label=_("等待时间"))
    expired_time = forms.IntegerField(required=True, label=_("过期时间"))

    def clean(self):
        form_data = super(FlowRealtimeNodeSessionForm, self).clean()
        if not self.errors:
            form_data["window_type"] = "session"
        return form_data


class FlowModelNodeForm(BaseForm):
    name = forms.CharField(max_length=50, label=_("节点名称"))
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    output_name = forms.CharField(max_length=50, label=_("输出中文名"))
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    model_params = JSONField(label=_("算法模型参数"))
    model_version_id = forms.IntegerField(label=_("算法模型版本"))
    model_id = forms.CharField(label=_("算法模型ID"))
    serving_scheduler_params = DictField(label=_("SERVING 调度参数"), required=True)
    training_scheduler_params = DictField(label=_("TRAINING 调度参数"), required=False)
    auto_upgrade = forms.BooleanField(required=False, initial=True)
    # 在界面不一定可见
    training_when_serving = forms.BooleanField(label=_("SERVING & TRAINING 是否一致"), required=False)
    training_from_instance = forms.IntegerField(label="是否复用其他示例训练任务配置", required=False)

    from_result_table_ids = ListField(label=_("父rt列表"), required=False)

    def clean_serving_scheduler_params(self):
        serving_scheduler_params = self.cleaned_data["serving_scheduler_params"]
        if "serving_mode" not in serving_scheduler_params or serving_scheduler_params["serving_mode"] not in [
            "realtime",
            "offline",
        ]:
            raise forms.ValidationError(_("SERVING 调度参数错误，应包含 serving_mode(realtime、offline)"))
        return serving_scheduler_params

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids


class FlowProcessModelNodeForm(BaseForm):
    name = forms.CharField(max_length=50, label=_("节点名称"))
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    output_name = forms.CharField(max_length=50, label=_("输出中文名"))
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    model_release_id = forms.IntegerField(label=_("算法模型版本"))
    model_id = forms.CharField(label=_("算法模型ID"))
    input_config = DictField(label=_("模型输入"))
    output_config = DictField(label=_("模型输出"))
    schedule_config = DictField(label=_("运行设置"))
    serving_mode = forms.ChoiceField(choices=(("realtime", _("实时")), ("offline", _("离线"))), label=_("算法模型运行模式"))
    upgrade_config = DictField(label="upgrade_config", required=False)
    sample_feedback_config = DictField(label="sample_feedback_config", required=False)
    model_extra_config = DictField(label="model_extra_config", required=False)
    from_result_table_ids = ListField(label=_("父rt列表"), required=False)
    scene_name = forms.CharField(label=_("scene_name"), required=False)

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids

    def clean(self):
        form_data = super(FlowProcessModelNodeForm, self).clean()
        if "upgrade_config" not in form_data:
            form_data["upgrade_config"] = {}
        if "sample_feedback_config" not in form_data:
            form_data["sample_feedback_config"] = {}
        return form_data


class SplitKafkaForm(BaseForm):
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    name = forms.CharField(max_length=50, label=_("节点名称"))
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
    # [
    #   {
    #      "bk_biz_id": 1,
    #      "logic_exp": "逻辑表达式的值"
    #   }
    # ]
    config = ListField(label="切分逻辑")
    from_result_table_ids = ListField(label=_("父rt列表"), required=False)
    description = forms.CharField(max_length=512, label=_("节点描述"))

    class SplitForm(BaseForm):
        bk_biz_id = forms.IntegerField(label=_("业务ID"))
        logic_exp = forms.CharField(label=_("逻辑表达式"))

    def clean_config(self):
        splits = self.cleaned_data["config"]
        _cleaned_splits = []
        for _split in splits:
            _f = self.SplitForm(_split)

            if not _f.is_valid():
                raise forms.ValidationError(_f.format_errmsg())

            _cleaned_splits.append(_f.cleaned_data)

        return _cleaned_splits

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids


class MergeKafkaForm(BaseForm):
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    name = forms.CharField(max_length=50, label=_("节点名称"))
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
    from_result_table_ids = ListField(label=_("输入结果表"), required=False)
    description = forms.CharField(max_length=512, label=_("节点描述"))
    config = ListField(label="固化节点配置", required=False)

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids

    def clean_config(self):
        return []


def flow_batch_node_form(cls, params, is_create):
    BATCH_NODE_FORM_MAP = {
        "fixed": FlowBatchNodeFixForm,
        "accumulate_by_hour": FlowBatchNodeAccumulateByHourForm,
    }

    if "window_type" in params and params["window_type"] in BATCH_NODE_FORM_MAP:
        return BATCH_NODE_FORM_MAP[params["window_type"]](params, is_create=is_create)
    else:
        return FlowBatchNodeFixForm(params, is_create=is_create)


class FlowOfflineNodeBaseForm(BaseForm):
    bk_biz_id = forms.IntegerField()
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    name = forms.CharField(max_length=50, label=_("计算描述"))
    sql = forms.CharField(label="SQL")

    expire_day = forms.IntegerField(required=False, label=_("过期时间"))
    output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
    advanced = DictField(required=False, label=_("高级配置"))
    from_result_table_ids = ListField(label=_("父rt列表"), required=False)
    # 增加数据修正 correct_config_id,is_open_correct
    correct_config_id = forms.IntegerField(label=_("修正配置ID"), required=False)
    is_open_correct = forms.BooleanField(label=_("是否开启修正配置"), required=False)

    def clean(self):
        form_data = super(FlowOfflineNodeBaseForm, self).clean()
        if not self.errors:
            # output_name 输出中文名为空时，与节点名称保持一致
            if not form_data["output_name"]:
                form_data["output_name"] = form_data["name"]

            # 校验高级配置
            form_data = self._clean_advanced_config(form_data)

        return form_data

    class BatchSelfDependencyConfigForm(BaseForm):
        """
        自依赖校验表单
        """

        dependency_rule = forms.ChoiceField(
            choices=(("self_finished", _("任务执行成功")), ("self_no_failed", _("任务无失败"))),
            label=_("调度依赖策略"),
        )
        fields = ListField(label=_("输出字段信息列表"))

        class FieldForm(BaseForm):
            """
            校验字段格式配置
            """

            field_name = forms.CharField(label="字段名称")
            field_type = forms.CharField(label="字段类型")
            description = forms.CharField(label="字段描述")

        def clean_fields(self):
            fields = self.cleaned_data["fields"]
            for _field in fields:
                _sub_form = self.FieldForm(_field)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
                    _whole_err_msg = "字段信息列表格式错误: {err_msg}".format(err_msg=_err_msg)
                    logger.error(_(_whole_err_msg))
                    self._errors["fields"] = self.error_class([forms.ValidationError(_whole_err_msg)])
                    break

            return fields

    class BatchAdvancedForm(BaseForm):
        """
        校验高级配置
        """

        start_time = forms.CharField(required=False, max_length=16, label=_("启动时间，格式如'2018-11-19 11:00'"))
        force_execute = forms.BooleanField(required=False, label=_("启动后立刻计算"))
        self_dependency = forms.BooleanField(required=False, label=_("是否自依赖"))
        recovery_enable = forms.BooleanField(required=False, label=_("是否启用失败重试"))
        recovery_times = forms.TypedChoiceField(
            required=False,
            empty_value=1,
            choices=((1, ""), (2, ""), (3, "")),
            label=_("重试次数"),
            coerce=int,
        )
        recovery_interval = forms.ChoiceField(
            required=False,
            choices=(("5m", ""), ("10m", ""), ("15m", ""), ("30m", ""), ("60m", "")),
            label=_("重试间隔"),
        )
        self_dependency_config = DictField(required=False, label=_("自依赖配置"))

        def clean_recovery_interval(self):
            recovery_interval = self.cleaned_data["recovery_interval"]
            # 设置默认值
            if not recovery_interval:
                recovery_interval = "5m"
            return recovery_interval

        def _clean_self_dependency_config(self, form_data):
            self_dependency_config = form_data.get("self_dependency_config", {})
            _sub_form = FlowOfflineNodeBaseForm.BatchSelfDependencyConfigForm(self_dependency_config)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "自依赖配置参数错误: %s" % _err_msg
                self._errors["self_dependency_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
            return form_data

        def clean(self):
            form_data = super(FlowOfflineNodeBaseForm.BatchAdvancedForm, self).clean()
            if not self.errors:
                if form_data.get("self_dependency"):
                    form_data = self._clean_self_dependency_config(form_data)
            return form_data

    def _clean_advanced_config(self, form_data):
        advanced = form_data["advanced"]
        if advanced is None:
            return form_data
        _sub_form = self.BatchAdvancedForm(advanced)
        if not _sub_form.is_valid():
            _err_msg = _sub_form.format_errmsg()
            _whole_err_msg = "解析高级配置参数错误: %s" % _err_msg
            logger.error(_("离线表单检验高级配置，参数解析失败，e=%(e)s，param=%(param)s") % {"e": _whole_err_msg, "param": advanced})
            self._errors["advanced"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        else:
            form_data["advanced"] = _sub_form.cleaned_data

        return form_data

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids


class FlowBatchNodeFixForm(FlowOfflineNodeBaseForm):
    count_freq = forms.IntegerField(min_value=1, label=_("统计频率"))
    # TODO: TDW 暂不支持周月任务
    schedule_period = forms.ChoiceField(
        choices=(("day", ""), ("hour", ""), ("week", ""), ("month", "")),
        label=_("调度单位"),
    )

    dependency_config_type = forms.ChoiceField(
        required=False,
        choices=(("unified", _("统一配置")), ("custom", _("自定义配置"))),
        label=_("父表差异化配置类型"),
    )
    unified_config = DictField(required=False, label=_("父表统一配置"))
    custom_config = DictField(required=False, label=_("父表自定义配置"))

    fixed_delay = forms.IntegerField(min_value=0, label=_("延迟时间"))
    delay_period = forms.ChoiceField(required=False, choices=(("day", ""), ("hour", "")), label=_("统计延迟单位"))
    # 兼容只有统一配置的旧接口
    fallback_window = forms.IntegerField(min_value=1, required=False, label=_("窗口长度"))

    class BatchConfigForm(BaseForm):
        """
        该表单类仅提供给 FlowOfflineNodeFixForm 类使用，用于校验父表配置
        """

        window_size = forms.IntegerField(min_value=1, label=_("窗口长度"))
        # TODO: TDW 暂不支持周月任务
        window_size_period = forms.ChoiceField(
            choices=(("day", ""), ("hour", ""), ("week", ""), ("month", "")),
            label=_("调度单位"),
        )
        dependency_rule = forms.ChoiceField(
            choices=(
                ("all_finished", _("全部成功")),
                ("at_least_one_finished", _("一次成功")),
                ("no_failed", _("无失败")),
            ),
            label=_("调度依赖策略"),
        )

    class BatchUnifiedConfigForm(BatchConfigForm):
        """
        校验父表统一配置
        """

        pass

    class BatchCustomConfigForm(BatchConfigForm):
        """
        校验父表自定义配置
        """

        window_delay = forms.IntegerField(min_value=0, label=_("窗口延迟"))

        def clean_window_delay(self):
            window_delay = self.cleaned_data["window_delay"]
            # TODO: 暂时不作窗口长度限制
            # if window_delay >= 24:
            #     raise forms.ValidationError(_(u"必须小于 24 小时"))
            return window_delay

    def _self_clean_count_freq(self):
        count_freq = self.cleaned_data["count_freq"]
        valid_arr = [1, 2, 3, 4, 6, 12]
        if count_freq not in valid_arr:
            raise forms.ValidationError(_("统计频率目前仅支持 %s") % valid_arr)
        return count_freq

    def _self_clean_fixed_delay(self):
        count_freq = self.cleaned_data["count_freq"]
        schedule_period = self.cleaned_data["schedule_period"]
        fixed_delay = self.cleaned_data["fixed_delay"]
        fixed_delay_period = self.cleaned_data.get("delay_period", "hour")
        hour_delay = fixed_delay * 24 if fixed_delay_period == "day" else fixed_delay
        # 基于小时和天统计频率的维持现状，否则统计频率应该小于一个统计频率周期
        if schedule_period in ["hour", "day"]:
            if fixed_delay >= 24:
                raise forms.ValidationError(_("必须小于 24 小时"))
        else:
            # TODO: fixed_delay 目前是按小时、天计，与调度频率进行比较需要月按某个具体天数转成小时，目前保守按 28 计
            # week, month
            if (schedule_period == "week" and hour_delay > 24 * 7 * count_freq) or (
                schedule_period == "month" and hour_delay > 24 * 30 * 28 * count_freq
            ):
                raise forms.ValidationError(_("必须小于一个统计频率周期"))
        return fixed_delay

    def clean_delay_period(self):
        delay_period = self.cleaned_data["delay_period"]
        if not delay_period:
            return "hour"
        else:
            return delay_period

    def clean_dependency_config_type(self):
        dependency_config_type = self.cleaned_data["dependency_config_type"]
        if not dependency_config_type:
            return "unified"
        else:
            return dependency_config_type

    def clean(self):
        form_data = super(FlowBatchNodeFixForm, self).clean()
        if not self.errors:
            # 按小时统计，需要校验统计频率的数值
            if form_data["schedule_period"] == "hour":
                self._self_clean_count_freq()
            self._self_clean_fixed_delay()
            # 统一校验父表配置
            form_data = self._clean_dependency_config(form_data)

        form_data["window_type"] = "fixed"
        form_data["accumulate"] = False

        return form_data

    def _clean_dependency_config(self, form_data):
        # 父表差异化配置处理
        dependency_config_type = form_data["dependency_config_type"]
        if dependency_config_type == "unified":
            unified_config = form_data["unified_config"]
            # 旧版本没这个参数，新版本回填兼容 start
            if not unified_config:
                form_data["unified_config"] = {
                    "window_size": form_data["fallback_window"],
                    "window_size_period": form_data["schedule_period"],
                    "dependency_rule": "all_finished",
                }
                return form_data
            # 兼容代码 end
            _sub_form = self.BatchUnifiedConfigForm(unified_config)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "解析父表统一参数错误: %s" % _err_msg
                logger.error(
                    _("离线表单检验父表差异化配置，参数解析失败，e=%(e)s，param=%(param)s") % {"e": _whole_err_msg, "param": unified_config}
                )
                self._errors["unified_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
            else:
                form_data["unified_config"] = _sub_form.cleaned_data
                if form_data["unified_config"]["window_size"] < form_data["count_freq"]:
                    raise forms.ValidationError(_("统一配置的窗口长度必须大于或等于统计频率"))
        else:
            custom_config = form_data["custom_config"]
            for _result_table_id, _config in list(custom_config.items()):
                _sub_form = self.BatchCustomConfigForm(_config)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
                    _whole_err_msg = "解析父表自定义参数错误: %s" % _err_msg
                    self._errors["custom_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
                else:
                    form_data["custom_config"][_result_table_id] = _sub_form.cleaned_data
                    if form_data["custom_config"][_result_table_id]["window_size"] < form_data["count_freq"]:
                        raise forms.ValidationError(_("自定义配置的窗口长度必须大于或等于统计频率"))
        return form_data


class FlowBatchNodeAccumulateByHourForm(FlowOfflineNodeBaseForm):
    data_start = forms.IntegerField(
        label=_("数据起点"),
        min_value=0,
        max_value=23,
        error_messages={"min_value": _("确保该值大于或等于0"), "max_value": _("确保该值小于或等于23")},
    )
    data_end = forms.IntegerField(
        label=_("数据起点"),
        min_value=0,
        max_value=23,
        error_messages={"min_value": _("确保该值大于或等于0"), "max_value": _("确保该值小于或等于23")},
    )
    delay = forms.IntegerField(label=_("延迟时间"))
    unified_config = DictField(required=False, label=_("父表统一配置"))

    class BatchUnifiedConfigForm(BaseForm):
        dependency_rule = forms.ChoiceField(
            choices=(
                ("all_finished", _("全部成功")),
                ("at_least_one_finished", _("一次成功")),
                ("no_failed", _("无失败")),
            ),
            label=_("调度依赖策略"),
        )

    def clean_unified_config(self):
        unified_config = self.cleaned_data["unified_config"]
        if not unified_config:
            unified_config = {}
        else:
            _sub_form = self.BatchUnifiedConfigForm(unified_config)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "调度依赖策略参数配置错误: %s" % _err_msg
                logger.error(_("调度依赖策略参数配置错误，e=%(e)s，param=%(param)s") % {"e": _whole_err_msg, "param": unified_config})
                self._errors["unified_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
            else:
                unified_config = _sub_form.cleaned_data
        return unified_config

    def clean(self):
        form_data = super(FlowBatchNodeAccumulateByHourForm, self).clean()
        if not self.errors:
            # 若数据起点选择 06:00, data_start 为 6
            # 若数据终点选择 06:59, data_end 为 6
            # 所以等于是允许的
            if form_data["data_end"] < form_data["data_start"]:
                raise forms.ValidationError(_("数据起点必须小于数据终点"))
        form_data["window_type"] = "accumulate_by_hour"
        form_data["accumulate"] = True
        form_data["schedule_period"] = "hour"
        form_data["count_freq"] = 1
        return form_data


def flow_tdw_sql_batch_node_form(cls, params, is_create):
    BATCH_NODE_FORM_MAP = {
        "fixed": FlowTDWSqlBatchNodeFixForm,
        "accumulate_by_hour": FlowTDWSqlBatchNodeAccumulateByHourForm,
    }

    if "window_type" in params and params["window_type"] in BATCH_NODE_FORM_MAP:
        return BATCH_NODE_FORM_MAP[params["window_type"]](params, is_create=is_create)
    else:
        return FlowTDWSqlBatchNodeFixForm(params, is_create=is_create)


# TDW 离线(提交jar包方式)计算节点
def flow_tdw_jar_batch_node_form(cls, params, is_create):
    TDW_JAR_BATCH_NODE_FROM_MAP = {
        "fixed": FlowTDWJarBatchNodeFixForm,
        "accumulate_by_hour": FlowTDWJarBatchNodeAccumulateByHourForm,
    }

    if "window_type" in params and params["window_type"] in TDW_JAR_BATCH_NODE_FROM_MAP:
        return TDW_JAR_BATCH_NODE_FROM_MAP[params["window_type"]](params, is_create=is_create)
    else:
        return FlowTDWJarBatchNodeFixForm(params, is_create=is_create)


class TDWBatchTaskForm(BaseForm):
    """
    TDW 任务配置
    """

    timeout = forms.IntegerField(label=_("任务超时"))
    program_specific_params = forms.CharField(required=False, label="程序参数")
    class_name = forms.CharField(label="类名")
    driver_memory = forms.CharField(label="driver 内存")
    executor_memory = forms.CharField(label="executor 内存")
    executor_cores = forms.CharField(label="每个 executor 的核数")
    num_executors = forms.CharField(label="启动的 executor 数量")
    options = forms.CharField(required=False, label="扩展参数")
    jar_info = ListField(label=_("待提交jar包信息"))

    class JarForm(BaseForm):
        jar_id = forms.IntegerField(label=_("jar id"))
        jar_name = forms.CharField(max_length=64, label=_("文件名称"))
        jar_created_by = forms.CharField(max_length=64, label=_("文件上传者"))
        jar_created_at = forms.DateTimeField(label=_("文件上传时间"))

    def clean_jar_info(self):
        jar_file_infos = self.cleaned_data["jar_info"]
        for _jar_file_info in jar_file_infos:
            _sub_form = self.JarForm(_jar_file_info)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "文件上传配置错误: {err_msg}".format(err_msg=_err_msg)
                logger.error(_(_whole_err_msg))
                self._errors["jar_info"] = self.error_class([forms.ValidationError(_whole_err_msg)])

        return jar_file_infos


class TDWJarOutputsForm(BaseForm):
    bk_biz_id = forms.IntegerField()
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
    cluster_id = forms.CharField(label=_("集群ID"), max_length=255)
    db_name = forms.CharField(label=_("数据库名称"), max_length=255)
    expires = forms.IntegerField(label=_("过期时间"))
    expires_unit = forms.CharField(max_length=1, label=_("过期时间单位"))
    fields = ListField(label=_("输出字段信息列表"))

    class FieldForm(BaseForm):
        """
        校验字段格式配置
        """

        field_name = forms.CharField(required=False, label="字段名称")
        field_type = forms.CharField(required=False, label="字段类型")
        description = forms.CharField(required=False, label="字段描述")

    def clean_fields(self):
        fields = self.cleaned_data["fields"]
        for _field in fields:
            _sub_form = self.FieldForm(_field)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "任务配置输出字段信息列表格式错误: {err_msg}".format(err_msg=_err_msg)
                logger.error(_(_whole_err_msg))
                self._errors["fields"] = self.error_class([forms.ValidationError(_whole_err_msg)])

        return fields


class FlowTDWJarBatchNodeBaseForm(StandardBaseForm):
    processing_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("数据处理名称"),
    )
    tdw_task_config = DictField(label=_("作业配置"))
    outputs = ListField(label=_("输出结果表信息列表"))

    def clean_outputs(self):
        outputs = self.cleaned_data["outputs"]
        for output in outputs:
            _sub_form = TDWJarOutputsForm(output)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = _("输出结果表信息格式错误: %(err_msg)s") % {"err_msg": _err_msg}
                logger.error(_whole_err_msg)
                self._errors["outputs"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return outputs

    def clean_tdw_task_config(self):
        tdw_task_config = self.cleaned_data["tdw_task_config"]
        _sub_form = TDWBatchTaskForm(tdw_task_config)
        if not _sub_form.is_valid():
            _err_msg = _sub_form.format_errmsg()
            raise forms.ValidationError(_err_msg)

        return tdw_task_config


class FlowTDWSqlBatchNodeBaseForm(StandardBaseForm):
    cluster_id = forms.CharField(label=_("集群ID"), max_length=255)
    db_name = forms.CharField(label=_("数据库名称"), max_length=255)
    expires = forms.IntegerField(label=_("过期时间"))
    expires_unit = forms.CharField(max_length=1, label=_("过期时间单位"))


class FlowTDWSqlBatchNodeFixForm(FlowBatchNodeFixForm, FlowTDWSqlBatchNodeBaseForm):
    pass


class FlowTDWSqlBatchNodeAccumulateByHourForm(FlowBatchNodeAccumulateByHourForm, FlowTDWSqlBatchNodeBaseForm):
    pass


class FlowTDWJarBatchNodeFixForm(FlowBatchNodeFixForm, FlowTDWJarBatchNodeBaseForm):
    sql = forms.CharField(required=False, label="SQL")
    table_name = forms.RegexField(
        required=False,
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )


class FlowTDWJarBatchNodeAccumulateByHourForm(FlowBatchNodeAccumulateByHourForm, FlowTDWJarBatchNodeBaseForm):
    sql = forms.CharField(required=False, label="SQL")


class SDKOutputsForm(BaseForm):
    bk_biz_id = forms.IntegerField()
    table_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("计算标识"),
    )
    output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
    fields = ListField(label=_("字段信息列表"))

    class FieldForm(BaseForm):
        """
        校验字段格式配置
        """

        field_name = forms.CharField(label="字段名称")
        field_type = forms.CharField(label="字段类型")
        field_alias = forms.CharField(label="字段中文名")
        event_time = forms.BooleanField(required=False, widget=CheckboxInput(default=False), label=_("是否标记为数据时间"))

    def clean_fields(self):
        fields = self.cleaned_data["fields"]
        event_time_fields = []
        for _field in fields:
            _sub_form = self.FieldForm(_field)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "字段信息列表格式错误: {err_msg}".format(err_msg=_err_msg)
                logger.error(_(_whole_err_msg))
                self._errors["fields"] = self.error_class([forms.ValidationError(_whole_err_msg)])
                break
            if _sub_form.data.get("event_time", False):
                event_time_fields.append(_sub_form.data.get("field_name"))
        if len(event_time_fields) > 1:
            self._errors["fields"] = self.error_class(
                [forms.ValidationError(_("一个结果表不可配置多个数据时间字段(%s)") % ", ".join(event_time_fields))]
            )

        return fields


class StreamSDKNodeForm(StandardBaseForm):
    processing_name = forms.RegexField(
        regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("数据处理名称"),
    )
    name = forms.CharField(max_length=50, label=_("计算描述"))
    programming_language = forms.CharField(max_length=50, label=_("编程语言"))
    user_main_class = forms.CharField(required=False, label="程序入口")
    code = forms.CharField(required=False, label=_("用户代码"))
    package = ListField(required=False, label=_("待提交包信息"))
    user_args = forms.CharField(required=False, label="程序参数")
    outputs = ListField(label=_("输出结果表信息列表"))
    advanced = DictField(label=_("高级配置"))
    bk_biz_id = forms.IntegerField(required=False, label=_("业务id"))

    class PackageForm(BaseForm):
        id = forms.IntegerField()
        name = forms.CharField(max_length=64, label=_("文件名称"))
        created_by = forms.CharField(max_length=64, label=_("文件上传者"))
        created_at = forms.DateTimeField(label=_("文件上传时间"))

    def clean_package(self):
        file_infos = self.cleaned_data["package"]
        for _file_info in file_infos:
            _sub_form = self.PackageForm(_file_info)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "文件上传配置错误: {err_msg}".format(err_msg=_err_msg)
                logger.error(_(_whole_err_msg))
                self._errors["package"] = self.error_class([forms.ValidationError(_whole_err_msg)])

        return file_infos

    def clean_outputs(self):
        outputs = self.cleaned_data["outputs"]
        for output in outputs:
            _sub_form = SDKOutputsForm(output)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = _("结果表信息格式错误: %(err_msg)s") % {"err_msg": _err_msg}
                logger.error(_whole_err_msg)
                self._errors["outputs"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return outputs

    class AdvancedForm(BaseForm):
        """
        校验高级配置
        """

        use_savepoint = forms.BooleanField(
            required=False,
            widget=CheckboxInput(default=True),
            label=_("是否启动 savepoint"),
        )

    def clean_advanced(self):
        advanced = self.cleaned_data["advanced"]
        _sub_form = self.AdvancedForm(advanced)
        if not _sub_form.is_valid():
            _err_msg = _sub_form.format_errmsg()
            _whole_err_msg = "高级配置格式错误: {err_msg}".format(err_msg=_err_msg)
            logger.error(_(_whole_err_msg))
            self._errors["advanced"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return advanced


# 存储节点
class StandardStorageBaseForm(StandardBaseForm):
    """
    当前最标准的存储节点表单基类
    """

    result_table_id = forms.CharField(label=_("存储节点输入result_table_id"))
    name = forms.CharField(max_length=255, label=_("节点名称"))
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    cluster = forms.CharField(label=_("存储集群"))

    def clean(self):
        """
        对于存储节点，限制 from_nodes 中元素必须唯一且与 result_table_id 一致
        @return:
        """
        form_data = super(StandardStorageBaseForm, self).clean()
        from_nodes = form_data.get("from_nodes", [])
        result_table_id = form_data["result_table_id"]
        if not from_nodes or len(from_nodes) != 1:
            raise forms.ValidationError(_("存储节点 from_nodes 中应有且仅有一个元素"))
        from_result_table_ids = from_nodes[0]["from_result_table_ids"]
        if len(from_result_table_ids) != 1 or from_result_table_ids[0] != result_table_id:
            raise forms.ValidationError(_("存储节点 from_nodes 中上游结果表应与 result_table_id 保持一致且唯一"))
        return form_data


class StorageBaseForm(BaseForm):
    result_table_id = forms.CharField(label=_("存储节点输入result_table_id"))
    name = forms.CharField(max_length=255, label=_("节点名称"))
    bk_biz_id = forms.IntegerField(label=_("业务ID"))
    cluster = forms.CharField(label=_("存储集群"))
    from_result_table_ids = ListField(label=_("父rt列表"), required=False)

    def clean_from_result_table_ids(self):
        from_result_table_ids = self.cleaned_data["from_result_table_ids"]
        if [x for x in from_result_table_ids if not x]:
            raise forms.ValidationError(_("传入父结果表列表包含空rt信息"))
        return from_result_table_ids


class FlowMysqlNodeForm(StorageBaseForm):
    indexed_fields = ListField(required=False, label=_("索引字段"))
    expires = forms.IntegerField(label=_("过期时间"))
    has_unique_key = forms.BooleanField(required=False, label=_("是否启用唯一键"))
    storage_keys = ListField(required=False, label=_("存储keys"))

    def clean_expires(self):
        expires = self.cleaned_data["expires"]
        return 30 if expires is None else expires


class StandardStorageForm(StandardStorageBaseForm):
    """
    当前最标准的存储节点表单
    """

    expires = forms.IntegerField(label=_("过期时间"))
    expires_unit = forms.CharField(required=False, max_length=1, label=_("过期时间单位"))


class PostgresqlStorageForm(StorageBaseForm):
    indexed_fields = ListField(required=False, label=_("索引字段"))
    expires = forms.IntegerField(label=_("过期时间"))
    expires_unit = forms.CharField(required=False, max_length=1, label=_("过期时间单位"))


class TDBankStorageForm(StorageBaseForm):
    cluster = forms.CharField(label=_("存储集群"), required=False)
    bid = forms.CharField(max_length=255, label=_("bid"))

    def clean(self):
        form_data = super(TDBankStorageForm, self).clean()
        # bid 即集群名称
        form_data["cluster"] = form_data["bid"]
        return form_data


class TpgStorageForm(StorageBaseForm):
    indexed_fields = ListField(required=False, label=_("索引字段"))


class ElasticStorageForm(StorageBaseForm):
    analyzed_fields = ListField(required=False, label=_("分词字段"))
    date_fields = ListField(required=False, label=_("时间字段"))
    doc_values_fields = ListField(required=False, label=_("聚合字段"))
    json_fields = ListField(required=False, label=_("嵌套类型字段json/object"))
    expires = forms.IntegerField(label=_("过期时间"))
    has_replica = forms.BooleanField(required=False, widget=CheckboxInput(default=False), label=_("是否启用副本存储"))
    has_unique_key = forms.BooleanField(required=False, label=_("是否启用唯一键"))
    storage_keys = ListField(required=False, label=_("存储keys"))


class HdfsStorageForm(StorageBaseForm):
    expires = forms.IntegerField(label=_("过期时间"))


class TDWStorageForm(StorageBaseForm):
    result_table_id = forms.RegexField(
        regex=r"^\d+_tdw_[a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
        error_messages={"invalid": _("由英文字母、下划线或数字组成，且以数字开头，不支持连续下划线或以下划线结尾")},
        max_length=255,
        label=_("存储节点输入result_table_id"),
    )
    expires = forms.IntegerField(label=_("过期时间"))
    expires_unit = forms.CharField(max_length=1, label=_("过期时间单位"))
    bid = forms.CharField(max_length=255, label=_("bid"))
    output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
    db_name = forms.CharField(label=_("数据库名称"), max_length=255)


class TsdbStorageForm(StorageBaseForm):
    expires = forms.IntegerField(label=_("过期时间"))
    dim_fields = ListField(required=False, label=_("维度字段"))


class CrateStorageForm(StorageBaseForm):
    expires = forms.IntegerField(label=_("过期时间"))


class HermesStorageForm(StorageBaseForm):
    expires = forms.IntegerField(label=_("过期时间"))


class TspiderStorageForm(StorageBaseForm):
    indexed_fields = ListField(required=False, label=_("索引字段"))
    expires = forms.IntegerField(label=_("过期时间"))
    has_unique_key = forms.BooleanField(required=False, label=_("是否启用唯一键"))
    storage_keys = ListField(required=False, label=_("存储keys"))


class TcaplusStorageForm(StorageBaseForm):
    primary_keys = ListField(required=False, label=_("主键keys"))
    set_id = forms.IntegerField(required=False, label=_("集群ID"))
    zone_id = forms.IntegerField(required=False, label=_("游戏区"))


class ClickHouseStorageForm(StorageBaseForm):
    order_by = ListField(required=False, label=_("主键排序"))
    expires = forms.IntegerField(label=_("过期时间"))
    has_unique_key = forms.BooleanField(required=False, label=_("是否启用唯一键"))
    primary_keys = ListField(required=False, label=_("主键keys"))


class QueueStorageForm(StorageBaseForm):
    expires = forms.IntegerField(label=_("过期时间"))


class DruidStorageForm(StorageBaseForm):
    expires = forms.IntegerField(label=_("过期时间"))


class TredisStorageBaseForm(StorageBaseForm):
    storage_type = forms.CharField(label="存储类型")
    # TODO: 之前没有expires字段，publish类型不需要设置过期时间?
    expires = forms.IntegerField(label="过期时间")


class TredisStorageKVForm(TredisStorageBaseForm):
    keys = ListField(label="keys")
    values = ListField(label="values")
    separator = forms.CharField(label=_("分割符"))
    setnx = forms.BooleanField(
        required=False,
        widget=CheckboxInput(default=False),
        label=_("是否覆盖已有key的值，默认值false，表示覆盖"),
    )
    key_prefix = forms.CharField(label=_("key 前缀"), required=False)


class TredisStorageJoinForm(TredisStorageBaseForm):
    keys = ListField(label=_("关联数据主键的字段列表"))
    separator = forms.CharField(label=_("关联数据前缀的分隔符"))
    key_separator = forms.CharField(label=_("key的分隔符"))


class TredisStorageListForm(TredisStorageBaseForm):
    pass


def TredisStorageForm(cls, params, is_create):
    TREDIS_STORAGE_FORM_MAP = {
        "kv": TredisStorageKVForm,
        "list": TredisStorageListForm,
        "join": TredisStorageJoinForm,
        "publish": TredisStorageBaseForm,
    }
    # 校验基类表单
    NodeUtils.validate_config(params, TredisStorageBaseForm, is_create)
    return TREDIS_STORAGE_FORM_MAP[params["storage_type"]](params, is_create=is_create)


class IgniteStorageForm(StandardStorageForm):
    storage_type = forms.ChoiceField(choices=(("join", _("关联")),), label=_("存储类型"))
    indexed_fields = ListField(required=False, label=_("索引字段"))
    storage_keys = ListField(required=False, label=_("key列表"))
    max_records = forms.IntegerField(label=_("最大数据量"))


def model_app_node_form(cls, params, is_create):
    MODEL_APP_NODE_FORM_MAP = {
        "fixed": FlowModelAppNodeFixForm,
        "accumulate_by_hour": FlowModelAppNodeAccumulateByHourForm,
    }

    if "window_type" in params and params["window_type"] in MODEL_APP_NODE_FORM_MAP:
        return MODEL_APP_NODE_FORM_MAP[params["window_type"]](params, is_create=is_create)
    else:
        return FlowModelAppNodeFixForm(params, is_create=is_create)


# 模型应用
class FlowModelAppNodeBaseForm(BaseForm):
    # def __init__(self, *args, **kwargs):
    #     super(FlowModelAppNodeBaseForm, self).__init__(*args, **kwargs)
    #     self.model_name_list = forms.ChoiceField(required=False, choices=[], label=_(u"模型名称列表"))
    #     # TODO: get model_name list from api
    #     self.model_name_list_choices = []
    #     self.fields['model_name_list'].choices = self.model_name_list_choices

    bk_biz_id = forms.IntegerField()
    name = forms.CharField(max_length=50, label=_("节点名称"))
    processing_name = forms.CharField(max_length=255, label=_("英文名称"), required=False)
    input_model = forms.CharField(max_length=255, label=_("输入模型标识"), required=False)
    model_version = forms.CharField(max_length=255, label=_("输入模型版本号"), required=False)
    enable_auto_update = forms.BooleanField(required=False, label=_("是否自动更新"))
    update_policy = forms.ChoiceField(
        required=False,
        choices=(
            ("update_policy_immediately", _("立即")),
            ("update_policy_delay", _("延迟")),
        ),
        label=_("更新策略"),
    )
    update_time = forms.CharField(required=False, max_length=255, label=_("定时时间"))
    # 页面上模型应用的参数
    model_params = DictField(label="模型参数")
    # model_config 仅透传
    model_config = DictField(label="模型配置")
    expire_day = forms.IntegerField(required=False, label=_("过期时间"))
    advanced = DictField(required=False, label=_("高级配置"))
    from_result_table_ids = ListField(label=_("父rt列表"), required=False)
    from_nodes = ListField(label=_("父节点信息列表"))
    outputs = ListField(label=_("输出信息列表"))

    class FromNodesForm(BaseForm):
        id = forms.IntegerField(label=_("父节点id"), required=False)
        from_result_table_ids = ListField(label=_("父节点对应的rt列表"), required=False)

    class OutputsForm(BaseForm):
        bk_biz_id = forms.IntegerField()
        table_name = forms.RegexField(
            regex=r"^[a-zA-Z][a-zA-Z0-9]*(_[a-zA-Z0-9]+)*$",
            error_messages={"invalid": _("由英文字母、下划线或数字组成，且以字母开头，不支持连续下划线或以下划线结尾")},
            max_length=255,
            label=_("结果数据表"),
        )
        output_name = forms.CharField(max_length=50, label=_("输出中文名"), required=False)
        fields = ListField(label=_("字段信息列表"))

        class OutputFieldForm(BaseForm):
            """
            输出字段格式配置
            """

            field_name = forms.CharField(label="字段名称")
            field_type = forms.CharField(label="字段类型")
            field_alias = forms.CharField(label="字段中文名")
            event_time = forms.BooleanField(
                required=False,
                widget=CheckboxInput(default=False),
                label=_("是否标记为数据时间"),
            )

        def clean_fields(self):
            fields = self.cleaned_data["fields"]
            event_time_fields = []
            for _field in fields:
                _sub_form = self.OutputFieldForm(_field)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
                    _whole_err_msg = "字段信息列表格式错误: {err_msg}".format(err_msg=_err_msg)
                    logger.error(_(_whole_err_msg))
                    self._errors["fields"] = self.error_class([forms.ValidationError(_whole_err_msg)])
                    break
                if _sub_form.data.get("event_time", False):
                    event_time_fields.append(_sub_form.data.get("field_name"))
            if len(event_time_fields) > 1:
                self._errors["fields"] = self.error_class(
                    [forms.ValidationError(_("一个结果表不可配置多个数据时间字段(%s)") % ", ".join(event_time_fields))]
                )
            return fields

    class ModelAppSelfDependencyConfigForm(BaseForm):
        """
        自依赖校验表单
        """

        dependency_rule = forms.ChoiceField(
            choices=(("self_finished", _("任务执行成功")), ("self_no_failed", _("任务无失败"))),
            label=_("调度依赖策略"),
        )
        fields = ListField(label=_("输出字段信息列表"))

        class FieldForm(BaseForm):
            """
            校验字段格式配置
            """

            field_name = forms.CharField(label="字段名称")
            field_type = forms.CharField(label="字段类型")
            description = forms.CharField(label="字段描述")

        def clean_fields(self):
            fields = self.cleaned_data["fields"]
            for _field in fields:
                _sub_form = self.FieldForm(_field)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
                    _whole_err_msg = "字段信息列表格式错误: {err_msg}".format(err_msg=_err_msg)
                    logger.error(_(_whole_err_msg))
                    self._errors["fields"] = self.error_class([forms.ValidationError(_whole_err_msg)])
                    break

            return fields

    class ModelAppAdvancedForm(BaseForm):
        """
        校验高级配置
        """

        start_time = forms.CharField(required=False, max_length=16, label=_("启动时间，格式如'2018-11-19 11:00'"))
        # force_execute = forms.BooleanField(required=False, label=_(u"启动后立刻计算"))
        self_dependency = forms.BooleanField(required=False, label=_("是否自依赖"))
        recovery_enable = forms.BooleanField(required=False, label=_("是否启用失败重试"))
        recovery_times = forms.TypedChoiceField(
            required=False,
            empty_value=1,
            choices=((1, ""), (2, ""), (3, "")),
            label=_("重试次数"),
            coerce=int,
        )
        recovery_interval = forms.ChoiceField(
            required=False,
            choices=(("5m", ""), ("10m", ""), ("15m", ""), ("30m", ""), ("60m", "")),
            label=_("重试间隔"),
        )
        self_dependency_config = DictField(required=False, label=_("自依赖配置"))

        def clean_recovery_interval(self):
            recovery_interval = self.cleaned_data["recovery_interval"]
            # 设置默认值
            if not recovery_interval:
                recovery_interval = "5m"
            return recovery_interval

        def _clean_self_dependency_config(self, form_data):
            self_dependency_config = form_data.get("self_dependency_config", {})
            _sub_form = FlowModelAppNodeBaseForm.ModelAppSelfDependencyConfigForm(self_dependency_config)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "自依赖配置参数错误: %s" % _err_msg
                self._errors["self_dependency_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
            return form_data

        def clean(self):
            form_data = super(FlowModelAppNodeBaseForm.ModelAppAdvancedForm, self).clean()
            if not self.errors:
                if form_data.get("self_dependency"):
                    form_data = self._clean_self_dependency_config(form_data)
            return form_data

    def _clean_advanced_config(self, form_data):
        advanced = form_data["advanced"]
        if advanced is None:
            return form_data
        _sub_form = self.ModelAppAdvancedForm(advanced)
        if not _sub_form.is_valid():
            _err_msg = _sub_form.format_errmsg()
            _whole_err_msg = "解析高级配置参数错误: %s" % _err_msg
            logger.error(_("模型应用表单检验高级配置，参数解析失败，e=%(e)s，param=%(param)s") % {"e": _whole_err_msg, "param": advanced})
            self._errors["advanced"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        else:
            form_data["advanced"] = _sub_form.cleaned_data
        return form_data

    def clean_from_nodes(self):
        from_nodes = self.cleaned_data["from_nodes"]
        if not from_nodes:
            logger.info("empty from_nodes")
        for one_from_node in from_nodes:
            _sub_form = self.FromNodesForm(one_from_node)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "解析from_nodes配置参数错误: %s" % _err_msg
                logger.error(
                    _("模型应用表单检验from_nodes配置，参数解析失败，e=%(e)s，param=%(param)s")
                    % {"e": _whole_err_msg, "param": from_nodes}
                )
                self._errors["from_nodes"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return from_nodes

    def clean_outputs(self):
        outputs = self.cleaned_data["outputs"]
        if not outputs:
            logger.info("empty outputs")
        for output in outputs:
            _sub_form = self.OutputsForm(output)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "解析outputs配置参数错误: %s" % _err_msg
                logger.error(
                    _("模型应用表单检验outputs配置，参数解析失败，e=%(e)s，param=%(param)s") % {"e": _whole_err_msg, "param": outputs}
                )
                self._errors["outputs"] = self.error_class([forms.ValidationError(_whole_err_msg)])
        return outputs

    # def _clean_from_nodes(self, form_data):
    #     from_nodes = form_data['from_nodes']
    #     if from_nodes is None:
    #         return form_data
    #     clean_from_nodes = []
    #     for one_from_node in from_nodes:
    #         _sub_form = self.FromNodesForm(one_from_node)
    #         if not _sub_form.is_valid():
    #             _err_msg = _sub_form.format_errmsg()
    #             _whole_err_msg = u"解析from_nodes配置参数错误: %s" % _err_msg
    #             logger.error(_(u"模型应用表单检验from_nodes配置，参数解析失败，e=%(e)s，param=%(param)s") % {
    #                 'e': _whole_err_msg,
    #                 'param': from_nodes
    #             })
    #             self._errors["from_nodes"] = self.error_class([forms.ValidationError(_whole_err_msg)])
    #         else:
    #             clean_from_nodes.append(_sub_form.cleaned_data)
    #     form_data['from_nodes'] = clean_from_nodes
    #     return form_data
    #
    # def _clean_outputs(self, form_data):
    #     outputs = form_data['outputs']
    #     if outputs is None:
    #         return form_data
    #     clean_outputs = []
    #     for one_output in outputs:
    #         _sub_form = self.OutputsForm(one_output)
    #         if not _sub_form.is_valid():
    #             _err_msg = _sub_form.format_errmsg()
    #             _whole_err_msg = u"解析outputs配置参数错误: %s" % _err_msg
    #             logger.error(_(u"模型应用表单检验outputs配置，参数解析失败，e=%(e)s，param=%(param)s") % {
    #                 'e': _whole_err_msg,
    #                 'param': outputs
    #             })
    #             self._errors["outputs"] = self.error_class([forms.ValidationError(_whole_err_msg)])
    #         else:
    #             clean_outputs.append(_sub_form.cleaned_data)
    #     form_data['outputs'] = clean_outputs
    #     return form_data

    # def clean_from_result_table_ids(self):
    #     from_result_table_ids = self.cleaned_data['from_result_table_ids']
    #     if filter(lambda x: not x, from_result_table_ids):
    #         raise forms.ValidationError(_(u"传入父结果表列表包含空rt信息"))
    #     return from_result_table_ids

    def clean(self):
        form_data = super(FlowModelAppNodeBaseForm, self).clean()
        if not self.errors:
            # 校验高级配置
            form_data = self._clean_advanced_config(form_data)
            # # 校验from_nodes信息
            # form_data = self._clean_from_nodes(form_data)
            # # 校验outputs信息
            # form_data = self._clean_outputs(form_data)

        return form_data


class FlowModelAppNodeFixForm(FlowModelAppNodeBaseForm):
    count_freq = forms.IntegerField(min_value=1, label=_("统计频率"))
    # TODO: TDW 暂不支持周月任务
    schedule_period = forms.ChoiceField(
        choices=(("day", ""), ("hour", ""), ("week", ""), ("month", "")),
        label=_("调度单位"),
    )

    dependency_config_type = forms.ChoiceField(
        required=False,
        choices=(("unified", _("统一配置")), ("custom", _("自定义配置"))),
        label=_("父表差异化配置类型"),
    )
    unified_config = DictField(required=False, label=_("父表统一配置"))
    custom_config = DictField(required=False, label=_("父表自定义配置"))

    fixed_delay = forms.IntegerField(min_value=0, label=_("延迟时间"))
    delay_period = forms.ChoiceField(required=False, choices=(("day", ""), ("hour", "")), label=_("统计延迟单位"))
    # 兼容只有统一配置的旧接口
    fallback_window = forms.IntegerField(min_value=1, required=False, label=_("窗口长度"))

    class ModelAppConfigForm(BaseForm):
        """
        该表单类仅提供给 FlowOfflineNodeFixForm 类使用，用于校验父表配置
        """

        window_size = forms.IntegerField(min_value=1, label=_("窗口长度"))
        # TODO: TDW 暂不支持周月任务
        window_size_period = forms.ChoiceField(
            choices=(("day", ""), ("hour", ""), ("week", ""), ("month", "")),
            label=_("调度单位"),
        )
        dependency_rule = forms.ChoiceField(
            choices=(
                ("all_finished", _("全部成功")),
                ("at_least_one_finished", _("一次成功")),
                ("no_failed", _("无失败")),
            ),
            label=_("调度依赖策略"),
        )

    class ModelAppUnifiedConfigForm(ModelAppConfigForm):
        """
        校验父表统一配置
        """

        pass

    class ModelAppCustomConfigForm(ModelAppConfigForm):
        """
        校验父表自定义配置
        """

        window_delay = forms.IntegerField(min_value=0, label=_("窗口延迟"))

        def clean_window_delay(self):
            window_delay = self.cleaned_data["window_delay"]
            # TODO: 暂时不作窗口长度限制
            # if window_delay >= 24:
            #     raise forms.ValidationError(_(u"必须小于 24 小时"))
            return window_delay

    def _self_clean_count_freq(self):
        count_freq = self.cleaned_data["count_freq"]
        valid_arr = [1, 2, 3, 4, 6, 12]
        if count_freq not in valid_arr:
            raise forms.ValidationError(_("统计频率目前仅支持 %s") % valid_arr)
        return count_freq

    def _self_clean_fixed_delay(self):
        count_freq = self.cleaned_data["count_freq"]
        schedule_period = self.cleaned_data["schedule_period"]
        fixed_delay = self.cleaned_data["fixed_delay"]
        fixed_delay_period = self.cleaned_data.get("delay_period", "hour")
        hour_delay = fixed_delay * 24 if fixed_delay_period == "day" else fixed_delay
        # 基于小时和天统计频率的维持现状，否则统计频率应该小于一个统计频率周期
        if schedule_period in ["hour", "day"]:
            if fixed_delay >= 24:
                raise forms.ValidationError(_("必须小于 24 小时"))
        else:
            # TODO: fixed_delay 目前是按小时、天计，与调度频率进行比较需要月按某个具体天数转成小时，目前保守按 28 计
            # week, month
            if (schedule_period == "week" and hour_delay > 24 * 7 * count_freq) or (
                schedule_period == "month" and hour_delay > 24 * 30 * 28 * count_freq
            ):
                raise forms.ValidationError(_("必须小于一个统计频率周期"))
        return fixed_delay

    def clean_delay_period(self):
        delay_period = self.cleaned_data["delay_period"]
        if not delay_period:
            return "hour"
        else:
            return delay_period

    def clean_dependency_config_type(self):
        dependency_config_type = self.cleaned_data["dependency_config_type"]
        if not dependency_config_type:
            return "unified"
        else:
            return dependency_config_type

    def clean(self):
        form_data = super(FlowModelAppNodeFixForm, self).clean()
        if not self.errors:
            # 按小时统计，需要校验统计频率的数值
            if form_data["schedule_period"] == "hour":
                self._self_clean_count_freq()
            self._self_clean_fixed_delay()
            # 统一校验父表配置
            form_data = self._clean_dependency_config(form_data)

        form_data["window_type"] = "fixed"
        form_data["accumulate"] = False

        return form_data

    def _clean_dependency_config(self, form_data):
        # 父表差异化配置处理
        dependency_config_type = form_data["dependency_config_type"]
        if dependency_config_type == "unified":
            unified_config = form_data["unified_config"]
            # 旧版本没这个参数，新版本回填兼容 start
            if not unified_config:
                form_data["unified_config"] = {
                    "window_size": form_data["fallback_window"],
                    "window_size_period": form_data["schedule_period"],
                    "dependency_rule": "all_finished",
                }
                return form_data
            # 兼容代码 end
            _sub_form = self.ModelAppUnifiedConfigForm(unified_config)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "解析父表统一参数错误: %s" % _err_msg
                logger.error(
                    _("离线表单检验父表差异化配置，参数解析失败，e=%(e)s，param=%(param)s") % {"e": _whole_err_msg, "param": unified_config}
                )
                self._errors["unified_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
            else:
                form_data["unified_config"] = _sub_form.cleaned_data
                if form_data["unified_config"]["window_size"] < form_data["count_freq"]:
                    raise forms.ValidationError(_("统一配置的窗口长度必须大于或等于统计频率"))
        else:
            custom_config = form_data["custom_config"]
            for _result_table_id, _config in list(custom_config.items()):
                _sub_form = self.ModelAppCustomConfigForm(_config)
                if not _sub_form.is_valid():
                    _err_msg = _sub_form.format_errmsg()
                    _whole_err_msg = "解析父表自定义参数错误: %s" % _err_msg
                    self._errors["custom_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
                else:
                    form_data["custom_config"][_result_table_id] = _sub_form.cleaned_data
                    if form_data["custom_config"][_result_table_id]["window_size"] < form_data["count_freq"]:
                        raise forms.ValidationError(_("自定义配置的窗口长度必须大于或等于统计频率"))
        return form_data


class FlowModelAppNodeAccumulateByHourForm(FlowModelAppNodeBaseForm):
    data_start = forms.IntegerField(
        label=_("数据起点"),
        min_value=0,
        max_value=23,
        error_messages={"min_value": _("确保该值大于或等于0"), "max_value": _("确保该值小于或等于23")},
    )
    data_end = forms.IntegerField(
        label=_("数据起点"),
        min_value=0,
        max_value=23,
        error_messages={"min_value": _("确保该值大于或等于0"), "max_value": _("确保该值小于或等于23")},
    )
    delay = forms.IntegerField(label=_("延迟时间"))
    unified_config = DictField(required=False, label=_("父表统一配置"))

    class ModelAppUnifiedConfigForm(BaseForm):
        dependency_rule = forms.ChoiceField(
            choices=(
                ("all_finished", _("全部成功")),
                ("at_least_one_finished", _("一次成功")),
                ("no_failed", _("无失败")),
            ),
            label=_("调度依赖策略"),
        )

    def clean_unified_config(self):
        unified_config = self.cleaned_data["unified_config"]
        if not unified_config:
            unified_config = {}
        else:
            _sub_form = self.ModelAppUnifiedConfigForm(unified_config)
            if not _sub_form.is_valid():
                _err_msg = _sub_form.format_errmsg()
                _whole_err_msg = "调度依赖策略参数配置错误: %s" % _err_msg
                logger.error(_("调度依赖策略参数配置错误，e=%(e)s，param=%(param)s") % {"e": _whole_err_msg, "param": unified_config})
                self._errors["unified_config"] = self.error_class([forms.ValidationError(_whole_err_msg)])
            else:
                unified_config = _sub_form.cleaned_data
        return unified_config

    def clean(self):
        form_data = super(FlowModelAppNodeAccumulateByHourForm, self).clean()
        if not self.errors:
            # 若数据起点选择 06:00, data_start 为 6
            # 若数据终点选择 06:59, data_end 为 6
            # 所以等于是允许的
            if form_data["data_end"] < form_data["data_start"]:
                raise forms.ValidationError(_("数据起点必须小于数据终点"))
        form_data["window_type"] = "accumulate_by_hour"
        form_data["accumulate"] = True
        form_data["schedule_period"] = "hour"
        form_data["count_freq"] = 1
        return form_data
