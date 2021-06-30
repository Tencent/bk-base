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

from django.utils.translation import ugettext as _

from dataflow.flow.node_types import NodeTypes
from dataflow.pizza_settings import FLINK_CODE_VERSION, SYSTEM_RESERVE_FIELD
from dataflow.shared.log import flow_logger as logger
from dataflow.shared.meta.result_table.result_table_helper import ResultTableHelper

FLINK_JAVA_TEMPLATE = """
import com.tencent.blueking.dataflow.common.api.AbstractFlinkBasicTransform;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

/**
 * 请注意以下几点，以防开发的 Code 节点不可用
 * 1.jdk 版本为 1.8
 * 2.Flink 版本 为 %s
 * 2.请不要修改数据处理类名 CodeTransform
 */
public class CodeTransform extends AbstractFlinkBasicTransform {

    /**
     * 数据处理，示例为将一个数据的所有字段选择，并输出
     *
     * @param input 存放数据源的 Map，key 为数据源表名 id，value 为数据源对应的Flink streaming 的数据结构 DataStream<Row>
     * @return 返回存放结果数据的 Map，key 为输出表名，value 为输出表对应的数据结构 DataStream<Row>，输出的字段必须要和节点输出配置一样
     */
    @Override
    public Map<String, DataStream<Row>> transform(Map<String, DataStream<Row>> input) {

        // 数据输入
        %s

        // 数据处理
        // 输出表字段名配置
        String[] fieldNames = new String[] {
            %s
        };

        // 输出表字段类型配置
        TypeInformation<?>[] rowType = new TypeInformation<?>[]{
            %s
        };

        // 获取数据源字段的index
        %s

        // 将第一个数据源的所有字段赋值给输出表
        DataStream<Row> newOutput = inputDataStream0.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) {
                Row ret = new Row(%s);
                %s
                return ret;
            }
        }).returns(new RowTypeInfo(rowType, fieldNames));

        // 数据输出
        Map<String, DataStream<Row>> output = new HashMap<>();
        output.put("%s_输出（需修改）", newOutput);
        return output;
    }
}
"""

SPARK_STRUCTURED_STREAMING_PYTHON_TEMPLATE = """
# -*- coding: utf-8 -*-

# python 版本为 3.6.9
# 请不要修改数据处理类名和方法名，以防开发的 Code 节点不可用
class spark_structured_streaming_code_transform:

    def __init__(self, user_args, spark):
        self.user_args = user_args
        self.spark = spark

    # 数据处理，示例为将输入表的所有字段选择，并输出
    # @param inputs 存放数据源及其字段名
    def transform(self, inputs):
        input_df = inputs["%s"]
        output_df = input_df.select(%s)
        return {"%s_输出（需修改）": output_df}

    # 输出模式：complete,append,update
    def set_output_mode(self):
        return {"%s_输出（需修改）": "append"}
"""


def load_code_template(programming_language, node_type, input_result_table_ids):
    if node_type == NodeTypes.FLINK_STREAMING:
        return generate_flink_code_template(programming_language, node_type, input_result_table_ids)
    elif node_type == NodeTypes.SPARK_STRUCTURED_STREAMING:
        return generate_spark_code_template(programming_language, node_type, input_result_table_ids)


def generate_spark_code_template(programming_language, node_type, input_result_table_ids):
    # 生成输入

    input_rt = input_result_table_ids[0]
    bk_biz_id = input_result_table_ids[0].split("_")[0]

    # 获取输入第一个rt的字段信息, 并生成对应的模版代码信息
    result_table = ResultTableHelper.get_result_table(input_rt, related="fields")
    field_names = []
    for field in result_table["fields"]:
        if field["field_name"] in SYSTEM_RESERVE_FIELD:
            continue
        field_names.append('"%s"' % field["field_name"])

    if programming_language.lower() == "python":
        return SPARK_STRUCTURED_STREAMING_PYTHON_TEMPLATE % (
            input_rt,  # 输入
            ",".join(field_names),  # 字段名称
            bk_biz_id,
            bk_biz_id,
        )
    else:
        logger.info("{} for {} is not supported".format(programming_language, node_type))
        raise Exception(_("暂不支持获取当前类型的代码模板."))


def generate_flink_code_template(programming_language, node_type, input_result_table_ids):
    # 生成输入
    source_template = []
    index = 0
    bk_biz_id = input_result_table_ids[0].split("_")[0]
    for result_table_id in input_result_table_ids:
        source_template.append('DataStream<Row> inputDataStream%d = input.get("%s");' % (index, result_table_id))
        index = index + 1

    # 获取输入第一个rt的字段信息, 并生成对应的模版代码信息
    result_table = ResultTableHelper.get_result_table(input_result_table_ids[0], related="fields")
    field_names = []
    field_types = []
    field_indexs = []
    set_fields = []
    index = 0
    for field in result_table["fields"]:
        if field["field_name"] in SYSTEM_RESERVE_FIELD:
            continue
        field_names.append('"%s"' % field["field_name"])
        field_types.append(convert_flink_type(field["field_type"]))
        field_indexs.append(
            'int index{} = this.getFieldIndexByName(inputDataStream0, "{}");'.format(index, field["field_name"])
        )
        set_fields.append("ret.setField({}, row.getField(index{}));".format(index, index))
        index = index + 1
    new_fields_size = len(field_names)

    if programming_language.lower() == "java" and node_type in [NodeTypes.FLINK_STREAMING]:
        return FLINK_JAVA_TEMPLATE % (
            FLINK_CODE_VERSION,  # 版本
            "\n\t\t".join(source_template),  # 输入
            ",\n\t\t\t".join(field_names),  # 字段名称
            ",\n\t\t\t".join(field_types),  # 字段类型
            "\n\t\t".join(field_indexs),  # 获取数据源字段的index
            new_fields_size,
            "\n\t\t\t\t".join(set_fields),  # 设置新的字段
            bk_biz_id,
        )
    else:
        logger.info("{} for {} is not supported".format(programming_language, node_type))
        raise Exception(_("暂不支持获取当前类型的代码模板."))


def convert_flink_type(type):
    type_map = {
        "string": "BasicTypeInfo.STRING_TYPE_INFO",
        "long": "BasicTypeInfo.LONG_TYPE_INFO",
        "int": "BasicTypeInfo.INT_TYPE_INFO",
        "double": "BasicTypeInfo.DOUBLE_TYPE_INFO",
        "float": "BasicTypeInfo.FLOAT_TYPE_INFO",
    }
    return type_map.get(type)
