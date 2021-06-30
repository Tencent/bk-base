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

from dataflow.udf.settings import UdfType

UDF_JAVA_TEMPLATE = """
/**
 * UDF(User-Defined Scalar Functions)将零个、一个或多个标量值映射为一个新的标量值。
 *
 * 请注意以下几点，防止开发的自定义函数不可用
 * 1.jdk版本为1.8
 * 2.请不要修改类名、父类和方法名；
 * 3.请保证代码中输入（输出）参数个数、类型和顺序和上面配置的输入（输出）参数信息一致。
 */
public class %(function_name)s extends %(super_class)s {

  public %(return_type)s call(%(input_params)s) {
    %(code_example)s
  }
}
"""

UDTF_JAVA_TEMPLATE = """
/**
 * UDTF(User-Defined Table Functions)将零个、一个或多个标量值做为输入参数，它返回的数据可以包含一列或多列、一行或多行。
 *
 * 请注意以下几点，防止开发的自定义函数不可用
 * 1.jdk版本为1.8
 * 2.请不要修改类名、父类和方法名；
 * 3.请保证代码中输入（输出）参数个数、类型和顺序和上面配置的输入（输出）参数信息一致。
 */
public class %(function_name)s extends %(super_class)s {

  public %(return_type)s call(%(input_params)s) {
    %(code_example)s
  }
}
"""

UDAF_JAVA_TEMPLATE = """
/**
 * UDAF(User-Defined Aggregate Functions)将一个表（一列或多列、一行或多行）聚合为标量值。
 *
 * 请注意以下几点，防止开发的自定义函数不可用
 * 1.jdk版本为1.8
 * 2.请不要修改类名、父类和方法名；
 * 3.请保证代码中输入（输出）参数个数、类型和顺序和上面配置的输入（输出）参数信息一致；
 * 4.不允许写内部类；
 * 5.Map为数据存储容器。
 */
public class %(function_name)s extends %(super_class)s<Map<String,Long>, %(return_type)s> {

  public Map<String,Long> createAccumulator() {
    return new HashMap<String, Long>() {
      {
        put("xx", 0L);
        put("xxxx", 0L);
      }
    };
  }

  public %(return_type)s getValue(Map<String,Long> acc) {

  }

  public void accumulate(Map<String,Long> acc, %(input_params)s) {

  }

  public void merge(Map<String,Long> acc, Map<String,Long> a) {

  }

  public void resetAccumulator(Map<String,Long> acc) {

  }
}
"""

UDF_PYTHON_TEMPLATE = """
# -*- coding: utf-8 -*-
\"\"\"
UDF(User-Defined Scalar Functions)将零个、一个或多个标量值映射为一个新的标量值。

请注意以下几点，防止开发的自定义函数不可用
1.Python版本为3.6.9
2.请不要修改方法名；
3.请保证代码中输入（输出）参数个数、类型和顺序和上面配置的输入（输出）参数信息一致。
\"\"\"
def call(%(input_params)s):
    %(code_example)s
"""

UDTF_PYTHON_TEMPLATE = """
# -*- coding: utf-8 -*-
\"\"\"
UDTF(User-Defined Table Functions)将零个、一个或多个标量值做为输入参数，它返回的数据可以包含一列或多列、一行或多行。

请注意以下几点，防止开发的自定义函数不可用
1.Python版本为3.6.9
2.请不要修改方法名；
3.请保证代码中输入（输出）参数个数、类型和顺序和上面配置的输入（输出）参数信息一致。
\"\"\"
def call(%(input_params)s):
    %(code_example)s
"""

UDAF_PYTHON_TEMPLATE = """
# -*- coding: utf-8 -*-
\"\"\"
UDAF(User-Defined Aggregate Functions)将一个表（一列或多列、一行或多行）聚合为标量值。

请注意以下几点，防止开发的自定义函数不可用
1.Python版本为3.6.9
2.请不要修改方法名；
3.请保证代码中输入（输出）参数个数、类型和顺序和上面配置的输入（输出）参数信息一致。
4.convert_dict是将累加器accumulator转换为dict，请勿删除
\"\"\"

from %(function_name)s_util import convert_dict

# 请保证init中字典的所有key的类型保持一致，value的类型也保持一致
def init():
    return {"a": 0, "b": 0}

@convert_dict
def get_value(accumulator):

@convert_dict
def accumulate(accumulator, %(input_params)s):

@convert_dict
def merge(accumulator1, accumulator2):

@convert_dict
def reset_accumulator(accumulator):

"""


def load_java_function_template(function_name, func_udf_type, input_type=[], return_type=[]):
    """
    加载 java 函数模板
    @param function_name:
    @param func_udf_type:
    @param input_type:
    @param return_type:
    @return:
    """
    code_frame = UDF_JAVA_TEMPLATE
    input_params = ", ".join(
        map(
            lambda index__type: "%s %s"
            % ("String" if index__type[1] == "string" else index__type[1], "param" + str(index__type[0])),
            enumerate(input_type),
        )
    )
    if func_udf_type == "udf":
        code_frame = code_frame % {
            "super_class": "AbstractUdf",
            "function_name": function_name,
            "input_params": input_params,
            "return_type": _java_convert_return_type(return_type[0]) if return_type else "Void",
            "code_example": "return null;",
        }
    elif func_udf_type == "udtf":
        code_frame = UDTF_JAVA_TEMPLATE
        code_frame = code_frame % {
            "super_class": "AbstractUdtf",
            "function_name": function_name,
            "input_params": input_params,
            "return_type": "void",
            "code_example": 'collect(Arrays.asList("1","2","3"));',
        }
    else:
        code_frame = UDAF_JAVA_TEMPLATE
        code_frame = code_frame % {
            "super_class": "AbstractUdaf",
            "function_name": function_name,
            "input_params": input_params,
            "return_type": _java_convert_return_type(return_type[0]) if return_type else "Void",
        }
    return code_frame


def _java_convert_return_type(type):
    if type == "string":
        return "String"
    elif type == "long":
        return "Long"
    elif type == "int":
        return "Integer"
    elif type == "double":
        return "Double"
    elif type == "float":
        return "Float"


def load_python_function_template(function_name, func_udf_type, input_type=[], return_type=[]):
    """
    加载 python 函数模板
    @param function_name:
    @param func_udf_type:
    @param input_type:
    @param return_type:
    @return:
    """
    code_frame = UDF_PYTHON_TEMPLATE
    input_params = ", ".join("param" + str(index__type1[0]) for index__type1 in enumerate(input_type))
    if func_udf_type == "udf":
        code_frame = code_frame % {
            "input_params": input_params,
            "code_example": "return None",
        }
    elif func_udf_type == "udtf":
        code_frame = UDTF_PYTHON_TEMPLATE
        code_frame = code_frame % {
            "input_params": input_params,
            "code_example": """
            object = [{input_params}]
            collect(object)
            """,
        }
    else:
        code_frame = UDAF_PYTHON_TEMPLATE
        code_frame = code_frame % {
            "input_params": input_params,
            "function_name": function_name,
        }
    return code_frame


def get_java_udf_prefix(udf_type):
    if udf_type.lower() == UdfType.udf.name:
        return """
            package com.tencent.bk.base.dataflow.udf.functions;

            import com.tencent.bk.base.dataflow.core.function.base.udf.AbstractUdf;
        """
    elif udf_type.lower() == UdfType.udtf.name:
        return """
            package com.tencent.bk.base.dataflow.udf.functions;

            import com.tencent.bk.base.dataflow.core.function.base.udtf.AbstractUdtf;
        """
    elif udf_type.lower() == UdfType.udaf.name:
        return """
            package com.tencent.bk.base.dataflow.udf.functions;

            import com.tencent.bk.base.dataflow.core.function.base.udaf.AbstractUdaf;
        """
    else:
        raise Exception("Not support udf type %s" % udf_type)
