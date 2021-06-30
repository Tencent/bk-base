/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/** 创建函数
 * @paramExample
    {
        "func_name": 'test',
        "func_alias": 'test',
        "func_language": 'java',
        "func_udf_type": 'udf',
        "input_type": ['test'],
        "return_type": ['test'],
        "explain": 'test',
        "example": 'test',
        "example_return_value": 'test',
        "code_config": {"dependencies": [], "code": "public ..."}
    }
 */
const createUDF = {
  url: '/v3/dataflow/flow/udfs/',
  method: 'POST',
};

/** 更新函数
 * @paramExample
    {
        "func_alias": 'test',
        "func_language": 'java',
        "func_udf_type": 'udf',
        "input_type": ['test'],
        "return_type": ['test'],
        "explain": 'test',
        "example": 'test',
        "example_return_value": 'test',
        "code_config": {"dependencies": [], "code": "public ..."}
    }
 */
const updateUDF = {
  url: '/v3/dataflow/flow/udfs/:func_name/',
  method: 'PUT',
};

/** 获取udf代码框架
 * @paramExample
    {
        "func_language": "java",
        "func_udf_type": "udf",
        "input_type": ["string", "string"],
        "return_type": ["string"]
    }
 */
const getUDFCodeFrame = {
  url: '/v3/dataflow/flow/udfs/:function_name/code_frame/',
};

/** 获取udf调试数据
 * @paramExample
    {
        'result_table_id': 'xxx'
    }
 */
const getDebugData = {
  url: '/v3/dataflow/flow/udfs/get_debug_data/',
};

/** 开始函数调试
 * @paramExample
    {
        'calculation_types': ['batch', 'stream'],
        'debug_data': {
            'schema': [
                {
                    'field_name': 'a',
                    'field_type': 'string'
                },
                {
                    'field_name': 'b',
                    'field_type': 'string'
                }
            ],
            'value': [
                ['x', 'xx']
            ]
        },
        'result_table_id': 'xxx',
        'sql': 'xxx'
    }
 */
const debugStart = {
  url: '/v3/dataflow/flow/udfs/:function_name/debuggers/start/',
  method: 'POST',
};

/** 获取调试信息
 */
const getDebugStatus = {
  url: '/v3/dataflow/flow/udfs/:function_name/debuggers/:debugger_id/latest/',
};

/** 编辑函数
 */
const editUDF = {
  url: '/v3/dataflow/flow/udfs/:function_name/edit/',
  method: 'POST',
};

/** 解锁函数
 */
const unloackUDF = {
  url: '/v3/dataflow/flow/udfs/:function_name/unlock/',
  method: 'POST',
};

/** 发布函数
 */
const publishUDF = {
  url: '/v3/dataflow/flow/udfs/:function_name/release/ ',
  method: 'POST',
};

/** 获取自定义函数列表
 */
const getUDFList = {
  url: '/v3/dataflow/flow/udfs/',
};

/** 获取指定自定义函数
 *  @pramExample {
        'add_project_info': false,       # 是否返回项目信息
        'add_flow_info': false,          # 是否返回任务信息
        'add_node_info': false,          # 是否返回节点信息
        'add_release_log_info': false
    }
 */
const getExactUDF = {
  url: '/v3/dataflow/flow/udfs/:function_name/',
};

/** 获取Python支持包
 */

const getPythonModule = {
  url: '/v3/dataflow/flow/udfs/support_python_packages/',
};

/** 获取udf详情的flow列表
 *  query: {
 *      object_type: project/flow
 *      object_id: xxx
 *  }
 */

const getUdfFlowOrNodes = {
  url: '/v3/dataflow/flow/udfs/:function_name/get_related_info/',
};

/** 校验UDF的使用样例
 *  query: {
 *      'sql': 'select...'
 *  }
 */
const checkUdfSql = {
  url: '/v3/dataflow/flow/udfs/:function_name/check_example_sql/',
};

/** 删除UDF */

const deleteUDF = {
  url: '/v3/dataflow/flow/udfs/:function_name/',
  method: 'DELETE',
};

/** 获取udf调试信息 */
const getUDFDebugerInfo = {
  url: '/v3/dataflow/flow/udfs/:function_name/debuggers/get_latest_config/',
};
export {
  createUDF,
  updateUDF,
  getUDFCodeFrame,
  getDebugData,
  debugStart,
  getDebugStatus,
  editUDF,
  unloackUDF,
  publishUDF,
  getUDFList,
  getExactUDF,
  getPythonModule,
  getUdfFlowOrNodes,
  checkUdfSql,
  deleteUDF,
  getUDFDebugerInfo,
};
