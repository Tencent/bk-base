/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.dataflow.ml.exceptions;

import java.util.HashMap;
import java.util.Map;

public class OneModelErrorMessage {

    private static String DATA_SOURCE_EMTPY_MSG = "The node {0} input data is empty.";
    private static String FIELD_NOT_EXISTS_MSG = "The node {0} data does not have field {1} ";
    private static String PARAM_NOT_FOUND_MSG = "Not found the parameter {0} in {1}";
    private static String ALGORITHM_RUN_MODE_MSG = "The algorithm {0} don't support {1}";
    private static String TYPE_TRANSFORM_MSG = "The node {0} field {1} type, actual {2}, expect {3}";
    private static String ALGORITHM_GENERATE_MSG = "Failed to generate algorithm:{0}";
    private static String ALGORITHM_PARAM_GENERATE_MSG = "Failed to generate algorithm {0} for params {1}";
    private static String PARAM_TYPE_NOT_SUPPORT_MSG = "Not support type {0} when set parameter.";
    private static String PARAM_NOT_SET_MSG = "Failed to set parameter {0} in {1}";
    private static String PARAM_INTERPRET_FAILED_MSG = "Failed to interprete parameters:{0}";
    private static String TRANSFORM_STAGE_ERROR_MSG = "Failed to transform node:{0}";

    private static Map<String, String> errorMap = new HashMap<>();

    static {
        errorMap.put(OneModelErrorCodes.DATA_SOURCE_EMTPY_ERROR_CODE, DATA_SOURCE_EMTPY_MSG);
        errorMap.put(OneModelErrorCodes.FIELD_NOT_EXISTS_ERROR_CODE, FIELD_NOT_EXISTS_MSG);
        errorMap.put(OneModelErrorCodes.PARAM_NOT_FOUND_ERROR_CODE, PARAM_NOT_FOUND_MSG);
        errorMap.put(OneModelErrorCodes.TYPE_TRANSFORM_ERROR_CODE, TYPE_TRANSFORM_MSG);
        errorMap.put(OneModelErrorCodes.ALGORITHM_RUN_MODE_ERROR_CODE, ALGORITHM_RUN_MODE_MSG);
        errorMap.put(OneModelErrorCodes.ALGORITHM_GENERATE_ERROR_CODE, ALGORITHM_GENERATE_MSG);
        errorMap.put(OneModelErrorCodes.PARAM_TYPE_NOT_SUPPORT_ERROR_CODE, PARAM_TYPE_NOT_SUPPORT_MSG);
        errorMap.put(OneModelErrorCodes.PARAM_NOT_SET_ERROR_CODE, PARAM_NOT_SET_MSG);
        errorMap.put(OneModelErrorCodes.ALGORITHM_PARAM_GENERATE_ERROR_CODE, ALGORITHM_PARAM_GENERATE_MSG);
        errorMap.put(OneModelErrorCodes.PARAM_INTERPRETER_ERROR_CODE, PARAM_INTERPRET_FAILED_MSG);
        errorMap.put(OneModelErrorCodes.TRANSFORM_STAGE_ERROR_CODE, TRANSFORM_STAGE_ERROR_MSG);
    }

    public static String getMessage(String code) {
        return errorMap.get(code);
    }
}
