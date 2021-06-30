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

import com.tencent.bk.base.dataflow.core.exceptions.ErrorCodes;

public class OneModelErrorCodes extends ErrorCodes {

    //one model error code:100-199
    //100-179：用户异常
    //180-199:系统异常
    public static final String DATA_SOURCE_EMTPY_ERROR_CODE = UC_CODE + "100";
    public static final String FIELD_NOT_EXISTS_ERROR_CODE = UC_CODE + "101";
    public static final String TYPE_TRANSFORM_ERROR_CODE = UC_CODE + "102";
    public static final String PARAM_NOT_FOUND_ERROR_CODE = UC_CODE + "103";
    public static final String ALGORITHM_RUN_MODE_ERROR_CODE = UC_CODE + "104";
    public static final String PARAM_TYPE_NOT_SUPPORT_ERROR_CODE = UC_CODE + "105";
    public static final String PARAM_INTERPRETER_ERROR_CODE = UC_CODE + "106";


    public static final String ALGORITHM_GENERATE_ERROR_CODE = UC_CODE + "180";
    public static final String ALGORITHM_PARAM_GENERATE_ERROR_CODE = UC_CODE + "181";
    public static final String PARAM_NOT_SET_ERROR_CODE = UC_CODE + "182";
    public static final String TRANSFORM_STAGE_ERROR_CODE = UC_CODE + "183";

    public static final String DEFAULT_ERROR_CODE = UC_CODE + "500";
}
