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

package com.tencent.bk.base.dataflow.bksql.mlsql.exceptions;

public class ErrorCode {

    private static String BKDATA_PLAT_CODE = "15";
    private static String UC_CODE = BKDATA_PLAT_CODE + "86";
    public static String DEFAULT_ERR = UC_CODE + "100";
    public static String SHOW_ERR = UC_CODE + "101";
    public static String DROP_ERR = UC_CODE + "102";
    public static String TRUNCATE_ERR = UC_CODE + "103";
    public static String TABLE_NOT_FOUND_ERR = UC_CODE + "104";
    public static String MODEL_OR_ALG_NOT_FOUNT_ERR = UC_CODE + "105";
    public static String SYNTAX_ERROR = UC_CODE + "106";
    public static String SYMANTIC_ERROR = UC_CODE + "107";
    public static String PARAM_NOT_INPUT_ERROR = UC_CODE + "108";
    public static String PARAM_TYPE_NOT_MATCH_ERROR = UC_CODE + "109";
    public static String MISS_CONFIG_ERROR = UC_CODE + "110";
    public static String SUCCESS_CODE = UC_CODE + "200";
}
