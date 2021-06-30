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

package com.tencent.bk.base.datahub.databus.commons.bean;

import com.tencent.bk.base.datahub.databus.commons.Consts;

public class ApiResult {

    // PIZZA API Result解析对象。默认result为false，代表请求失败。
    private String code = null;
    private String message = null;
    private boolean result = false;
    private Object data = null;
    private Object errors = null;

    /**
     * 构造函数
     */
    public ApiResult() {
        this("", "", false, null, null);
    }

    /**
     * 构造函数
     *
     * @param data 返回json response中的data内容
     */
    public ApiResult(Object data) {
        this(Consts.NORMAL_RETCODE, "", true, data, null);
    }

    /**
     * 构造函数
     *
     * @param code 返回码
     * @param message 提示信息
     * @param result 请求成功与否
     */
    public ApiResult(String code, String message, boolean result) {
        this(code, message, result, null, null);
    }

    /**
     * 构造函数
     *
     * @param code 返回码
     * @param message 提示信息
     * @param result 请求成功与否
     * @param data 返回的结果集
     * @param errors 错误信息
     */
    public ApiResult(String code, String message, boolean result, Object data, Object errors) {
        this.code = code;
        this.message = message;
        this.result = result;
        this.data = data;
        this.errors = errors;
    }

    // getters & setters

    /**
     * 设置错误码
     *
     * @param code 错误码
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * 设置提示消息
     *
     * @param message 提示消息
     */
    public void setMessage(String message) {
        this.message = message;
    }

    /**
     * 设置API请求结果
     *
     * @param result API请求结果
     */
    public void setResult(boolean result) {
        this.result = result;
    }

    /**
     * 设置API请求返回值
     *
     * @param data 请求返回值
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * 设置错误信息
     *
     * @param errors 错误信息
     */
    public void setErrors(Object errors) {
        this.errors = errors;
    }

    /**
     * 获取返回码
     *
     * @return 返回码
     */
    public String getCode() {
        return code;
    }

    /**
     * 获取提示信息
     *
     * @return 提示信息
     */
    public String getMessage() {
        return message;
    }

    /**
     * 获取请求成功与否
     *
     * @return 请求成功与否
     */
    public boolean isResult() {
        return result;
    }

    /**
     * 获取请求结果集
     *
     * @return 请求结果集
     */
    public Object getData() {
        return data;
    }

    /**
     * 获取错误信息
     *
     * @return 错误信息
     */
    public Object getErrors() {
        return errors;
    }

    /**
     * toString方法
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return String
                .format("Code: %s, Message: %s, Result: %s, Errors: %s, Data: %s", code, message, result, errors, data);
    }
}
