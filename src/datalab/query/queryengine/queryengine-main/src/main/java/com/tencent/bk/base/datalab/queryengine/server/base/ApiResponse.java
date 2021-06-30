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

package com.tencent.bk.base.datalab.queryengine.server.base;

import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.SUCCESS;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.SYSTEM_INNER_ERROR;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import java.util.Map;
import lombok.Data;

@Data
public final class ApiResponse<T> {

    public boolean result;
    public String message;
    public String code;
    public T data;
    public Map<String, String> errors;

    public ApiResponse() {
    }

    @JsonCreator
    public ApiResponse(@JsonProperty("result") boolean result,
            @JsonProperty("message") String message,
            @JsonProperty("code") String code,
            @JsonProperty("data") T data,
            @JsonProperty("errors") Map<String, String> errors) {
        this.result = result;
        this.message = message;
        this.code = code;
        this.data = data;
        this.errors = errors;
    }

    /**
     * 返回处理成功
     *
     * @return ApiResponse
     */
    public static ApiResponse success() {
        ApiResponse response = new ApiResponse();
        response.setResult(true);
        response.setResultCode(SUCCESS);
        return response;
    }

    /**
     * 返回处理成功
     *
     * @param data 返回数据
     * @param <T> 返回数据类型
     * @return ApiResponse
     */
    public static <T> ApiResponse<T> success(T data) {
        ApiResponse<T> response = new ApiResponse<>();
        response.setResult(true);
        response.setResultCode(SUCCESS);
        response.setData(data);
        return response;
    }

    /**
     * 返回处理成功
     *
     * @param message 提示消息
     * @return ApiResponse
     */
    public static ApiResponse success(String message) {
        ApiResponse response = new ApiResponse();
        response.setResult(true);
        response.setResultCode(ResultCodeEnum.SUCCESS);
        response.setMessage(MessageLocalizedUtil.getMessage(message));
        return response;
    }

    /**
     * 处理失败
     *
     * @return ApiResponse
     */
    public static ApiResponse error() {
        ApiResponse response = new ApiResponse();
        response.setResult(false);
        response.setResultCode(SYSTEM_INNER_ERROR);
        return response;
    }

    /**
     * 处理失败
     *
     * @param message 失败提示消息
     * @return ApiResponse
     */
    public static ApiResponse error(String message) {
        ApiResponse response = new ApiResponse();
        response.setResult(false);
        response.setResultCode(ResultCodeEnum.SYSTEM_INNER_ERROR);
        response.setMessage(MessageLocalizedUtil.getMessage(message));
        return response;
    }

    /**
     * 处理失败
     *
     * @param resultCode 错误类型
     * @return ApiResponse
     */
    public static ApiResponse error(ResultCodeEnum resultCode) {
        ApiResponse response = new ApiResponse();
        response.setResult(false);
        response.setResultCode(resultCode);
        return response;
    }

    /**
     * 处理失败
     *
     * @param resultCode 错误类型
     * @param message 错误提示信息
     * @return ApiResponse
     */
    public static ApiResponse error(ResultCodeEnum resultCode, String message) {
        ApiResponse response = new ApiResponse();
        response.setResult(false);
        response.setResultCode(resultCode);
        response.setMessage(MessageLocalizedUtil.getMessage(message));
        return response;
    }

    /**
     * 处理失败
     *
     * @param resultCode 错误类型
     * @param message 错误提示信息
     * @return ApiResponse
     */
    public static ApiResponse error(ResultCodeEnum resultCode, String message,
            Map<String, String> errors) {
        ApiResponse response = new ApiResponse();
        response.setResult(false);
        response.setResultCode(resultCode);
        response.setMessage(MessageLocalizedUtil.getMessage(message));
        response.setErrors(errors);
        return response;
    }

    /**
     * 设置 ResultCode
     *
     * @param code ResultCode
     */
    private void setResultCode(ResultCodeEnum code) {
        this.code = code.code();
        this.message = MessageLocalizedUtil.getMessage((code.message()));
    }
}
