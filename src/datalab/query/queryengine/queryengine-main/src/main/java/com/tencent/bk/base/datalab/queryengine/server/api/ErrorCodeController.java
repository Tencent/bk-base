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

package com.tencent.bk.base.datalab.queryengine.server.api;

import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.model.ErrorCode;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * 系统 Controller
 */
@Slf4j
@RateLimiter(name = "global")
@RestController
@RequestMapping(value = "/queryengine/errorcodes",
        produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
public class ErrorCodeController {

    private static final String DEFAULT_VALUE = "-";

    /**
     * 获取系统错误码
     *
     * @return 系统错误码列表
     * @throws Exception 接口调用异常
     */
    @GetMapping(value = "/")
    public ApiResponse<Object> getErrorCodes()
            throws Exception {
        List<ErrorCode> errorCodeList = new ArrayList<>();
        ResultCodeEnum[] resultCodeEnums = ResultCodeEnum.values();
        for (ResultCodeEnum resultCode : resultCodeEnums) {
            ErrorCode errorCode = ErrorCode.builder()
                    .code(resultCode.code())
                    .message(MessageLocalizedUtil.getMessage(resultCode.message()))
                    .name(DEFAULT_VALUE)
                    .solution(DEFAULT_VALUE).build();
            errorCodeList.add(errorCode);
            log.debug("Added errorCode[code:{},message:{},name:{},solution:{}]",
                    errorCode.getCode(), errorCode.getMessage(), errorCode.getName(),
                    errorCode.getSolution());
        }
        return ApiResponse.success(errorCodeList);
    }
}
