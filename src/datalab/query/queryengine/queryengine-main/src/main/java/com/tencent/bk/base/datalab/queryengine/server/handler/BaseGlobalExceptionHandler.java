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

package com.tencent.bk.base.datalab.queryengine.server.handler;

import com.tencent.bk.base.datalab.bksql.exception.FailedOnCheckException;
import com.tencent.bk.base.datalab.bksql.exception.FailedOnDeParserException;
import com.tencent.bk.base.datalab.bksql.exception.MessageLocalizedException;
import com.tencent.bk.base.datalab.bksql.exception.ParseException;
import com.tencent.bk.base.datalab.bksql.exception.TokenMgrException;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.BaseException;
import com.tencent.bk.base.datalab.queryengine.server.exception.ParameterInvalidException;
import com.tencent.bk.base.datalab.queryengine.server.exception.PermissionForbiddenException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.util.ParameterInvalidItem;
import com.tencent.bk.base.datalab.queryengine.server.util.ParameterInvalidItemUtil;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolationException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.stereotype.Component;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;

/**
 * 全局异常处理基础类
 */
@Slf4j
@Component
public class BaseGlobalExceptionHandler {

    @Autowired
    private MessageSourceHandler messageSourceHandler;

    /**
     * 违反约束异常
     */
    protected ApiResponse handleConstraintViolationException(ConstraintViolationException e,
            HttpServletRequest request) {
        log.error("handleConstraintViolationException start, uri:{}, caused by: ",
                request.getRequestURI(), e);
        List<ParameterInvalidItem> parameterInvalidItemList = ParameterInvalidItemUtil
                .convertCvSet(e.getConstraintViolations());
        StringBuffer errorMsg = new StringBuffer();
        if (parameterInvalidItemList != null) {
            parameterInvalidItemList.forEach(error -> errorMsg.append(error.toString())
                    .append(";"));
        }
        return ApiResponse.error(ResultCodeEnum.PARAM_IS_INVALID, errorMsg.toString());
    }

    /**
     * 处理验证参数封装错误时异常
     */
    protected ApiResponse handleConstraintViolationException(HttpMessageNotReadableException e,
            HttpServletRequest request) {
        log.error("handleConstraintViolationException start, uri:{}, caused by: ",
                request.getRequestURI(), e);
        return ApiResponse.error(ResultCodeEnum.PARAM_IS_INVALID);
    }

    /**
     * 处理参数绑定时异常
     */
    protected ApiResponse handleBindException(BindException e, HttpServletRequest request) {
        log.info("handleBindException start, uri:{}, caused by: ", request.getRequestURI(), e);
        List<ParameterInvalidItem> parameterInvalidItemList = ParameterInvalidItemUtil
                .convertBindingResult(e.getBindingResult());
        StringBuffer errorMsg = new StringBuffer();
        parameterInvalidItemList.forEach(error -> {
            errorMsg.append(error.toString())
                    .append(";");
        });
        return ApiResponse.error(ResultCodeEnum.PARAM_IS_INVALID, errorMsg.toString());
    }

    /**
     * 处理使用 @Validated 注解时，参数验证错误异常
     */
    protected ApiResponse handleMethodArgsInValidException(MethodArgumentNotValidException e,
            HttpServletRequest request) {
        List<ParameterInvalidItem> parameterInvalidItemList = ParameterInvalidItemUtil
                .convertBindingResult(e.getBindingResult());
        StringBuffer errorMsg = new StringBuffer();
        parameterInvalidItemList.forEach(error -> {
            errorMsg.append(error)
                    .append(";");
        });
        return ApiResponse.error(ResultCodeEnum.PARAM_IS_INVALID, errorMsg.toString());
    }

    /**
     * 处理通用自定义业务异常
     */
    protected ApiResponse handleBusinessException(BaseException e, HttpServletRequest request) {
        log.error("handleBusinessException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(ResultCodeEnum.SYSTEM_INNER_ERROR, e.getMessage(), e.getErrors());
    }

    /**
     * 处理 BkSql 语法解析异常
     */
    protected ApiResponse handleBkSqlParseException(ParseException e, HttpServletRequest request) {
        log.error("handleBkSqlParseException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, e.getMessage());
    }

    /**
     * 处理 BkSql 词法解析异常
     */
    protected ApiResponse handleBkSqlTokenMgrException(TokenMgrException e,
            HttpServletRequest request) {
        log.error("handleBkSqlTokenMgrException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(ResultCodeEnum.QUERY_SQL_TOKEN_ERROR, e.getMessage());
    }

    /**
     * 处理 BkSql 校验异常
     */
    protected ApiResponse handleBkSqlCheckerException(FailedOnCheckException e,
            HttpServletRequest request) {
        log.error("handleBkSqlCheckerException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, e.getMessage());
    }

    /**
     * 处理 BkSql 解析异常
     */
    protected ApiResponse handleBkSqlDeParserException(FailedOnDeParserException e,
            HttpServletRequest request) {
        log.error("handleBkSqlDeParserException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, e.getMessage());
    }

    /**
     * 处理本地化异常
     */
    protected ApiResponse handleMessageLocalizedException(MessageLocalizedException e,
            HttpServletRequest request) {
        log.error("handleMessageLocalizedException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(ResultCodeEnum.RESOURCE_NOT_EXISTED, e.getMessage());
    }

    /**
     * 处理查询异常
     */
    protected ApiResponse handleQueryDetailException(QueryDetailException e,
            HttpServletRequest request) {
        log.error("handleQueryDetailException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(e.getResultCode(), e.getMessage(), e.getErrors());
    }

    /**
     * 处理权限不足异常
     */
    protected ApiResponse handlePermissionForbiddenException(PermissionForbiddenException e,
            HttpServletRequest request) {
        log.error(
                "handlePermissionForbiddenException start, uri:{}, exceptionClass:{}, caused by: "
                        + "{}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(e.getResultCode(), e.getMessage(), e.getErrors());
    }

    /**
     * 处理参数无效异常
     */
    protected ApiResponse handleParameterInvalidException(ParameterInvalidException e,
            HttpServletRequest request) {
        log.error("handleParameterInvalidException start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(e.getResultCode(), e.getMessage(), e.getErrors());
    }

    protected ApiResponse handleRequestNotPermitted(RequestNotPermitted e,
            HttpServletRequest request) {
        log.error("handleRequestNotPermitted start, uri:{}, exceptionClass:{}, caused by: {}",
                request.getRequestURI(), e.getClass(), e.getMessage());
        return ApiResponse.error(ResultCodeEnum.INTERFACE_EXCEED_LOAD,
                ResultCodeEnum.INTERFACE_EXCEED_LOAD.message());
    }

    /**
     * 处理未预测到的其他错误
     */
    protected ApiResponse handleThrowable(Throwable e, HttpServletRequest request) {
        log.error("handleThrowable start, uri:{}, caused by: ", request.getRequestURI(), e);
        if (StringUtils.isNotBlank(e.getMessage())) {
            return ApiResponse
                    .error(ResultCodeEnum.SYSTEM_INNER_ERROR, e.getMessage());
        }
        return ApiResponse
                .error(ResultCodeEnum.SYSTEM_INNER_ERROR,
                        ResultCodeEnum.SYSTEM_INNER_ERROR.message());
    }
}
