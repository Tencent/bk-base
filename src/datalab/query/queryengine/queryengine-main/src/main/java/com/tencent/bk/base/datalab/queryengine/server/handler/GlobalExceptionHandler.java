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
import com.tencent.bk.base.datalab.queryengine.server.exception.BaseException;
import com.tencent.bk.base.datalab.queryengine.server.exception.ParameterInvalidException;
import com.tencent.bk.base.datalab.queryengine.server.exception.PermissionForbiddenException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import javax.servlet.http.HttpServletRequest;
import javax.validation.ConstraintViolationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.validation.BindException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 * 统一异常处理器
 */
@RestController
@ControllerAdvice
public class GlobalExceptionHandler extends BaseGlobalExceptionHandler {

    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(ConstraintViolationException.class)
    public ApiResponse handleConstraintViolationException(ConstraintViolationException e,
            HttpServletRequest request) {
        return super.handleConstraintViolationException(e, request);
    }

    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ApiResponse handleConstraintViolationException(HttpMessageNotReadableException e,
            HttpServletRequest request) {
        return super.handleConstraintViolationException(e, request);
    }

    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(BindException.class)
    public ApiResponse handleBindException(BindException e, HttpServletRequest request) {
        return super.handleBindException(e, request);
    }

    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ApiResponse handleMethodArgsInValidException(MethodArgumentNotValidException e,
            HttpServletRequest request) {
        return super.handleMethodArgsInValidException(e, request);
    }

    /**
     * 处理自定义异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(BaseException.class)
    public ApiResponse handleBusinessException(BaseException e, HttpServletRequest request) {
        return super.handleBusinessException(e, request);
    }

    /**
     * 处理 BKSql 语法解析异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({ParseException.class})
    public ApiResponse handleBkSqlParseException(ParseException e, HttpServletRequest request) {
        return super.handleBkSqlParseException(e, request);
    }

    /**
     * 处理 BKSql 词法解析异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({TokenMgrException.class})
    public ApiResponse handleBkSqlTokenMgrException(TokenMgrException e,
            HttpServletRequest request) {
        return super.handleBkSqlTokenMgrException(e, request);
    }

    /**
     * 处理 BkSql 校验异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({FailedOnCheckException.class})
    public ApiResponse handleBkSqlCheckerException(FailedOnCheckException e,
            HttpServletRequest request) {
        return super.handleBkSqlCheckerException(e, request);
    }

    /**
     * 处理 BkSql 解析异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({FailedOnDeParserException.class})
    public ApiResponse handleBkSqlDeParserException(FailedOnDeParserException e,
            HttpServletRequest request) {
        return super.handleBkSqlDeParserException(e, request);
    }

    /**
     * 处理本地化异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({MessageLocalizedException.class})
    public ApiResponse handleMessageLocalizedException(MessageLocalizedException e,
            HttpServletRequest request) {
        return super.handleMessageLocalizedException(e, request);
    }

    /**
     * 处理查询异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({QueryDetailException.class})
    protected ApiResponse handleQueryDetailException(QueryDetailException e,
            HttpServletRequest request) {
        return super.handleQueryDetailException(e, request);
    }

    /**
     * 处理权限不足异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({PermissionForbiddenException.class})
    protected ApiResponse handlePermissionForbiddenException(PermissionForbiddenException e,
            HttpServletRequest request) {
        return super.handlePermissionForbiddenException(e, request);
    }

    /**
     * 处理参数无效异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler({ParameterInvalidException.class})
    protected ApiResponse handleParameterInvalidException(ParameterInvalidException e,
            HttpServletRequest request) {
        return super.handleParameterInvalidException(e, request);
    }

    /**
     * 处理流控异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(RequestNotPermitted.class)
    public ApiResponse handleRequestNotPermitted(RequestNotPermitted e,
            HttpServletRequest request) {
        return super.handleRequestNotPermitted(e, request);
    }

    /**
     * 处理运行时异常
     */
    @Override
    @ResponseStatus(HttpStatus.OK)
    @ExceptionHandler(Throwable.class)
    public ApiResponse handleThrowable(Throwable e, HttpServletRequest request) {
        return super.handleThrowable(e, request);
    }


}
