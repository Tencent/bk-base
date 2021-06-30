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

package com.tencent.bk.base.datalab.queryengine.server.interceptor;

import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.RequestConstants;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.ParameterInvalidException;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.RequestWrapper;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

@Slf4j
@Component("authInterceptor")

public class AuthInterceptor extends HandlerInterceptorAdapter {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse httpServletResponse,
            Object o) throws IOException {
        if (HttpMethod.GET.equals(request.getMethod()) || HttpMethod.DELETE
                .equals(request.getMethod())) {
            return true;
        }
        RequestWrapper requestWrapper = new RequestWrapper(request);
        Map<String, Object> body = JacksonUtil.convertJson2Map(requestWrapper.getBody());
        if (body == null) {
            throw new ParameterInvalidException(ResultCodeEnum.PARAM_IS_BLANK);
        }
        BkAuthContext bkAuthContext = BkAuthContext.builder().build();
        Object bkAppCode =
                body.containsKey(RequestConstants.REQUEST_PARAMS_APP_CODE) ? body.get(
                        RequestConstants.REQUEST_PARAMS_APP_CODE)
                        : body.get(RequestConstants.REQUEST_PARAMS_BK_APP_CODE);
        Object bkAppSecret =
                body.containsKey(RequestConstants.REQUEST_PARAMS_APP_SECRET) ? body.get(
                        RequestConstants.REQUEST_PARAMS_APP_SECRET)
                        : body.get(RequestConstants.REQUEST_PARAMS_BK_APP_SECRET);
        if (bkAppCode == null) {
            throw new ParameterInvalidException(ResultCodeEnum.PARAM_NOT_COMPLETE, "bk_app_code不可为空");
        }
        bkAuthContext.setBkAppCode(bkAppCode.toString());
        bkAuthContext.setBkAppSecret(Optional.ofNullable(bkAppSecret).orElse("").toString());
        Object bkDataAuthenticationMethod = body
                .getOrDefault(RequestConstants.REQUEST_PARAMS_BKDATA_AUTHENTICATION_METHOD, "");
        Object bkDataDataToken = body.getOrDefault(RequestConstants.REQUEST_PARAMS_BKDATA_DATA_TOKEN, "");
        Object bkUserName = body.getOrDefault(RequestConstants.REQUEST_PARAMS_BK_USERNAME, "");
        bkAuthContext.setBkDataAuthenticationMethod(bkDataAuthenticationMethod.toString());
        bkAuthContext.setBkDataDataToken(bkDataDataToken.toString());
        bkAuthContext.setBkUserName(bkUserName.toString());
        BkAuthContextHolder.set(bkAuthContext);
        return true;
    }

    @Override
    public void postHandle(HttpServletRequest httpServletRequest,
            HttpServletResponse httpServletResponse, Object o, ModelAndView modelAndView) {
    }

    @Override
    public void afterCompletion(HttpServletRequest httpServletRequest,
            HttpServletResponse httpServletResponse, Object o, Exception e) {
    }
}

