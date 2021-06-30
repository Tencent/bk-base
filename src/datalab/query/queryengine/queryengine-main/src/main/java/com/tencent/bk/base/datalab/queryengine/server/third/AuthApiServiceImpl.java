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

package com.tencent.bk.base.datalab.queryengine.server.third;

import static com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants.INNER_APP_CODE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants.INNER_APP_SECRET;
import static com.tencent.bk.base.datalab.queryengine.server.constant.RequestConstants.ACTION_ID;
import static com.tencent.bk.base.datalab.queryengine.server.constant.RequestConstants.OBJECT_ID;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.DATA;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.RESULT;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_INITIAL_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_ATTEMPTS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MULTIPLIER;

import com.google.common.collect.ImmutableMap;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.PermissionForbiddenException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.util.HttpUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ResultTableUtil;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class AuthApiServiceImpl implements AuthApiService {

    public static final String CHECK_METHOD_TOKEN = "token";
    public static final String CHECK_METHOD_USER = "user";
    public static final String DATA_TOKEN_BK_APP_CODE = "data_token_bk_app_code";
    public static final String DATA_TOKEN = "data_token";
    public static final String CREATED_BY = "created_by";
    public static final String CHECK_APP_CODE = "check_app_code";
    public static final String CHECK_DATA_TOKEN = "check_data_token";
    @Autowired
    private AuthApiConfig authApiConfig;

    /**
     * 获取权限校验结果
     *
     * @param url 权限校验路径
     * @param params 权限校验参数
     * @return 校验结果
     * @throws UnirestException 异常
     */
    public static Object getAuthApiResult(String url, Map<String, Object> params) {
        String errorInfo;
        boolean apiResult = true;
        Exception apiExp = null;
        try {
            JsonNode response = HttpUtil.httpPost(url, params);
            Map<String, Object> respond = JacksonUtil.convertJson2Map(response.getObject()
                    .toString());
            if (respond != null && (boolean) respond.get(RESULT)) {
                Object data = respond.get(DATA);
                if (data instanceof Boolean && !(boolean) data) {
                    String resultTableId = (String) params.get(OBJECT_ID);
                    if (!ResultTableUtil.isExistResultTable(resultTableId)) {
                        errorInfo = MessageLocalizedUtil
                                .getMessage("结果表 {0} 不存在，请确认结果表名是否正确", new Object[]{resultTableId});
                        throw new QueryDetailException(ResultCodeEnum.SQL_ERROR_TABLE_NOT_FOUND,
                                ImmutableMap.of(ERROR, errorInfo));
                    }
                    errorInfo = MessageLocalizedUtil.getMessage("结果表 {0} 无访问权限，请联系小助手排查问题",
                            new Object[]{params.get(OBJECT_ID)});
                    throw new PermissionForbiddenException(ResultCodeEnum.PERMISSION_NO_ACCESS,
                            ImmutableMap.of(ERROR, errorInfo));
                }
                return respond.get(DATA);
            } else {
                throw new QueryDetailException(ResultCodeEnum.INTERFACE_AUTHAPI_INVOKE_ERROR);
            }
        } catch (QueryDetailException | PermissionForbiddenException e) {
            apiResult = false;
            apiExp = e;
            throw e;
        } catch (Exception e) {
            apiResult = false;
            apiExp = e;
            throw new QueryDetailException(ResultCodeEnum.INTERFACE_AUTHAPI_INVOKE_ERROR);
        } finally {
            if (!apiResult) {
                log.error("调用AuthApi失败 url:{} params:{} 错误原因:{}  异常堆栈:{}", url, params,
                        ExceptionUtils.getMessage(apiExp), ExceptionUtils.getStackTrace(apiExp));
            }
        }
    }

    @Override
    public boolean checkAuth(String resultTableId, String authMethod, String bkAppCode,
            String bkAppSecret, String token, String bkUserName) {
        return true;
    }

    @Override
    @Retryable(
            value = {UnirestException.class, QueryDetailException.class},
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public void checkAuth(String[] rtArray, String actionId) throws Exception {
        for (String resultTableId : rtArray) {
            Map<String, Object> params = new HashMap<>(16);
            params.put(ACTION_ID, actionId);
            params.put(OBJECT_ID, resultTableId);
            BkAuthContext authContext = BkAuthContextHolder.get();
            if (authContext.getBkAppCode() != null && authContext.getBkAppSecret() != null) {
                if (authContext.getBkAppCode()
                        .equals(INNER_APP_CODE)
                        && authContext.getBkAppSecret()
                        .equals(INNER_APP_SECRET)) {
                    return;
                }
            }
            String authCheckMethod = authContext.getBkDataAuthenticationMethod();
            if ("".equals(authCheckMethod) || CHECK_METHOD_TOKEN.equals(authCheckMethod)) {
                authTokenCheck(authContext, params);
            }
            if (CHECK_METHOD_USER.equals(authCheckMethod)) {
                String url = MessageFormat
                        .format(authApiConfig.getUserCheckUrl(), authContext.getBkUserName());
                getAuthApiResult(url, params);
            }
        }
    }

    /**
     * token 校验
     *
     * @param authContext 认证授权信息
     * @param params 请求参数
     * @throws Exception 异常
     */
    private void authTokenCheck(BkAuthContext authContext, Map<String, Object> params)
            throws Exception {
        // 兼容旧版 get_data 接口不传 token 的方式，此时会根据 app_code 置换 token 进行填充
        if ("".equals(authContext.getBkDataAuthenticationMethod())) {
            params.put(DATA_TOKEN_BK_APP_CODE, authContext.getBkAppCode());
            Object data = getAuthApiResult(authApiConfig.getExchangeDefaultTokenUrl(), params);
            if (data == null) {
                throw new PermissionForbiddenException(ResultCodeEnum.PERMISSION_NO_ACCESS);
            }
            Map<String, Object> dataTokenMap = (Map<String, Object>) data;
            authContext.setBkDataAuthenticationMethod(CHECK_METHOD_TOKEN);
            authContext.setBkDataDataToken(dataTokenMap.get(DATA_TOKEN)
                    .toString());
            authContext.setBkUserName(dataTokenMap.get(CREATED_BY)
                    .toString());
        }
        params.put(CHECK_APP_CODE, authContext.getBkAppCode());
        params.put(CHECK_DATA_TOKEN, authContext.getBkDataDataToken());
        getAuthApiResult(authApiConfig.getTokenCheckUrl(), params);
    }
}
