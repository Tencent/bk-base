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

import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.DATA;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.MESSAGE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.RESULT;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.STATUS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_INITIAL_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MULTIPLIER;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.INTERFACE_BATCHAPI_INVOKE_ERROR;

import com.google.common.base.Preconditions;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.util.HttpUtil;
import java.text.MessageFormat;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class BatchApiServiceImpl implements BatchApiService {

    public static final String BATCH_JOB_PROP_JOB_ID = "job_id";
    public static final String BATCH_JOB_PROP_EXECUTE_ID = "execute_id";

    @Autowired
    private BatchApiConfig batchApiConfig;

    @Override
    public BatchJob createOneTimeSql(BatchJobConfig jobConfig) {
        Map<String, Object> respond;
        String url;
        Map<String, Object> params;
        url = batchApiConfig.getCreateOneTimeSqlUrl();
        params = JacksonUtil.convertJson2Map(JacksonUtil.object2Json(jobConfig));
        try {
            JsonNode response = HttpUtil.httpPost(url, params);
            if (response == null) {
                log.error("调用BatchApi失败,接口无返回 url:{} params:{}", url,
                        params);
                throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR);
            }
            respond = JacksonUtil.convertJson2Map(response.getObject()
                    .toString());
            boolean result = (boolean) respond.get(RESULT);
            if (result) {
                Map<String, Object> dataMap = (Map<String, Object>) respond.get(DATA);
                return BatchJob.builder().jobId((String) dataMap.get(BATCH_JOB_PROP_JOB_ID))
                        .executeId((String) dataMap.get(BATCH_JOB_PROP_EXECUTE_ID)).build();
            } else {
                String message = (String) respond.get(MESSAGE);
                log.error("调用BatchApi失败 url:{} params:{} respond:{}", url,
                        params, respond);
                throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR,
                        message);
            }
        } catch (Exception e) {
            log.error("调用BatchApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR, e.getMessage());
        }
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = 10,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public BatchJobStateInfo getOneTimeQueryState(String jobId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(jobId),
                "jobId can not be null or empty.");
        Map<String, Object> respond;
        String url = MessageFormat
                .format(batchApiConfig.getGetOneTimeQueryStateUrl(), jobId);
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response == null) {
                log.error("调用BatchApi失败,接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR);
            }
            respond = JacksonUtil.convertJson2Map(response.getObject()
                    .toString());
            boolean result = (boolean) respond.get(RESULT);
            if (result) {
                Map<String, Object> dataMap = (Map<String, Object>) respond.get(DATA);
                return BatchJobStateInfo.builder().status((String) dataMap.get(STATUS))
                        .message(
                                (String) dataMap.get(MESSAGE)).build();
            } else {
                String message = (String) respond.get(MESSAGE);
                log.error("调用BatchApi失败, url:{}, respond:{}", url, respond);
                throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR, message);
            }
        } catch (Exception e) {
            log.error("调用BatchApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR, e.getMessage());
        }
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = 10,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public String deleteJobConfig(String jobId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(jobId),
                "jobId can not be null or empty.");
        Map<String, Object> respond;
        String url = MessageFormat
                .format(batchApiConfig.getDeleteJobConfigUrl(), jobId);
        String deleteBody = "{\"queryset_delete\": true}";
        try {
            JsonNode response = HttpUtil.httpDelete(url, deleteBody);
            if (response == null) {
                log.error("调用BatchApi失败,接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR);
            }
            respond = JacksonUtil.convertJson2Map(response.getObject()
                    .toString());
            return (String) respond.get("code");
        } catch (Exception e) {
            log.error("调用BatchApi失败 url:{}  错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_BATCHAPI_INVOKE_ERROR, e.getMessage());
        }
    }
}
