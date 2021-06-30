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
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.SQL;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.STATUS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_INITIAL_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_ATTEMPTS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MULTIPLIER;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.INTERFACE_LABAPI_INVOKE_ERROR;

import com.google.common.base.Preconditions;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTableProperty;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.util.HttpUtil;
import java.text.MessageFormat;
import java.util.List;
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

public class DataLabApiServiceImpl implements DataLabApiService {

    public static final String MODELS = "models";
    public static final String RESULT_TABLES = "result_tables";
    public static final String OUTPUT_TYPE = "output_type";
    @Autowired
    private DataLabApiConfig dataLabApiConfig;

    @Override
    public boolean registerResultTable(String noteBookId, String cellId,
            ResultTableProperty resultTable) {
        Preconditions.checkArgument(resultTable != null, "resultTable can not be null or empty.");
        noteBookId = StringUtils.isBlank(noteBookId) ? dataLabApiConfig.getDefaultNoteBookId()
                : noteBookId;
        cellId = StringUtils.isBlank(cellId) ? dataLabApiConfig.getDefaultNoteBookId()
                : cellId;
        Map<String, Object> respond;
        String url;
        Map<String, Object> params;
        url = MessageFormat.format(dataLabApiConfig.getAddResultTableUrl(), noteBookId, cellId);
        params = JacksonUtil.convertJson2Map(JacksonUtil.object2Json(resultTable));
        try {
            JsonNode response = HttpUtil.httpPost(url, params);
            if (response == null) {
                log.error("调用DataLabApi失败, 接口无返回 url:{}, params:{}", url,
                        params);
                throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR);
            }
            respond = JacksonUtil.convertJson2Map(response.getObject()
                    .toString());
            boolean result = (boolean) respond.get(RESULT);
            if (!result) {
                String message = (String) respond.get(MESSAGE);
                log.error("调用DataLabApi失败, url:{}, params:{}, respond:{}", url,
                        params, respond);
                throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR,
                        message);
            }
            return true;
        } catch (Exception e) {
            log.error("调用DataLabApi失败 url:{} 错误原因:{} 异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR,
                    e.getMessage());
        }
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public NoteBookOutputs showOutputs(String noteBookId) {
        Map<String, Object> respond;
        String url = MessageFormat
                .format(dataLabApiConfig.getShowOutputsUrl(), noteBookId);
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response == null) {
                log.error("调用DataLabApi失败,接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR);
            }
            respond = JacksonUtil.convertJson2Map(response.getObject()
                    .toString());
            boolean result = (boolean) respond.get(RESULT);
            if (result) {
                Map<String, Object> dataMap = (Map<String, Object>) respond.get(DATA);
                NoteBookOutputs.NoteBookOutputsBuilder builder = NoteBookOutputs.builder();
                Object modelsObj = dataMap.get(MODELS);
                Object rtObj = dataMap.get(RESULT_TABLES);
                if (modelsObj != null) {
                    builder.models((List<Map<String, Object>>) modelsObj);
                }
                if (rtObj != null) {
                    builder.resultTables((List<Map<String, Object>>) rtObj);
                }
                return builder.build();
            } else {
                String message = (String) respond.get(MESSAGE);
                log.error("调用DataLabApi失败, url:{}, respond:{}", url, respond);
                throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR, message);
            }
        } catch (Exception e) {
            log.error("调用DataLabApi失败 url:{} 错误原因:{} 异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR,
                    e.getMessage());
        }
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public NoteBookOutputDetail showOutputDetail(String noteBookId, String resultTableId) {
        Map<String, Object> respond;
        String url = MessageFormat
                .format(dataLabApiConfig.getShowOutputDetailUrl(), noteBookId, resultTableId);
        try {
            JsonNode response = HttpUtil.httpGet(url);
            if (response == null) {
                log.error("调用DataLabApi失败,接口无返回 url:{}", url);
                throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR);
            }
            respond = JacksonUtil.convertJson2Map(response.getObject()
                    .toString());
            boolean result = (boolean) respond.get(RESULT);
            if (result) {
                Map<String, Object> dataMap = (Map<String, Object>) respond.get(DATA);
                NoteBookOutputDetail.NoteBookOutputDetailBuilder builder = NoteBookOutputDetail
                        .builder();
                Object statusObj = dataMap.get(STATUS);
                Object sqlObj = dataMap.get(SQL);
                Object outputTypeObj = dataMap.get(OUTPUT_TYPE);
                if (statusObj != null) {
                    builder.status((String) statusObj);
                }
                if (sqlObj != null) {
                    builder.sql((String) sqlObj);
                }
                if (outputTypeObj != null) {
                    builder.outputType((String) outputTypeObj);
                }
                return builder.build();
            } else {
                String message = (String) respond.get(MESSAGE);
                log.error("调用DataLabApi失败, url:{}, respond:{}", url, respond);
                throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR, message);
            }
        } catch (Exception e) {
            log.error("调用DataLabApi失败 url:{} 错误原因:{} 异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR,
                    e.getMessage());
        }
    }
}
