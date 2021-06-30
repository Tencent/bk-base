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

import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.QUESTION_MARK_SEPERATOR;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.DATA;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_INITIAL_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_ATTEMPTS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MAX_DELAY;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.DEFAULT_MULTIPLIER;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.INTERFACE_METAAPI_INVOKE_ERROR;

import com.google.common.base.Preconditions;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.tencent.bk.base.datalab.bksql.table.BlueKingMetaApiFetcher;
import com.tencent.bk.base.datalab.meta.TableMeta;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.util.HttpUtil;
import java.text.MessageFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.yaml.snakeyaml.util.UriEncoder;

@Slf4j
@Service
public class MetaApiServiceImpl implements MetaApiService {

    @Autowired
    private MetaApiConfig metaApiConfig;

    @Override
    @Retryable(
            value = {UnirestException.class, QueryDetailException.class},
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public TableMeta fetchTableFields(String resultTableId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(resultTableId),
                "result_table_id can not be null or empty.");
        String url = metaApiConfig.getFiledsUrlPattern();
        return fetchTableMeta(url, resultTableId);
    }

    @Override
    @Retryable(
            value = {UnirestException.class, QueryDetailException.class},
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public TableMeta fetchTableStorages(String resultTableId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(resultTableId),
                "resultTableId can not be null or empty.");
        String url = metaApiConfig.getStoragesUrlPattern();
        return fetchTableMeta(url, resultTableId);
    }

    @Override
    @Retryable(
            value = UnirestException.class,
            maxAttempts = DEFAULT_MAX_ATTEMPTS,
            backoff = @Backoff(delay = DEFAULT_INITIAL_DELAY, maxDelay = DEFAULT_MAX_DELAY,
                    multiplier = DEFAULT_MULTIPLIER)
    )
    public String fetchCodeArea(String resultTableId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(resultTableId),
                "resultTableId can not be null or empty.");
        try {
            String[] urlParts = metaApiConfig.getTagsUrlPattern()
                    .split("\\" + QUESTION_MARK_SEPERATOR);
            StringBuilder newUrl = new StringBuilder(urlParts[0]);
            for (int i = 1; i < urlParts.length; i++) {
                String encodedParams = UriEncoder.encode(urlParts[i]);
                newUrl.append(QUESTION_MARK_SEPERATOR)
                        .append(encodedParams);
            }
            String url = MessageFormat
                    .format(newUrl.toString(), resultTableId);
            JsonNode response = HttpUtil.httpGet(url);
            if (response != null) {
                com.fasterxml.jackson.databind.JsonNode respNode = JacksonUtil
                        .readTree(response.getObject()
                                .toString());
                com.fasterxml.jackson.databind.JsonNode dataNode = respNode.get(DATA);
                if (dataNode.isArray() && dataNode.size() > 0) {
                    return dataNode.get(0).get("tag_code").asText(StringUtils.EMPTY);
                }
            }
            return StringUtils.EMPTY;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.SYSTEM_INNER_ERROR,
                    e.getMessage());
        }
    }

    /**
     * 获取结果表元信息
     *
     * @param url 接口地址
     * @param resultTableId 结果表 Id
     * @return 结果表元信息
     */
    private TableMeta fetchTableMeta(String url, String resultTableId) {
        try {
            String[] urlParts = url.split("\\" + QUESTION_MARK_SEPERATOR);
            StringBuilder newUrl = new StringBuilder(urlParts[0]);
            for (int i = 1; i < urlParts.length; i++) {
                String encodedParams = UriEncoder.encode(urlParts[i]);
                newUrl.append(QUESTION_MARK_SEPERATOR)
                        .append(encodedParams);
            }
            final BlueKingMetaApiFetcher meta = BlueKingMetaApiFetcher.forUrl(newUrl.toString());
            return meta.fetchMeta(resultTableId);
        } catch (Exception e) {
            log.error("调用MetaApi失败 url:{} 错误原因:{}  异常堆栈:{}", url,
                    ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
            throw new QueryDetailException(INTERFACE_METAAPI_INVOKE_ERROR);
        }
    }
}
