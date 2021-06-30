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

package com.tencent.bk.base.datalab.queryengine.server.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.tencent.bk.base.datalab.queryengine.server.base.BaseModelServiceImpl;
import com.tencent.bk.base.datalab.queryengine.server.configure.DownloadBlacklistConfig;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.PermissionForbiddenException;
import com.tencent.bk.base.datalab.queryengine.server.mapper.QueryTaskDataSetMapper;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskDataSet;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskDataSetService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskResultTableService;
import com.tencent.bk.base.datalab.queryengine.server.third.AuthApiService;
import com.tencent.bk.base.datalab.queryengine.server.util.DataLakeUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.DownLoadUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ResultTableUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

@Slf4j
@Service
public class QueryTaskDataSetServiceImpl extends BaseModelServiceImpl<QueryTaskDataSet> implements
        QueryTaskDataSetService {

    @Autowired
    private QueryTaskDataSetMapper queryTaskDataSetMapper;

    @Autowired
    private QueryTaskResultTableService queryTaskResultTableService;

    @Autowired
    private AuthApiService authApiService;

    public QueryTaskDataSetServiceImpl() {
        this(CommonConstants.TB_DATAQUERY_QUERYTASK_DATASET);
    }

    private QueryTaskDataSetServiceImpl(String tableName) {
        super(tableName);
    }

    @Override
    public QueryTaskDataSet insert(QueryTaskDataSet queryTaskDataSet) {
        Preconditions.checkArgument(queryTaskDataSet != null, "queryTaskDataSet can not be null");
        int rows = queryTaskDataSetMapper.insert(queryTaskDataSet);
        if (rows > 0) {
            return queryTaskDataSetMapper
                    .load(CommonConstants.TB_DATAQUERY_QUERYTASK_DATASET, queryTaskDataSet.getId());
        }
        return null;
    }

    @Override
    public QueryTaskDataSet update(QueryTaskDataSet queryTaskDataSet) {
        Preconditions.checkArgument(queryTaskDataSet != null, "queryTaskDataSet can not be null");
        int rows = queryTaskDataSetMapper.update(queryTaskDataSet);
        if (rows > 0) {
            return queryTaskDataSetMapper
                    .load(CommonConstants.TB_DATAQUERY_QUERYTASK_DATASET, queryTaskDataSet.getId());
        }
        return null;
    }

    @Override
    public QueryTaskDataSet loadByQueryId(String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        return queryTaskDataSetMapper.loadByQueryId(queryId);
    }

    @Override
    public String[] extractDownloadElements(String secretKey) {
        String downloadTxt = new String(Base64.getUrlDecoder()
                .decode(secretKey), StandardCharsets.UTF_8);
        String[] downloadElements = downloadTxt.split(",");
        // 密文应该由三部分组成：queryId、bkUserName、timestamp
        if (downloadElements.length != CommonConstants.ELEMENT_NUMS) {
            throw new PermissionForbiddenException(ResultCodeEnum.PERMISSION_NO_ACCESS);
        }
        String downloadRequestTime = downloadElements[2];
        long timeDifference =
                System.currentTimeMillis() / 1000 - Long.parseLong(downloadRequestTime);
        // 如果获取秘钥和下载链接的时间差超过1分钟，则校验失败
        if (timeDifference > CommonConstants.TIME_DIFFERENCE) {
            throw new PermissionForbiddenException(ResultCodeEnum.PERMISSION_NO_ACCESS);
        }
        return downloadElements;
    }

    @Override
    public void checkDownloadPermission(String queryId, String bkUserName) {
        List<QueryTaskResultTable> resultTableList = queryTaskResultTableService
                .loadByQueryId(queryId);
        List<String> rtIds = new ArrayList<>();
        DownloadBlacklistConfig downloadBlacklistConfig = SpringBeanUtil
                .getBean(DownloadBlacklistConfig.class);
        List<String> bkBizIdList = Splitter.on(CommonConstants.COMMA_SEPERATOR).trimResults()
                .splitToList(downloadBlacklistConfig.getBkBizIdListStr());
        for (QueryTaskResultTable resultTable : resultTableList) {
            String resultTableId = resultTable.getResultTableId();
            String bkBizId = ResultTableUtil.extractBizId(resultTableId);
            if (bkBizIdList.contains(bkBizId)) {
                String message = MessageLocalizedUtil
                        .getMessage("业务 {0} 数据下载已被禁用", new Object[]{bkBizId});
                throw new PermissionForbiddenException(ResultCodeEnum.DOWNLOAD_ACTION_FORBIDDEN,
                        message);
            }
            rtIds.add(resultTableId);
        }
        MDC.put(ResponseConstants.RESULT_TABLES, rtIds.toString());
        BkAuthContext bkAuthContext = BkAuthContext.builder()
                .bkDataAuthenticationMethod(BkDataConstants.AUTH_METHOD_USERNAME)
                .bkUserName(bkUserName)
                .build();
        BkAuthContextHolder.set(bkAuthContext);
        try {
            authApiService.checkAuth(rtIds.toArray(new String[]{}),
                    BkDataConstants.PIZZA_AUTH_ACTION_QUERY_RT);
        } catch (Exception e) {
            throw new PermissionForbiddenException(ResultCodeEnum.PERMISSION_NO_ACCESS);
        } finally {
            BkAuthContextHolder.remove();
        }
    }

    @Override
    public ResponseEntity<StreamingResponseBody> downloadFile(String queryId,
            String downloadFormat) {
        QueryTaskDataSet dataSet = loadByQueryId(queryId);
        StreamingResponseBody responseBody = DataLakeUtil
                .convertRecordsToGivenType(dataSet.getResultTableId(), downloadFormat);
        String fileName = DownLoadUtil.getDownLoadFileName(downloadFormat);
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION,
                        "attachment; filename=\"" + fileName + "\"")
                .header(HttpHeaders.CONTENT_TYPE,
                        DownLoadUtil.DOWNLOAD_FORMAT_TYPE.get(downloadFormat))
                .body(responseBody);
    }
}
