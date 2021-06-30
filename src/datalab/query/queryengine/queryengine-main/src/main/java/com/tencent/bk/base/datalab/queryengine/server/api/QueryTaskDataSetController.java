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


import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.CSV;
import static com.tencent.bk.base.datalab.queryengine.server.constant.RequestConstants.REQUEST_PARAMS_BK_USERNAME;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.FALSE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.QUERY_ID;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.SQL;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.TRUE;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.SUCCESS;
import static com.tencent.bk.base.datalab.queryengine.server.filter.ResponseFilter.RESULT;
import static com.tencent.bk.base.datalab.queryengine.server.filter.ResponseFilter.RESULT_CODE;
import static com.tencent.bk.base.datalab.queryengine.server.util.DownLoadUtil.generateSecretKey;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.base.BaseController;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContext;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.exception.PermissionForbiddenException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskDataSet;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskDataSetService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskInfoService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskResultTableService;
import com.tencent.bk.base.datalab.queryengine.server.util.ResultTableUtil;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * 查询结果集 Controller
 */
@RateLimiter(name = "global")
@Slf4j
@RestController
@RequestMapping(value = "/queryengine/dataset")
public class QueryTaskDataSetController extends BaseController {

    public static final String SECRET_KEY = "secret_key";
    public static final String TOTAL_RECORDS = "total_records";

    @Autowired
    private QueryTaskResultTableService queryTaskResultTableService;

    @Autowired
    private QueryTaskDataSetService queryTaskDataSetService;

    @Autowired
    private QueryTaskInfoService queryTaskInfoService;

    /**
     * 根据 queryId 来获取结果集元数据
     *
     * @param queryId 查询 Id
     * @return ApiResponse
     */
    @GetMapping("/{queryId}")
    public ApiResponse<Object> loadByQueryId(@PathVariable String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        QueryTaskDataSet dataSet = queryTaskDataSetService.loadByQueryId(queryId);
        return ApiResponse.success(dataSet);
    }

    /**
     * 获取下载密钥
     *
     * @param queryId 查询 Id
     * @return ApiResponse
     */
    @GetMapping("/download/generate_secret_key")
    public ApiResponse<Object> getSecretKey(@RequestParam(name = "query_id") String queryId,
            @RequestParam(name = "bk_username") String bkUserName) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId),
                        "query_id can not be null or empty");
        Preconditions
                .checkArgument(StringUtils.isNotBlank(bkUserName),
                        "bk_username can not be null or empty");
        BkAuthContextHolder
                .set(BkAuthContext.builder().bkUserName(bkUserName).build());
        final List<QueryTaskResultTable> queryTaskResultTables = queryTaskResultTableService
                .loadByQueryId(queryId);
        String resultTableId = CollectionUtils.isNotEmpty(queryTaskResultTables)
                ? queryTaskResultTables.get(0).getResultTableId() : "";
        ResultTableContextHolder
                .set(ResultTableContext.builder().bizId(NumberUtils.toInt(ResultTableUtil
                        .extractBizId(resultTableId))).build());
        String secretKey = generateSecretKey(queryId, bkUserName);
        QueryTaskDataSet dataSet = queryTaskDataSetService.loadByQueryId(queryId);
        long totalRecords = dataSet.getTotalRecords();
        return ApiResponse
                .success(ImmutableMap.of(SECRET_KEY, secretKey, TOTAL_RECORDS, totalRecords));
    }

    /**
     * 根据密钥来下载结果集
     *
     * @param secretKey 密钥
     */
    @GetMapping("/download/{secretKey}")
    public ResponseEntity<StreamingResponseBody> downLoad(@PathVariable String secretKey,
            @RequestParam(name = "download_format", required = false, defaultValue = CSV)
                    String downloadFormat) {
        Preconditions.checkArgument(StringUtils.isNotBlank(secretKey),
                "secretKey can not be null or empty");
        try {
            String[] downloadElements = queryTaskDataSetService.extractDownloadElements(secretKey);
            String queryId = downloadElements[0];
            String bkUserName = downloadElements[1];
            MDC.clear();
            MDC.put(QUERY_ID, queryId);
            MDC.put(REQUEST_PARAMS_BK_USERNAME, bkUserName);
            queryTaskDataSetService.checkDownloadPermission(queryId, bkUserName);
            QueryTaskInfo queryTaskInfo = queryTaskInfoService.loadByQueryId(queryId);
            MDC.put(SQL, queryTaskInfo.getSqlText());
            MDC.put(TOTAL_RECORDS, Long.toString(queryTaskInfo.getTotalRecords()));
            return queryTaskDataSetService.downloadFile(queryId, downloadFormat);
        } catch (PermissionForbiddenException | QueryDetailException e) {
            MDC.put(RESULT_CODE, e.getCode());
            MDC.put(RESULT, FALSE);
            MDC.put(ERROR, e.getMessage());
            throw e;
        } finally {
            if (MDC.get(RESULT_CODE) == null) {
                MDC.put(RESULT_CODE, SUCCESS.code());
                MDC.put(RESULT, TRUE);
            }
        }
    }
}
