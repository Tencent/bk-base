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

import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.ASYNC;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.DATETIME_FORMAT;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.FAILED;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.FINISHED;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.QUERYID_PREFIX;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.RUNNING;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.STAGES;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.INFO;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.MESSAGE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.RESULTS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.STATE;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.STATUS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.TIMETAKEN;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.TIME_TAKEN;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.TOTALRECORDS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants.TOTAL_PAGE;
import static com.tencent.bk.base.datalab.queryengine.server.util.QueryTaskContextUtil.genAsyncResponse;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.tencent.bk.base.datalab.queryengine.common.numeric.SimpleIdGenerator;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.base.ApiResponse;
import com.tencent.bk.base.datalab.queryengine.server.base.BaseController;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContext;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskDataSet;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryProxyService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskDataSetService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskInfoService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskResultTableService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskStageService;
import com.tencent.bk.base.datalab.queryengine.server.util.DataLakeUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ResultTableUtil;
import com.tencent.bk.base.datalab.queryengine.server.vo.QueryInfoListVo;
import com.tencent.bk.base.datalab.queryengine.server.vo.QueryTaskVo;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 异步查询 Controller
 */
@RateLimiter(name = "global")
@Slf4j
@RestController
@RequestMapping(value = "/queryengine/query_async", produces =
        MediaType.APPLICATION_JSON_UTF8_VALUE,
        consumes = MediaType.APPLICATION_JSON_UTF8_VALUE)
public class QueryTaskAsyncController extends BaseController {

    @Autowired
    private SimpleIdGenerator simpleIdGenerator;

    @Autowired
    private QueryTaskInfoService queryTaskInfoService;

    @Autowired
    private QueryTaskResultTableService queryTaskResultTableService;

    @Autowired
    private QueryTaskStageService queryTaskStageService;

    @Autowired
    private QueryProxyService queryProxyService;

    @Autowired
    private QueryTaskDataSetService queryTaskDataSetService;

    /**
     * 提交异步 SQL 作业
     *
     * @param queryTaskVo 查询请求体
     * @return queryId
     */
    @PostMapping(value = "/")
    public ApiResponse<Object> createQueryAsync(@RequestBody @Valid QueryTaskVo queryTaskVo)
            throws Exception {
        String stageStartTime = DateUtil.getCurrentDate(DATETIME_FORMAT);
        QueryTaskInfo queryTaskInfo = new QueryTaskInfo();
        queryTaskInfo.setPreferStorage(queryTaskVo.getPreferStorage());
        queryTaskInfo.setSqlText(queryTaskVo.getSql());
        queryTaskInfo.setConvertedSqlText(StringUtils.EMPTY);
        queryTaskInfo.setQueryId(QUERYID_PREFIX + simpleIdGenerator.nextId());
        queryTaskInfo.setProperties(queryTaskVo.getProperties());
        queryTaskInfo.setQueryStartTime(stageStartTime);
        queryTaskInfo.setQueryMethod(ASYNC);
        queryTaskInfo.setDescription(ASYNC);
        BkAuthContext authContext = BkAuthContextHolder.get();
        if (StringUtils.isNotEmpty(authContext.getBkUserName())) {
            queryTaskInfo.setCreatedBy(authContext.getBkUserName());
        } else {
            queryTaskInfo.setCreatedBy(authContext.getBkAppCode());
        }
        QueryTaskContext queryTaskContext = new QueryTaskContext();
        queryTaskContext.setQueryTaskInfo(queryTaskInfo);
        queryTaskContext.setQueryTaskStageMap(new LinkedHashMap<>(16));
        queryTaskContext.setLocale(LocaleHolder.instance()
                .get());
        queryProxyService.doProxy(queryTaskContext);
        Map<String, Object> fillResult = genAsyncResponse(queryTaskContext);
        return ApiResponse.success(fillResult);
    }

    /**
     * 取消异步 SQL 作业
     *
     * @param queryId 查询 id
     * @return ApiResponse
     */
    @GetMapping(value = "/cancel/{queryId}")
    public ApiResponse<Object> cancelQuery(@PathVariable String queryId) {
        Preconditions.checkArgument(StringUtils.isNotBlank(queryId),
                "Query id can not be null or empty");
        return ApiResponse.success(queryId);
    }

    /**
     * 查询异步 SQL 作业详情
     *
     * @param queryId 查询 id
     * @return ApiResponse
     */
    @GetMapping(value = "/info/{queryId}")
    public ApiResponse<Object> getQueryInfo(@PathVariable String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        QueryTaskInfo queryTaskInfo = queryTaskInfoService.loadByQueryId(queryId);
        return ApiResponse.success(queryTaskInfo);
    }

    /**
     * 查询异步 SQL 作业详情列表
     *
     * @param queryInfoListVo 查询 id 列表和分页信息
     * @return ApiResponse
     */
    @PostMapping(value = "/info_list")
    public ApiResponse<Object> getQueryInfoList(@RequestBody QueryInfoListVo queryInfoListVo) {
        List<QueryTaskInfo> queryTaskInfos = queryTaskInfoService
                .loadByQueryIds(queryInfoListVo.getQueryIdList());
        return ApiResponse.success(queryTaskInfos);
    }

    /**
     * 查询异步 SQL 作业详情列表
     *
     * @param queryInfoListVo 查询 id 列表和分页信息
     * @return ApiResponse
     */
    @PostMapping(value = "/info_page_list")
    public ApiResponse<Object> getQueryInfoPageList(@RequestBody QueryInfoListVo queryInfoListVo) {
        List<QueryTaskInfo> queryTaskInfos = queryTaskInfoService
                .loadByQueryIds(queryInfoListVo.getQueryIdList(),
                        queryInfoListVo.getPage(), queryInfoListVo.getPageSize());
        int count = queryTaskInfoService.infoPageListCount(queryInfoListVo.getQueryIdList());
        // 向上取整
        int totalPage = (count + queryInfoListVo.getPageSize() - 1) / queryInfoListVo.getPageSize();
        Map<String, Object> resultMap = new HashMap<>(16);
        resultMap.put(TOTAL_PAGE, totalPage);
        resultMap.put(RESULTS, queryTaskInfos);
        return ApiResponse.success(resultMap);
    }

    /**
     * 查询异步 SQL 作业列表
     *
     * @return ApiResponse
     */
    @GetMapping(value = "/loadAll")
    public ApiResponse<Object> getList() {
        List<QueryTaskInfo> queryTaskInfoList = queryTaskInfoService.loadAll();
        return ApiResponse.success(queryTaskInfoList);
    }

    /**
     * 查询异步 SQL 作业轨迹
     *
     * @param queryId 查询 id
     * @return ApiResponse
     */
    @GetMapping(value = "/stage/{queryId}")
    public ApiResponse<Object> getQueryStage(@PathVariable String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        final List<QueryTaskResultTable> queryTaskResultTables = queryTaskResultTableService
                .loadByQueryId(queryId);
        final QueryTaskInfo queryTaskInfo = queryTaskInfoService.loadByQueryId(queryId);
        cacheUserAndResultTable(queryTaskInfo, queryTaskResultTables);
        List<QueryTaskStage> queryTaskStageList = queryTaskStageService.loadByQueryId(queryId);
        return ApiResponse.success(queryTaskStageList);
    }

    /**
     * 查询异步 SQL 任务执行状态
     *
     * @param queryId 查询 id
     * @return ApiResponse
     */
    @GetMapping(value = "/state/{queryId}")
    public ApiResponse<Object> getQueryState(@PathVariable String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        final QueryTaskInfo queryTaskInfo = queryTaskInfoService.loadByQueryId(queryId);
        final List<QueryTaskResultTable> queryTaskResultTables = queryTaskResultTableService
                .loadByQueryId(queryId);
        cacheUserAndResultTable(queryTaskInfo, queryTaskResultTables);
        List<QueryTaskStage> stageList = queryTaskStageService.loadByQueryId(queryId);
        String state = RUNNING;
        String errorMsg = "";
        final QueryTaskStage lastStage = stageList.get(stageList.size() - 1);
        boolean isFailed = FAILED.equalsIgnoreCase(queryTaskInfo.getQueryState())
                || FAILED.equalsIgnoreCase(lastStage
                .getStageStatus());
        if (isFailed) {
            state = FAILED;
            errorMsg = lastStage.getErrorMessage();
        } else {
            boolean isFinished = STAGES == stageList.size() && FINISHED
                    .equalsIgnoreCase(lastStage
                            .getStageStatus());
            if (isFinished) {
                state = FINISHED;
            }
        }
        ApiResponse<Object> response = ApiResponse
                .success(ImmutableMap.of(STATE, state, MESSAGE, errorMsg));
        return response;
    }

    /**
     * 删除异步 SQL 作业历史
     *
     * @param queryId 查询 id
     * @return ApiResponse
     */
    @DeleteMapping(value = "/{queryId}")
    public ApiResponse<Object> deleteQuery(@PathVariable String queryId) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        int result = queryTaskInfoService.deleteByQueryId(queryId);
        queryTaskStageService.deleteByQueryId(queryId);
        if (result > 0) {
            return ApiResponse.success();
        } else {
            return ApiResponse.error(ResultCodeEnum.DATA_NOT_FOUND);
        }
    }

    /**
     * 查询结果集
     *
     * @return ApiResponse
     */
    @GetMapping(value = "/result/{queryId}")
    public ApiResponse<Object> getQueryResult(@PathVariable String queryId,
            @RequestParam int size) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(queryId), "queryId can not be null or empty");
        Preconditions
                .checkArgument(size >= 0, "size can not be negative");
        QueryTaskInfo queryTaskInfo = queryTaskInfoService.loadByQueryId(queryId);
        List<QueryTaskStage> queryTaskStageList = queryTaskStageService.loadByQueryId(queryId);
        // 判断查询失败直接返回
        boolean isFailed = FAILED.equals(queryTaskInfo.getQueryState()) || FAILED
                .equals(queryTaskStageList.get(queryTaskStageList.size() - 1).getStageStatus());
        if (isFailed) {
            return ApiResponse.error(MessageLocalizedUtil.getMessage("查询失败，请查询执行阶段详情"));
        }
        Map<String, Object> records = new HashMap<>(16);
        // 判断查询是否已完成
        boolean isFinished = queryTaskStageList.size() == STAGES && FINISHED
                .equals(queryTaskStageList.get(STAGES - 1)
                        .getStageStatus());
        if (isFinished) {
            // 获取query_id对应的结果集存储集群信息
            QueryTaskDataSet dataSet = queryTaskDataSetService.loadByQueryId(queryId);
            String resultTableId = dataSet.getResultTableId();
            records = DataLakeUtil.readFromDataLake(resultTableId, size);
            Map<String, Integer> timeTaken = new LinkedHashMap<>(16);
            for (QueryTaskStage queryTaskStage : queryTaskStageList) {
                int costTime = queryTaskStage.getStageCostTime();
                timeTaken.put(queryTaskStage.getStageType(), costTime);
            }
            timeTaken.put(TIMETAKEN, queryTaskInfo.getCostTime());
            records.put(TIMETAKEN, timeTaken);
            records.put(TIME_TAKEN, queryTaskInfo.getCostTime());
            records.put(TOTALRECORDS, dataSet.getTotalRecords());
            records.put(STATUS, FINISHED);
            return ApiResponse.success(records);
        } else {
            records.put(STATUS, RUNNING);
            records.put(INFO, MessageLocalizedUtil.getMessage("查询任务还在执行中"));
            return ApiResponse.success(records);
        }
    }

    /**
     * 缓存用户名和结果表信息
     *
     * @param queryTaskInfo 查询任务信息
     * @param queryTaskResultTables 查询结果表信息
     */
    private void cacheUserAndResultTable(QueryTaskInfo queryTaskInfo,
            List<QueryTaskResultTable> queryTaskResultTables) {
        String user = queryTaskInfo.getCreatedBy();
        String resultTableId = CollectionUtils.isNotEmpty(queryTaskResultTables)
                ? queryTaskResultTables.get(0).getResultTableId() : "";
        BkAuthContextHolder
                .set(BkAuthContext.builder().bkUserName(user).build());
        ResultTableContextHolder
                .set(ResultTableContext.builder().bizId(NumberUtils.toInt(ResultTableUtil
                        .extractBizId(resultTableId))).build());
    }
}
