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

package com.tencent.bk.base.datalab.queryengine.server.util;

import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.QUERY_OTHER_ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum.DML_UPDATE;

import com.beust.jcommander.internal.Lists;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.third.NoteBookOutputDetail;
import com.tencent.bk.base.datalab.queryengine.server.third.NoteBookOutputs;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryTaskContextUtil {

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    static {
        JSON_MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        JSON_MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    /**
     * 渲染同步结果返回
     *
     * @param queryTaskContext 查询上下文
     * @return 渲染后的结果
     */
    public static Map<String, Object> genSyncResponse(QueryTaskContext queryTaskContext) {
        Map<String, Object> result = Maps.newHashMap();
        try {
            Preconditions.checkNotNull(queryTaskContext, "queryTaskContext can not be null");
            result.put(ResponseConstants.LIST,
                    Optional.ofNullable(queryTaskContext.getResultData()).orElse(
                            Lists.newArrayList()));
            result.put(ResponseConstants.TOTALRECORDS, queryTaskContext.getTotalRecords());
            result.put(ResponseConstants.SELECT_FIELDS_ORDER,
                    Optional.ofNullable(queryTaskContext.getColumnOrder())
                            .orElse(Lists.newArrayList()));
            result.put(ResponseConstants.BKSQL_CALL_ELAPSED_TIME, 0);
            result.put(ResponseConstants.DEVICE, queryTaskContext.getPickedClusterType());
            result.put(ResponseConstants.CLUSTER, queryTaskContext.getPickedClusterName());
            result.put(ResponseConstants.SQL, queryTaskContext.getRealSql());
            result.put(ResponseConstants.RESULT_TABLE_SCAN_RANGE, queryTaskContext.getSourceRtRangeMap());
            result.put(ResponseConstants.RESULT_TABLE_IDS, queryTaskContext.getResultTableIdList());
            fillQueryTimeTaken(queryTaskContext, result);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(QUERY_OTHER_ERROR);
        }
        return result;
    }

    /**
     * 渲染异步查询返回
     *
     * @param queryTaskContext 查询上下文
     * @return 渲染后的结果
     */
    public static Map<String, Object> genAsyncResponse(QueryTaskContext queryTaskContext) {
        Map<String, Object> result = Maps.newLinkedHashMap();
        try {
            Preconditions.checkNotNull(queryTaskContext, "queryTaskContext can not be null");
            StatementTypeEnum st = queryTaskContext.getStatementType();
            switch (st) {
                case DDL_CTAS:
                case DML_SELECT:
                    fillQueryResponse(queryTaskContext, result);
                    break;
                case DDL_CREATE:
                case DML_DELETE:
                case DML_UPDATE:
                    fillCreateOrUpdateResponse(queryTaskContext, result);
                    break;
                case SHOW_TABLES:
                    fillShowTablesResponse(queryTaskContext, result);
                    break;
                case SHOW_SQL:
                    fillShowSqlResponse(queryTaskContext, result);
                    break;
                default:
                    throw new QueryDetailException(QUERY_SQL_SYNTAX_ERROR,
                            String.format("当前 sql语法:%s 暂未支持!", st.toString()));
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(QUERY_OTHER_ERROR);
        }
        return result;
    }

    /**
     * 渲染查询结果集
     *
     * @param queryTaskContext 查询上下文
     * @param result 渲染后的返回内容
     */
    private static void fillQueryResponse(QueryTaskContext queryTaskContext,
            Map<String, Object> result) {
        QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
        result.put(ResponseConstants.QUERY_ID, queryTaskInfo.getQueryId());
        result.put(ResponseConstants.TIMETAKEN, getTimeTaken(queryTaskInfo));
        result.put(ResponseConstants.PREFER_STORAGE, queryTaskInfo.getPreferStorage());
        result.put(ResponseConstants.RESULT_SCHEMA, queryTaskContext.getSelectFields());
        result.put(ResponseConstants.STATEMENT_TYPE, queryTaskContext.getStatementType()
                .toString());
        result.put(ResponseConstants.SQL, queryTaskContext.getRealSql());
        result.put(ResponseConstants.CREATED_RESULT_TABLE_ID, queryTaskContext.getCreateTableName());
        result.put(ResponseConstants.RESULT_TABLE_SCAN_RANGE, queryTaskContext.getSourceRtRangeMap());
        result.put(ResponseConstants.RESULT_TABLE_IDS, queryTaskContext.getResultTableIdList());
    }

    /**
     * 渲染纯建表语句返回
     *
     * @param queryTaskContext 查询上下文
     * @param result 渲染后的返回内容
     */
    private static void fillCreateOrUpdateResponse(QueryTaskContext queryTaskContext,
            Map<String, Object> result) {
        QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
        result.put(ResponseConstants.QUERY_ID, queryTaskInfo.getQueryId());
        String stageEndTime = DateUtil.getCurrentDate(CommonConstants.DATETIME_FORMAT);
        int timeTaken = (int) DateUtil.getTimeDiff(queryTaskInfo
                .getQueryStartTime(), stageEndTime, CommonConstants.DATETIME_FORMAT);
        result.put(ResponseConstants.TIMETAKEN, timeTaken);
        result.put(ResponseConstants.STATEMENT_TYPE, queryTaskContext.getStatementType()
                .toString());
        if (DML_UPDATE == queryTaskContext.getStatementType()) {
            result.put(ResponseConstants.TOTALRECORDS, queryTaskContext.getTotalRecords());
        }
        result.put(ResponseConstants.CREATED_RESULT_TABLE_ID, queryTaskContext.getCreateTableName());
    }

    /**
     * 渲染 show create table 语句返回
     *
     * @param queryTaskContext 查询上下文
     * @param result 渲染后的返回内容
     */
    private static void fillShowSqlResponse(QueryTaskContext queryTaskContext,
            Map<String, Object> result) {
        QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
        result.put(ResponseConstants.QUERY_ID, queryTaskInfo.getQueryId());
        result.put(ResponseConstants.TIMETAKEN, getTimeTaken(queryTaskInfo));
        result.put(ResponseConstants.STATEMENT_TYPE, queryTaskContext.getStatementType()
                .toString());
        NoteBookOutputDetail outputDetail = queryTaskContext.getNoteBookOutputDetail();
        if (outputDetail != null) {
            String sql = outputDetail.getSql();
            result.put(ResponseConstants.VALUE, sql);
        }
    }

    /**
     * 渲染 show tables 语句返回
     *
     * @param queryTaskContext 查询上下文
     * @param result 渲染后的返回内容
     */
    private static void fillShowTablesResponse(QueryTaskContext queryTaskContext,
            Map<String, Object> result) {
        QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
        result.put(ResponseConstants.QUERY_ID, queryTaskInfo.getQueryId());
        result.put(ResponseConstants.TIMETAKEN, getTimeTaken(queryTaskInfo));
        result.put(ResponseConstants.STATEMENT_TYPE, queryTaskContext.getStatementType()
                .toString());
        NoteBookOutputs outputs = queryTaskContext.getNoteBookOutputs();
        if (outputs != null) {
            List<Map<String, Object>> resultTables = outputs.getResultTables();
            result.put(ResponseConstants.VALUES, resultTables);
        }
    }

    /**
     * 获取 sql 语句执行耗时
     *
     * @param queryTaskInfo 查询上下文
     * @return 执行耗时
     */
    private static int getTimeTaken(QueryTaskInfo queryTaskInfo) {
        String stageEndTime = DateUtil.getCurrentDate(CommonConstants.DATETIME_FORMAT);
        return (int) DateUtil.getTimeDiff(queryTaskInfo
                .getQueryStartTime(), stageEndTime, CommonConstants.DATETIME_FORMAT);
    }

    /**
     * 渲染查询总耗时
     *
     * @param queryTaskContext 查询上下文
     * @param result 渲染后的返回内容
     */
    private static void fillQueryTimeTaken(QueryTaskContext queryTaskContext,
            Map<String, Object> result) {
        if (CommonConstants.SYNC.equals(queryTaskContext.getQueryTaskInfo()
                .getQueryMethod())) {
            // 各阶段耗时的总时长，作为查询的总时长
            int totalTime = 0;
            Map<String, QueryTaskStage> queryTaskStageMap = queryTaskContext
                    .getQueryTaskStageMap();
            for (Map.Entry<String, QueryTaskStage> entry : queryTaskStageMap.entrySet()) {
                int costTime = entry.getValue()
                        .getStageCostTime();

                totalTime += costTime;
            }
            result.put(ResponseConstants.TIMETAKEN, totalTime / 1000.0f);
        }
    }
}