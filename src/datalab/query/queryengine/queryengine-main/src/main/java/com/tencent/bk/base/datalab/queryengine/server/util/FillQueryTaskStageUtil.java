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

import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.ASYNC;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.CHECK_QUERY_SYNTAX;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.DATETIME_FORMAT;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.FAILED;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.FINISHED;

import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskInfoService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskStageService;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class FillQueryTaskStageUtil {

    /**
     * 初始化 QueryTaskStage
     *
     * @param queryTaskContext 查询上下文
     * @param stageSeq 查询阶段序号
     * @param stageType 查询阶段类型
     * @param startTime 阶段开始时间
     */
    public static void initQueryTaskStage(QueryTaskContext queryTaskContext, int stageSeq,
            String stageType, long startTime) {
        Map<String, QueryTaskStage> queryTaskStageMap = queryTaskContext.getQueryTaskStageMap();
        QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
        String queryId = queryTaskInfo.getQueryId();
        String stageStartTime = DateUtil
                .getSpecifiedDate(DATETIME_FORMAT, DateUtil.millsUnix2Date(startTime));
        QueryTaskStage queryTaskStage = fillStage(stageSeq, stageType, queryId, stageStartTime);
        // 针对异步查询做处理
        if (StringUtils.isEmpty(queryTaskStage.getCreatedBy())) {
            queryTaskStage.setCreatedBy(queryTaskStageMap.get(CHECK_QUERY_SYNTAX)
                    .getCreatedBy());
        }
        queryTaskStageMap.put(stageType, queryTaskStage);
        if (ASYNC.equals(queryTaskInfo.getQueryMethod())) {
            QueryTaskStageService queryTaskStageService = SpringBeanUtil
                    .getBean(QueryTaskStageService.class);
            queryTaskStageService.insert(queryTaskStage);
        }
    }

    /**
     * 更新 QueryTaskStage
     *
     * @param queryTaskContext 查询上下文
     * @param stageType 查询阶段类型
     */
    public static void updateQueryTaskStage(QueryTaskContext queryTaskContext, String stageType) {
        QueryTaskStage queryTaskStage = queryTaskContext.getQueryTaskStageMap()
                .get(stageType);
        updateStage(queryTaskStage);
        if (ASYNC.equals(queryTaskContext.getQueryTaskInfo()
                .getQueryMethod())) {
            QueryTaskStageService queryTaskStageService = SpringBeanUtil
                    .getBean(QueryTaskStageService.class);
            queryTaskStageService.update(queryTaskStage);
        }
    }

    /**
     * 更新异常时的查询阶段状态
     *
     * @param queryTaskContext 查询上下文
     * @param stageType 查询阶段类型
     * @param errorMessage 异常消息
     */
    public static void updateFailedQueryTaskStage(QueryTaskContext queryTaskContext,
            String stageType, String errorMessage) {
        QueryTaskStage queryTaskStage = queryTaskContext.getQueryTaskStageMap()
                .get(stageType);
        queryTaskStage.setStageStatus(FAILED);
        queryTaskStage.setErrorMessage(errorMessage);
        updateQueryTaskStage(queryTaskContext, stageType);
        if (ASYNC.equals(queryTaskContext.getQueryTaskInfo()
                .getQueryMethod())) {
            QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
            String queryEndTime = DateUtil.getCurrentDate(DATETIME_FORMAT);
            queryTaskInfo.setQueryEndTime(queryEndTime);
            queryTaskInfo.setCostTime((int) DateUtil
                    .getTimeDiff(queryTaskInfo.getQueryStartTime(), queryEndTime, DATETIME_FORMAT));
            queryTaskInfo.setQueryState(FAILED);
            queryTaskInfo.setActive(1);
            queryTaskInfo.setTotalRecords(0L);
            queryTaskInfo.setUpdatedBy(queryTaskInfo.getCreatedBy());
            // 更新info表
            QueryTaskInfoService queryTaskInfoService = SpringBeanUtil
                    .getBean(QueryTaskInfoService.class);
            queryTaskInfoService.update(queryTaskInfo);
        }
    }

    /**
     * 填充查询各阶段时间记录
     *
     * @param stageSeq 查询阶段序号
     * @param stageType 查询阶段类型
     * @param queryId 查询 Id
     * @param stageStartTime 查询阶段开始时间
     * @return QueryTaskStage 实例
     */
    private static QueryTaskStage fillStage(int stageSeq, String stageType, String queryId,
            String stageStartTime) {
        QueryTaskStage queryTaskStage = new QueryTaskStage();
        queryTaskStage.setQueryId(queryId);
        queryTaskStage.setStageSeq(stageSeq);
        queryTaskStage.setStageType(stageType);
        queryTaskStage.setStageStartTime(stageStartTime);
        BkAuthContext authContext = BkAuthContextHolder.get();
        if (authContext != null) {
            if (StringUtils.isNotEmpty(authContext.getBkUserName())) {
                queryTaskStage.setCreatedBy(authContext.getBkUserName());
            } else {
                queryTaskStage.setCreatedBy(authContext.getBkAppCode());
            }
        }
        queryTaskStage.setDescription(queryId + "_" + stageType);
        return queryTaskStage;
    }

    /**
     * 更新查询各阶段时间记录
     *
     * @param queryTaskStage 查询阶段
     */
    private static void updateStage(QueryTaskStage queryTaskStage) {
        String stageEndTime = DateUtil.getCurrentDate(DATETIME_FORMAT);
        queryTaskStage.setStageEndTime(stageEndTime);
        queryTaskStage.setStageCostTime((int) DateUtil
                .getTimeDiff(queryTaskStage.getStageStartTime(), stageEndTime, DATETIME_FORMAT));
        queryTaskStage.setUpdatedBy(queryTaskStage.getCreatedBy());
        if (StringUtils.isEmpty(queryTaskStage.getStageStatus())) {
            queryTaskStage.setStageStatus(FINISHED);
        }
        if (queryTaskStage.getErrorMessage() == null) {
            queryTaskStage.setErrorMessage("");
        }
    }
}