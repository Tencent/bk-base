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

package com.tencent.bk.base.datalab.queryengine.server.querydriver;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.configure.BatchJobPoolConfig;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.BatchJobStateEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.InternalServerException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.third.BatchApiService;
import com.tencent.bk.base.datalab.queryengine.server.third.BatchJob;
import com.tencent.bk.base.datalab.queryengine.server.third.BatchJobConfig;
import com.tencent.bk.base.datalab.queryengine.server.third.BatchJobConfig.InputResultTable;
import com.tencent.bk.base.datalab.queryengine.server.third.BatchJobConfig.OutputResultTable;
import com.tencent.bk.base.datalab.queryengine.server.third.BatchJobStateInfo;
import com.tencent.bk.base.datalab.queryengine.server.util.ErrorMessageItem;
import com.tencent.bk.base.datalab.queryengine.server.util.FillQueryTaskStageUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_BATCH)
public class BatchQueryDriver extends AsyncQueryDriver {

    private static final String START = "start";
    private static final String END = "end";
    private static final String PARTITION_RANGE = "range";
    private static final String PARTITION_LIST = "list";
    private static final String OVERWRITE_MODE = "overwrite";
    private static final String APPEND_MODE = "append";
    private static final String SUCCESS = "success";
    private static final String THREAD_NAME_PATTERN = "Batch-Task-Thread-%d";
    private static final int ACTIVE = 1;
    private static final BatchJobPoolConfig BATCHJOB_POOL_CONFIG = SpringBeanUtil
            .getBean(BatchJobPoolConfig.class);
    private static final String ILLEGAL_PARTITION_RANGE_ERROR =
            "请使用日期闭区间格式，比如：thedate>2021010 and thedate<2021010";

    /**
     * 默认构造函数
     */
    public BatchQueryDriver() {
        super(false, THREAD_NAME_PATTERN, BATCHJOB_POOL_CONFIG.getCorePoolSize(),
                BATCHJOB_POOL_CONFIG.getMaxPoolSize(), BATCHJOB_POOL_CONFIG.getKeepAliveTime(),
                BATCHJOB_POOL_CONFIG.getTaskQueueSize(),
                BATCHJOB_POOL_CONFIG.getReceiveQueueSize());
    }

    @Override
    protected void latchJobStateFetcher() {
        Thread fetchThread = new Thread(() -> {
            if (receiveQueryQueue != null) {
                while (loopFlag) {
                    try {
                        TimeUnit.MILLISECONDS.sleep(POOL_INTERVAL_MILLS);
                        final QueryTaskContext queryTask = receiveQueryQueue
                                .poll(BATCHJOB_POOL_CONFIG.getPullTimeOut(), TimeUnit.SECONDS);
                        if (queryTask != null) {
                            queryTaskPool.execute(() -> {
                                try {
                                    BatchJob batchJob = queryTask.getBatchJob();
                                    BatchApiService dataFlowApiService = SpringBeanUtil
                                            .getBean(BatchApiService.class);
                                    BatchJobStateInfo state = dataFlowApiService
                                            .getOneTimeQueryState(batchJob.getJobId());
                                    if (state != null) {
                                        updateStageByJobStatus(queryTask, state);
                                    } else {
                                        receiveQueryQueue
                                                .offer(queryTask,
                                                        BATCHJOB_POOL_CONFIG.getOfferTimeOut(),
                                                        TimeUnit.MILLISECONDS);
                                    }
                                } catch (Exception e) {
                                    updateQueryDbStage(queryTask, e);
                                }
                            });
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }
            }
        });
        fetchThread.start();
    }

    /**
     * 根据离线任务状态更新查询任务阶段状态
     *
     * @param queryTaskContext 查询上下文
     * @param state 离线任务状态
     * @throws InterruptedException 内部服务调用异常
     */
    private void updateStageByJobStatus(QueryTaskContext queryTaskContext,
            BatchJobStateInfo state)
            throws InterruptedException {
        BatchJobStateEnum status = BatchJobStateEnum
                .valueOf(state.getStatus().toUpperCase(
                        Locale.ENGLISH));
        switch (status) {
            case FINISHED:
            case FAILED_SUCCEEDED:
                processQueryFinished(queryTaskContext);
                break;
            case FAILED:
                processQueryFailed(queryTaskContext, state);
                break;
            case RUNNING:
            case PREPARING:
                receiveQueryQueue
                        .offer(queryTaskContext,
                                BATCHJOB_POOL_CONFIG.getOfferTimeOut(),
                                TimeUnit.MILLISECONDS);
                break;
            default:
                log.error("BatchApi调用异常，未知返回码:{}", state.getStatus());
                throw new InternalServerException(
                        ResultCodeEnum.INTERFACE_BATCHAPI_INVOKE_ERROR);
        }
    }

    /**
     * 任务运行失败处理逻辑
     *
     * @param queryTaskContext 查询上下文
     * @param state 任务状态信息
     */
    private void processQueryFailed(QueryTaskContext queryTaskContext, BatchJobStateInfo state) {
        ErrorMessageItem item =
                new ErrorMessageItem(ResultCodeEnum.INTERFACE_BATCHAPI_INVOKE_ERROR.code(),
                        ResultCodeEnum.INTERFACE_BATCHAPI_INVOKE_ERROR.message(),
                        state.getMessage());
        FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB,
                JacksonUtil.object2Json(item));
    }

    /**
     * 任务运行完成处理逻辑
     *
     * @param queryTaskContext 查询上下文
     */
    private void processQueryFinished(QueryTaskContext queryTaskContext) {
        FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB);
        FillQueryTaskStageUtil
                .initQueryTaskStage(queryTaskContext, CommonConstants.WRITE_CACHE_SEQ, CommonConstants.WRITE_CACHE,
                        System.currentTimeMillis());
        FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.WRITE_CACHE);
    }

    @Override
    public void executeQuery(QueryTaskContext queryTaskContext) throws Exception {
        try {
            initCreateTableName(queryTaskContext);
            BatchJob job = SpringBeanUtil
                    .getBean(BatchApiService.class)
                    .createOneTimeSql(getBatchJobConfig(queryTaskContext));
            log.debug("createOneTimeSql finished jobId:{} ExecuteId:{}", job.getJobId(),
                    job.getExecuteId());
            queryTaskContext.setBatchJob(job);
            boolean addResult = receiveQueryQueue
                    .offer(queryTaskContext, BATCHJOB_POOL_CONFIG.getOfferTimeOut(),
                            TimeUnit.MILLISECONDS);
            if (!addResult) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR, String.format(
                        "Failed to add batchjob to query-task-pool, queryId=%s, "
                                + "currentBatchPoolSize=%d",
                        queryTaskContext.getQueryTaskInfo()
                                .getQueryId(), receiveQueryQueue.size()));
            }
        } catch (QueryDetailException e) {
            QueryDriverUtil.updateQueryStage(queryTaskContext, CommonConstants.QUERY_DB, e);
            throw e;
        } catch (Throwable e) {
            QueryDriverUtil.updateQueryStage(queryTaskContext, CommonConstants.QUERY_DB, e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    /**
     * 获取 BatchJobConfig (封装 batch 任务提交参数配置)
     *
     * @param queryTaskContext 查询上下文
     * @return
     */
    private BatchJobConfig getBatchJobConfig(QueryTaskContext queryTaskContext) {
        //获取batch输出RT
        String outputTbName = queryTaskContext.getCreateTableName();
        BatchJobConfig batchJobConfig = new BatchJobConfig();
        batchJobConfig.setJobId(outputTbName);
        batchJobConfig.setUsername(queryTaskContext.getQueryTaskInfo().getCreatedBy());
        String sql = queryTaskContext.getQueryTaskInfo().getSqlText()
                .replaceAll(BkSqlContants.PATTERN_STORAGE, "");
        batchJobConfig.setProcessingLogic(sql);
        batchJobConfig.setQuerysetParams(Optional
                .ofNullable(queryTaskContext.getQueryTaskInfo()
                        .getProperties())
                .orElse(Maps.newHashMap()));
        batchJobConfig.setOutputs(
                Lists.newArrayList(
                        OutputResultTable.builder().mode(OVERWRITE_MODE).resultTableId(outputTbName)
                                .build()));
        List<InputResultTable> inputRts = Lists.newArrayList();
        Map<String, Map<String, String>> inputRtRangeMap = queryTaskContext.getSourceRtRangeMap();
        for (Entry<String, Map<String, String>> entry : inputRtRangeMap.entrySet()) {
            InputResultTable input = InputResultTable.builder().resultTableId(entry.getKey())
                    .storageType(StorageConstants.DEVICE_TYPE_HDFS).partition(ImmutableMap.of(
                            PARTITION_RANGE, Lists.newArrayList(entry.getValue()))).build();
            List<Map<String, String>> range = (List<Map<String, String>>) input.getPartition()
                    .get(PARTITION_RANGE);
            if (!CollectionUtils.isEmpty(range)) {
                if (StringUtils.isAnyBlank(range.get(0).get(START), range.get(0).get(END))) {
                    throw new QueryDetailException(ResultCodeEnum.PARAM_IS_INVALID,
                            MessageLocalizedUtil.getMessage(
                                    ILLEGAL_PARTITION_RANGE_ERROR));
                }
            }
            inputRts.add(input);
        }
        if (CollectionUtils.isEmpty(inputRts)) {
            throw new QueryDetailException(ResultCodeEnum.PARAM_IS_INVALID,
                    MessageLocalizedUtil.getMessage(
                            ILLEGAL_PARTITION_RANGE_ERROR));
        }
        batchJobConfig.setInputs(inputRts);
        return batchJobConfig;
    }
}