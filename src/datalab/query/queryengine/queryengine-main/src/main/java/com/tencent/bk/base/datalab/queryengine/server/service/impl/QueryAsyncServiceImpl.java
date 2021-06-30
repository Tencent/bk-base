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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.configure.QueryTaskPoolConfig;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.querydriver.QueryDriver;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryAsyncService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskInfoService;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ThreadUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class QueryAsyncServiceImpl extends BaseQueryServiceImpl implements QueryAsyncService {

    private static final String THREAD_NAME_PATTERN = "Query-Task-Thread-%d";
    private static final int ACTIVE = 1;
    private QueryTaskPoolConfig queryTaskPoolConfig;
    private ExecutorService queryTaskPool;
    private ArrayBlockingQueue<QueryTaskContext> receiveQueryQueue;
    private volatile boolean loopFlag = true;

    @Autowired
    private QueryTaskInfoService queryTaskInfoService;

    /**
     * 构造函数
     *
     * @param queryTaskPoolConfig 异步查询任务队列配置实例
     */
    @Autowired
    public QueryAsyncServiceImpl(QueryTaskPoolConfig queryTaskPoolConfig) {
        this.queryTaskPoolConfig = queryTaskPoolConfig;
        try {
            ThreadFactory queryTaskThreadFactory = new ThreadFactoryBuilder()
                    .setNameFormat(THREAD_NAME_PATTERN)
                    .build();
            this.queryTaskPool = new ThreadPoolExecutor(queryTaskPoolConfig.getCorePoolSize(),
                    queryTaskPoolConfig.getMaxPoolSize(), queryTaskPoolConfig.getKeepAliveTime(),
                    TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(queryTaskPoolConfig.getTaskQueueSize()),
                    queryTaskThreadFactory, new ThreadPoolExecutor.AbortPolicy());
            this.receiveQueryQueue = new ArrayBlockingQueue<>(
                    queryTaskPoolConfig.getReceiveQueueSize());
            latchFetchQueryTask();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> closeResources()));
        }
    }

    @Override
    public void executeQuery(QueryTaskContext queryTaskContext) throws Exception {
        try {
            boolean addResult = receiveQueryQueue
                    .offer(queryTaskContext, queryTaskPoolConfig.getOfferTimeOut(),
                            TimeUnit.MILLISECONDS);
            String queryId = queryTaskContext.getQueryTaskInfo()
                    .getQueryId();
            if (!addResult) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR,
                        MessageLocalizedUtil.getMessage(
                                "Failed to insert task into query-task-pool,queryId[{0}] "
                                        + "currentTaskPoolSize[{1}]",
                                new Object[]{queryId, receiveQueryQueue.size()}),
                        ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, queryId)
                                .build());
            }
        } catch (Exception e) {
            throw e;
        } finally {
            log.debug("Put task into QueryTaskPool queryId={}, currentTaskPoolSize={}",
                    queryTaskContext.getQueryTaskInfo()
                            .getQueryId(), receiveQueryQueue.size());
        }
    }

    @Override
    public void latchFetchQueryTask() {
        Thread fetchThread = new Thread(() -> {
            if (receiveQueryQueue != null) {
                while (loopFlag) {
                    try {
                        final QueryTaskContext queryTask = receiveQueryQueue
                                .poll(queryTaskPoolConfig.getPullTimeOut(), TimeUnit.SECONDS);
                        if (queryTask != null) {
                            QueryTaskContext finalQueryTask = queryTask;
                            queryTaskPool.execute(() -> {
                                LocaleHolder.instance()
                                        .set(finalQueryTask.getLocale());
                                Locale.setDefault(finalQueryTask.getLocale());
                                QueryDriver queryDriver = finalQueryTask.getQueryDriver();
                                try {
                                    queryDriver.query(finalQueryTask);
                                } catch (Exception e) {
                                    log.error(e.getMessage(), e);
                                } finally {
                                    LocaleHolder.instance()
                                            .remove();
                                }
                                refreshQueryTaskInfo(finalQueryTask);
                            });
                        }
                    } catch (InterruptedException e) {
                        log.error("Failed to poll queryTask errorMessage:" + e.getMessage(), e);
                    }
                }
            }
        });
        fetchThread.start();
    }

    /**
     * 更新查询任务信息
     *
     * @param queryTaskContext 查询上下文
     */
    private void refreshQueryTaskInfo(QueryTaskContext queryTaskContext) {
        try {
            int totalTime = 0;
            Map<String, QueryTaskStage> queryTaskStageMap = queryTaskContext
                    .getQueryTaskStageMap();
            for (QueryTaskStage stage : queryTaskStageMap.values()) {
                totalTime += stage.getStageCostTime();
            }
            QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
            queryTaskInfo
                    .setQueryEndTime(DateUtil.getCurrentDate(CommonConstants.DATETIME_FORMAT));
            queryTaskInfo.setCostTime(totalTime);
            String state = queryTaskInfo.getQueryState();
            if (!CommonConstants.FAILED.equalsIgnoreCase(state)) {
                queryTaskInfo.setQueryState(CommonConstants.FINISHED);
            }
            queryTaskInfo.setActive(ACTIVE);
            queryTaskInfo.setTotalRecords(
                    (long) queryTaskContext.getTotalRecords());
            queryTaskInfoService.update(queryTaskInfo);
        } catch (Exception e) {
            log.error(
                    "faild to refresh queryTaskInfo queryId:{} exception:{} error:{} "
                            + "stack_trace:{}",
                    queryTaskContext.getQueryTaskInfo()
                            .getQueryId(), e.getClass()
                            .getName(),
                    e.getMessage(),
                    ExceptionUtils.getStackTrace(e));
        }
    }

    @Override
    public QueryTaskContext getQueryResult(String queryId) throws Exception {
        return null;
    }

    /**
     * 停止处理任务
     */
    private void stopProcess() {
        this.loopFlag = false;
    }

    /**
     * 资源释放
     */
    private void closeResources() {
        stopProcess();
        ThreadUtil.shutdownThreadPool(queryTaskPool);
    }
}
