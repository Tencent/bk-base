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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.BaseException;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.util.ErrorMessageItem;
import com.tencent.bk.base.datalab.queryengine.server.util.FillQueryTaskStageUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ThreadUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncQueryDriver extends AbstractQueryDriver {

    protected static final long POOL_INTERVAL_MILLS = 5000L;
    protected ExecutorService queryTaskPool;
    protected ArrayBlockingQueue<QueryTaskContext> receiveQueryQueue;
    protected volatile boolean loopFlag = true;
    protected boolean shouldRegisterTable;

    /**
     * 构造函数
     *
     * @param shouldRegisterTable 是否需要进行表注册
     * @param threadName 线程名称格式
     * @param corePoolSize 核心线程数
     * @param maxPoolSize 最大线程数
     * @param keepAliveTime 空闲时长
     * @param workQueueSize 工作队列长度
     * @param receiveQueueSize 任务队列长度
     */
    public AsyncQueryDriver(boolean shouldRegisterTable, String threadName, int corePoolSize,
            int maxPoolSize,
            long keepAliveTime, int workQueueSize, int receiveQueueSize) {
        this.shouldRegisterTable = shouldRegisterTable;
        ThreadFactory queryTaskThreadFactory = new ThreadFactoryBuilder()
                .setNameFormat(threadName)
                .build();
        this.queryTaskPool = new ThreadPoolExecutor(corePoolSize,
                maxPoolSize, keepAliveTime,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(workQueueSize),
                queryTaskThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        this.receiveQueryQueue = new ArrayBlockingQueue<>(
                receiveQueueSize);
        try {
            latchJobStateFetcher();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            Runtime.getRuntime()
                    .addShutdownHook(new Thread(this::closeResources));
        }
    }

    /**
     * 更新 QUERY_DB 阶段的状态
     *
     * @param queryTaskContext QueryTaskContext
     * @param e Exception
     */
    protected void updateQueryDbStage(QueryTaskContext queryTaskContext, Exception e) {
        QueryTaskStage queryTaskStage = queryTaskContext.getQueryTaskStageMap()
                .get(CommonConstants.QUERY_DB);
        if (!CommonConstants.FAILED.equals(queryTaskStage.getStageStatus())) {
            ResultCodeEnum resultCode = ResultCodeEnum.QUERY_OTHER_ERROR;
            if (e instanceof BaseException) {
                resultCode = ((BaseException) e).getResultCode();
            }
            ErrorMessageItem item = new ErrorMessageItem(resultCode.code(),
                    resultCode.message(), e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB,
                    JacksonUtil.object2Json(item));
        }
        log.error(String.format("handle exception in queryTask thread. %s", e.getMessage()), e);
    }

    @Override
    public void query(QueryTaskContext queryTaskContext) throws Exception {
        FillQueryTaskStageUtil
                .initQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB_SEQ, CommonConstants.CONNECT_DB,
                        System.currentTimeMillis());
        FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB);
        FillQueryTaskStageUtil
                .initQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB_SEQ, CommonConstants.QUERY_DB,
                        System.currentTimeMillis());
        executeQuery(queryTaskContext);
        QueryDriverUtil.addDataSetInfo(queryTaskContext);
        if (shouldRegisterTable) {
            QueryDriverUtil.registerResultTable(queryTaskContext);
        }
    }

    /**
     * 启动任务状态轮询服务
     */
    protected void latchJobStateFetcher() {
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
    public void closeResources() {
        stopProcess();
        Preconditions.checkState(queryTaskPool != null, "queryTaskPool is null");
        ThreadUtil.shutdownThreadPool(queryTaskPool);
    }
}
