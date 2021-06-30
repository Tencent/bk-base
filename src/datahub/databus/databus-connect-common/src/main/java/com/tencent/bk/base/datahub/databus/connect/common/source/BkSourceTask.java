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

package com.tencent.bk.base.datahub.databus.connect.common.source;


import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.callback.TaskContextChangeCallback;
import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BkSourceTask extends SourceTask implements TaskContextChangeCallback {

    private static final Logger log = LoggerFactory.getLogger(BkSourceTask.class);

    protected Map<String, String> configProperties;
    protected AtomicBoolean isStop = new AtomicBoolean(false);
    protected String dataId;
    protected boolean isTaskContextChanged = false;
    protected Converter converter = null;
    protected TaskContext ctx = null;
    protected String rtId = "";
    protected String name;
    // 统计信息
    protected int recordCount = 0;
    protected int recordSize = 0;
    protected int failedCount = 0;
    protected long recordCountTotal = 0;
    protected long recordSizeTotal = 0;
    protected long start = 0;


    @Override
    public String version() {
        return "";
    }

    /**
     * 启动task,初始化必要的资源。
     *
     * @param props 任务配置信息
     */
    @Override
    public final void start(Map<String, String> props) {
        configProperties = props;
        name = configProperties.get(BkConfig.CONNECTOR_NAME);
        Thread.currentThread().setName(name);    // 将任务名称设置到当前线程上，便于上报数据
        start = System.currentTimeMillis();
        dataId = configProperties.get(BkConfig.DATA_ID);
        String resultTableId = configProperties.get(BkConfig.RT_ID);
        Map<String, String> connProps = BasicProps.getInstance().getConnectorProps();
        LogUtils.info(log, "adding connector props from cluster config {}", connProps);
        configProperties.putAll(connProps);

        try {
            if (StringUtils.isNoneBlank(resultTableId)) {
                initRtTaskCtx(resultTableId);
            }
            startTask();
        } catch (Exception e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.CONFIG_ERR, log,
                    rtId + " config error, maybe get_rt_info failed!", e);
            Metric.getInstance()
                    .reportEvent(name, Consts.TASK_START_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            throw new ConfigException("bad configuration: " + configProperties, e);
        }
    }


    /**
     * 执行实际拉取数据的逻辑
     *
     * @return 拉取到的数据集
     * @throws InterruptException 异常
     */
    @Override
    public final List<SourceRecord> poll() throws InterruptException {
        if (isStop.get()) {
            return null;
        }

        try {
            return pollData();
        } catch (Exception e) {
            Metric.getInstance()
                    .reportEvent(name, Consts.TASK_RUN_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            throw e;
        }
    }

    /**
     * 停止task
     */
    @Override
    public final void stop() {
        isStop.set(true);
        try {
            stopTask();
        } catch (Exception e) {
            Metric.getInstance()
                    .reportEvent(name, Consts.TASK_STOP_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            throw e;
        }
        long duration = System.currentTimeMillis() - start;
        if (StringUtils.isNotBlank(rtId)) {
            LogUtils.info(log, "processed {} records, {} bytes in {} (ms) for result table {}", recordCountTotal,
                    recordSizeTotal, duration, rtId);
        } else {
            LogUtils.info(log, "processed {} records, {} bytes in {} (ms) for dataid {}", recordCountTotal,
                    recordSizeTotal, duration, dataId);
        }
    }

    /**
     * 启动task
     */
    protected abstract void startTask();

    /**
     * 拉取记录，用于发送到kafka中
     */
    protected abstract List<SourceRecord> pollData();

    /**
     * 停止task
     */
    protected void stopTask() {
        // do nothing
    }

    /**
     * 当source task和rt_id关联的时候，调用此方法，初始化rt相关的资源和配置
     */
    private void initRtTaskCtx(String resultTableId) {
        // 获取rt,通过接口获取rt对应的配置
        rtId = resultTableId;
        Map<String, String> rtProps = HttpUtils.getRtInfo(rtId);
        ctx = new TaskContext(rtProps);
        // 初始化数据解析器
        converter = ConverterFactory.getInstance().createConverter(ctx);
    }

    /**
     * task的rt配置发生变化，在本方法中仅仅记录此变化，如果需要监听配置变化，
     * 需要在子类方法里检查isTaskContextChanged的值，实现自身的逻辑
     */
    public void markBkTaskCtxChanged() {
        if (StringUtils.isNotBlank(rtId)) {
            LogUtils.info(log, "{} task rt config is changed, going to reload rt config!", rtId);
            isTaskContextChanged = true;
        }
    }
}
