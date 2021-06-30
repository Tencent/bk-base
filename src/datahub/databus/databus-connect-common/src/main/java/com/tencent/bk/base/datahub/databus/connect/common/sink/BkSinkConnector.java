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

package com.tencent.bk.base.datahub.databus.connect.common.sink;

import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class BkSinkConnector extends SinkConnector {

    protected Map<String, String> configProperties;
    protected String name;

    @Override
    public String version() {
        return "";
    }


    /**
     * 启动connector
     *
     * @param props connector配置属性
     */
    @Override
    public final void start(Map<String, String> props) {
        // 检查配置是否正确，启动状态检查
        configProperties = props;
        name = props.get(BkConfig.CONNECTOR_NAME);
        Thread.currentThread().setName(name);    // 将任务名称设置到当前线程上，便于上报数据
        try {
            if (StringUtils.isBlank(configProperties.get(BkConfig.RT_ID))) {
                throw new ConfigException("rt.id is empty, bad config " + props);
            }
            startConnector();
        } catch (Exception e) {
            // 上报启动失败事件
            Metric.getInstance()
                    .reportEvent(name, Consts.CONNECTOR_START_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            throw e;
        }
    }

    /**
     * 停止connector
     */
    @Override
    public final void stop() {
        try {
            stopConnector();
        } catch (Exception e) {
            Metric.getInstance()
                    .reportEvent(name, Consts.CONNECTOR_STOP_FAIL, ExceptionUtils.getStackTrace(e), e.getMessage());
            throw e;
        }
    }

    /**
     * 创建connector下的task的配置,每个task创建一份配置
     *
     * @param maxTasks task数量
     * @return task的配置列表
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        Map<String, String> taskProps = new HashMap<>(configProperties);
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }


    /**
     * 启动connector，需要在子类中实现此方法
     */
    protected abstract void startConnector();


    /**
     * 停止connector
     */
    protected void stopConnector() {
        // 默认啥也不需要做
    }
}
