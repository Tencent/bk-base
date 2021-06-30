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

package com.tencent.bk.base.datahub.databus.connect.druid;

import com.tencent.bk.base.datahub.databus.connect.common.sink.BkSinkConnector;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DruidSinkConnector extends BkSinkConnector {

    private static final Logger log = LoggerFactory.getLogger(DruidSinkConnector.class);
    private DruidSinkConfig config;

    /**
     * 启动connector
     */
    @Override
    public void startConnector() {
        LogUtils.info(log, "kafka connector task name and configProperties: {} and {}", name, configProperties);
        config = new DruidSinkConfig(configProperties);
    }


    /**
     * 设置关联的task类
     *
     * @return connector对应的task类
     */
    @Override
    public Class<? extends Task> taskClass() {
        return DruidSinkTask.class;
    }


    /**
     * 获取connector的配置定义
     *
     * @return connector的配置定义
     */
    @Override
    public ConfigDef config() {
        return DruidSinkConfig.CONFIG_DEF;
    }
}
