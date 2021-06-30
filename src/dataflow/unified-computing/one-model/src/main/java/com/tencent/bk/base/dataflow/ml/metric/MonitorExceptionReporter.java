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

package com.tencent.bk.base.dataflow.ml.metric;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.core.common.Tools;
import com.tencent.bk.base.dataflow.ml.topology.ModelTopology;
import java.text.SimpleDateFormat;
import java.util.Date;

public class MonitorExceptionReporter implements ExceptionReporter {

    private ModelTopology topology;
    private String module;
    private String component;
    private String time;
    private int monitorLevel;
    private int alarmAttrId;
    private int alarmConfigId;
    private String monitorName;

    public MonitorExceptionReporter(String module, String component, ModelTopology topology) {
        this.topology = topology;
        this.module = module;
        this.component = component;
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        time = dateFormat.format(new Date());
        monitorLevel = 1;
        alarmAttrId = 414;
        alarmConfigId = 2;
        monitorName = "错误码监控";
    }

    @Override
    public void report(String nodeId, String errorCode, String errorMsg, String errorMsgEn) {

    }

    /**
     * 根据提供信息生成特定格式的上报消息
     *
     * @param errorCode 错误码
     * @param errorMsg 错误信息
     * @return 固定格式的错误信息
     */
    public String getMessage(String errorCode, String errorMsg) {
        return Tools.writeValueAsString(ImmutableMap.<String, Object>builder()
                .put("module", module)
                .put("component", component)
                .put("alarm_config_id", alarmConfigId)
                .put("alarm_desc", errorMsg)
                .put("alarm_time", time)
                .put("event_time", time)
                .put("source_id", "")
                .put("monitor_level", monitorLevel)
                .put("alarm_dimension",
                        ImmutableMap.<String, String>builder()
                                .put("topo", topology.getJobId())
                                .put("err_code", errorCode)
                                .build())
                .put("alarm_attr_id", alarmAttrId)
                .put("monitor_name", monitorName)
                .build());
    }
}
