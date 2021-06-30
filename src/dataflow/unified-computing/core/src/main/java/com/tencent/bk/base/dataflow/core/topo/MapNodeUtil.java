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

package com.tencent.bk.base.dataflow.core.topo;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapNodeUtil {

    private static final String LENGTH = "length";
    private static final String COUNT_FREQ = "count_freq";
    private static final String WAITING_TIME = "waiting_time";
    private static final String PERIOD_UNIT = "period_unit";
    private static final String DELAY = "delay";
    private static final String SESSION_GAP = "session_gap";
    private static final String EXPIRED_TIME = "expired_time";
    private static final String ALLOWED_LATENESS = "allowed_lateness";
    private static final String LATENESS_TIME = "lateness_time";
    private static final String LATENESS_COUNT_FREQ = "lateness_count_freq";
    private static Logger Logger = LoggerFactory.getLogger(MapNodeUtil.class);

    /**
     * 根据窗口参数实例化 WindowConfig
     *
     * @param windowInfo 窗口参数
     * @return WindowConfig实例化
     */
    public static WindowConfig mapWindowConfig(Map<String, Object> windowInfo) {
        WindowConfig windowConfig = new WindowConfig();
        Object windowType = windowInfo.get("type");
        if (windowType != null) {
            windowConfig.setWindowType(ConstantVar.WindowType.valueOf(windowType.toString()));
        }
        windowConfig.setWindowLength(Integer.parseInt(windowInfo.getOrDefault(LENGTH, 0).toString()));
        windowConfig.setCountFreq(Integer.parseInt(windowInfo.getOrDefault(COUNT_FREQ, 0).toString()));
        windowConfig.setWaitingTime(Integer.parseInt(windowInfo.getOrDefault(WAITING_TIME, 0).toString()));

        if (windowInfo.get(PERIOD_UNIT) != null) {
            windowConfig.setPeriodUnit(ConstantVar.PeriodUnit.valueOf(windowInfo.get(PERIOD_UNIT).toString()));
        }

        windowConfig.setDelay(Integer.parseInt(windowInfo.getOrDefault(DELAY, 0).toString()));
        windowConfig.setSessionGap(Integer.parseInt(windowInfo.getOrDefault(SESSION_GAP, 0).toString()));
        windowConfig.setExpiredTime(Integer.parseInt(windowInfo.getOrDefault(EXPIRED_TIME, 0).toString()));
        windowConfig.setAllowedLateness(
                Boolean.parseBoolean(windowInfo.getOrDefault(ALLOWED_LATENESS, "false").toString()));
        windowConfig.setLatenessTime(Integer.parseInt(windowInfo.getOrDefault(LATENESS_TIME, 0).toString()));
        windowConfig.setLatenessCountFreq(
                Integer.parseInt(windowInfo.getOrDefault(LATENESS_COUNT_FREQ, 0).toString()));

        Object segmentObj = windowInfo.get("segment");
        if (segmentObj != null) {
            Map<String, Object> segmentMap = ((Map<String, Object>) segmentObj);
            Segment segment = new Segment();
            segment.setStart(Integer.parseInt(segmentMap.get("start").toString()));
            segment.setEnd(Integer.parseInt(segmentMap.get("end").toString()));
            segment.setUnit(ConstantVar.PeriodUnit.valueOf(segmentMap.get("unit").toString()));
            windowConfig.setSegment(segment);
        }
        return windowConfig;
    }

    /**
     * 构造node的fields
     *
     * @param serializedFields fields
     */
    public static List<NodeField> mapFields(List<Map<String, Object>> serializedFields) {
        List<NodeField> nodeFields = new ArrayList<>();
        for (Map<String, Object> serializedField : serializedFields) {
            NodeField nodeField = new NodeField();
            nodeField.setField(serializedField.get("field").toString());
            nodeField.setType(serializedField.get("type").toString());
            nodeField.setOrigin(serializedField.get("origin").toString());
            Object eventTime = serializedField.get("event_time");
            if (null != eventTime) {
                nodeField.setEventTime(Boolean.parseBoolean(eventTime.toString()));
            }
            Object description = serializedField.get("description");
            if (null != description) {
                nodeField.setDescription(description.toString());
            }
            nodeFields.add(nodeField);
        }
        return nodeFields;
    }


}
