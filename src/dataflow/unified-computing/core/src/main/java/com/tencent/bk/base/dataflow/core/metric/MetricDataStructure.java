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

package com.tencent.bk.base.dataflow.core.metric;

import java.util.HashMap;
import java.util.Map;

/**
 * 用来记录不规范数据的打点
 */
public class MetricDataStructure {

    private Map<String, Long> malformedMetricMap;

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();

        // 记录数据不规范的指标
        Map<String, Object> malformedMap = new HashMap<>();
        for (final Map.Entry entry : malformedMetricMap.entrySet()) {
            malformedMap.put((String) entry.getKey(), new HashMap<String, Object>() {
                {
                    put("cnt", entry.getValue());
                    put("reason", String.format(("[%s]格式不规范的数据"), entry.getKey()));
                }
            });
        }
        map.put("data_malformed", malformedMap);
        return map;
    }

    public void setMalformedMap(Map<String, Long> malformedMetricMap) {
        this.malformedMetricMap = malformedMetricMap;
    }
}
