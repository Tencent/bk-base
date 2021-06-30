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

package com.tencent.bk.base.datahub.databus.commons.metric.custom;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClientMetricCache implements MetricCache {

    private ClientMetrics metricCache = new ClientMetrics();
    private String tags;
    private final Object lock = new Object();

    /**
     * 设置metric tag
     */
    public void name(String name) {
        metricCache.setName(name);
    }

    /**
     * 计算最大值
     *
     * @param cost cost
     */
    public void onCost(long cost) {
        metricCache.onCosts(cost);
        metricCache.onMaxcost(cost);
        metricCache.onMincost(cost);
    }

    /**
     * 次数+1
     */
    public void count() {
        metricCache.onCounters();
    }

    /**
     * 记录数
     */
    public void onRecords(long size) {
        metricCache.onRecords(size);
    }

    /**
     * 错误数+1
     */
    public void fails() {
        metricCache.onFails();
    }


    /**
     * getcache 通过乐观锁，防止在获取cache的时候丢失数据
     */
    public ClientMetrics swap() {
        ClientMetrics metrics = new ClientMetrics();
        synchronized (lock) {
            // 阻塞写入，取出数据，防止数据丢失
            metricCache.isOn = false;
            // metrics = metricCache.swap();
            metrics.setCosts(metricCache.getCosts());
            metrics.setCounters(metricCache.getCounters());
            metrics.setFails(metricCache.getFails());
            metrics.setMaxcost(metricCache.getMaxcost());
            metrics.setMincost(metricCache.getMincost());
            metrics.setRecords(metricCache.getRecords());
            metrics.setName(metricCache.getName());

            metricCache.clear();
            // 停止阻塞
            metricCache.isOn = true;
        }

        return metrics;
    }

    /**
     * 生成tag ,formatter : tag1,tag1val,tag2,tag2val
     */
    public void tags(String... tags) {
        if (tags.length % 2 != 0) {
            throw new IllegalArgumentException("param error");
        }

        if (tags.length <= 0) {
            this.tags = "";
        }
        this.tags = IntStream.range(0, tags.length / 2)
                .mapToObj(i -> String.format("%s=%s", tags[2 * i], tags[2 * i + 1]))
                .collect(Collectors.joining(","));
    }

    public String getTags() {
        return tags;
    }
}
