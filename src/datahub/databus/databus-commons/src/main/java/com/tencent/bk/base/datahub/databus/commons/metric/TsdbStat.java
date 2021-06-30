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

package com.tencent.bk.base.datahub.databus.commons.metric;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TsdbStat {

    private String measurement;
    private String tags;
    private List<String> logMaxFields = new ArrayList<>();
    private AtomicInteger reportCount = new AtomicInteger(0);

    private ConcurrentHashMap<String, AtomicLong> fields = new ConcurrentHashMap<>();

    /**
     * 构造函数
     *
     * @param measurement influxdb中的指标名称，相当于数据库的表名
     * @param tags 标签集合，标签相同的数据打点会在一个周期内持续累积
     * @param maxFields 需要记录最大值的字段列表，逗号分隔
     */
    TsdbStat(String measurement, String tags, String maxFields) {
        this.measurement = measurement;
        this.tags = tags;
        // 初始化那些需要记录字段最大值的数据
        if (StringUtils.isNoneBlank(maxFields)) {
            for (String field : StringUtils.split(maxFields, ",")) {
                logMaxFields.add(field);
                fields.put(Consts.MAX_PREFIX + field, new AtomicLong(0));
            }
        }
    }

    /**
     * 累积打点数据
     *
     * @param stats 当前上报的打点数据
     */
    protected void accumulate(Map<String, Number> stats) {
        reportCount.incrementAndGet();
        for (Map.Entry<String, Number> entry : stats.entrySet()) {
            // 支持多线程并发修改
            fields.putIfAbsent(entry.getKey(), new AtomicLong(0));
            fields.get(entry.getKey()).addAndGet(entry.getValue().longValue());
        }
        // 对于需要记录一个周期内最大值的字段，通过对比值的大小更新数据
        for (String key : logMaxFields) {
            AtomicLong max = fields.get(Consts.MAX_PREFIX + key);
            if (max == null) {
                fields.putIfAbsent(Consts.MAX_PREFIX + key, new AtomicLong(0));
                max = fields.get(Consts.MAX_PREFIX + key);
            }

            // 对比数据大小然后设置值
            if (stats.containsKey(key)) {
                long newVal = stats.get(key).longValue();
                if (newVal > max.longValue()) {
                    // 并发更新max的值时，当前线程可能会失败，别的线程已先更新，此时需对比新的max值和当前需要设置的值
                    while (!max.compareAndSet(max.longValue(), newVal)) {
                        if (max.longValue() > newVal) {
                            // 当前max值已经大于newVal，无需继续
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * 获取累积上报的次数
     *
     * @return 累积的上报次数
     */
    protected int getReportCount() {
        return reportCount.get();
    }

    /**
     * 获取需要记录最大值的字段列表
     *
     * @return 需要记录最大值的字段列表，逗号分隔
     */
    protected String getMaxFields() {
        return StringUtils.join(logMaxFields, ",");
    }

    /**
     * 重置打点统计数据
     */
    protected void reset() {
        // 把fields的值全部重置为0
        fields.forEach((key, value) -> value.set(0));
        reportCount.set(0);
    }

    /**
     * 获取tsdb数据的measurement信息
     *
     * @return measurement
     */
    protected String getMeasurement() {
        return measurement;
    }

    /**
     * 获取tsdb数据的tags信息
     *
     * @return tags
     */
    protected String getTags() {
        return tags;
    }

    /**
     * 将对象转换为符合influxdb格式的一条记录
     *
     * @return 符合influxdb格式的一条记录，字符串。
     */
    @Override
    public String toString() {
        if (fields.size() > 0) {
            List<String> kvStr = new ArrayList<>(fields.size());
            fields.forEach((key, value) -> kvStr.add(key + "=" + value + "i"));
            return String.format("%s,%s %s=%si,%s %s000000", measurement, tags, Consts.REPORT_CNT, getReportCount(),
                    StringUtils.join(kvStr, ","), System.currentTimeMillis());
        } else {
            return String.format("%s,%s %s=%si %s000000", measurement, tags, Consts.REPORT_CNT, getReportCount(),
                    System.currentTimeMillis());
        }
    }
}
