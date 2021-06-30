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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ClientMetrics implements Metrics {

    static final Object segementMaxLock = new Object();//分段锁
    static final Object segementMinLock = new Object();//分段锁
    public volatile boolean isOn = true;// 乐观锁

    private AtomicLong counters = new AtomicLong(); //累加次数
    private AtomicLong records = new AtomicLong(); //总记录数
    private AtomicLong fails = new AtomicLong(); //错误数
    private AtomicLong costs = new AtomicLong(); // 累加耗时
    private long maxcost = 0L; //最大耗时
    private long mincost = 0L; //最小耗时
    private String name; //纬度tag

    /**
     * 计算最大值
     *
     * @param cost cost
     */
    public void onMaxcost(long cost) {
        synchronized (segementMaxLock) {
            if (isOn) {
                if (cost > this.maxcost) {
                    this.maxcost = cost;
                }
            }
        }
    }

    /**
     * 计算最小值
     *
     * @param cost cost
     */
    public void onMincost(long cost) {
        synchronized (segementMinLock) {
            if (isOn) {
                if (cost < this.mincost) {
                    this.mincost = cost;
                }
            }
        }
    }

    /**
     * 次数+1
     */
    public void onCounters() {
        if (isOn) {
            this.counters.incrementAndGet();
        }
    }

    /**
     * 错误数+1
     */
    public void onFails() {
        if (isOn) {
            this.fails.incrementAndGet();
        }
    }

    /**
     * 总耗时
     *
     * @param cost cost
     */
    public void onCosts(long cost) {
        if (isOn) {
            this.costs.addAndGet(cost);
        }
    }

    /**
     * 总记录数
     *
     * @param size size
     */
    public void onRecords(long size) {
        if (isOn) {
            this.records.addAndGet(size);
        }
    }

    /**
     * 清零
     */
    public void clear() {

        this.costs.set(0L);
        this.fails.set(0L);
        this.counters.set(0L);
        this.records.set(0L);
        this.maxcost = 0L;
        this.mincost = 0L;

    }

    public long getCounters() {
        return this.counters.get();
    }

    public long getRecords() {
        return this.records.get();
    }

    public long getFails() {
        return this.fails.get();
    }

    public long getCosts() {
        return this.costs.get();
    }

    public long getMaxcost() {
        return this.maxcost;
    }

    public long getMincost() {
        return this.mincost;
    }

    public float getAvgcost() {
        return this.costs.get() * 100F
                / (((this.counters.get() == 0) ? 1 : this.counters.get()) * 100F);
    }

    public void setCounters(long counters) {
        this.counters.set(counters);
    }

    public void setFails(long fails) {
        this.fails.set(fails);
    }

    public void setCosts(long costs) {
        this.costs.set(costs);
    }

    public void setMaxcost(long maxcost) {
        this.maxcost = maxcost;
    }

    public void setMincost(long mincost) {
        this.mincost = mincost;
    }

    public void setRecords(long size) {
        this.records.set(size);
    }

    /**
     * @return 根据返map
     */
    public Map<String, Number> toMap() {
        Map<String, Number> map = new HashMap<>();
        map.put("counters", this.counters);
        map.put("costs", this.costs);
        map.put("fails", this.fails);
        map.put("maxcost", this.maxcost);
        map.put("mincost", this.mincost);
        map.put("records", this.records);
        map.put("avgcost", this.getAvgcost());

        return map;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
