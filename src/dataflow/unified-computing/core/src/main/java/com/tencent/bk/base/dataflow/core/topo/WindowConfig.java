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
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.PeriodUnit;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.WindowType;
import java.io.Serializable;

public class WindowConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private WindowType windowType;
    private int countFreq;
    private Segment segment;
    private int waitingTime;
    private int windowLength;
    private int delay;
    private int sessionGap;
    private int expiredTime;
    private PeriodUnit periodUnit = ConstantVar.PeriodUnit.second;

    /**
     * 是否计算延迟数据
     */
    private boolean allowedLateness = false;

    /**
     * 允许计算延迟多久的数据
     */
    private int latenessTime;

    /**
     * 延迟数据的统计输出频率
     */
    private int latenessCountFreq;

    public ConstantVar.WindowType getWindowType() {
        return windowType;
    }

    public void setWindowType(ConstantVar.WindowType windowType) {
        this.windowType = windowType;
    }

    public int getCountFreq() {
        return countFreq;
    }

    public void setCountFreq(int countFreq) {
        this.countFreq = countFreq;
    }

    public Segment getSegment() {
        return segment;
    }

    public void setSegment(Segment segment) {
        this.segment = segment;
    }

    public int getWaitingTime() {
        return waitingTime;
    }

    public void setWaitingTime(int waitingTime) {
        this.waitingTime = waitingTime;
    }

    public int getWindowLength() {
        return windowLength;
    }

    public void setWindowLength(int windowLength) {
        this.windowLength = windowLength;
    }

    public ConstantVar.PeriodUnit getPeriodUnit() {
        return periodUnit;
    }

    public void setPeriodUnit(ConstantVar.PeriodUnit periodUnit) {
        this.periodUnit = periodUnit;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public int getSessionGap() {
        return sessionGap;
    }

    public void setSessionGap(int sessionGap) {
        this.sessionGap = sessionGap;
    }

    public int getExpiredTime() {
        return expiredTime;
    }

    public void setExpiredTime(int expiredTime) {
        this.expiredTime = expiredTime;
    }

    public boolean isAllowedLateness() {
        return allowedLateness;
    }

    public void setAllowedLateness(boolean allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    public int getLatenessTime() {
        return latenessTime;
    }

    public void setLatenessTime(int latenessTime) {
        this.latenessTime = latenessTime;
    }

    public int getLatenessCountFreq() {
        return latenessCountFreq;
    }

    public void setLatenessCountFreq(int latenessCountFreq) {
        this.latenessCountFreq = latenessCountFreq;
    }
}
