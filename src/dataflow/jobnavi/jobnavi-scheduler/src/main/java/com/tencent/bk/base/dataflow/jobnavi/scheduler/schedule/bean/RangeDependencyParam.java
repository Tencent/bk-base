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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.DependencyManager;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import java.util.TimeZone;

public class RangeDependencyParam extends AbstractDependencyParam {

    /**
     * construct range dependency param
     *
     * @param paramStr nH~nH
     * @param baseTime dependence base time im milliseconds
     * @param timeZone timezone
     */
    public RangeDependencyParam(String paramStr, long baseTime, TimeZone timeZone) {
        if (paramStr != null && !paramStr.isEmpty()) {
            try {
                String[] range = paramStr.split(DependencyConstants.DEPENDENCY_PARAM_RANGE_SPILT);
                String startTimePeriodStr = DependencyManager.replaceKeyWord(range[0], baseTime, timeZone);
                String endTimePeriodStr = DependencyManager.replaceKeyWord(range[1], baseTime, timeZone);
                baseTime = baseTime - baseTime % MILLS_IN_AN_HOUR - 1; //aligned with HH:59:59.999
                Long[] timeRange = CronUtil
                        .getTimeRangeByPeriod(startTimePeriodStr, endTimePeriodStr, timeZone, baseTime);
                startTime = timeRange[0];
                endTime = timeRange[1];
            } catch (Throwable e) {
                throw new IllegalArgumentException("Invalid accumulate dependency param:" + paramStr, e);
            }
        } else {
            throw new IllegalArgumentException("Invalid fixed dependency param:" + paramStr);
        }
    }

    /**
     * construct range dependency param
     *
     * @param paramStr nH~nH
     * @param baseTime dependence base time im milliseconds
     * @param timeZone timezone
     * @param parentPeriod unused for range dependence
     */
    public RangeDependencyParam(String paramStr, long baseTime, TimeZone timeZone, Period parentPeriod) {
        this(paramStr, baseTime, timeZone);
    }
}



