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

import com.tencent.bk.base.dataflow.jobnavi.util.cron.CronUtil;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import java.util.TimeZone;

public class FixedDependencyParam extends AbstractDependencyParam {

    private final Long windowLength;

    /**
     * construct fixed dependency param
     *
     * @param paramStr nH/nd/nM/nW/nc
     * @param baseTime dependence base time im milliseconds
     * @param timeZone timezone
     */
    public FixedDependencyParam(String paramStr, long baseTime, TimeZone timeZone) {
        /*
                                                      baseTime
                                                         ^
                                                         |
        ----|----------------windowLength----------------|---
            ^                                            ^
        startTime                                     endTime
         */
        if (paramStr != null && !paramStr.isEmpty()) {
            final int periodInt = Integer.parseInt(paramStr.substring(0, paramStr.length() - 1));
            final PeriodUnit periodUnit = PeriodUnit.value(paramStr.charAt(paramStr.length() - 1));
            windowLength = CronUtil.parsePeriodStringToMills(periodInt, periodUnit, baseTime, timeZone);
            endTime = baseTime - baseTime % MILLS_IN_AN_HOUR; //aligned with HH:00:00
            startTime = endTime - windowLength;
        } else {
            throw new IllegalArgumentException("Invalid fixed dependency param:" + paramStr);
        }
    }

    /**
     * construct fixed dependency param
     *
     * @param paramStr nH/nd/nM/nW/nc
     * @param baseTime dependence base time im milliseconds
     * @param timeZone timezone
     * @param parentPeriod parent cycle period
     */
    public FixedDependencyParam(String paramStr, long baseTime, TimeZone timeZone, Period parentPeriod) {
        if (paramStr != null && !paramStr.isEmpty()) {
            final int periodInt = Integer.parseInt(paramStr.substring(0, paramStr.length() - 1));
            final PeriodUnit periodUnit = PeriodUnit.value(paramStr.charAt(paramStr.length() - 1));
            if (periodUnit == PeriodUnit.cycle) {
                /*
                                                                       baseTime
                                                                          ^
                                                                          |
                ----|---------------windowLength(parentCycle*N)-----------|---
                    ^                                                     ^
                startTime                                              endTime
                 */
                windowLength = CronUtil
                        .parsePeriodStringToMills(parentPeriod.getFrequency() * periodInt, parentPeriod.getPeriodUnit(),
                                baseTime, timeZone);
            } else {
                windowLength = CronUtil.parsePeriodStringToMills(periodInt, periodUnit, baseTime, timeZone);
            }
            endTime = baseTime - baseTime % MILLS_IN_AN_HOUR; //aligned with HH:00:00
            startTime = endTime - windowLength;
        } else {
            throw new IllegalArgumentException("Invalid fixed dependency param:" + paramStr);
        }
    }

    public Long getWindowLength() {
        return windowLength;
    }
}



