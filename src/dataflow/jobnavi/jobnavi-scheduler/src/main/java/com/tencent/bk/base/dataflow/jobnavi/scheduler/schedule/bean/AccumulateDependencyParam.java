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
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUtil;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class AccumulateDependencyParam extends AbstractDependencyParam {

    /**
     * construct accumulate dependency param
     *
     * @param paramStr $accumulate_start_time:$accumulate_cycle:$begin~$end
     * @param baseTime dependence base time im milliseconds
     * @param timeZone timezone
     */
    public AccumulateDependencyParam(String paramStr, long baseTime, TimeZone timeZone) {
        if (paramStr != null && !paramStr.isEmpty()) {
            String[] params = paramStr.split(DependencyConstants.DEPENDENCY_ACCUMULATE_PARAM_SPLIT);
            if (params.length != 3) {
                throw new IllegalArgumentException("Invalid accumulate dependency param:" + paramStr);
            }
            try {
                /*
                                                                                                baseTime
                                                                                                   ^
                                                                   |--------accumulateRange--------|
                ---------|------------accumulateCycle*N------------|-------------accumulateCycle-------------|
                         ^                                         ^                                         ^
                accumulateStartTime                     accumulateStartBaseTime                    accumulateEndBaseTime
                 */
                long accumulateStartTime = Long.parseLong(params[0]);
                accumulateStartTime = accumulateStartTime
                        - accumulateStartTime % MILLS_IN_AN_HOUR; //aligned with HH:00:00.000
                final PeriodUnit periodUnit = PeriodUnit.value(params[1].charAt(params[1].length() - 1));
                final int periodInt = Integer.parseInt(params[1].substring(0, params[1].length() - 1));
                if (periodInt <= 0) {
                    throw new Exception("Invalid accumulate cycle period int:" + periodInt);
                }
                baseTime = baseTime - baseTime
                        % MILLS_IN_AN_HOUR; //aligned with HH:00:00, base time is not included in data time range
                long accumulateStartBaseTime = accumulateStartTime;
                long accumulateEndBaseTime;
                //calculate accumulate start base time base on accumulate start time
                if (periodUnit == PeriodUnit.month) {
                    //plus/minus periodInt*N months to find the first time less than baseTime
                    DateTime accumulateStartBaseDateTime = new DateTime(accumulateStartTime,
                            DateTimeZone.forTimeZone(timeZone));
                    if (accumulateStartTime < baseTime) {
                        while (accumulateStartBaseDateTime.plusMonths(periodInt).getMillis() < baseTime) {
                            accumulateStartBaseDateTime = accumulateStartBaseDateTime.plusMonths(periodInt);
                        }
                    } else {
                        while (accumulateStartBaseDateTime.getMillis() >= baseTime) {
                            accumulateStartBaseDateTime = accumulateStartBaseDateTime.minusMonths(periodInt);
                        }
                    }
                    accumulateStartBaseTime = accumulateStartBaseDateTime.getMillis();
                    accumulateEndBaseTime = accumulateStartBaseDateTime.plusMonths(periodInt).getMillis();
                } else {
                    //plus/minus N cycles to find the first time less than baseTime
                    long accumulateCycle = CronUtil.parsePeriodStringToMills(periodInt, periodUnit);
                    if (accumulateStartTime < baseTime) {
                        while (accumulateStartBaseTime + accumulateCycle < baseTime) {
                            accumulateStartBaseTime += accumulateCycle;
                        }
                    } else {
                        while (accumulateStartBaseTime >= baseTime) {
                            accumulateStartBaseTime -= accumulateCycle;
                        }
                    }
                    accumulateEndBaseTime = accumulateStartBaseTime + accumulateCycle;
                }
                String[] rangeStr = params[2].split(DependencyConstants.DEPENDENCY_PARAM_RANGE_SPILT);
                if (rangeStr.length != 2) {
                    throw new Exception("Invalid accumulate range param:" + params[2]);
                }
                Long[] accumulateRange = PeriodUtil
                        .getTimeMillsRangeByPeriodStr(rangeStr[0], rangeStr[1], timeZone, accumulateStartBaseTime,
                                accumulateEndBaseTime);
                startTime = accumulateRange[0];
                endTime = Math.min(accumulateRange[1], baseTime);
            } catch (Throwable e) {
                throw new IllegalArgumentException("Invalid accumulate dependency param:" + paramStr, e);
            }
        } else {
            throw new IllegalArgumentException("Invalid fixed dependency param:" + paramStr);
        }
    }

    /**
     * construct accumulate dependency param
     *
     * @param paramStr $accumulate_start_time:$accumulate_cycle:$begin~$end
     * @param baseTime dependence base time im milliseconds
     * @param timeZone timezone
     * @param parentPeriod unused for accumulate dependence
     */
    public AccumulateDependencyParam(String paramStr, long baseTime, TimeZone timeZone, Period parentPeriod) {
        this(paramStr, baseTime, timeZone);
    }
}



