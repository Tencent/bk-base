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

import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import org.apache.commons.lang.StringUtils;

public class Period implements Cloneable {

    private TimeZone timezone = TimeZone.getTimeZone("UTC");
    private String cronExpression;
    private String delay;
    private int frequency;
    private PeriodUnit periodUnit;
    private Date firstScheduleTime;

    public TimeZone getTimezone() {
        return timezone;
    }

    /**
     * setPeriod timezone
     *
     * @param timezoneID timezone ID
     */
    public void setTimezone(String timezoneID) {
        if (timezoneID != null) {
            this.timezone = TimeZone.getTimeZone(timezoneID);
        }
    }

    public void setTimezone(TimeZone timezone) {
        this.timezone = timezone;
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    public String getDelay() {
        return delay;
    }

    public void setDelay(String delay) {
        this.delay = delay;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public PeriodUnit getPeriodUnit() {
        return periodUnit;
    }

    public void setPeriodUnit(PeriodUnit periodUnit) {
        this.periodUnit = periodUnit;
    }

    public Date getFirstScheduleTime() {
        return firstScheduleTime;
    }

    public void setFirstScheduleTime(Date firstScheduleTime) {
        this.firstScheduleTime = firstScheduleTime;
    }

    /**
     * parse period from json string
     * @param json
     */
    public void parseJson(Map<?, ?> json) {
        if (json.get("delay") != null) {
            String delayStr = (String) json.get("delay");
            if (StringUtils.isNotEmpty(delayStr)) {
                delay = delayStr;
            } else {
                delay = null;
            }
        }
        if (json.get("cron_expression") != null) {
            String cronExpressionStr = (String) json.get("cron_expression");
            if (StringUtils.isNotEmpty(cronExpressionStr)) {
                cronExpression = cronExpressionStr;
            } else {
                cronExpression = null;
            }
        }
        if (json.get("timezone") != null) {
            setTimezone((String) json.get("timezone"));
        }
        if (json.get("frequency") != null) {
            frequency = (Integer) json.get("frequency");
        }
        if (json.get("period_unit") != null) {
            String periodUnitStr = (String) json.get("period_unit");
            if (StringUtils.isNotEmpty(periodUnitStr)) {
                periodUnit = PeriodUnit.value(periodUnitStr);
            } else {
                periodUnit = null;
            }
        }
        if (json.get("first_schedule_time") != null) {
            if (json.get("first_schedule_time") instanceof Long) {
                firstScheduleTime = new Date((Long) json.get("first_schedule_time"));
            } else {
                firstScheduleTime = new Date(Long.parseLong(json.get("first_schedule_time").toString()));
            }
        }
    }

    public String toJsonString() {
        return JsonUtils.writeValueAsString(this);
    }

    public Map<String, Object> toHTTPJson() {
        Map<String, Object> httpJson = new HashMap<>();
        httpJson.put("timezone", timezone.getID());
        httpJson.put("cron_expression", cronExpression);
        httpJson.put("delay", delay);
        httpJson.put("frequency", frequency);
        httpJson.put("period_unit", periodUnit);
        httpJson.put("first_schedule_time", firstScheduleTime);
        return httpJson;
    }

    @Override
    public Period clone() {
        try {
            Period period = (Period) super.clone();
            period.timezone = timezone != null ? (TimeZone) timezone.clone() : null;
            period.firstScheduleTime = firstScheduleTime != null ? (Date) firstScheduleTime.clone() : null;
            return period;
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }

}


