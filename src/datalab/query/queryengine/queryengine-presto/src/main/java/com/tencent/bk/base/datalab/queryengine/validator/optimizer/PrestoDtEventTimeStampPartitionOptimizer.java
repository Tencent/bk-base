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

package com.tencent.bk.base.datalab.queryengine.validator.optimizer;

import static com.tencent.bk.base.datalab.bksql.constant.Constants.ASIA_SHANGHAI;
import static com.tencent.bk.base.datalab.bksql.constant.Constants.BK_TIMEZONE;
import static com.tencent.bk.base.datalab.bksql.constant.Constants.DATE_TIME_FORMATTER_HOUR;
import static com.tencent.bk.base.datalab.bksql.constant.Constants.DTEVENTTIMESTAMP;
import static com.tencent.bk.base.datalab.bksql.constant.Constants.DTPARUNIT;

import com.tencent.bk.base.datalab.bksql.validator.optimizer.AbstractColumnReplacementOptimizer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class PrestoDtEventTimeStampPartitionOptimizer extends AbstractColumnReplacementOptimizer {

    public PrestoDtEventTimeStampPartitionOptimizer() {
        super(DTEVENTTIMESTAMP, DTPARUNIT, true, true);
    }

    @Override
    protected String getReplacementColumnValue(String strVal) {
        ZonedDateTime zonedDateTime = null;
        try {
            zonedDateTime = ZonedDateTime
                    .ofInstant(Instant.ofEpochMilli(Long.parseLong(strVal)),
                            ZoneId.of(System.getProperty(BK_TIMEZONE, ASIA_SHANGHAI)));
        } catch (Exception e) {
            fail("illegal.dteventtimestamp.format");
        }
        if (zonedDateTime == null) {
            return null;
        }
        String partitionVal = zonedDateTime.format(DATE_TIME_FORMATTER_HOUR);
        return partitionVal;
    }
}
