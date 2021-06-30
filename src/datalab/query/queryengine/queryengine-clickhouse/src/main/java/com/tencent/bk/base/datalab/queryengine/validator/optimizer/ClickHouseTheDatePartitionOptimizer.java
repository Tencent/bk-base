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

import static com.tencent.bk.base.datalab.bksql.constant.Constants.PATTERN_OF_YYYYMMDD;
import static com.tencent.bk.base.datalab.bksql.constant.Constants.PATTERN_OF_YYYY_MM_DD;

import com.tencent.bk.base.datalab.bksql.validator.optimizer.AbstractTheDatePartitionOptimizer;
import com.tencent.bk.base.datalab.queryengine.constants.ClickHouseConstants;
import java.text.ParseException;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

public class ClickHouseTheDatePartitionOptimizer extends AbstractTheDatePartitionOptimizer {

    private static final String START_TIME_OF_DAY = "00:00:00";
    private static final String END_TIME_OF_DAY = "23:59:59";

    public ClickHouseTheDatePartitionOptimizer() {
        super(ClickHouseConstants.DOUBLE_UNDERLINE_TIME, CHAR_STRING);
    }

    @Override
    protected String getPartitionStartValue(String value) {
        return getPartitionValue(value, START_TIME_OF_DAY);
    }

    @Override
    protected String getPartitionEndValue(String value) {
        return getPartitionValue(value, END_TIME_OF_DAY);
    }

    private String getPartitionValue(String value, String endTimeOfDay) {
        String partitionValue = "";
        try {
            String newDate = DateFormatUtils
                    .format(DateUtils.parseDate(value, PATTERN_OF_YYYYMMDD),
                            PATTERN_OF_YYYY_MM_DD);
            partitionValue = newDate + " " + endTimeOfDay;
        } catch (ParseException e) {
            fail("illegal.thedate.format");
        }
        return partitionValue;
    }
}
