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

package com.tencent.bk.base.datalab.queryengine.server.eval;

import static com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants.DEVICE_TYPE_HDFS;

import com.google.common.math.LongMath;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.third.StoreKitApiService;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.math.RoundingMode;
import java.util.Map;
import java.util.Map.Entry;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
@QueryEvaluatorDesc(name = DEVICE_TYPE_HDFS)
public class PrestoQueryEvaluator extends BaseQueryEvaluator {

    private static final long DAY_MILLS = 24 * 60 * 60 * 1000;

    @Override
    public double getRows(QueryTaskContext queryTaskContext) {
        StoreKitApiService storeKitApiService = SpringBeanUtil.getBean(StoreKitApiService.class);
        Map<String, Map<String, String>> sourceRtRangeMap = queryTaskContext.getSourceRtRangeMap();
        long rows = 0;
        for (Entry<String, Map<String, String>> sr : sourceRtRangeMap.entrySet()) {
            long lastRows = storeKitApiService.fetchResultTableRows(sr.getKey());
            long allRows = calculateRows(sr.getValue(), lastRows);
            rows += allRows;
        }
        log.debug("sourceRtRangeMap:{} rows:{}", sourceRtRangeMap, rows);
        return rows;
    }

    /**
     * 获取一定时间范围内的rt数据总量
     *
     * @param dateMap 时间范围 range:[{"start": "2020062801" ,"end": "2020062814"}]
     * @param lastRows 前一天rt的打点上报数据量
     * @return 一定时间范围内的rt数据总量
     */
    private long calculateRows(Map<String, String> dateMap, long lastRows) {
        String beginDate = dateMap.get("start");
        String endDate = dateMap.get("end");
        if (StringUtils.isAnyBlank(beginDate, endDate)) {
            return 0;
        }
        long dayDiff = LongMath.divide(DateUtil.getTimeDiff(beginDate, endDate, "yyyyMMddHH"),
                DAY_MILLS, RoundingMode.CEILING);
        return dayDiff * lastRows;
    }

    @Override
    public double getCpu(QueryTaskContext queryTaskContext) {
        return 0;
    }

    @Override
    public double getIo(QueryTaskContext queryTaskContext) {
        return 0;
    }
}
