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

package com.tencent.bk.base.datahub.databus.connect.source.jdbc;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class TimestampIncrementingOffset {

    public static final String INCREMENTING_FIELD = "incrementing";
    public static final String TIMESTAMP_FIELD = "timestamp";
    public static final String LAST_LIMIT_OFFSET = "lastLimitOffset"; // lastLimitOffset值为LOAD_HISTORY_END_VALUE时,
    // 历史数据已经加载完毕
    public static final String START_TIMESTAMP = "startTimestamp";    // limit ${startTimestamp}, batchSize
    private static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

    private final Long incrementingOffset;
    private final Timestamp timestampOffset;
    private final Long lastLimitOffset;
    private final Long startTimestamp;
    private Long nextLimitOffset;

    /**
     * @param timestampOffset the timestamp offset.
     *         If null, {@link #getTimestampOffset()} will return {@code new Timestamp(0)}.
     * @param incrementingOffset the incrementing offset.
     *         If null, {@link #getIncrementingOffset()} will return -1.
     */
    public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset, Long lastLimitOffset,
            Long startTimestamp, Long nextLimitOffset) {
        this.timestampOffset = timestampOffset;
        this.incrementingOffset = incrementingOffset;
        this.lastLimitOffset = lastLimitOffset;
        this.startTimestamp = startTimestamp;
        this.nextLimitOffset = nextLimitOffset;
    }

    public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset, Long lastLimitOffset,
            Long startTimestamp) {
        this(timestampOffset, incrementingOffset, lastLimitOffset, startTimestamp,
                JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE);
    }

    public TimestampIncrementingOffset(Timestamp timestampOffset, Long incrementingOffset) {
        this(timestampOffset, incrementingOffset, JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE, 0L);
    }

    public long getIncrementingOffset() {
        return incrementingOffset == null ? -1 : incrementingOffset;
    }

    public Timestamp getTimestampOffset() {
        return timestampOffset == null ? new Timestamp(0) : timestampOffset;
    }

    public long getLastLimitOffset() {
        return lastLimitOffset;
    }

    public long getNextLimitOffset() {
        return nextLimitOffset;
    }

    public long getLongStartTimestamp() {
        return startTimestamp;
    }

    public Timestamp getStartTimestamp() {
        return new Timestamp(startTimestamp);
    }

    public void setNextLimitOffset(long nextLimitOffset) {
        this.nextLimitOffset = nextLimitOffset;
    }

    /**
     * 转成map
     *
     * @return map
     */
    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>(3);
        if (incrementingOffset != null) {
            map.put(INCREMENTING_FIELD, incrementingOffset);
        }
        if (timestampOffset != null) {
            map.put(TIMESTAMP_FIELD, timestampOffset.getTime());
            map.put(LAST_LIMIT_OFFSET, lastLimitOffset);
            map.put(START_TIMESTAMP, startTimestamp);
            map.put(TIMESTAMP_NANOS_FIELD, (long) timestampOffset.getNanos());
        }

        return map;
    }

    /**
     * fromMap
     *
     * @param map map
     * @return TimestampIncrementingOffset
     */
    public static TimestampIncrementingOffset fromMap(Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return new TimestampIncrementingOffset(null, null, JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE, 0L);
        }

        Long incr = (Long) map.get(INCREMENTING_FIELD);
        Long millis = (Long) map.get(TIMESTAMP_FIELD);
        Long startTimestamp = (Long) map.getOrDefault(START_TIMESTAMP, 0L);
        Long lastLimitOffset = (Long) map
                .getOrDefault(LAST_LIMIT_OFFSET, JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE);
        Timestamp ts = null;
        if (millis != null) {
            ts = new Timestamp(millis);
            Long nanos = (Long) map.get(TIMESTAMP_NANOS_FIELD);
            if (nanos != null) {
                ts.setNanos(nanos.intValue());
            }
        }
        return new TimestampIncrementingOffset(ts, incr, lastLimitOffset, startTimestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TimestampIncrementingOffset that = (TimestampIncrementingOffset) o;

        if (incrementingOffset != null ? !incrementingOffset.equals(that.incrementingOffset)
                : that.incrementingOffset != null) {
            return false;
        }
        return timestampOffset != null ? timestampOffset.equals(that.timestampOffset) : that.timestampOffset == null;
    }

    @Override
    public int hashCode() {
        int result = incrementingOffset != null ? incrementingOffset.hashCode() : 0;
        result = 31 * result + (timestampOffset != null ? timestampOffset.hashCode() : 0);
        return result;
    }

    /**
     * 根据上一次的offset更新新的offset
     */
    public TimestampIncrementingOffset generateNextOffset(Timestamp time, long incrementingColumnValue, int recordIndex,
            int batchSize) {
        if (time == null) {
            // 自增列,offset更新
            return new TimestampIncrementingOffset(null, incrementingColumnValue, 0L, 0L, nextLimitOffset);
        } else if (JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE.equals(nextLimitOffset)) {
            // 上个批次不满一个batchSize, 更新startTimestame
            return new TimestampIncrementingOffset(time, incrementingColumnValue, 0L, time.getTime(),
                    JdbcSourceConnectorConfig.LOAD_HISTORY_END_VALUE);
        } else {
            // 应写入的offset readRecordSize 等于 下次sql查询的offset - (本次已发送 + 本次已过滤的)
            long realRecordSize = nextLimitOffset - (batchSize - recordIndex);
            realRecordSize = realRecordSize < 0 ? 0L : realRecordSize;
            return new TimestampIncrementingOffset(time, incrementingColumnValue, realRecordSize, startTimestamp,
                    nextLimitOffset);
        }
    }
}
