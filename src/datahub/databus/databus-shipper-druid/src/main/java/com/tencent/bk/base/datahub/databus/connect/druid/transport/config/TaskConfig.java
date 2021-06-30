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

package com.tencent.bk.base.datahub.databus.connect.druid.transport.config;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class TaskConfig {

    private final int shards;
    private final String dataSourceName;
    private final List<MetricSpec> metrics;
    private final List<DimensionSpec> dimensions;
    private final List<String> dimensionExclusions;
    private final String timestampColumn;
    private final String windowPeriod;
    private final String segmentGranularity;
    private final String zookeeperConnect;
    private final Map<String, Object> context;
    private final int requiredCapacity;
    private final int shipperFlushSizeThreshold;
    private final int shipperHttpRequestTimeout;
    private final int shipperFlushOvertimeThreshold;
    private final int shipperCacheSize;
    private final String maxIdleTime;
    private final String druidVersion;
    private final String timestampStrategy;
    private final String bufferSize;
    private final String intermediateHandoffPeriod;
    private final String maxRowsPerSegment;
    private final String maxRowsInMemory;
    private final String maxTotalRows;
    private final String intermediatePersistPeriod;

    private TaskConfig(Builder builder) {
        Preconditions.checkArgument(builder.shards > 0, "shards");
        Preconditions.checkArgument(builder.requiredCapacity > 0, "requiredCapacity");
        Preconditions.checkNotNull(builder.dataSourceName, "dataSourceName");
        Preconditions.checkNotNull(builder.metrics, "metrics");
        Preconditions.checkNotNull(builder.dimensions, "dimensions");
        Preconditions.checkNotNull(builder.dimensionExclusions, "dimensionExclusions");
        Preconditions.checkNotNull(builder.timestampColumn, "timestampColumn");
        Preconditions.checkNotNull(builder.windowPeriod, "windowPeriod");
        Preconditions.checkNotNull(builder.segmentGranularity, "segmentGranularity");
        Preconditions.checkNotNull(builder.zookeeperConnect, "zookeeperConnect");
        Preconditions.checkNotNull(builder.context, "context");
        Preconditions.checkNotNull(builder.bufferSize, "bufferSize");
        Preconditions.checkNotNull(builder.intermediateHandoffPeriod, "intermediateHandoffPeriod");
        Preconditions.checkNotNull(builder.maxRowsPerSegment, "maxRowsPerSegment");
        Preconditions.checkNotNull(builder.maxRowsInMemory, "maxRowsInMemory");
        Preconditions.checkNotNull(builder.maxTotalRows, "maxTotalRows");
        Preconditions.checkNotNull(builder.intermediatePersistPeriod, "intermediatePersistPeriod");

        this.shards = builder.shards;
        this.dataSourceName = builder.dataSourceName;
        this.metrics = builder.metrics;
        this.dimensions = builder.dimensions;
        this.dimensionExclusions = builder.dimensionExclusions;
        this.timestampColumn = builder.timestampColumn;
        this.windowPeriod = builder.windowPeriod;
        this.segmentGranularity = builder.segmentGranularity;
        this.zookeeperConnect = builder.zookeeperConnect;
        this.context = builder.context;
        this.requiredCapacity = builder.requiredCapacity;
        this.shipperFlushOvertimeThreshold = builder.shipperFlushOvertimeThreshold;
        this.shipperHttpRequestTimeout = builder.shipperHttpRequestTimeout;
        this.shipperFlushSizeThreshold = builder.shipperFlushSizeThreshold;
        this.shipperCacheSize = builder.shipperCacheSize;
        this.maxIdleTime = builder.maxIdleTime;
        this.druidVersion = builder.druidVersion;
        this.timestampStrategy = builder.timestampStrategy;
        this.bufferSize = builder.bufferSize;
        this.intermediateHandoffPeriod = builder.intermediateHandoffPeriod;
        this.maxRowsPerSegment = builder.maxRowsPerSegment;
        this.maxRowsInMemory = builder.maxRowsInMemory;
        this.maxTotalRows = builder.maxTotalRows;
        this.intermediatePersistPeriod = builder.intermediatePersistPeriod;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getShards() {
        return shards;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public List<MetricSpec> getMetrics() {
        return metrics;
    }

    public List<DimensionSpec> getDimensions() {
        return dimensions;
    }

    public List<String> getDimensionExclusions() {
        return dimensionExclusions;
    }

    public String getTimestampColumn() {
        return timestampColumn;
    }

    public String getWindowPeriod() {
        return windowPeriod;
    }

    public String getBufferSize() {
        return bufferSize;
    }

    public String getIntermediateHandoffPeriod() {
        return intermediateHandoffPeriod;
    }

    public String getMaxRowsPerSegment() {
        return maxRowsPerSegment;
    }

    public String getMaxRowsInMemory() {
        return maxRowsInMemory;
    }

    public String getMaxTotalRows() {
        return maxTotalRows;
    }

    public String getIntermediatePersistPeriod() {
        return intermediatePersistPeriod;
    }

    public String getSegmentGranularity() {
        return segmentGranularity;
    }

    public String getzookeeperConnect() {
        return zookeeperConnect;
    }

    public Map<String, Object> getContext() {
        return context;
    }

    public int getRequiredCapacity() {
        return requiredCapacity;
    }

    public String getMaxIdleTime() {
        return maxIdleTime;
    }

    public int getShipperCacheSize() {
        return shipperCacheSize;
    }

    public int getShipperFlushSizeThreshold() {
        return shipperFlushSizeThreshold;
    }

    public int getShipperHttpRequestTimeout() {
        return shipperHttpRequestTimeout;
    }

    public int getShipperFlushOvertimeThreshold() {
        return shipperFlushOvertimeThreshold;
    }

    public String getDruidVersion() {
        return druidVersion;
    }

    public String getTimestampStrategy() {
        return timestampStrategy;
    }

    public static class Builder {

        private final Map<String, Object> context = new HashMap<>();
        private int shards = 3;
        private String dataSourceName = null;
        private String zookeeperConnect = null;
        private List<DimensionSpec> dimensions = null;
        private List<MetricSpec> metrics = new ArrayList<>();
        private List<String> dimensionExclusions = Collections.emptyList();
        private String timestampColumn = null;
        private String windowPeriod = "PT3M";
        private String segmentGranularity = "five_minute";
        private int requiredCapacity = 1;
        private int shipperFlushSizeThreshold;
        private int shipperHttpRequestTimeout;
        private int shipperFlushOvertimeThreshold;
        private int shipperCacheSize = 1000;
        private String maxIdleTime = "600000";
        private String druidVersion = null;
        private String timestampStrategy = null;
        private String bufferSize = "10000";
        private String intermediateHandoffPeriod = "PT1H";
        private String maxRowsPerSegment = "5000000";
        private String maxRowsInMemory = "1000000";
        private String maxTotalRows = "20000000";
        private String intermediatePersistPeriod = "PT6M";

        /**
         * 构造函数
         */
        private Builder() {
        }

        /**
         * 设置shard数量
         *
         * @param shards shard数量
         * @return builder
         */
        public Builder shards(int shards) {
            this.shards = shards;
            return this;
        }

        /**
         * 设置 bufferSize
         *
         * @param bufferSize firehose bufferSize的大小
         * @return builder
         */
        public Builder bufferSize(String bufferSize) {
            this.bufferSize = bufferSize;
            return this;
        }

        /**
         * 设置 intermediateHandoffPeriod
         *
         * @param intermediateHandoffPeriod segment流转周期
         * @return builder
         */
        public Builder intermediateHandoffPeriod(String intermediateHandoffPeriod) {
            this.intermediateHandoffPeriod = intermediateHandoffPeriod;
            return this;
        }

        /**
         * 设置 maxRowsPerSegment
         *
         * @param maxRowsPerSegment 单个segment最大行数
         * @return builder
         */
        public Builder maxRowsPerSegment(String maxRowsPerSegment) {
            this.maxRowsPerSegment = maxRowsPerSegment;
            return this;
        }

        /**
         * 设置 maxRowsInMemory
         *
         * @param maxRowsInMemory 内存中的最大行数
         * @return builder
         */
        public Builder maxRowsInMemory(String maxRowsInMemory) {
            this.maxRowsInMemory = maxRowsInMemory;
            return this;
        }

        /**
         * 设置 maxTotalRows
         *
         * @param maxTotalRows 所有segment总行数
         * @return builder
         */
        public Builder maxTotalRows(String maxTotalRows) {
            this.maxTotalRows = maxTotalRows;
            return this;
        }

        /**
         * 设置 intermediatePersistPeriod
         *
         * @param intermediatePersistPeriod segment持久化周期
         * @return builder
         */
        public Builder intermediatePersistPeriod(String intermediatePersistPeriod) {
            this.intermediatePersistPeriod = intermediatePersistPeriod;
            return this;
        }

        /**
         * 设置datasource名称
         *
         * @param dataSourceName datasource的名称
         * @return builder
         */
        public Builder dataSourceName(String dataSourceName) {
            this.dataSourceName = dataSourceName;
            return this;
        }

        /**
         * 设置druid版本
         *
         * @param druidVersion druid的版本
         * @return builder
         */
        public Builder druidVersion(String druidVersion) {
            this.druidVersion = druidVersion;
            return this;
        }

        /**
         * 设置datasource的时间序列的策略
         *
         * @param timestampStrategy datasource的时序策略
         * @return builder
         */
        public Builder timestampStrategy(String timestampStrategy) {
            this.timestampStrategy = timestampStrategy;
            return this;
        }

        /**
         * 添加metric
         *
         * @param type 类型
         * @param name 名称
         * @return builder
         */
        public Builder addMetric(String type, String name) {
            metrics.add(new MetricSpec(name, type));
            return this;
        }

        /**
         * 添加metric
         *
         * @param type 类型
         * @param name 名称
         * @param fieldName 字段名称
         * @return builder
         */
        public Builder addMetric(String type, String name, String fieldName) {
            if (metrics == null) {
                metrics = new ArrayList<>();
            }
            metrics.add(new MetricSpec(name, type, fieldName));
            return this;
        }

        /**
         * 添加维度
         *
         * @param type 类型
         * @param name 名称
         * @return builder
         */
        public Builder addDimension(String type, String name) {
            if (dimensions == null) {
                dimensions = new ArrayList<>();
            }
            dimensions.add(new DimensionSpec(name, type));
            return this;
        }

        /**
         * 设置timestamp字段
         *
         * @param timestampColumn 字段名称
         * @return builder
         */
        public Builder timestampColumn(String timestampColumn) {
            this.timestampColumn = timestampColumn;
            return this;
        }

        /**
         * 设置窗口周期
         *
         * @param windowPeriod 窗口周期
         * @return builder
         */
        public Builder windowPeriod(String windowPeriod) {
            this.windowPeriod = windowPeriod;
            return this;
        }

        /**
         * 设置segement granularity
         *
         * @param segmentGranularity segment granularity
         * @return builder
         */
        public Builder segmentGranularity(String segmentGranularity) {
            this.segmentGranularity = segmentGranularity;
            return this;
        }

        /**
         * 设置zk连接串
         *
         * @param zookeeperConnect zk连接串
         * @return builder
         */
        public Builder zookeeper(String zookeeperConnect) {
            this.zookeeperConnect = zookeeperConnect;
            return this;
        }

        /**
         * 添加context
         *
         * @param key key
         * @param value value
         * @return builder
         */
        public Builder addContext(String key, Object value) {
            context.put(key, value);
            return this;
        }

        /**
         * 设置所需的容量
         *
         * @param requiredCapacity 所需的容量
         * @return builder
         */
        public Builder requiredCapacity(int requiredCapacity) {
            this.requiredCapacity = requiredCapacity;
            return this;
        }

        /**
         * 设置shiper cache size
         *
         * @param shipperCacheSize 最大idle时间
         * @return builder
         */
        public Builder shipperCacheSize(int shipperCacheSize) {
            this.shipperCacheSize = shipperCacheSize;
            return this;
        }

        /**
         * 设置shiper cache flush size threshold
         *
         * @param shipperFlushSizeThreshold 最大idle时间
         * @return builder
         */
        public Builder shipperFlushSizeThreshold(int shipperFlushSizeThreshold) {
            this.shipperFlushSizeThreshold = shipperFlushSizeThreshold;
            return this;
        }

        /**
         * 设置shiper cache flush size threshold
         *
         * @param shipperHttpRequestTimeout 最大idle时间
         * @return builder
         */
        public Builder shipperHttpRequestTimeout(int shipperHttpRequestTimeout) {
            this.shipperHttpRequestTimeout = shipperHttpRequestTimeout;
            return this;
        }

        /**
         * 设置shiper cache flush overtime threshold
         *
         * @param shipperFlushOvertimeThreshold 最大idle时间
         * @return builder
         */
        public Builder shipperFlushOvertimeThreshold(int shipperFlushOvertimeThreshold) {
            this.shipperFlushOvertimeThreshold = shipperFlushOvertimeThreshold;
            return this;
        }

        /**
         * 设置最大idle的时间
         *
         * @param maxIdleTimeMilli 最大idle时间
         * @return builder
         */
        public Builder maxIdleTime(String maxIdleTimeMilli) {
            this.maxIdleTime = maxIdleTimeMilli;
            return this;
        }

        /**
         * build task config对象
         *
         * @return task config对象
         */
        public TaskConfig build() {
            return new TaskConfig(this);
        }
    }
}
