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

package com.tencent.bk.base.datahub.databus.connect.sink.clickhouse;

import com.tencent.bk.base.datahub.databus.commons.BulkProcessor;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.bean.ApiResult;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.monitor.Metric;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Getter
@Setter
@Accessors(chain = true)
public class ClickHouseBean {

    private static final Logger log = LoggerFactory.getLogger(ClickHouseBean.class);
    public List<BulkProcessor<Map<String, Object>>> bulkProcessors = new ArrayList<>();
    public ConcurrentMap<String, Integer> processorsProps = new ConcurrentHashMap<>();
    public Long updateTime = System.currentTimeMillis();
    public String weightsUrl;
    public ConcurrentMap<String, Integer> weightMap;
    public PrizeWheel wheel;
    public ReentrantLock lock = new ReentrantLock();

    /**
     * 维护bulkProcessors
     *
     * @param props bulkProcessors的配置属性
     * @param flush 刷盘函数
     */
    public void maintainProcessors(ConcurrentMap<String, Integer> props, Consumer<List<Map<String, Object>>> flush) {
        if (lock.tryLock()) {
            try {
                Integer flushSize = props.get(ClickHouseConsts.FLUSH_SIZE);
                Integer flushInterval = props.get(ClickHouseConsts.FLUSH_INTERVAL);
                Integer processorsSize = props.get(ClickHouseConsts.PROCESSORS_SIZE);
                // todo: 处理集群配置变更后BulkProcessor参数也需要更新的情况
                while (bulkProcessors.size() < processorsSize) {
                    bulkProcessors.add(new BulkProcessor<>(flushSize, flushInterval.longValue(), flush));
                }
            } catch (Exception e) {
                LogUtils.error(ClickHouseConsts.CLICKHOUSE_MAINTAIN_PROCESSORS_ERROR, log,
                        "maintain bulkProcessors failed", e);
            } finally {
                lock.unlock();
            }
        }

    }

    /**
     * 删除指定权重映射
     *
     * @param url clickhouse server jdbc url
     */
    public void removeWeight(String url) {
        if (lock.tryLock()) {
            try {
                weightMap.remove(url);
                wheel = new PrizeWheel(weightMap);
            } catch (Exception e) {
                LogUtils.warn(log, "{}: remove jdbc url failed, exception {}", url, e.getMessage());
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * 计算每个可用jdbc url的写入权重
     * 步骤：从storekit获取，失败后使用旧值；如果旧值未初始化，则从clickhouse server 查询并计算
     */
    public boolean updateWeights(String rtId) {
        if (lock.tryLock()) {
            try {
                weightMap = getWeightMap(weightsUrl);
                wheel = new PrizeWheel(weightMap);
                updateTime = System.currentTimeMillis();
                return true;
            } catch (Exception e) {
                LogUtils.warn(log, "{}: update weights failed, exception {}", rtId, e.getMessage());
            } finally {
                lock.unlock();
            }
        }

        return false;
    }

    /**
     * 计算每个可用jdbc url的写入权重
     * 步骤：从storekit获取，失败后使用旧值；如果旧值未初始化，则从clickhouse server 查询并计算
     *
     * @param weightsUrl
     * @return jdbc_url的权重映射
     */
    public static ConcurrentMap<String, Integer> getWeightMap(String weightsUrl) {
        try {
            ApiResult result = HttpUtils.getApiResult(weightsUrl);
            if (result.isResult()) {
                Map<String, Object> data = (Map<String, Object>) result.getData();
                LogUtils.info(log, "query weights from {}, data:{}", weightsUrl, data.toString());
                long delta = System.currentTimeMillis() - (Long) data.get(Consts.TIMESTAMP);
                if (delta > ClickHouseConsts.DEFAULT_UPDATE_WEIGHTS_INTERVAL * 60 * 60 * 1000) {
                    String msg = String.format("storekit exception, weights timestamp not updated, url %s, data %s",
                            weightsUrl, result.getData().toString());
                    Metric.getInstance().reportEvent(weightsUrl, ClickHouseConsts.CLICKHOUSE_QUERY_WEIGHTS_ERROR,
                            ClickHouseConsts.CLICKHOUSE, msg);
                    throw new ConnectException(msg);
                }
                ConcurrentMap<String, Integer> weights = ((Map<String, Integer>) data.get(ClickHouseConsts.WEIGHTS))
                        .entrySet()
                        .stream()
                        .filter(e -> e.getValue() >= 1)
                        .collect(Collectors.toConcurrentMap(e -> "jdbc:clickhouse://" + e.getKey(), e -> e.getValue()));

                if (weights.isEmpty()) {
                    String msg = "storekit exception, empty weights from url: " + weightsUrl;
                    Metric.getInstance().reportEvent(weightsUrl, ClickHouseConsts.CLICKHOUSE_QUERY_WEIGHTS_ERROR,
                            ClickHouseConsts.CLICKHOUSE, msg);
                    throw new ConnectException(msg);
                }

                return weights;
            } else {
                String msg = "storekit exception, bad response from url: " + weightsUrl;
                Metric.getInstance().reportEvent(weightsUrl, ClickHouseConsts.CLICKHOUSE_QUERY_WEIGHTS_ERROR,
                        ClickHouseConsts.CLICKHOUSE, msg);
                throw new ConnectException(msg);
            }
        } catch (Exception e) {
            LogUtils.error(ClickHouseConsts.CLICKHOUSE_QUERY_WEIGHTS_ERROR, log,
                    "query weights from url: " + weightsUrl, e);
        }

        return new ConcurrentHashMap<>();
    }
}
