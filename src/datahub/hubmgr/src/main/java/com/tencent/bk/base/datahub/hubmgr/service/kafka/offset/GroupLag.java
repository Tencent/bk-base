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

package com.tencent.bk.base.datahub.hubmgr.service.kafka.offset;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.MgrConsts;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class GroupLag {

    private static final Logger log = LoggerFactory.getLogger(GroupLag.class);
    private LoadingCache<String, ConsumerLag> cache;
    private String group;

    public GroupLag(String group) {
        this.group = group;
        cache = CacheBuilder.newBuilder()
                .expireAfterAccess(2, TimeUnit.MINUTES)
                .build(new CacheLoader<String, ConsumerLag>() {
                    @Override
                    public ConsumerLag load(@NotNull String pattern) {
                        return new ConsumerLag();
                    }
                });
    }

    /**
     * 设置消费组lag信息
     *
     * @param offsetStat consumer offset信息
     * @param lag lag长度
     * @param percent 延迟比率
     */
    public void addLag(ConsumerOffsetStat offsetStat, long lag, double percent) {
        String key = offsetStat.getKafka() + "#" + offsetStat.getTopic() + "-" + offsetStat.getPartition();
        try {
            ConsumerLag consumerLag = cache.get(key);
            consumerLag.update(offsetStat, lag, percent);
        } catch (ExecutionException e) {
            LogUtils.warn(log, "fail to addLag: consumerOffsetStat={}, lag={}, percent={}", offsetStat, lag, percent);
        }
    }

    /**
     * 生成消费组lag延迟告警信息
     *
     * @return 消费组lag延迟告警信息
     */
    public String getMsg(Set<String> ignoredClusters) {
        Map<String, List<String>> info = new HashMap<>();
        for (ConsumerLag consumerLag : cache.asMap().values()) {
            String kafkaCluster = consumerLag.getKafka();
            // 部分集群忽略告警
            if (ignoredClusters.contains(kafkaCluster)) {
                continue;
            }
            List<String> list = info.computeIfAbsent(kafkaCluster, k -> new ArrayList<>());
            list.add(consumerLag.getMsg());
        }

        if (info.isEmpty()) {
            return "";
        }

        StringBuilder sb = new StringBuilder();
        String tag = DatabusProps.getInstance().getOrDefault(MgrConsts.ALARM_TAG, "");
        sb.append("[").append(CommUtils.getDatetime()).append("][").append(tag).append("]");
        sb.append(group).append("消费延迟:\n");
        for (Map.Entry<String, List<String>> entry : info.entrySet()) {
            sb.append("集群 ").append(entry.getKey()).append("\n");
            for (String item : entry.getValue()) {
                sb.append(" ").append(item).append("\n");
            }
        }
        return sb.toString();
    }

}
