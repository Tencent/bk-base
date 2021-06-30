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

package com.tencent.bk.base.datahub.databus.connect.source.kafka;

import org.apache.kafka.common.TopicPartition;

public class LeaderTopicPartition extends Object {

    private static String STRING_DELIMITER = ":";

    private int hash = 0;

    private final int leaderId;
    private final String topicName;
    private final int partition;

    public LeaderTopicPartition(int leaderId, String topicName, int partition) throws IllegalArgumentException {
        this.leaderId = leaderId;
        if (topicName == null) {
            throw new IllegalArgumentException("topicName can not be null");
        }
        this.topicName = topicName;
        this.partition = partition;
    }

    public static LeaderTopicPartition fromString(String leaderTopicPartitionString) {
        String[] tokens = leaderTopicPartitionString.split(STRING_DELIMITER);
        if (tokens.length != 3) {
            throw new IllegalArgumentException(
                    "leaderTopicPartitionString must be in the format <leader>:<topic>:<partition>");
        }
        return new LeaderTopicPartition(Integer.parseInt(tokens[0], 10), tokens[1], Integer.parseInt(tokens[2], 10));
    }

    @Override
    public String toString() {
        return String.valueOf(leaderId) + STRING_DELIMITER + topicName + STRING_DELIMITER + String.valueOf(partition);
    }

    public TopicPartition toTopicPartition() {
        return new TopicPartition(topicName, partition);
    }

    public String toTopicPartitionString() {
        return topicName + STRING_DELIMITER + String.valueOf(partition);
    }

    @Override
    public int hashCode() {
        if (hash != 0) {
            return hash;
        }
        int result = 1;
        result = result * 23 + leaderId;
        result = result * 37 + (topicName == null ? 0 : topicName.hashCode());
        result = result * 11 + partition;
        this.hash = result;
        return result;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof LeaderTopicPartition)) {
            return false;
        }
        LeaderTopicPartition otherLeaderTopicPartition = (LeaderTopicPartition) other;
        return leaderId == otherLeaderTopicPartition.leaderId
                && ((topicName == null) ? otherLeaderTopicPartition.topicName == null
                : topicName.equals(otherLeaderTopicPartition.topicName))
                && partition == otherLeaderTopicPartition.partition;
    }

}
