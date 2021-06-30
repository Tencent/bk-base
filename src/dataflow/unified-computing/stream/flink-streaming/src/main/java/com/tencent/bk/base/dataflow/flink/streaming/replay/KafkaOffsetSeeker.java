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

package com.tencent.bk.base.dataflow.flink.streaming.replay;

import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar.WindowType;
import com.tencent.bk.base.dataflow.core.topo.Node;
import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.topo.TransformNode;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.AbstractFlinkStreamingCheckpointManager;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.CheckpointValue.OutputCheckpoint;
import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.types.OffsetCheckpointKey;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaOffsetSeeker implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOffsetSeeker.class);
    private static final int STOP_SEEK_IF_RANGE_SMALL_THAN_N = 1000;
    private static final int SEARCH_RETRY_NUM = 100;
    private static final int WINDOW_ACCUMULATE_REPLAY_TIME = 24 * 60 * 60;
    private static final int WINDOW_SESSION_REPLAY_TIME = 30 * 60;
    private static final int WINDOW_DEFAULT_REPLAY_TIME = 600;


    private final List<String> timedStreamNames = new ArrayList<>();
    private Node node;
    private AbstractFlinkStreamingCheckpointManager checkpointManager;
    private Topology topology;

    public KafkaOffsetSeeker(Topology topology, Node node, AbstractFlinkStreamingCheckpointManager checkpointManager) {
        collectTimedStreamNames(timedStreamNames, node);
        this.topology = topology;
        this.node = node;
        this.checkpointManager = checkpointManager;
    }


    /**
     * seek replay init offset
     *
     * @param topic topic
     * @param partitionCount par num
     * @return int offset
     */
    public Map<Integer, Long> seekInitialOffsets(String topic, Integer partitionCount, AvroKafkaReader seekConsumer) {
        // 判断checkpoint类型，若是offset，则读取redis中的checkpoint直接返回，即走下面的逻辑。
        Map<Integer, Long> initialOffsets = new HashMap<>();
        if (timedStreamNames.isEmpty()) {
            LOGGER.info("there is no timed stream for node: " + node.getNodeId() + " fallback to use offset directly");
            Map<Integer, Long> recoverInitialOffsets = recoverInitialOffsets(topic);
            for (Map.Entry<Integer, Long> entry : recoverInitialOffsets.entrySet()) {
                int partition = entry.getKey();
                long offset = entry.getValue();
                if (offset < 0) {
                    offset = 0;
                }
                initialOffsets.put(partition, offset);
            }
            return initialOffsets;
        }

        // 若checkpoint中包含时间，则需要计算target time
        long targetTime = estimateStartTime();

        if (targetTime <= 0) {
            LOGGER.info("[REPLAY] target time is negative, reset to 0");
            targetTime = 0;
        }
        LOGGER.info("[REPLAY] seek target {}.", targetTime);
        // 获取kafka中topic的partition以及对应的offset
        Map<String, Map<String, Number>> partitions = seekConsumer.getTopicRange(topic);
        // 通过二分法查找有时间的head
        for (int partition = 0; partition < partitionCount; partition++) {
            if (targetTime == 0) {
                initialOffsets.put(partition, 0L);
            } else {
                try {
                    Map<String, Number> offsetRange = partitions.get(String.valueOf(partition));
                    long head = offsetRange.get("head").longValue();
                    long tail = offsetRange.get("tail").longValue();
                    Long headTime = 0L;
                    HashMap<Long, Long> offsetAntTimestamp = getTimeAtOffset(seekConsumer, topic, partition, head,
                            tail);
                    //offset out range
                    if (null == offsetAntTimestamp) {
                        initialOffsets.put(partition, head);
                        continue;
                    } else {
                        for (Map.Entry<Long, Long> entry : offsetAntTimestamp.entrySet()) {
                            head = entry.getKey();
                            headTime = entry.getValue();
                        }
                    }

                    //kafka中msg的时间若比targetTime大，说明已经在基准timestamp之后，则记下对应的offset
                    LOGGER.info("partition: {}, headTime: {}, targetTime: {}", partition, headTime, targetTime);
                    if (headTime > targetTime) {
                        LOGGER.info(String.format("partition[%d] head %d > %d so seek to head offset %s",
                                partition, headTime, targetTime, head));
                        initialOffsets.put(partition, head);
                    } else {
                        //kafka中msg的时间若比小，说明在基准timestamp之前，需要继续向后查找
                        long seekedOffset = seekOffset(seekConsumer, topic, partition, head, tail, targetTime,
                                STOP_SEEK_IF_RANGE_SMALL_THAN_N);
                        initialOffsets.put(partition, seekedOffset);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return initialOffsets;
    }

    private Map<Integer, Long> recoverInitialOffsets(String topic) {
        ArrayList<Node> nodes = listAllDescendants(node);
        return recoverInitialOffsetFromCheckpoint(nodes, topic);
    }

    private ArrayList<Node> listAllDescendants(Node node) {
        ArrayList<Node> nodes = new ArrayList<>();
        if (node instanceof TransformNode) {
            nodes.add(node);
        }
        for (Node child : node.getChildren()) {
            nodes.addAll(listAllDescendants(child));
        }
        return nodes;
    }

    /**
     * 若存在过滤节点，直接取 offset 最小值，对应的 offset 可能过于滞后
     *
     * @param nodes
     * @param topic
     * @return
     */
    private Map<Integer, Long> recoverInitialOffsetFromCheckpoint(List<Node> nodes, String topic) {
        Map<Integer, Long> smallestOffsets = new HashMap<>();
        for (Node node : nodes) {
            //if (node instanceof SourceNode || node instanceof SinkNode) {
            //  continue;
            //}
            // offset的checkpoint key增加topic字段组合：[前缀][nodeId]|[topic]
            Map<Integer, OutputCheckpoint> offsets = checkpointManager
                    .listCheckpoints(new OffsetCheckpointKey(topic, node.getNodeId(), "0").getKey());
            for (Map.Entry<Integer, OutputCheckpoint> entry : offsets.entrySet()) {
                Long smallestOffset = smallestOffsets.get(entry.getKey());
                Long offset = entry.getValue().getPointValue();
                if (null == smallestOffset) {
                    smallestOffsets.put(entry.getKey(), offset);
                } else {
                    smallestOffsets.put(entry.getKey(), Math.min(smallestOffset, offset));
                }
            }
        }
        return smallestOffsets;
    }

    /**
     * 计算target time
     *
     * @return target time
     */
    private long estimateStartTime() {
        long overallStartTime = 0;
        for (String timedStreamName : timedStreamNames) {
            //取redis中所有checkpoint为timestamp的min，redis存储的是10位时间戳
            long streamStartTime = checkpointManager.estimateStartTimeFromCheckpoints(timedStreamName).getPointValue();
            int streamBackoffTime = estimateBackoffTime(timedStreamName);
            LOGGER.info("[REPLAY] node: " + timedStreamName + ", startTime: " + streamStartTime + ", backoffTime: "
                    + streamBackoffTime);
            streamStartTime -= streamBackoffTime;
            if (streamStartTime <= 0) {
                continue;
            }
            if (0 == overallStartTime) {
                overallStartTime = streamStartTime;
            } else {
                overallStartTime = Math.min(overallStartTime, streamStartTime);
            }
        }
        return overallStartTime;
    }

    /**
     * 不同的窗口类型，backoff的时间不一样
     *
     * @param timedStreamName node id
     * @return backoff time
     */
    private int estimateBackoffTime(String timedStreamName) {
        TransformNode transformNode = (TransformNode) topology.queryNode(timedStreamName);
        String windowType = null;
        if (transformNode.getWindowConfig().getWindowType() != null) {
            windowType = transformNode.getWindowConfig().getWindowType().toString();
        }
        LOGGER.info("[REPALY] the node " + timedStreamName + " window type is " + windowType);
        System.out.println("[REPALY] the node " + timedStreamName + " window type is " + windowType);
        int backOffTimestamp;
        // 非窗口
        if (null == windowType || "".equals(windowType)) {
            backOffTimestamp = 300;
            return backOffTimestamp;
        }
        WindowType windowTypeEnum = WindowType.valueOf(windowType.toLowerCase());
        switch (windowTypeEnum) {
            case tumbling:
                backOffTimestamp = transformNode.getWindowConfig().getCountFreq() * 2;
                break;
            case sliding:
                int counter =
                        transformNode.getWindowConfig().getWindowLength() / transformNode.getWindowConfig()
                                .getCountFreq();
                backOffTimestamp = transformNode.getWindowConfig().getCountFreq() * (counter + 1);
                break;
            case accumulate:
                // 向前推一天,用于重放
                backOffTimestamp = WINDOW_ACCUMULATE_REPLAY_TIME;
                break;
            case session:
                // 30 minutes
                backOffTimestamp = WINDOW_SESSION_REPLAY_TIME;
                break;
            default:
                backOffTimestamp = WINDOW_DEFAULT_REPLAY_TIME;
        }
        LOGGER.info("[RELAY] node " + transformNode.getNodeId() + ", backoff seconds: " + backOffTimestamp
                + ", window type: " + windowType);
        return backOffTimestamp;
    }

    private void collectTimedStreamNames(List<String> timedStreamNames, Node node) {
        if (node instanceof TransformNode) {
            TransformNode transformNode = (TransformNode) node;
            if (!transformNode.getCommonFields().contains(ConstantVar.BKDATA_PARTITION_OFFSET)) {
                timedStreamNames.add(node.getNodeId());
            }
        }
        for (Node child : node.getChildren()) {
            collectTimedStreamNames(timedStreamNames, child);
        }
    }

    /**
     * 根据head和tail获取 10位的timestamp
     *
     * @param seekConsumer seek consumer类
     * @param topic topic
     * @param partition partition
     * @param head head
     * @param tail tail
     * @return map, key: offset, value: timestamp
     */
    public HashMap<Long, Long> getTimeAtOffset(AvroKafkaReader seekConsumer, String topic, int partition, long head,
            long tail) {
        if ((tail - head) <= 1) {
            HashMap<Long, Long> offsetAndTimestamp = new HashMap<>();
            Long time = 0L;
            byte[] headRawData = seekConsumer.getMessageAsByte(topic, partition, head);
            //需要判断headRawData=0时，kafka中无数据
            if (null != headRawData) {
                time = extractTime(headRawData);
                if (null != time) {
                    offsetAndTimestamp.put(head, time);
                    return offsetAndTimestamp;
                } else {
                    byte[] tailRawData = seekConsumer.getMessageAsByte(topic, partition, tail);
                    if (null == tailRawData) {
                        return offsetAndTimestamp;
                    }
                    time = extractTime(tailRawData);
                    if (null != time) {
                        offsetAndTimestamp.put(tail, time);
                        return offsetAndTimestamp;
                    }
                }
            } else {
                byte[] tailRawData = seekConsumer.getMessageAsByte(topic, partition, tail);
                if (null != tailRawData) {
                    time = extractTime(tailRawData);
                    if (null != time) {
                        offsetAndTimestamp.put(tail, time);
                        return offsetAndTimestamp;
                    }
                }
            }
            return offsetAndTimestamp;
            //throw new RuntimeException("can not find timestamp by offset");
        } else {
            long mid = (head + tail) / 2;
            byte[] midRawData = seekConsumer.getMessageAsByte(topic, partition, mid);
            if (null == midRawData) {
                return getTimeAtOffset(seekConsumer, topic, partition, mid, tail);
            } else {
                Long time = extractTime(midRawData);
                if (null == time) {
                    //从offset一直向后找，直到解析出timestamp
                    return getTimeAtOffset(seekConsumer, topic, partition, mid, tail);
                } else {
                    return getTimeAtOffset(seekConsumer, topic, partition, head, mid);
                }
            }
        }
    }

    /**
     * 注意: cnt 一般小于等于 100 根据 offset 获取 avro 中的第一条数据(没有则获取下一条)的 timestamp 当前 offset 的一批数据可能信息为 null, 则最多继续寻找至 offset + N,
     * 0 < N <= 100 - cnt
     *
     * @param seekConsumer seek consumer
     * @param topic topic
     * @param partition partition
     * @param offset offset
     * @param cnt cnt
     * @return map, key: offset, value: timestamp
     */
    private HashMap<Long, Long> getTimeAtOffset(AvroKafkaReader seekConsumer, String topic, int partition, long offset,
            int cnt) {
        HashMap<Long, Long> offsetAndTimestamp = new HashMap<>();
        byte[] rawData = seekConsumer.getMessageAsByte(topic, partition, offset);
        if (null == rawData && cnt < SEARCH_RETRY_NUM) {
            return getTimeAtOffset(seekConsumer, topic, partition, offset + 1, cnt + 1);
        } else if (null == rawData) {
            throw new RuntimeException("Find the data as null for 100 consecutive times.");
        }
        Long time = extractTime(rawData);
        if (null == time && cnt < SEARCH_RETRY_NUM) {
            LOGGER.info("failed to parse timestamp at " + offset);
            return getTimeAtOffset(seekConsumer, topic, partition, offset + 1, cnt + 1);
        } else if (null == time && cnt == SEARCH_RETRY_NUM) {
            throw new RuntimeException("Find the data as null for 100 consecutive times.");
        } else {
            offsetAndTimestamp.put(offset, time);
            return offsetAndTimestamp;
        }
    }

    private Long extractTime(byte[] rawData) {
        try {
            ByteArrayInputStream input = new ByteArrayInputStream(new String(rawData, "utf8").getBytes("ISO-8859-1"));
            DatumReader<GenericRecord> reader = new GenericDatumReader<>();
            DataFileStream<GenericRecord> dataReader;
            try {
                dataReader = new DataFileStream<>(input, reader);
            } catch (IOException e) {
                LOGGER.warn("failed parse the data. ", e);
                return null;
            }
            GenericRecord inputAvroRecord = null;
            Long timestamp = null;
            while (dataReader.hasNext()) {
                inputAvroRecord = dataReader.next(inputAvroRecord);
                GenericArray avroValues = (GenericArray) inputAvroRecord.get("_value_");
                if (null == avroValues) {
                    throw new RuntimeException("The message value is null when reply the data.");
                }
                for (int i = 0; i < avroValues.size(); i++) {
                    GenericRecord rec = (GenericRecord) avroValues.get(i);

                    Object dtEventTimeStamp = rec.get(ConstantVar.EVENT_TIMESTAMP);
                    if (dtEventTimeStamp == null) {
                        continue;
                    }
                    timestamp = Long.parseLong(dtEventTimeStamp.toString()) / 1000;
                }
            }
            return timestamp;
        } catch (Exception e) {
            LOGGER.error("failed to extract time from kafka message.");
            throw new RuntimeException(e);
        }
    }

    /**
     * 二分法，根据head和tail及target time查找offset
     *
     * @param seekConsumer seek kafka reader
     * @param topic topic
     * @param partition partition
     * @param head head
     * @param tail tail
     * @param targetTime target time
     * @param stopSeekIfRangeSmallThanN stop seek if range small than n
     * @return
     */
    private Long seekOffset(AvroKafkaReader seekConsumer, String topic, int partition, long head, long tail,
            long targetTime, int stopSeekIfRangeSmallThanN) {
        LOGGER.info("seek partition " + partition + " between [" + head + ", " + tail + "]");
        if ((tail - head) < stopSeekIfRangeSmallThanN) {
            LOGGER.info("seek partition " + partition + " ended at [" + head + ", " + tail + "]");
            return head;
        }
        // 二分查找
        long middle = (head + tail) / 2;
        long time = 0;
        int cnt = 0;
        HashMap<Long, Long> offsetAtTimestamp = getTimeAtOffset(seekConsumer, topic, partition, middle, cnt);
        for (Map.Entry<Long, Long> entry : offsetAtTimestamp.entrySet()) {
            time = entry.getValue();
            middle = entry.getKey();
        }
        if (time > targetTime) {
            return seekOffset(seekConsumer, topic, partition, head, middle, targetTime, stopSeekIfRangeSmallThanN);
        } else {
            return seekOffset(seekConsumer, topic, partition, middle, tail, targetTime, stopSeekIfRangeSmallThanN);
        }
    }

    /**
     * close redis pool
     */
    public void close() {
        // 关掉redis连接
        if (checkpointManager != null) {
            this.checkpointManager.close();
        }
    }
}
