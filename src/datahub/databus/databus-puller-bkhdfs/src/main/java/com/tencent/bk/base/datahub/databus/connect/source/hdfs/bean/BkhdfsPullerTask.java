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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs.bean;


import com.tencent.bk.base.datahub.databus.commons.TaskContext;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkhdfsPullerTask {

    private static final Logger log = LoggerFactory.getLogger(BkhdfsPullerTask.class);
    /**
     * 任务id
     */
    private long taskId;
    /**
     * hdfs配置目录
     */
    private String hdfsConfDir;
    /**
     * hdfs自定义配置
     */
    private String hdfsCustomProperty;
    /**
     * 队列的服务地址
     */
    private String channelServer;
    /**
     * 结果表
     */
    private String resultTableId;
    /**
     * 代拉取的数据在hdfs上的目录
     */
    private String dataDir;
    /**
     * 拉取到目标队列所在的topic
     */
    private String topic;
    /**
     * 任务状态
     */
    private String status;

    /**
     * 任务rt 对应的上下文
     */
    private TaskContext context;

    private String lastReadFileName = "";
    private int lastReadLineNumber;
    private String[] colsInOrder;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private Schema recordSchema;
    private Schema msgSchema;
    private Map<String, String> columns;
    private long schemaByteSize;


    public BkhdfsPullerTask(Map<String, Object> task, int instanceId) {
        this.taskId = Long.parseLong(task.get("id").toString());
        this.hdfsConfDir = task.get("hdfs_conf_dir").toString();
        this.hdfsCustomProperty = task.get("hdfs_custom_property").toString();
        this.channelServer = task.get("kafka_bs").toString();
        this.resultTableId = task.get("rt_id").toString();
        this.dataDir = task.get("data_dir").toString();
        this.topic = "table_" + resultTableId;
        this.status = task.get("status").toString();
        if (StringUtils.isNotBlank(status)) {
            String[] arr = StringUtils.split(status, "|");
            if (arr.length >= 2) {
                this.lastReadFileName = arr[0];
                this.lastReadLineNumber = Integer.parseInt(arr[1]);
            }
        }
        try {
            context = new TaskContext(HttpUtils.getRtInfo(this.resultTableId));
            columns = context.getDbColumnsTypes();
            recordSchema = AvroUtils.getRecordSchema(columns);
            msgSchema = AvroUtils.getMsgSchema(resultTableId, recordSchema);
            dataFileWriter = AvroUtils.getDataFileWriter(msgSchema);
            colsInOrder = columns.keySet().toArray(new String[columns.size()]);
            schemaByteSize = recordSchema.toString().getBytes(StandardCharsets.UTF_8).length + msgSchema.toString()
                    .getBytes(StandardCharsets.UTF_8).length;
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.ERR_PREFIX,
                    String.format("worker %s: failed to init avro for %s!", instanceId, resultTableId), e);
            this.context = null;
        }

    }

    public long getTaskId() {
        return taskId;
    }

    public String getHdfsConfDir() {
        return hdfsConfDir;
    }

    public String getHdfsCustomProperty() {
        return hdfsCustomProperty;
    }

    public String getChannelServer() {
        return channelServer;
    }

    public String getResultTableId() {
        return resultTableId;
    }

    public String getDataDir() {
        return dataDir;
    }

    public String getTopic() {
        return topic;
    }

    public String getStatus() {
        return status;
    }

    public TaskContext getContext() {
        return context;
    }

    public String getLastReadFileName() {
        return lastReadFileName;
    }

    public int getLastReadLineNumber() {
        return lastReadLineNumber;
    }

    public String[] getColsInOrder() {
        return colsInOrder;
    }

    public DataFileWriter<GenericRecord> getDataFileWriter() {
        return dataFileWriter;
    }

    public Schema getRecordSchema() {
        return recordSchema;
    }

    public Schema getMsgSchema() {
        return msgSchema;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public long getSchemaByteSize() {
        return schemaByteSize;
    }

    @Override
    public String toString() {
        StringBuilder taskStringBuilder = new StringBuilder();
        taskStringBuilder.append("{")
                .append("taskId=").append(taskId)
                .append(", hdfsConfDir=").append(hdfsConfDir)
                .append(", hdfsCustomProperty=").append(hdfsCustomProperty)
                .append(", channelServer=").append(channelServer)
                .append(", resultTableId=").append(resultTableId)
                .append(", dataDir=").append(dataDir)
                .append(", topic=").append(topic)
                .append(", status=").append(status).append("}");
        return taskStringBuilder.toString();
    }
}

