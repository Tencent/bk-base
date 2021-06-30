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

package com.tencent.bk.base.dataflow.hdfs.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.jobnavi.api.JobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;

import java.util.HashMap;
import java.util.Map;

/**
 * parquet 读取服务
 */
public class ParquetReaderService implements JobNaviTask {

    private static final Logger logger = Logger.getLogger(ParquetReaderService.class);

    public void run(TaskEvent taskEvent) throws Throwable {
        Thread.currentThread().join();
    }

    /**
     * get event listener
     * @param eventName event name
     * @return event listener
     */
    public EventListener getEventListener(String eventName) {

        if ("last_line".equals(eventName)) {
            //读取最新一条数据
            return new EventListener() {
                public TaskEventResult doEvent(TaskEvent taskEvent) {
                    TaskEventResult result = new TaskEventResult();
                    try {
                        String eventInfo = taskEvent.getContext().getEventInfo();
                        ObjectMapper objectMapper = new ObjectMapper();
                        Map<?, ?> parameters = objectMapper.readValue(eventInfo, HashMap.class);
                        String path = parameters.get("path").toString();
                        ParquetReader<GenericData.Record> reader = AvroParquetReader
                                .<GenericData.Record>builder(new Path(path)).build();
                        String lastLine = reader.read().toString();
                        logger.info("lastLine: " + lastLine);
                        result.setSuccess(true);
                        result.setProcessInfo(lastLine);
                    } catch (Throwable e) {
                        logger.error("read parquet file error", e);
                        result.setSuccess(false);
                        result.setProcessInfo(e.getMessage());
                    }
                    return result;
                }
            };
        }
        return null;
    }
}
