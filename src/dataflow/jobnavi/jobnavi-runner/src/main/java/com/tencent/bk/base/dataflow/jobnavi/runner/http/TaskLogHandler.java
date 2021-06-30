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

package com.tencent.bk.base.dataflow.jobnavi.runner.http;

import com.tencent.bk.base.dataflow.jobnavi.runner.service.LogAggregationService;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class TaskLogHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(TaskLogHandler.class);

    /**
     * get task log content in lines
     * @param executeId
     * @param begin
     * @param end
     * @param aggregateTime
     * @return task log content in lines
     */
    public static Map<String, Object> getTaskLogLines(long executeId, long begin, long end, long aggregateTime) {
        Map<String, Object> result = new HashMap<>();
        Long logFileSize = LogAggregationService.getTaskLogFileSize(executeId, aggregateTime);
        if (logFileSize == null) {
            return null;
        }
        if (begin >= logFileSize) {
            result.put("lines_begin", logFileSize);
            result.put("lines_end", logFileSize);
            result.put("lines", "");
            return result;
        }
        //get last char to check if begin is start of a line
        long sentinelBegin = (begin == 0) ? begin : begin - 1;
        String logContent = LogAggregationService.getTaskLog(executeId, sentinelBegin, end, aggregateTime);
        if (logContent == null) {
            return null;
        }
        int linesBegin;
        if (begin == 0) {
            linesBegin = 0;
        } else {
            linesBegin = 1;
            //find first line begin
            while (linesBegin < logContent.length() && logContent.charAt(linesBegin - 1) != '\n') {
                ++linesBegin;
            }
        }
        int linesEnd = logContent.length();
        if (end != logFileSize) {
            //find last line end
            while (linesEnd > linesBegin && logContent.charAt(linesEnd - 1) != '\n') {
                --linesEnd;
            }
        }
        long linesBeginByte = sentinelBegin + logContent.substring(0, linesBegin)
                .getBytes(StandardCharsets.UTF_8).length;
        long linesEndByte = sentinelBegin + logContent.substring(0, linesEnd).getBytes(StandardCharsets.UTF_8).length;
        result.put("lines_begin", linesBeginByte);
        if (linesEnd > linesBegin) {
            result.put("lines_end", linesEndByte);
            result.put("lines", logContent.substring(linesBegin, linesEnd));
        } else {
            result.put("lines_end", linesBeginByte);
            result.put("lines", "");
        }
        return result;
    }

    @Override
    public void doGet(Request request, Response response) throws Exception {
        try {
            long executeId = Long.parseLong(request.getParameter("execute_id"));
            long aggregateTime = Long.parseLong(request.getParameter("aggregate_time"));
            String operate = request.getParameter("operate");
            switch (operate) {
                case "file_size":
                    Long fileSize = LogAggregationService.getTaskLogFileSize(executeId, aggregateTime);
                    if (fileSize == null) {
                        writeData(false, "can not find task log, log file maybe removed.",
                                HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                    } else {
                        writeData(true, "get task log success.", HttpReturnCode.DEFAULT,
                                JsonUtils.writeValueAsString(fileSize), response);
                    }
                    break;
                case "content":
                    long begin = Long.parseLong(request.getParameter("begin"));
                    long end = Long.parseLong(request.getParameter("end"));
                    Map<String, Object> logContent = getTaskLogLines(executeId, begin, end, aggregateTime);
                    if (logContent == null) {
                        writeData(false, "can not find task log, log file maybe removed.",
                                HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                    } else {
                        writeData(true, "get task log success.", HttpReturnCode.DEFAULT,
                                JsonUtils.writeValueAsString(logContent), response);
                    }
                    break;
                case "extract":
                    String regex = request.getParameter("regex");
                    String result = LogAggregationService.extractLastFromTaskLog(executeId, regex, aggregateTime);
                    if (result == null) {
                        writeData(false, "failed to extract:'" + regex + "' from log of task:" + executeId,
                                HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                    } else {
                        writeData(true, "get task log success.", HttpReturnCode.DEFAULT, result, response);
                    }
                    break;
                default:
                    throw new IllegalArgumentException("operate " + operate + " is not support.");
            }
        } catch (Throwable e) {
            logger.error("get task log error.", e);
            writeData(false, e.getMessage(), HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
        }
    }
}
