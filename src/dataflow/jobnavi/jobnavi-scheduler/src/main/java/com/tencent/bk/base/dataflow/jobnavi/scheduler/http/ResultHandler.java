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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.http;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.RecoveryExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class ResultHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(ResultHandler.class);

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        AbstractJobDao dao = MetaDataManager.getJobDao();
        if ("query_execute_status".equals(operate)) {
            String executeIdStr = request.getParameter("execute_id");
            if (StringUtils.isEmpty(executeIdStr)) {
                writeData(false, "[execute_id] required. ", response);
                return;
            }
            Long executeId;
            try {
                executeId = Long.parseLong(executeIdStr);
                ExecuteResult executeResult = dao.queryExecuteResult(executeId);
                if (executeResult != null) {
                    writeData(true, "query execute " + executeId + " status success.",
                            HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(executeResult.toHTTPJson()), response);
                } else {
                    writeData(false, "can not find execute " + executeId + ".",
                            HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
                }

            } catch (Exception e) {
                logger.error("query execute " + executeIdStr + " status error.", e);
                writeData(false, "query execute " + executeIdStr + " status error." + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else if ("query_execute".equals(operate)) {
            String scheduleId = request.getParameter("schedule_id");
            String status = request.getParameter("status");
            String limit = request.getParameter("limit");
            if (StringUtils.isEmpty(scheduleId)) {
                writeData(false, "[schedule_id] required. ", response);
                return;
            }

            try {
                List<ExecuteResult> executeResults;
                if (StringUtils.isNotEmpty(status)) {
                    if (TaskStatus.values(status) == null) {
                        writeData(false, "status [" + status + "] is not validate. ", response);
                        return;
                    }
                    String scheduleTimeStr = request.getParameter("schedule_time");
                    if (StringUtils.isNotEmpty(scheduleTimeStr)) {
                        Long scheduleTime = Long.parseLong(scheduleTimeStr);
                        executeResults = dao
                                .queryExecuteResultByTimeAndStatus(scheduleId, scheduleTime, TaskStatus.values(status));
                    } else {
                        executeResults = dao.queryExecuteResultByStatus(scheduleId, TaskStatus.values(status));
                    }

                } else if (StringUtils.isNotEmpty(limit)) {
                    executeResults = dao.queryExecuteResultBySchedule(scheduleId, Long.parseLong(limit));
                } else {
                    throw new NaviException("query execute must contain support filter. like 'status' or 'limit'");
                }
                List<Map<String, Object>> results = new ArrayList<>();
                for (ExecuteResult executeResult : executeResults) {
                    results.add(executeResult.toHTTPJson());
                }
                writeData(true, "query execute success.", HttpReturnCode.DEFAULT, JsonUtils.writeValueAsString(results),
                        response);
            } catch (NaviException e) {
                logger.error("query execute error.", e);
                writeData(false, "query execute error." + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else if ("query_recovery_execute".equals(operate)) {
            String scheduleId = request.getParameter("schedule_id");
            String limit = request.getParameter("limit");
            if (StringUtils.isEmpty(scheduleId)) {
                writeData(false, "[schedule_id] required. ", response);
                return;
            }
            try {
                List<RecoveryExecuteResult> executeResults;
                if (StringUtils.isNotEmpty(limit)) {
                    executeResults = dao.queryRecoveryResultByScheduleId(scheduleId, Long.parseLong(limit));
                } else {
                    throw new NaviException("query recovery execute must contain support filter. like 'limit'");
                }
                List<Map<String, Object>> results = new ArrayList<>();
                for (RecoveryExecuteResult executeResult : executeResults) {
                    results.add(executeResult.toHTTPJson());
                }
                writeData(true, "query recovery execute success.", HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(results), response);
            } catch (NaviException e) {
                logger.error("query recovery execute error.", e);
                writeData(false, "query recovery execute error." + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else if ("query_execute_by_time".equals(operate)) {
            final int RESULT_LIST_LIMIT = 48;
            String scheduleId = request.getParameter("schedule_id");
            String startTimeStr = request.getParameter("start_time");
            String endTimeStr = request.getParameter("end_time");
            if (StringUtils.isEmpty(scheduleId) || StringUtils.isEmpty(startTimeStr) || StringUtils
                    .isEmpty(endTimeStr)) {
                writeData(false, "[schedule_id][start_time][end_time] required. ", response);
                return;
            }
            try {
                List<Map<String, Object>> result;
                Long startTime = Long.parseLong(startTimeStr);
                Long endTime = Long.parseLong(endTimeStr);
                result = dao.queryAllExecuteByTimeRange(scheduleId, startTime, endTime, RESULT_LIST_LIMIT);
                writeData(true, "query execute by time range success.", HttpReturnCode.DEFAULT,
                        JsonUtils.writeValueAsString(result), response);
            } catch (Exception e) {
                logger.error("query execute by time range error.", e);
                writeData(false, "query execute by time range error." + e.getMessage(),
                        HttpReturnCode.ERROR_RUNTIME_EXCEPTION, response);
            }
        } else {
            writeData(false, "operate " + operate + " not support.", HttpReturnCode.ERROR_PARAM_INVALID, response);
        }
    }
}
