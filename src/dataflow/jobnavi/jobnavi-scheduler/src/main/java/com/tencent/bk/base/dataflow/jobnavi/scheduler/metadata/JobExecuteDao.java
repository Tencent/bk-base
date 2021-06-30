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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.RecoveryExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface JobExecuteDao {

    void addExecute(TaskEvent eventInfo) throws NaviException;

    long getCurrentExecuteIdIndex() throws NaviException;

    void updateExecuteHost(long executeId, String host) throws NaviException;

    void updateExecuteStatus(long executeId, TaskStatus status, String info) throws NaviException;

    void updateExecuteRunningTime(long executeId) throws NaviException;

    //execute status
    TaskStatus getExecuteStatus(long executeId) throws NaviException;

    TaskStatus getExecuteStatus(String scheduleId, long scheduleTime) throws NaviException;

    //execute result
    ExecuteResult queryExecuteResult(long executeId) throws NaviException;

    ExecuteResult queryLastExecuteResult(String scheduleId, Long scheduleTime) throws NaviException;

    ExecuteResult queryLatestExecuteResult(String scheduleId, Long scheduleTime) throws NaviException;

    ExecuteResult queryLatestExecuteResultByDataTime(String scheduleId, Long dataTime) throws NaviException;

    List<ExecuteResult> queryExecuteResultByTimeRange(String scheduleId, Long[] timeRange) throws NaviException;

    List<ExecuteResult> queryExecuteResultByDataTimeRange(String scheduleId, Long[] timeRange) throws NaviException;

    List<ExecuteResult> queryExecuteResultByStatus(String scheduleId, TaskStatus status) throws NaviException;

    List<ExecuteResult> queryExecuteResultByTimeAndStatus(String scheduleId, Long scheduleTime, TaskStatus status)
            throws NaviException;

    List<ExecuteResult> queryExecuteResultBySchedule(String scheduleId, Long limit) throws NaviException;

    List<ExecuteResult> queryExecuteResultByHost(String host, TaskStatus status) throws NaviException;

    List<ExecuteResult> queryPreparingExecute(String scheduleId) throws NaviException;

    List<ExecuteResult> queryFailedExecutes(long beginTime, long endTime, String typeId) throws NaviException;

    //execute record
    List<Map<String, Object>> queryExecuteByTimeRange(
            String scheduleId, Long startTime, Long endTime, Integer limit) throws NaviException;

    List<Map<String, Object>> queryExecuteByTimeRangeAndStatus(String scheduleId, Long startTime, Long endTime,
            TaskStatus status, Integer limit) throws NaviException;

    List<Map<String, Object>> queryAllExecuteByTimeRange(String scheduleId, Long startTime, Long endTime, Integer limit)
            throws NaviException;

    Map<String, Map<String, Integer>> queryExecuteStatusAmount(Long startTime, Long endTime)
            throws NaviException;

    Map<String, Map<String, Integer>> queryExecuteStatusAmountByCreateAt(Long startTime, Long endTime)
            throws NaviException;

    Map<String, Integer> queryRunningExecuteNumbers() throws NaviException;

    void deleteExpireExecute(long expire) throws NaviException;

    //recovery execute
    void addRecoveryExecute(long executeId, TaskInfo info) throws NaviException;

    void updateRecoveryExecute(long executeId, TaskInfo info) throws NaviException;

    void setRecoveryExecuteSuccess(TaskInfo info) throws NaviException;

    List<TaskInfo> getNotRecoveryTask() throws NaviException;

    List<RecoveryExecuteResult> queryRecoveryResultByScheduleId(String scheduleId, Long limit)
            throws NaviException;

    int getRecoveryExecuteTimes(String scheduleId, Long scheduleTime) throws NaviException;

    void deleteExpireRecovery(long expire) throws NaviException;

    //runner execute
    Map<String, Set<Long>> listRunnerExecute() throws NaviException;
}
