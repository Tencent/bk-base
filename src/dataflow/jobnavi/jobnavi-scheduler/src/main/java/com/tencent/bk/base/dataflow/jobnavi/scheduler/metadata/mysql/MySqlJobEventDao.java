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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.mysql;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.JobEventDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MySqlJobTaskTypeDao;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.CustomTaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventContext;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventFactory;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEventResult;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MySqlJobEventDao implements JobEventDao {

    private static final String SQL_EVENT_ADD
            = "INSERT INTO jobnavi_event_log(event_id, exec_id, event_name, event_time, event_info, change_status) "
            + "VALUES(?, ?, ?, ?, ?, ?)";
    private static final String SQL_EVENT_UPDATE_RESULT
            = "UPDATE jobnavi_event_log SET process_status = 1, process_success = ?, process_info = ? "
            + "WHERE event_id = ?";
    private static final String SQL_EVENT_QEURY_RESULT
            = "SELECT process_success, process_info FROM jobnavi_event_log WHERE event_id=? AND process_status=1";
    private static final String SQL_EVENT_CONTEXT_QUERY_BY_ID
            = "SELECT schedule_id, schedule_time, data_time, host, status, info, rank, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE exec_id = ?";
    private static final String SQL_EVENT_QUERY_MAX_ID = "SELECT max(event_id) FROM jobnavi_event_log";
    private static final String SQL_EVENT_QUERY_ALL_NOT_PROCESS
            = "SELECT e.event_id, e.exec_id, e.event_name, e.event_info, e.change_status, c.rank, c.schedule_id, "
            + "    c.schedule_time, c.host, s.extra_info, r.retry_times AS retry_quota, "
            + "    r.interval_time AS retry_interval, re.retry_times AS retry_count, s.type_id, s.type_tag, "
            + "    s.decommission_timeout, s.node_label_name, c.status, c.data_time "
            + "FROM "
            + "    jobnavi_event_log e "
            + "JOIN jobnavi_execute_log c "
            + "ON e.exec_id = c.exec_id "
            + "JOIN "
            + "    jobnavi_schedule_info s "
            + "ON c.schedule_id = s.schedule_id "
            + "LEFT JOIN "
            + "    jobnavi_recovery_info r "
            + "ON s.schedule_id = r.schedule_id "
            + "LEFT JOIN "
            + "    jobnavi_recovery_execute_log re "
            + "ON re.exec_id = e.exec_id "
            + "WHERE e.process_status = 0 AND s.schedule_id IS NOT NULL "
            + "ORDER BY e.event_id";
    private static final String SQL_EVENT_DELETE_EXPIRE = "DELETE FROM jobnavi_event_log WHERE event_time < ?";

    private static final MySqlJobExecuteSqlDao MY_JOB_EXECUTE_SQL_DAO = new MySqlJobExecuteSqlDao();
    private static final MySqlJobTaskTypeDao MY_SQL_JOB_TASK_TYPE_DAO = new MySqlJobTaskTypeDao();

    @Override
    public void addEvent(TaskEventInfo eventInfo) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        TaskEvent event = eventInfo.getTaskEvent();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EVENT_ADD);
            ps.setLong(1, eventInfo.getId());
            ps.setLong(2, event.getContext().getExecuteInfo().getId());
            ps.setString(3, event.getEventName());
            ps.setLong(4, System.currentTimeMillis());
            ps.setString(5, event.getContext().getEventInfo());
            ps.setString(6, event.getChangeStatus() == null ? null : event.getChangeStatus().toString());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void updateEventResult(long eventId, TaskEventResult result) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EVENT_UPDATE_RESULT);
            ps.setInt(1, result.isSuccess() ? 1 : 0);
            ps.setString(2, result.getProcessInfo());
            ps.setLong(3, eventId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public TaskEventResult queryEventResult(long eventId) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EVENT_QEURY_RESULT);
            ps.setLong(1, eventId);
            rs = ps.executeQuery();
            if (rs.next()) {
                TaskEventResult result = new TaskEventResult();
                result.setSuccess(rs.getBoolean(1));
                result.setProcessInfo(rs.getString(2));
                return result;
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return null;
    }

    @Override
    public EventContext getEventContext(Long executeId) throws NaviException {
        EventContext context = new EventContext();
        ExecuteInfo executeInfo = new ExecuteInfo();
        executeInfo.setId(executeId);
        TaskInfo taskInfo = new TaskInfo();
        context.setTaskInfo(taskInfo);
        context.setExecuteInfo(executeInfo);

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EVENT_CONTEXT_QUERY_BY_ID);
            ps.setLong(1, executeId);
            rs = ps.executeQuery();
            while (rs.next()) {
                taskInfo.setScheduleId(rs.getString("schedule_id"));
                taskInfo.setScheduleTime(rs.getLong("schedule_time"));
                taskInfo.setDataTime(rs.getLong("data_time"));
                executeInfo.setHost(rs.getString("host"));
                executeInfo.setRank(rs.getDouble("rank"));
            }
            int recoveryTimes = MY_JOB_EXECUTE_SQL_DAO
                    .getRecoveryExecuteTimes(taskInfo.getScheduleId(), taskInfo.getScheduleTime());
            TaskRecoveryInfo taskRecoveryInfo = new TaskRecoveryInfo();
            taskRecoveryInfo.setRecoveryTimes(recoveryTimes);
            taskInfo.setRecoveryInfo(taskRecoveryInfo);
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return context;
    }

    @Override
    public long getCurrentEventIdIndex() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EVENT_QUERY_MAX_ID);
            rs = ps.executeQuery();
            while (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return 0;
    }

    @Override
    public List<TaskEventInfo> getNotProcessTaskEvents() throws NaviException {
        List<TaskEventInfo> eventInfos = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            //query all task type info
            Map<String, Map<String, TaskType>> taskTypeInfo = MY_SQL_JOB_TASK_TYPE_DAO.listTaskType();
            //query all task type default tag
            Map<String, String> taskTypeDefaultTag = MY_SQL_JOB_TASK_TYPE_DAO.listTaskTypeDefaultTag();
             //query all not processed event
            ps = con.prepareStatement(SQL_EVENT_QUERY_ALL_NOT_PROCESS);
            rs = ps.executeQuery();
            while (rs.next()) {
                TaskEventInfo info = new TaskEventInfo();
                info.setId(rs.getLong("event_id"));
                String eventName = rs.getString("event_name");
                TaskEvent event = TaskEventFactory.generateTaskEvent(eventName);
                event.setEventName(eventName);
                event.setChangeStatus(TaskStatus.values(rs.getString("change_status")));
                 //execute info
                ExecuteInfo executeInfo = new ExecuteInfo();
                executeInfo.setId(rs.getLong("exec_id"));
                executeInfo.setHost(rs.getString("host"));
                TaskStatus executeStatus = TaskStatus.valueOf(rs.getString("status"));
                if (event instanceof CustomTaskEvent && (executeInfo.getHost() == null
                        || executeStatus != TaskStatus.running)) {
                    continue;
                }
                executeInfo.setRank(rs.getDouble("rank"));
                //event context
                EventContext context = new EventContext();
                context.setExecuteInfo(executeInfo);
                 //task info
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.setScheduleId(rs.getString("schedule_id"));
                taskInfo.setScheduleTime(rs.getLong("schedule_time"));
                taskInfo.setDataTime(rs.getLong("data_time"));
                taskInfo.setExtraInfo(rs.getString("extra_info"));
                taskInfo.setDecommissionTimeout(rs.getString("decommission_timeout"));
                String nodeLabel = rs.getString("node_label_name");
                taskInfo.setNodeLabel(nodeLabel);
                String typeId = rs.getString("type_id");
                String typeTag = rs.getString("type_tag");
                if (typeTag == null) {
                    typeTag = taskTypeDefaultTag.get(typeId);
                    if (nodeLabel != null && taskTypeDefaultTag.containsKey(typeId + "@" + nodeLabel)) {
                        typeTag = taskTypeDefaultTag.get(typeId + "@" + nodeLabel);
                    }
                }
                if (taskTypeInfo.containsKey(typeId) && taskTypeInfo.get(typeId).containsKey(typeTag)) {
                    taskInfo.setType(taskTypeInfo.get(typeId).get(typeTag));
                }
                TaskRecoveryInfo recoveryInfo = new TaskRecoveryInfo();
                if (rs.getString("retry_quota") != null) {
                    recoveryInfo.setRecoveryEnable(true);
                    recoveryInfo.setMaxRecoveryTimes(rs.getInt("retry_quota"));
                    recoveryInfo.setIntervalTime(rs.getString("retry_interval"));
                    recoveryInfo.setRecoveryTimes(rs.getString("retry_count") == null ? 0 : rs.getInt("retry_count"));
                }
                taskInfo.setRecoveryInfo(recoveryInfo);
                context.setTaskInfo(taskInfo);
                context.setEventInfo(rs.getString("event_info"));

                event.setContext(context);
                info.setTaskEvent(event);
                eventInfos.add(info);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return eventInfos;
    }

    @Override
    public void deleteExpireEvent(long expire) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EVENT_DELETE_EXPIRE);
            ps.setLong(1, expire);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }
}
