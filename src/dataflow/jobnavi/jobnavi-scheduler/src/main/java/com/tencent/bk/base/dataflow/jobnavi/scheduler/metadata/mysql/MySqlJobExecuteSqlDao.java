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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.JobExecuteDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MySqlJobTaskTypeDao;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.ExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.RecoveryExecuteResult;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskRecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MySqlJobExecuteSqlDao implements JobExecuteDao {

    private static final String SQL_EXECUTE_ADD
            = "INSERT INTO jobnavi_execute_log(exec_id, schedule_id, schedule_time, data_time, status, host, info, "
            + "type_id, rank) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_EXECUTE_QUERY_MAX_ID = "SELECT max(exec_id) FROM jobnavi_execute_log";
    private static final String SQL_EXECUTE_UPDATE_HOST = "UPDATE jobnavi_execute_log SET host = ? WHERE exec_id = ?";
    private static final String SQL_EXECUTE_UPDATE_STATUS_BY_ID
            = "UPDATE jobnavi_execute_log SET status = ?, info = ? WHERE exec_id = ?";
    private static final String SQL_EXECUTE_UPDATE_START_AT_BY_ID
            = "UPDATE jobnavi_execute_log SET started_at = ? WHERE exec_id = ?";
    private static final String SQL_EXECUTE_QUERY_STATUS_BY_ID
            = "SELECT status FROM jobnavi_execute_log WHERE exec_id = ?";
    private static final String SQL_EXECUTE_QUERY_STATUS_BY_NAME_AND_SCHEDULE_TIME
            = "SELECT status FROM jobnavi_execute_log WHERE schedule_id = ? AND schedule_time = ? "
            + "ORDER BY exec_id DESC LIMIT 1";
    private static final String SQL_EXECUTE_QUERY_BY_ID
            = "SELECT schedule_id, schedule_time, data_time, host, status, info, rank, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE exec_id = ?";
    private static final String SQL_EXECUTE_QUERY_LAST_BY_SCHEDULE_TIME
            = "SELECT exec_id, schedule_id, schedule_time, status, host, info, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND schedule_time < ? "
            + "ORDER BY schedule_time DESC, exec_id DESC LIMIT 1";
    private static final String SQL_EXECUTE_QUERY_LATEST_BY_SCHEDULE_TIME
            = "SELECT exec_id, schedule_id, schedule_time, status, host, info, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND schedule_time = ? ORDER BY exec_id DESC LIMIT 1";
    private static final String SQL_EXECUTE_QUERY_LATEST_BY_DATA_TIME
            = "SELECT exec_id, schedule_id, schedule_time, data_time, status, host, info, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND data_time = ? ORDER BY exec_id DESC LIMIT 1";
    private static final String SQL_EXECUTE_QUERY_TIME_RANGE
            = "SELECT exec_id, schedule_id, schedule_time, data_time, status, host, info, rank, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND schedule_time >= ? AND schedule_time <= ?";
    private static final String SQL_EXECUTE_QUERY_DATA_TIME_RANGE
            = "SELECT exec_id, schedule_id, schedule_time, data_time, status, host, info, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND data_time >= ? AND data_time < ?";
    private static final String SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_STATUS
            = "SELECT exec_id, schedule_id, schedule_time, status, host, info, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND status = ? ORDER BY exec_id DESC";
    private static final String SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_TIME_AND_STATUS
            = "SELECT exec_id, schedule_id, schedule_time, status, host, info, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND status = ? AND schedule_time = ? "
            + "ORDER BY exec_id DESC";
    private static final String SQL_EXECUTE_QUERY_BY_SCHEDULE_ID
            = "SELECT exec_id, schedule_id, schedule_time, status, host, info, created_at, started_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? ORDER BY schedule_time DESC LIMIT ?";
    private static final String SQL_EXECUTE_QUERY_BY_HOST_AND_STATUS
            = "SELECT exec_id, schedule_id, schedule_time, status, host, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE host = ? AND status = ?";
    private static final String SQL_EXECUTE_QUERY_PREPARING_BY_SCHEDULE_ID
            = "SELECT exec_id, schedule_id, schedule_time, host, info, rank, created_at, updated_at "
            + "FROM jobnavi_execute_log WHERE schedule_id = ? AND status = 'preparing'";
    private static final String SQL_EXECUTE_QUERY_FAILED
            = "SELECT `failed_execute`.`exec_id` AS `exec_id`, `schedule_id`, `schedule_time`, `data_time`, "
            + "    `status`, `host`, `created_at`, `updated_at` "
            + "FROM "
            + "("
            + "    SELECT MAX(`exec_id`) AS `exec_id` "
            + "    FROM "
            + "    ("
            + "        SELECT `schedule_id`, `schedule_time` "
            + "        FROM "
            + "            `jobnavi_execute_log` "
            + "        WHERE `status` in ('failed', 'lost') AND `schedule_time` >= ? "
            + "            AND `schedule_time` < ? AND (`type_id` = ? || '' = ?)"
            + "    ) l "
            + "    LEFT JOIN "
            + "        `jobnavi_execute_log` r "
            + "    ON l.`schedule_id` = r.`schedule_id` AND l.`schedule_time` = r.`schedule_time` "
            + "    GROUP BY l.`schedule_id`, l.`schedule_time` "
            + ") `failed_execute` "
            + "LEFT JOIN "
            + "    `jobnavi_execute_log` "
            + "ON `failed_execute`.`exec_id` = `jobnavi_execute_log`.`exec_id` "
            + "WHERE `status` in ('failed', 'lost')";
    private static final String SQL_EXECUTE_QUERY_BY_TIME_RANGE
            = "SELECT schedule_time, data_time, `status`, created_at, started_at, updated_at, r.info, r.exec_id "
            + "FROM "
            + "    jobnavi_execute_log r "
            + "RIGHT JOIN "
            + "("
            + "    SELECT MAX(exec_id) exec_id "
            + "    FROM "
            + "        jobnavi_execute_log "
            + "    WHERE schedule_id = ? AND schedule_time between ? and ? "
            + "    GROUP BY schedule_time ORDER BY schedule_time DESC"
            + ") l "
            + "ON l.`exec_id` = r.`exec_id` LIMIT ?";
    private static final String SQL_EXECUTE_QUERY_BY_TIME_RANGE_AND_STATUS
            = "SELECT schedule_time, data_time, created_at, started_at, updated_at, r.exec_id "
            + "FROM "
            + "    jobnavi_execute_log r "
            + "RIGHT JOIN "
            + "("
            + "    SELECT MAX(exec_id) exec_id "
            + "    FROM "
            + "        jobnavi_execute_log "
            + "    WHERE schedule_id = ? AND schedule_time between ? and ? and `status` = ? "
            + "    GROUP BY schedule_time ORDER BY schedule_time DESC"
            + ") l "
            + "ON l.`exec_id` = r.`exec_id` LIMIT ?";
    private static final String SQL_EXECUTE_QUERY_ALL_BY_TIME_RANGE
            = "SELECT schedule_time, data_time, exec_id, `status`, created_at, updated_at, info "
            + "FROM "
            + "    jobnavi_execute_log "
            + "WHERE schedule_id = ? AND schedule_time between ? and ? "
            + "ORDER BY schedule_time DESC, exec_id DESC LIMIT ?";
    private static final String SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_TIME_RANGE
            = "SELECT count(*), status, from_unixtime(schedule_time / 1000, '%Y%m%d%H') AS time "
            + "FROM jobnavi_execute_log WHERE schedule_time >= ? AND schedule_time <= ? "
            + "GROUP BY status, time ORDER BY time, status";
    private static final String SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_CREATE_TIME_RANGE
            = "SELECT count(*), status, from_unixtime(unix_timestamp(created_at), '%Y%m%d%H') AS time "
            + "FROM jobnavi_execute_log "
            + "WHERE created_at >= from_unixtime(? / 1000, '%Y%-%m-%d %H:%i:%s') "
            + "    AND created_at <= from_unixtime(? / 1000, '%Y%-%m-%d %H:%i:%s') "
            + "GROUP BY status, time ORDER BY time, status";
    private static final String SQL_EXECUTE_QUERY_RUNNING_NUMBER
            = "SELECT s.schedule_id, count(*) FROM jobnavi_schedule_info s JOIN jobnavi_execute_log e "
            + "ON s.schedule_id = e.schedule_id AND s.max_running_task <> -1 AND e.status = 'running' "
            + "GROUP BY s.schedule_id";
    private static final String SQL_EXECUTE_DELETE_EXPIRE
            = "DELETE l "
            + "FROM "
            + "    `jobnavi_execute_log` l "
            + "LEFT JOIN "
            + "    `jobnavi_execute_reference_cache` r "
            + "ON l.`schedule_id` = r.`schedule_id` AND l.`data_time` >= r.`begin_data_time` "
            + "    AND l.`data_time` < r.`end_data_time` "
            + "WHERE r.`schedule_id` IS NULL AND l.`updated_at` < ? AND `status` NOT IN ('preparing','running')";

    private static final String SQL_RECOVERY_EXECUTE_ADD
            = "INSERT INTO jobnavi_recovery_execute_log(exec_id,schedule_id, schedule_time, data_time, retry_times, "
            + "recovery_status, rank) VALUES(?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_RECOVERY_EXECUTE_UPDATE_EXEC_ID
            = "UPDATE jobnavi_recovery_execute_log SET exec_id = ? "
            + "WHERE schedule_id = ? AND data_time = ? AND retry_times = ?";
    private static final String SQL_RECOVERY_EXECUTE_UPDATE_RECOVERY
            = "UPDATE jobnavi_recovery_execute_log SET recovery_status = 1 "
            + "WHERE schedule_id = ? AND schedule_time = ? AND retry_times = ?";
    private static final String SQL_RECOVERY_EXECUTE_NOT_PROCESS
            = "SELECT j.`retry_times` AS `retry_count`, j.`schedule_id` AS `schedule_id`, "
            + "    j.`schedule_time` AS `schedule_time`, j.`data_time` AS `data_time`, "
            + "    r.`retry_times` AS `retry_quota`, r.`interval_time` AS `retry_interval`, "
            + "    `extra_info`, `type_id`, `type_tag`, `decommission_timeout`, `node_label_name`, "
            + "    UNIX_TIMESTAMP(j.created_at) AS `created_at`, `rank` "
            + "FROM "
            + "( "
            + "    SELECT l.`retry_times`, l.`schedule_id`, l.`schedule_time`, l.`data_time`, l.`recovery_status`, "
            + "        l.`exec_id`, l.`rank`, l.`created_at` "
            + "    FROM "
            + "        `jobnavi_recovery_execute_log` l "
            + "    JOIN "
            + "    ( "
            + "        SELECT MAX(`retry_times`) AS `retry_times`, `schedule_id`, `schedule_time` "
            + "        FROM "
            + "            `jobnavi_recovery_execute_log` rel "
            + "        LEFT JOIN "
            + "            `jobnavi_event_log` el "
            + "        ON rel.`exec_id` = el.`exec_id` AND el.`event_name` = 'preparing' "
            + "        WHERE `recovery_status` = 0 AND el.`exec_id` IS NULL "
            + "        GROUP BY `schedule_id`, `schedule_time` "
            + "    ) c "
            + "    ON c.`retry_times` = l.`retry_times` AND c.`schedule_id` = l.`schedule_id` "
            + "        AND c.`schedule_time` = l.`schedule_time` "
            + ") j "
            + "JOIN "
            + "    `jobnavi_recovery_info` r "
            + "ON j.`schedule_id` = r.`schedule_id` "
            + "JOIN "
            + "    `jobnavi_schedule_info` s "
            + "ON j.`schedule_id` = s.`schedule_id`";
    private static final String SQL_RECOVERY_EXECUTE_DELETE_EXPIRE
            = "DELETE FROM jobnavi_recovery_execute_log WHERE created_at < ?";
    private static final String SQL_RECOVERY_EXECUTE_RESULT_BY_SCHEDULE_ID
            = "SELECT exec_id, schedule_id, schedule_time,retry_times,recovery_status,created_at "
            + "FROM jobnavi_recovery_execute_log WHERE schedule_id = ? ORDER BY schedule_time DESC LIMIT ?";
    private static final String SQL_RECOVERY_EXECUTE_QUERY_TIMES
            = "SELECT max(retry_times) FROM jobnavi_recovery_execute_log WHERE schedule_id = ? AND schedule_time = ?";

    private static final String SQL_RUNNER_EXECUTE_LIST
            = "SELECT `host`, `exec_id` FROM jobnavi_execute_log WHERE `status` = 'running' ";

    private static final MySqlJobTaskTypeDao MY_SQL_JOB_TASK_TYPE_DAO = new MySqlJobTaskTypeDao();

    @Override
    public void addExecute(TaskEvent event) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_ADD);
            ps.setLong(1, event.getContext().getExecuteInfo().getId());
            ps.setString(2, event.getContext().getTaskInfo().getScheduleId());
            ps.setLong(3, event.getContext().getTaskInfo().getScheduleTime());
            ps.setLong(4, event.getContext().getTaskInfo().getDataTime());
            ps.setString(5, event.getChangeStatus().name());
            ps.setString(6, event.getContext().getExecuteInfo().getHost());
            ps.setString(7, event.getContext().getEventInfo());
            ps.setString(8, event.getContext().getTaskInfo().getType().getName());
            ps.setDouble(9, event.getContext().getExecuteInfo().getRank());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public long getCurrentExecuteIdIndex() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_MAX_ID);
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
    public void updateExecuteHost(long executeId, String host) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_UPDATE_HOST);
            ps.setString(1, host);
            ps.setLong(2, executeId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }


    @Override
    public void updateExecuteStatus(long executeId, TaskStatus status, String info) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_UPDATE_STATUS_BY_ID);
            ps.setString(1, status.toString());
            ps.setString(2, info);
            ps.setLong(3, executeId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void updateExecuteRunningTime(long executeId) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_UPDATE_START_AT_BY_ID);
            ps.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            ps.setLong(2, executeId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public TaskStatus getExecuteStatus(long executeId) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_STATUS_BY_ID);
            ps.setLong(1, executeId);
            rs = ps.executeQuery();
            if (rs.next()) {
                return TaskStatus.valueOf(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return TaskStatus.none;
    }

    @Override
    public TaskStatus getExecuteStatus(String scheduleId, long scheduleTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_STATUS_BY_NAME_AND_SCHEDULE_TIME);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            rs = ps.executeQuery();
            if (rs.next()) {
                return TaskStatus.valueOf(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return TaskStatus.none;
    }

    @Override
    public ExecuteResult queryExecuteResult(long executeId) throws NaviException {
        ExecuteResult result = new ExecuteResult();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_BY_ID);
            ps.setLong(1, executeId);
            rs = ps.executeQuery();
            if (rs.next()) {
                result.setScheduleId(rs.getString("schedule_id"));
                result.setScheduleTime(rs.getLong("schedule_time"));
                result.setStatus(TaskStatus.valueOf(rs.getString("status")));
                ExecuteInfo executeInfo = new ExecuteInfo();
                executeInfo.setId(executeId);
                executeInfo.setHost(rs.getString("host"));
                result.setExecuteInfo(executeInfo);
                result.setResultInfo(rs.getString("info"));
                result.setCreatedAt(
                        rs.getTimestamp("created_at") != null ? rs.getTimestamp("created_at").getTime() : -1);
                result.setUpdatedAt(
                        rs.getTimestamp("updated_at") != null ? rs.getTimestamp("updated_at").getTime() : -1);
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
    public ExecuteResult queryLastExecuteResult(String scheduleId, Long scheduleTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_LAST_BY_SCHEDULE_TIME);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            rs = ps.executeQuery();
            if (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(5));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setStatus(TaskStatus.valueOf(rs.getString(4)));
                result.setResultInfo(rs.getString(6));
                result.setCreatedAt(rs.getTimestamp(7).getTime());
                result.setUpdatedAt(rs.getTimestamp(8) != null ? rs.getTimestamp(8).getTime() : 0);
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
    public ExecuteResult queryLatestExecuteResult(String scheduleId, Long scheduleTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_LATEST_BY_SCHEDULE_TIME);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            rs = ps.executeQuery();
            if (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(5));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setStatus(TaskStatus.valueOf(rs.getString(4)));
                result.setResultInfo(rs.getString(6));
                result.setCreatedAt(rs.getTimestamp(7).getTime());
                result.setUpdatedAt(rs.getTimestamp(8) != null ? rs.getTimestamp(8).getTime() : 0);
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
    public ExecuteResult queryLatestExecuteResultByDataTime(String scheduleId, Long dataTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_LATEST_BY_DATA_TIME);
            ps.setString(1, scheduleId);
            ps.setLong(2, dataTime);
            rs = ps.executeQuery();
            if (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(6));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setDataTime(rs.getLong(4));
                result.setStatus(TaskStatus.valueOf(rs.getString(5)));
                result.setResultInfo(rs.getString(7));
                result.setCreatedAt(rs.getTimestamp(8).getTime());
                result.setUpdatedAt(rs.getTimestamp(9) != null ? rs.getTimestamp(9).getTime() : 0);
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
    public List<ExecuteResult> queryExecuteResultByTimeRange(String scheduleId, Long[] timeRange) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_TIME_RANGE);
            ps.setString(1, scheduleId);
            ps.setLong(2, timeRange[0]);
            ps.setLong(3, timeRange[1]);
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(6));
                info.setRank(rs.getDouble(8));
                ExecuteResult result = new ExecuteResult();
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setDataTime(rs.getLong(4));
                result.setStatus(TaskStatus.valueOf(rs.getString(5)));
                result.setResultInfo(rs.getString(7));
                result.setCreatedAt(rs.getTimestamp(9).getTime());
                result.setUpdatedAt(rs.getTimestamp(10) != null ? rs.getTimestamp(10).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executes;
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByDataTimeRange(String scheduleId, Long[] timeRange)
            throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_DATA_TIME_RANGE);
            ps.setString(1, scheduleId);
            ps.setLong(2, timeRange[0]);
            ps.setLong(3, timeRange[1]);
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(6));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setDataTime(rs.getLong(4));
                result.setStatus(TaskStatus.valueOf(rs.getString(5)));
                result.setResultInfo(rs.getString(7));
                result.setCreatedAt(rs.getTimestamp(8).getTime());
                result.setUpdatedAt(rs.getTimestamp(9) != null ? rs.getTimestamp(9).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executes;
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByStatus(String scheduleId, TaskStatus status) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_STATUS);
            ps.setString(1, scheduleId);
            ps.setString(2, status.name());
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(5));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setStatus(TaskStatus.valueOf(rs.getString(4)));
                result.setResultInfo(rs.getString(6));
                result.setCreatedAt(rs.getTimestamp(7).getTime());
                result.setUpdatedAt(rs.getTimestamp(8) != null ? rs.getTimestamp(8).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executes;
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByTimeAndStatus(String scheduleId, Long scheduleTime,
            TaskStatus status) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_TIME_AND_STATUS);
            ps.setString(1, scheduleId);
            ps.setString(2, status.name());
            ps.setLong(3, scheduleTime);
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(5));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setStatus(TaskStatus.valueOf(rs.getString(4)));
                result.setResultInfo(rs.getString(6));
                result.setCreatedAt(rs.getTimestamp(7).getTime());
                result.setUpdatedAt(rs.getTimestamp(8) != null ? rs.getTimestamp(8).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executes;
    }

    @Override
    public List<ExecuteResult> queryExecuteResultBySchedule(String scheduleId, Long limit) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_BY_SCHEDULE_ID);
            ps.setString(1, scheduleId);
            ps.setLong(2, limit);
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(5));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setStatus(TaskStatus.valueOf(rs.getString(4)));
                result.setResultInfo(rs.getString(6));
                result.setCreatedAt(rs.getTimestamp(7).getTime());
                result.setStartedAt(rs.getTimestamp(8) != null ? rs.getTimestamp(8).getTime() : 0);
                result.setUpdatedAt(rs.getTimestamp(9) != null ? rs.getTimestamp(9).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executes;
    }

    @Override
    public List<ExecuteResult> queryExecuteResultByHost(String host, TaskStatus status) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_BY_HOST_AND_STATUS);
            ps.setString(1, host);
            ps.setString(2, status.name());
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(5));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setStatus(TaskStatus.valueOf(rs.getString(4)));
                result.setCreatedAt(rs.getTimestamp(6).getTime());
                result.setUpdatedAt(rs.getTimestamp(7) != null ? rs.getTimestamp(7).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executes;
    }

    @Override
    public List<ExecuteResult> queryPreparingExecute(String scheduleId) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_PREPARING_BY_SCHEDULE_ID);
            ps.setString(1, scheduleId);
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(4));
                info.setRank(rs.getDouble(6));
                ExecuteResult result = new ExecuteResult();
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setStatus(TaskStatus.preparing);
                result.setResultInfo(rs.getString(5));
                result.setCreatedAt(rs.getTimestamp(7).getTime());
                result.setUpdatedAt(rs.getTimestamp(8) != null ? rs.getTimestamp(8).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }

        return executes;
    }

    @Override
    public List<ExecuteResult> queryFailedExecutes(long beginTime, long endTime, String typeId) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteResult> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_FAILED);
            ps.setLong(1, beginTime);
            ps.setLong(2, endTime);
            ps.setString(3, typeId);
            ps.setString(4, typeId);
            rs = ps.executeQuery();
            while (rs.next()) {
                ExecuteResult result = new ExecuteResult();
                ExecuteInfo info = new ExecuteInfo();
                info.setId(rs.getInt(1));
                info.setHost(rs.getString(6));
                result.setExecuteInfo(info);
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setDataTime(rs.getLong(4));
                result.setStatus(TaskStatus.valueOf(rs.getString(5)));
                result.setCreatedAt(rs.getTimestamp(7).getTime());
                result.setUpdatedAt(rs.getTimestamp(8) != null ? rs.getTimestamp(8).getTime() : 0);
                executes.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executes;
    }

    @Override
    public List<Map<String, Object>> queryExecuteByTimeRange(String scheduleId, Long startTime, Long endTime,
            Integer limit) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Map<String, Object>> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_BY_TIME_RANGE);
            ps.setString(1, scheduleId);
            ps.setLong(2, startTime);
            ps.setLong(3, endTime);
            ps.setInt(4, limit);
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> result = new HashMap<>();
                result.put("schedule_time", rs.getLong(1));
                result.put("data_time", rs.getLong(2));
                String status = rs.getString(3);
                result.put("status", status);
                String errMsg = "";
                if ("failed".equals(status) || "killed".equals(status) || "failed_succeeded".equals(status)) {
                    errMsg = rs.getString(7);
                }
                result.put("err_msg", errMsg);
                long createdAt = rs.getTimestamp(4).getTime();
                result.put("created_at", createdAt);
                long startedAt = (rs.getTimestamp(5) != null ? rs.getTimestamp(5).getTime() : createdAt);
                result.put("started_at", startedAt);
                long updatedAt = (rs.getTimestamp(6) != null ? rs.getTimestamp(6).getTime() : startedAt);
                result.put("updated_at", updatedAt);
                result.put("execute_id", rs.getLong(8));
                executes.add(result);
            }
            return executes;
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
    }

    @Override
    public List<Map<String, Object>> queryExecuteByTimeRangeAndStatus(String scheduleId, Long startTime, Long endTime,
            TaskStatus status, Integer limit) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Map<String, Object>> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_BY_TIME_RANGE_AND_STATUS);
            ps.setString(1, scheduleId);
            ps.setLong(2, startTime);
            ps.setLong(3, endTime);
            ps.setString(4, status.name());
            ps.setInt(5, limit);
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> result = new HashMap<>();
                result.put("schedule_time", rs.getLong(1));
                result.put("data_time", rs.getLong(2));
                result.put("status", status.name());
                long createdAt = rs.getTimestamp(3).getTime();
                result.put("created_at", createdAt);
                long startedAt = (rs.getTimestamp(4) != null ? rs.getTimestamp(4).getTime() : createdAt);
                result.put("started_at", startedAt);
                long updatedAt = (rs.getTimestamp(5) != null ? rs.getTimestamp(5).getTime() : startedAt);
                result.put("updated_at", updatedAt);
                result.put("execute_id", rs.getLong(6));
                executes.add(result);
            }
            return executes;
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
    }

    @Override
    public List<Map<String, Object>> queryAllExecuteByTimeRange(String scheduleId, Long startTime, Long endTime,
            Integer limit) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<Map<String, Object>> executes = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_ALL_BY_TIME_RANGE);
            ps.setString(1, scheduleId);
            ps.setLong(2, startTime);
            ps.setLong(3, endTime);
            ps.setInt(4, limit);
            rs = ps.executeQuery();
            while (rs.next()) {
                Map<String, Object> result = new HashMap<>();
                result.put("schedule_time", rs.getLong(1));
                result.put("data_time", rs.getLong(2));
                result.put("execute_id", rs.getLong(3));
                String status = rs.getString(4);
                result.put("status", status);
                long createdAt = rs.getTimestamp(5).getTime();
                result.put("created_at", createdAt);
                long updatedAt = (rs.getTimestamp(6) != null ? rs.getTimestamp(5).getTime() : createdAt);
                result.put("updated_at", updatedAt);
                result.put("err_msg", rs.getString(7));
                executes.add(result);
            }
            return executes;
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
    }

    @Override
    public Map<String, Map<String, Integer>> queryExecuteStatusAmount(Long startTime, Long endTime)
            throws NaviException {
        Map<String, Map<String, Integer>> statusAmounts = new HashMap<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_TIME_RANGE);
            ps.setLong(1, startTime);
            ps.setLong(2, endTime);
            rs = ps.executeQuery();
            while (rs.next()) {
                String time = rs.getString(3);
                Map<String, Integer> amountMap;
                if (statusAmounts.get(time) == null) {
                    amountMap = new HashMap<>();
                    statusAmounts.put(time, amountMap);
                } else {
                    amountMap = statusAmounts.get(time);
                }
                Integer amount = rs.getInt(1);
                String status = rs.getString(2);
                amountMap.put(status, amount);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return statusAmounts;
    }

    @Override
    public Map<String, Map<String, Integer>> queryExecuteStatusAmountByCreateAt(Long startTime, Long endTime)
            throws NaviException {
        Map<String, Map<String, Integer>> statusAmounts = new HashMap<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_STATUS_AMOUNT_BY_CREATE_TIME_RANGE);
            ps.setLong(1, startTime);
            ps.setLong(2, endTime);
            rs = ps.executeQuery();
            while (rs.next()) {
                String time = rs.getString(3);
                Map<String, Integer> amountMap;
                if (statusAmounts.get(time) == null) {
                    amountMap = new HashMap<>();
                    statusAmounts.put(time, amountMap);
                } else {
                    amountMap = statusAmounts.get(time);
                }
                Integer amount = rs.getInt(1);
                String status = rs.getString(2);
                amountMap.put(status, amount);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return statusAmounts;
    }

    @Override
    public Map<String, Integer> queryRunningExecuteNumbers() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Integer> map = new HashMap<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_QUERY_RUNNING_NUMBER);
            rs = ps.executeQuery();
            while (rs.next()) {
                map.put(rs.getString(1), rs.getInt(2));
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return map;
    }

    @Override
    public void deleteExpireExecute(long expire) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_DELETE_EXPIRE);
            ps.setTimestamp(1, new Timestamp(expire));
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void addRecoveryExecute(long executeId, TaskInfo info) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_RECOVERY_EXECUTE_ADD);
            ps.setLong(1, executeId);
            ps.setString(2, info.getScheduleId());
            ps.setLong(3, info.getScheduleTime());
            ps.setLong(4, info.getDataTime());
            ps.setInt(5, info.getRecoveryInfo().getRecoveryTimes());
            ps.setInt(6, 0);
            ps.setDouble(7, info.getRecoveryInfo().getRank());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void updateRecoveryExecute(long executeId, TaskInfo info) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_RECOVERY_EXECUTE_UPDATE_EXEC_ID);
            ps.setLong(1, executeId);
            ps.setString(2, info.getScheduleId());
            ps.setLong(3, info.getDataTime());
            ps.setInt(4, info.getRecoveryInfo().getRecoveryTimes());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void setRecoveryExecuteSuccess(TaskInfo info) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_RECOVERY_EXECUTE_UPDATE_RECOVERY);
            ps.setString(1, info.getScheduleId());
            ps.setLong(2, info.getScheduleTime());
            ps.setInt(3, info.getRecoveryInfo().getRecoveryTimes());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    /**
     * <p>Query not recovery task.</p>
     */
    @Override
    public List<TaskInfo> getNotRecoveryTask() throws NaviException {
        List<TaskInfo> taskInfos = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            //query all task type info
            Map<String, Map<String, TaskType>> taskTypeInfo = MY_SQL_JOB_TASK_TYPE_DAO.listTaskType();
            //query all task type default tag
            Map<String, String> taskTypeDefaultTag = MY_SQL_JOB_TASK_TYPE_DAO.listTaskTypeDefaultTag();
            ps = con.prepareStatement(SQL_RECOVERY_EXECUTE_NOT_PROCESS);
            rs = ps.executeQuery();
            while (rs.next()) {
                TaskInfo taskInfo = new TaskInfo();
                taskInfo.setScheduleId(rs.getString("schedule_id"));
                taskInfo.setScheduleTime(rs.getLong("schedule_time"));
                taskInfo.setDataTime(rs.getLong("data_time"));
                taskInfo.setExtraInfo(rs.getString("extra_info"));
                taskInfo.setDecommissionTimeout(rs.getString("decommission_timeout"));
                String nodeLabel = rs.getString("node_label_name");
                taskInfo.setNodeLabel(nodeLabel);
                String typeId = rs.getString("type_id");
                taskInfo.setNodeLabel(nodeLabel);
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
                recoveryInfo.setRecoveryEnable(true);
                recoveryInfo.setMaxRecoveryTimes(rs.getInt("retry_quota"));
                recoveryInfo.setIntervalTime(rs.getString("retry_interval"));
                recoveryInfo.setRecoveryTimes(rs.getInt("retry_count"));
                recoveryInfo.setCreatedAt(rs.getLong("created_at") * 1000);
                recoveryInfo.setRank(rs.getDouble("rank"));
                taskInfo.setRecoveryInfo(recoveryInfo);
                taskInfos.add(taskInfo);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return taskInfos;
    }

    @Override
    public List<RecoveryExecuteResult> queryRecoveryResultByScheduleId(String scheduleId, Long limit)
            throws NaviException {
        List<RecoveryExecuteResult> results = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_RECOVERY_EXECUTE_RESULT_BY_SCHEDULE_ID);
            ps.setString(1, scheduleId);
            ps.setLong(2, limit);
            rs = ps.executeQuery();
            while (rs.next()) {
                RecoveryExecuteResult result = new RecoveryExecuteResult();
                result.setExecuteId(rs.getLong(1));
                result.setScheduleId(rs.getString(2));
                result.setScheduleTime(rs.getLong(3));
                result.setRetryTimes(rs.getLong(4));
                result.setRecoveryStatus(rs.getBoolean(5));
                result.setCreatedAt(rs.getTimestamp(6).getTime());
                results.add(result);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return results;
    }

    @Override
    public int getRecoveryExecuteTimes(String scheduleId, Long scheduleTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_RECOVERY_EXECUTE_QUERY_TIMES);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return 0;
    }

    @Override
    public void deleteExpireRecovery(long expire) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_RECOVERY_EXECUTE_DELETE_EXPIRE);
            ps.setTimestamp(1, new Timestamp(expire));
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public Map<String, Set<Long>> listRunnerExecute() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Set<Long>> runnerExecute = new HashMap<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_RUNNER_EXECUTE_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                String host = rs.getString(1);
                Long execId = rs.getLong(2);
                if (runnerExecute.get(host) == null) {
                    runnerExecute.put(host, new HashSet<Long>());
                }
                runnerExecute.get(host).add(execId);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return runnerExecute;
    }
}
