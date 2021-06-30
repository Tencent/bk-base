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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.JobScheduleDao;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyParamType;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.DependencyRule;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.Period;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.RecoveryInfo;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.schedule.bean.ScheduleInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.cron.PeriodUnit;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;

public class MySqlJobScheduleDao implements JobScheduleDao {
    private static final String SQL_SCHEDULE_ADD = "INSERT INTO jobnavi_schedule_info(schedule_id, description, "
            + "cron_expression, frequency, period_unit, first_schedule_time, delay, timezone, execute_oncreate, "
            + "extra_info, type_id, active, execute_before_now, created_by, decommission_timeout, node_label_name, "
            + "max_running_task, data_time_offset) "
            + "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_SCHEDULE_DELETE = "DELETE FROM jobnavi_schedule_info WHERE schedule_id = ?";
    private static final String SQL_SCHEDULE_ACTIVE
            = "UPDATE jobnavi_schedule_info SET active = ? WHERE schedule_id = ?";
    private static final String SQL_SCHEDULE_UPDATE_START_TIME
            = "UPDATE jobnavi_schedule_info SET first_schedule_time = ? WHERE schedule_id = ?";

    private static final String SQL_DEPENDENCY_LIST
            = "SELECT schedule_id, parent_id, dependency_rule, param_type, param_value, window_offset "
            + "FROM jobnavi_dependency_info";
    private static final String SQL_DEPENDENCY_QUERY_PARENT
            = "SELECT parent_id, dependency_rule, param_type, param_value, window_offset "
            + "FROM jobnavi_dependency_info WHERE schedule_id = ?";
    private static final String SQL_DEPENDENCY_QUERY_CHILDREN
            = "SELECT schedule_id FROM jobnavi_dependency_info WHERE parent_id = ?";

    private static final String SQL_DEPENDENCY_ADD
            = "INSERT INTO jobnavi_dependency_info(schedule_id, parent_id, dependency_rule, param_type, param_value, "
            + "window_offset) VALUES(?, ?, ?, ?, ?, ?)";
    private static final String SQL_DEPENDENCY_DELETE = "DELETE FROM jobnavi_dependency_info WHERE schedule_id = ?";

    private static final String SQL_RECOVERY_ADD
            = "INSERT INTO jobnavi_recovery_info(schedule_id, retry_times, interval_time) VALUES(?, ?, ?)";
    private static final String SQL_RECOVERY_DELETE = "DELETE FROM jobnavi_recovery_info WHERE schedule_id = ?";

    private static final String SQL_SCHEDULE_INFO_LIST
            = "SELECT s.schedule_id, s.description, s.cron_expression, s.frequency, s.period_unit, "
            + "    s.first_schedule_time, s.delay, s.timezone, s.data_time_offset, s.execute_oncreate, "
            + "    s.extra_info, s.type_id, s.type_tag, s.active, s.execute_before_now, s.created_by, "
            + "    s.decommission_timeout, s.node_label_name, s.max_running_task, r.schedule_id AS is_enable, "
            + "    r.retry_times, r.interval_time "
            + "FROM "
            + "    jobnavi_schedule_info s "
            + "LEFT JOIN "
            + "    jobnavi_recovery_info r "
            + "ON s.schedule_id=r.schedule_id";
    private static final String SQL_SCHEDULE_TIME_LIST
            = "SELECT s.schedule_id, e.schedule_time "
            + "FROM "
            + "    jobnavi_schedule_info s "
            + "LEFT JOIN "
            + "("
            + "    SELECT max(schedule_time) AS schedule_time, schedule_id "
            + "    FROM "
            + "        jobnavi_execute_log "
            + "    GROUP BY schedule_id"
            + ") e "
            + "ON s.schedule_id=e.schedule_id";
    private static final String SQL_SCHEDULE_EXPIRE_LIST
            = "SELECT l.schedule_id FROM jobnavi_schedule_info l "
            + "LEFT JOIN "
            + "("
            + "    SELECT s.schedule_id "
            + "    FROM "
            + "        jobnavi_schedule_info s "
            + "    LEFT JOIN "
            + "        jobnavi_execute_log e "
            + "    ON s.`schedule_id` = e.`schedule_id` "
            + "    WHERE s.`period_unit` = 'once' AND e.`status` in ('preparing', 'running', 'recovering')"
            + ") r "
            + "ON l.schedule_id = r.schedule_id "
            + "WHERE l.`period_unit` = 'once' AND r.`schedule_id` IS NULL AND created_at < ?";

    @Override
    public void addScheduleInfo(ScheduleInfo info) throws NaviException {
        Connection con = null;
        try {
            con = ConnectionPool.getConnection();
            con.setAutoCommit(false);
            addSchedule(con, info);
            addDependency(con, info);
            if (info.getRecoveryInfo().isEnable()) {
                addRecovery(con, info);
            }
            con.commit();
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException e1) {
                throw new NaviException(e1);
            }
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con);
        }
    }

    @Override
    public void deleteScheduleInfo(String scheduleId) throws NaviException {
        Connection con = null;
        try {
            con = ConnectionPool.getConnection();
            con.setAutoCommit(false);
            deleteDependency(con, scheduleId);
            deleteRecovery(con, scheduleId);
            deleteSchedule(con, scheduleId);
            con.commit();
        } catch (SQLException e) {
            try {
                if (con != null) {
                    con.rollback();
                }
            } catch (SQLException e1) {
                throw new NaviException(e1);
            }
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con);
        }
    }

    @Override
    public void updateScheduleInfo(ScheduleInfo info) throws NaviException {
        deleteScheduleInfo(info.getScheduleId());
        addScheduleInfo(info);
    }

    @Override
    public void updateScheduleStartTime(String scheduleId, Long startTimeMills) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SCHEDULE_UPDATE_START_TIME);
            if (startTimeMills != null) {
                ps.setLong(1, startTimeMills);
            } else {
                ps.setNull(1, Types.BIGINT);
            }
            ps.setString(2, scheduleId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public List<ScheduleInfo> listScheduleInfo() throws NaviException {
        List<ScheduleInfo> infos = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
             //query all dependence info
            Map<String, List<DependencyInfo>> dependenceInfo = new HashMap<>();
            ps = con.prepareStatement(SQL_DEPENDENCY_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                DependencyInfo info = new DependencyInfo();
                info.setParentId(rs.getString(2));
                info.setRule(DependencyRule.valueOf(rs.getString(3)));
                info.setType(DependencyParamType.valueOf(rs.getString(4)));
                info.setValue(rs.getString(5));
                info.setWindowOffset(rs.getString(6));
                String scheduleId = rs.getString(1);
                if (!dependenceInfo.containsKey(scheduleId)) {
                    dependenceInfo.put(scheduleId, new ArrayList<DependencyInfo>());
                }
                List<DependencyInfo> parents = dependenceInfo.get(scheduleId);
                parents.add(info);
            }
             //query all schedule info
            ps.close();
            ps = con.prepareStatement(SQL_SCHEDULE_INFO_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                ScheduleInfo info = new ScheduleInfo();
                String scheduleId = rs.getString("schedule_id");
                info.setScheduleId(scheduleId);
                info.setDescription(rs.getString("description"));
                int frequency = rs.getInt("frequency");
                String cronExpression = rs.getString("cron_expression");
                if (frequency != 0 || cronExpression != null) {
                    Period period = new Period();
                    period.setCronExpression(cronExpression);
                    period.setFrequency(frequency);
                    if (rs.getString("period_unit") != null) {
                        period.setPeriodUnit(PeriodUnit.valueOf(rs.getString("period_unit")));
                    }
                    period.setFirstScheduleTime(new Date(rs.getLong("first_schedule_time")));
                    period.setDelay(rs.getString("delay"));
                    period.setTimezone(rs.getString("timezone"));
                    info.setPeriod(period);
                }
                info.setDataTimeOffset(rs.getString("data_time_offset"));
                info.setExecOnCreate(rs.getBoolean("execute_oncreate"));
                info.setExtraInfo(rs.getString("extra_info"));
                info.setTypeId(rs.getString("type_id"));
                info.setTypeTag(rs.getString("type_tag"));
                info.setActive(rs.getBoolean("active"));
                info.setExecuteBeforeNow(rs.getBoolean("execute_before_now"));
                info.setCreatedBy(rs.getString("created_by"));
                info.setDecommissionTimeout(rs.getString("decommission_timeout"));
                info.setNodeLabel(rs.getString("node_label_name"));
                info.setMaxRunningTask(rs.getInt("max_running_task"));
                RecoveryInfo recovery = new RecoveryInfo();
                String isEnable = rs.getString("is_enable");
                if (StringUtils.isNotEmpty(isEnable)) {
                    recovery.setEnable(true);
                    recovery.setRetryTimes(rs.getInt("retry_times"));
                    recovery.setIntervalTime(rs.getString("interval_time"));
                } else {
                    recovery.setEnable(false);
                }
                info.setRecoveryInfo(recovery);
                if (dependenceInfo.containsKey(scheduleId)) {
                    info.setParents(dependenceInfo.get(scheduleId));
                } else {
                    info.setParents(new ArrayList<DependencyInfo>());
                }
                infos.add(info);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return infos;
    }

    @Override
    public Map<String, Long> listCurrentScheduleTime() throws NaviException {
        Map<String, Long> scheduleTimes = new HashMap<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SCHEDULE_TIME_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                String scheduleId = rs.getString(1);
                Long time = rs.getString(2) == null ? null : rs.getLong(2);
                scheduleTimes.put(scheduleId, time);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return scheduleTimes;
    }

    @Override
    public void activeSchedule(String scheduleId, boolean active) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SCHEDULE_ACTIVE);
            ps.setBoolean(1, active);
            ps.setString(2, scheduleId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public List<String> listExpireSchedule(long expire) throws NaviException {
        List<String> scheduleIdList = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SCHEDULE_EXPIRE_LIST);
            ps.setTimestamp(1, new Timestamp(expire));
            rs = ps.executeQuery();
            while (rs.next()) {
                scheduleIdList.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return scheduleIdList;
    }

    @Override
    public List<DependencyInfo> getParents(String scheduleId) throws NaviException {
        List<DependencyInfo> parents = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_DEPENDENCY_QUERY_PARENT);
            ps.setString(1, scheduleId);
            rs = ps.executeQuery();
            while (rs.next()) {
                DependencyInfo info = new DependencyInfo();
                info.setParentId(rs.getString(1));
                info.setRule(DependencyRule.valueOf(rs.getString(2)));
                info.setType(DependencyParamType.valueOf(rs.getString(3)));
                info.setValue(rs.getString(4));
                info.setWindowOffset(rs.getString(5));
                parents.add(info);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return parents;
    }

    @Override
    public List<String> getChildrenByParentId(String scheduleId) throws NaviException {
        List<String> children = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_DEPENDENCY_QUERY_CHILDREN);
            ps.setString(1, scheduleId);
            rs = ps.executeQuery();
            while (rs.next()) {
                children.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return children;
    }

    private void addSchedule(Connection con, ScheduleInfo info) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(SQL_SCHEDULE_ADD);
            ps.setString(1, info.getScheduleId());
            ps.setString(2, info.getDescription());
            ps.setString(3, info.getPeriod() == null ? null : info.getPeriod().getCronExpression());
            ps.setInt(4, info.getPeriod() == null ? 0 : info.getPeriod().getFrequency());
            ps.setString(5, info.getPeriod() == null || info.getPeriod().getPeriodUnit() == null
                    ? null : info.getPeriod().getPeriodUnit().toString());
            ps.setLong(6, info.getPeriod() == null || info.getPeriod().getFirstScheduleTime() == null
                    ? -1 : info.getPeriod().getFirstScheduleTime().getTime());
            ps.setString(7, info.getPeriod() == null ? null : info.getPeriod().getDelay());
            ps.setString(8, info.getPeriod() == null || info.getPeriod().getTimezone() == null
                    ? null : info.getPeriod().getTimezone().getID());
            ps.setInt(9, info.isExecOnCreate() ? 1 : 0);
            ps.setString(10, info.getExtraInfo());
            ps.setString(11, info.getTypeId());
            ps.setInt(12, info.isActive() ? 1 : 0);
            ps.setInt(13, info.isExecuteBeforeNow() ? 1 : 0);
            ps.setString(14, info.getCreatedBy());
            ps.setString(15, info.getDecommissionTimeout());
            ps.setString(16, info.getNodeLabel());
            ps.setInt(17, info.getMaxRunningTask());
            if (info.getDataTimeOffset() != null) {
                ps.setString(18, info.getDataTimeOffset());
            } else {
                ps.setNull(18, Types.VARCHAR);
            }
            ps.execute();
        } finally {
            MySqlUtil.close(ps);
        }
    }

    private void deleteSchedule(Connection con, String scheduleId) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(SQL_SCHEDULE_DELETE);
            ps.setString(1, scheduleId);
            ps.execute();
        } finally {
            MySqlUtil.close(ps);
        }
    }

    private void deleteDependency(Connection con, String scheduleId) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(SQL_DEPENDENCY_DELETE);
            ps.setString(1, scheduleId);
            ps.execute();
        } finally {
            MySqlUtil.close(ps);
        }
    }

    private void addRecovery(Connection con, ScheduleInfo info) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(SQL_RECOVERY_ADD);
            ps.setString(1, info.getScheduleId());
            ps.setInt(2, info.getRecoveryInfo().getRetryTimes());
            ps.setString(3, info.getRecoveryInfo().getIntervalTime());
            ps.execute();
        } finally {
            MySqlUtil.close(ps);
        }
    }

    private void deleteRecovery(Connection con, String scheduleId) throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(SQL_RECOVERY_DELETE);
            ps.setString(1, scheduleId);
            ps.execute();
        } finally {
            MySqlUtil.close(ps);
        }
    }

    private void addDependency(Connection con, ScheduleInfo info) throws SQLException {
        List<DependencyInfo> parents = info.getParents();
        if (parents == null) {
            return;
        }

        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(SQL_DEPENDENCY_ADD);
            for (DependencyInfo parent : parents) {
                ps.setString(1, info.getScheduleId());
                ps.setString(2, parent.getParentId());
                ps.setString(3, parent.getRule().toString());
                ps.setString(4, parent.getType().toString());
                ps.setString(5, parent.getValue());
                ps.setString(6, parent.getWindowOffset());
                ps.execute();
            }
        } finally {
            MySqlUtil.close(ps);
        }
    }
}
