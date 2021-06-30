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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.JobExecuteStatusCacheDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.state.dependency.ExecuteStatusCache;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySqlJobExecuteStatusCacheDao implements JobExecuteStatusCacheDao {

    private static final String SQL_EXECUTE_REFERENCE_CACHE_LIST
            = "SELECT `schedule_id`, `begin_data_time`, `end_data_time`, "
            + "    `child_schedule_id`, `child_data_time`, `is_hot` "
            + "FROM "
            + "    `jobnavi_execute_reference_cache` "
            + "ORDER BY updated_at";
    private static final String SQL_EXECUTE_STATUS_CACHE_LIST
            = "SELECT `schedule_id`, `data_time`, `status` "
            + "FROM "
            + "("
            + "    SELECT MAX(exec_id) AS `exec_id` "
            + "    FROM "
            + "        `jobnavi_execute_reference_cache` l "
            + "    LEFT JOIN "
            + "        `jobnavi_execute_log` r "
            + "    ON l.`schedule_id` = r.`schedule_id` AND r.`data_time` >= l.`begin_data_time` "
            + "        AND r.`data_time` < l.`end_data_time` "
            + "    GROUP BY l.`schedule_id`, `data_time`"
            + ") exec_id_list "
            + "INNER JOIN "
            + "    `jobnavi_execute_log` "
            + "ON exec_id_list.`exec_id` = `jobnavi_execute_log`.`exec_id`";
    private static final String SQL_EXECUTE_REFERENCE_CACHE_ADD
            = "INSERT INTO `jobnavi_execute_reference_cache`(`schedule_id`, `begin_data_time`, `end_data_time`, "
            + "`child_schedule_id`, `child_data_time`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE is_hot = TRUE";
    private static final String SQL_EXECUTE_REFERENCE_CACHE_UPDATE_IS_HOT_FLAG
            = "UPDATE `jobnavi_execute_reference_cache` SET `is_hot` = ? "
            + "WHERE `child_schedule_id` = ? AND `child_data_time` = ?";
    private static final String SQL_EXECUTE_REFERENCE_CACHE_UPDATE_UPDATED_AT
            = "UPDATE `jobnavi_execute_reference_cache` SET `updated_at` = now() "
            + "WHERE `child_schedule_id` = ? AND `child_data_time` = ? AND `schedule_id` = ?";
    private static final String SQL_EXECUTE_REFERENCE_CACHE_DELETE
            = "DELETE FROM `jobnavi_execute_reference_cache` "
            + "WHERE `schedule_id` = ? AND `child_schedule_id` = ? AND `child_data_time` = ?";


    @Override
    public List<ExecuteStatusCache.ExecuteReference> listExecuteReference() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<ExecuteStatusCache.ExecuteReference> executeReferenceList = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_REFERENCE_CACHE_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                String scheduleId = rs.getString("schedule_id");
                long beginDataTime = rs.getLong("begin_data_time");
                long endDataTime = rs.getLong("end_data_time");
                String childScheduleId = rs.getString("child_schedule_id");
                long childDataTime = rs.getLong("child_data_time");
                ExecuteStatusCache.ExecuteReference executeReference =
                        new ExecuteStatusCache.ExecuteReference(
                                scheduleId, beginDataTime, endDataTime,
                                childScheduleId, childDataTime);
                boolean isHot = rs.getBoolean("is_hot");
                executeReference.setHot(isHot);
                executeReferenceList.add(executeReference);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executeReferenceList;
    }

    @Override
    public Map<String, Map<Long, TaskStatus>> listExecuteStatusCache() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Map<Long, TaskStatus>> executeStatusCache = new HashMap<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_STATUS_CACHE_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                String scheduleId = rs.getString("schedule_id");
                long dataTime = rs.getLong("data_time");
                TaskStatus status = TaskStatus.valueOf(rs.getString("status"));
                if (!executeStatusCache.containsKey(scheduleId)) {
                    executeStatusCache.put(scheduleId, new HashMap<Long, TaskStatus>());
                }
                executeStatusCache.get(scheduleId).put(dataTime, status);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return executeStatusCache;
    }

    @Override
    public void saveExecuteReferenceCache(String scheduleId, long beginDataTime, long endDataTime,
            String childScheduleId, long childDataTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_REFERENCE_CACHE_ADD);
            ps.setString(1, scheduleId);
            ps.setLong(2, beginDataTime);
            ps.setLong(3, endDataTime);
            ps.setString(4, childScheduleId);
            ps.setLong(5, childDataTime);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void updateExecuteReferenceCache(String childScheduleId, long childDataTime, Boolean isHot)
            throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_REFERENCE_CACHE_UPDATE_IS_HOT_FLAG);
            ps.setBoolean(1, isHot);
            ps.setString(2, childScheduleId);
            ps.setLong(3, childDataTime);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void activateExecuteReferenceCache(String scheduleId, String childScheduleId, long childDataTime)
            throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_REFERENCE_CACHE_UPDATE_UPDATED_AT);
            ps.setString(1, childScheduleId);
            ps.setLong(2, childDataTime);
            ps.setString(3, scheduleId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void removeExecuteReferenceCache(String scheduleId, String childScheduleId, long childDataTime)
            throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_EXECUTE_REFERENCE_CACHE_DELETE);
            ps.setString(1, scheduleId);
            ps.setString(2, childScheduleId);
            ps.setLong(3, childDataTime);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }
}
