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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.JobSavepointDao;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySqlJobSavepointDao implements JobSavepointDao {

    private static final String SQL_SAVEPOINT_ADD
            = "INSERT INTO jobnavi_savepoint_info(schedule_id, schedule_time, save_point) VALUES(?, ?, ?)";
    private static final String SQL_SAVEPOINT_SCHEDULE_ADD
            = "INSERT INTO jobnavi_savepoint_info(schedule_id, save_point) VALUES(?, ?)";
    private static final String SQL_SAVEPOINT_DELETE
            = "DELETE FROM jobnavi_savepoint_info WHERE schedule_id=? AND schedule_time = ?";
    private static final String SQL_SAVEPOINT_SCHEDULE_DELETE
            = "DELETE FROM jobnavi_savepoint_info WHERE schedule_id=? AND schedule_time IS NULL";
    private static final String SQL_SAVEPOINT_GET
            = "SELECT save_point FROM jobnavi_savepoint_info WHERE schedule_id = ? AND schedule_time = ?";
    private static final String SQL_SAVEPOINT_SCHEDULE_GET
            = "SELECT save_point FROM jobnavi_savepoint_info WHERE schedule_id = ? AND schedule_time IS NULL";

    @Override
    public void saveSavepoint(String scheduleId, String savepointStr) throws NaviException {
        Connection con = null;
        try {
            con = ConnectionPool.getConnection();
            PreparedStatement ps = null;
            try {
                ps = con.prepareStatement(SQL_SAVEPOINT_SCHEDULE_DELETE);
                ps.setString(1, scheduleId);
                ps.execute();
            } finally {
                MySqlUtil.close(ps);
            }
            try {
                ps = con.prepareStatement(SQL_SAVEPOINT_SCHEDULE_ADD);
                ps.setString(1, scheduleId);
                ps.setString(2, savepointStr);
                ps.execute();
            } finally {
                MySqlUtil.close(ps);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con);
        }
    }

    @Override
    public void saveSavepoint(String scheduleId, Long scheduleTime, String savepointStr) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SAVEPOINT_DELETE);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            ps.execute();
            MySqlUtil.close(ps);
            ps = con.prepareStatement(SQL_SAVEPOINT_ADD);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            ps.setString(3, savepointStr);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public String getSavepoint(String scheduleId) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SAVEPOINT_SCHEDULE_GET);
            ps.setString(1, scheduleId);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return null;
    }

    @Override
    public String getSavepoint(String scheduleId, Long scheduleTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SAVEPOINT_GET);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString(1);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return null;
    }

    @Override
    public void deleteSavepoint(String scheduleId) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SAVEPOINT_SCHEDULE_DELETE);
            ps.setString(1, scheduleId);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void deleteSavepoint(String scheduleId, Long scheduleTime) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_SAVEPOINT_DELETE);
            ps.setString(1, scheduleId);
            ps.setLong(2, scheduleTime);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }
}
