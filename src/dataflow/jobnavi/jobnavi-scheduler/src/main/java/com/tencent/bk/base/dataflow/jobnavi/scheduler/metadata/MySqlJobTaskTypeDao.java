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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.mysql.MySqlUtil;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.Language;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySqlJobTaskTypeDao implements JobTaskTypeDao {

    private static final String SQL_TASK_TYPE_ADD
            = "INSERT INTO `jobnavi_task_type_info`(`type_id`,`tag`,`main`,`env`,`sys_env`,`language`,`task_mode`,"
            + "`recoverable`,`created_by`,`description`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    private static final String SQL_TASK_TYPE_DELETE
            = "DELETE FROM `jobnavi_task_type_info` WHERE `type_id` = ? AND `tag` = ?";
    private static final String SQL_TASK_TYPE_TAG_ALIAS_ADD
            = "INSERT INTO `jobnavi_task_type_tag_alias`(`type_id`,`tag`,`alias`,`description`) VALUES (?, ?, ?, ?)";
    private static final String SQL_TASK_TYPE_TAG_ALIAS_DELETE
            = "DELETE FROM `jobnavi_task_type_tag_alias` WHERE `type_id` = ? AND `tag` = ? AND `alias` = ?";
    private static final String SQL_TASK_TYPE_TAG_ALIAS_QUERY
            = "SELECT `alias` FROM `jobnavi_task_type_tag_alias` WHERE `type_id` = ? AND `tag` = ?";
    private static final String SQL_TASK_TYPE_DEFAULT_TAG_ADD
            = "INSERT INTO `jobnavi_task_type_default_tag`(`type_id`,`node_label`,`default_tag`) VALUES (?, ?, ?)";
    private static final String SQL_TASK_TYPE_DEFAULT_TAG_DELETE_BY_TYPE
            = "DELETE FROM `jobnavi_task_type_default_tag` WHERE `type_id` = ? AND `node_label` IS NULL";
    private static final String SQL_TASK_TYPE_DEFAULT_TAG_DELETE_BY_TYPE_AND_NODE_LABEL
            = "DELETE FROM `jobnavi_task_type_default_tag` WHERE `type_id` = ? AND `node_label` = ?";
    private static final String SQL_TASK_TYPE_DEFAULT_TAG_QUERY_BY_TYPE
            = "SELECT `default_tag` FROM `jobnavi_task_type_default_tag` WHERE `type_id` = ? AND `node_label` IS NULL";
    private static final String SQL_TASK_TYPE_DEFAULT_TAG_QUERY_BY_TYPE_AND_NODE_LABEL
            = "SELECT `default_tag` FROM `jobnavi_task_type_default_tag` WHERE `type_id` = ? AND `node_label` = ?";
    private static final String SQL_TASK_TYPE_LIST
            = "SELECT `type_id`, `tag`, `tag` AS `original_tag`, `main`, `env`, `sys_env`, `language`, `task_mode`, "
            + "    `recoverable`, `description` "
            + "FROM "
            + "    `jobnavi_task_type_info` "
            + "UNION "
            + "SELECT l.`type_id` AS `type_id`, l.`alias` AS `tag`, l.`tag` AS `original_tag`, `main`, `env`, "
            + "    `sys_env`, `language`, `task_mode`, `recoverable`, r.`description` "
            + "FROM "
            + "    `jobnavi_task_type_tag_alias` l "
            + "INNER JOIN "
            + "    `jobnavi_task_type_info` r "
            + "ON l.`type_id` = r.`type_id` AND l.`tag` = r.`tag`";
    private static final String SQL_TASK_TYPE_DEFAULT_TAG_LIST
            = "SELECT `type_id`, `node_label`, `default_tag` FROM `jobnavi_task_type_default_tag`";
    private static final String SQL_TASK_TYPE_QUERY_DEFAULT_TAG
            = "SELECT `default_tag` "
            + "FROM"
            + "("
            + "    SELECT `default_tag`, 2 AS `priority` "
            + "    FROM"
            + "        `jobnavi_task_type_default_tag`"
            + "    WHERE `type_id` = ? AND `node_label` = ?"
            + "    UNION ALL"
            + "    SELECT `default_tag`, 1 AS `priority`"
            + "    FROM"
            + "        `jobnavi_task_type_default_tag`"
            + "    WHERE `type_id` = ? AND `node_label` IS NULL"
            + ") t "
            + "ORDER BY `priority` DESC LIMIT 1";
    private static final String SQL_TASK_TYPE_QUERY_BY_ID_AND_TAG
            = "SELECT * "
            + "FROM "
            + "("
            + "    SELECT `type_id`, `tag`, `tag` AS `original_tag`, `main`, `env`, `sys_env`, `language`, "
            + "        `task_mode`, `recoverable`, `description` "
            + "    FROM "
            + "        `jobnavi_task_type_info` "
            + "    UNION"
            + "    SELECT l.`type_id` AS `type_id`, l.`alias` AS `tag`, l.`tag` AS `original_tag`, `main`, `env`, "
            + "        `sys_env`, `language`, `task_mode`, `recoverable`, r.`description` "
            + "    FROM "
            + "        `jobnavi_task_type_tag_alias` l "
            + "    INNER JOIN "
            + "        `jobnavi_task_type_info` r "
            + "    ON l.`type_id` = r.`type_id` AND l.`tag` = r.`tag`"
            + ") t "
            + "WHERE `type_id` = ? AND `tag` = ?";

    @Override
    public void addTaskType(TaskType taskType)
            throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_ADD);
            ps.setString(1, taskType.getName());
            ps.setString(2, taskType.getTag());
            ps.setString(3, taskType.getMain());
            ps.setString(4, taskType.getEnv());
            ps.setString(5, taskType.getSysEnv());
            ps.setString(6, taskType.getLanguage().toString());
            ps.setString(7, taskType.getTaskMode().toString());
            ps.setBoolean(8, taskType.isRecoverable());
            ps.setString(9, taskType.getCreatedBy());
            ps.setString(10, taskType.getDescription());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void deleteTaskType(String typeId, String typeTag) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_DELETE);
            ps.setString(1, typeId);
            ps.setString(2, typeTag);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public Map<String, Map<String, TaskType>> listTaskType() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, Map<String, TaskType>> taskTypeInfo = new HashMap<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                String typeId = rs.getString("type_id");
                String originalTypeTag = rs.getString("original_tag");
                TaskType type = new TaskType();
                type.setName(typeId);
                type.setTag(originalTypeTag);
                type.setMain(rs.getString("main"));
                type.setEnv(rs.getString("env"));
                type.setSysEnv(rs.getString("sys_env"));
                type.setLanguage(Language.valueOf(rs.getString("language")));
                type.setDescription(rs.getString("description"));
                type.setTaskMode(TaskMode.valueOf(rs.getString("task_mode")));
                type.setRecoverable(rs.getInt("recoverable") != 0);
                if (!taskTypeInfo.containsKey(typeId)) {
                    taskTypeInfo.put(typeId, new HashMap<String, TaskType>());
                }
                String typeTag = rs.getString("tag");
                taskTypeInfo.get(typeId).put(typeTag, type);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return taskTypeInfo;
    }

    @Override
    public TaskType queryTaskType(String typeId, String typeTag, String nodeLabel) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            if (typeTag == null) {
                ps = con.prepareStatement(SQL_TASK_TYPE_QUERY_DEFAULT_TAG);
                ps.setString(1, typeId);
                ps.setString(2, nodeLabel);
                ps.setString(3, typeId);
                rs = ps.executeQuery();
                if (rs.next()) {
                    typeTag = rs.getString("default_tag");
                }
                ps.close();
            }
            ps = con.prepareStatement(SQL_TASK_TYPE_QUERY_BY_ID_AND_TAG);
            ps.setString(1, typeId);
            ps.setString(2, typeTag);
            rs = ps.executeQuery();
            if (rs.next()) {
                TaskType type = new TaskType();
                type.setName(rs.getString("type_id"));
                type.setTag(rs.getString("original_tag"));
                type.setMain(rs.getString("main"));
                type.setEnv(rs.getString("env"));
                type.setSysEnv(rs.getString("sys_env"));
                type.setLanguage(Language.valueOf(rs.getString("language")));
                type.setDescription(rs.getString("description"));
                type.setTaskMode(TaskMode.valueOf(rs.getString("task_mode")));
                type.setRecoverable(rs.getInt("recoverable") != 0);
                return type;
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return null;
    }

    @Override
    public void addTaskTypeTagAlias(String typeId, String typeTag, String alias, String description)
            throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_TAG_ALIAS_ADD);
            ps.setString(1, typeId);
            ps.setString(2, typeTag);
            ps.setString(3, alias);
            ps.setString(4, description);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void deleteTaskTypeTagAlias(String typeId, String typeTag, String alias) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_TAG_ALIAS_DELETE);
            ps.setString(1, typeId);
            ps.setString(2, typeTag);
            ps.setString(3, alias);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public List<String> queryTaskTypeTagAlias(String typeId, String typeTag) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> aliasList = new ArrayList<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_TAG_ALIAS_QUERY);
            ps.setString(1, typeId);
            ps.setString(2, typeTag);
            rs = ps.executeQuery();
            while (rs.next()) {
                String alias = rs.getString("alias");
                aliasList.add(alias);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return aliasList;
    }

    @Override
    public void addTaskTypeDefaultTag(String typeId, String nodeLabel, String defaultTag) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_DEFAULT_TAG_ADD);
            ps.setString(1, typeId);
            ps.setString(2, nodeLabel);
            ps.setString(3, defaultTag);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void deleteTaskTypeDefaultTag(String typeId, String nodeLabel) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            if (nodeLabel != null) {
                ps = con.prepareStatement(SQL_TASK_TYPE_DEFAULT_TAG_DELETE_BY_TYPE_AND_NODE_LABEL);
                ps.setString(1, typeId);
                ps.setString(2, nodeLabel);
            } else {
                ps = con.prepareStatement(SQL_TASK_TYPE_DEFAULT_TAG_DELETE_BY_TYPE);
                ps.setString(1, typeId);
            }
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public String queryTaskTypeDefaultTag(String typeId, String nodeLabel) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        String defaultTag = null;
        try {
            con = ConnectionPool.getConnection();
            if (nodeLabel != null) {
                ps = con.prepareStatement(SQL_TASK_TYPE_DEFAULT_TAG_QUERY_BY_TYPE_AND_NODE_LABEL);
                ps.setString(1, typeId);
                ps.setString(2, nodeLabel);
            } else {
                ps = con.prepareStatement(SQL_TASK_TYPE_DEFAULT_TAG_QUERY_BY_TYPE);
                ps.setString(1, typeId);
            }
            rs = ps.executeQuery();
            if (rs.next()) {
                defaultTag = rs.getString("default_tag");
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return defaultTag;
    }

    @Override
    public Map<String, String> listTaskTypeDefaultTag() throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, String> taskTypeDefaultTag = new HashMap<>();
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_TASK_TYPE_DEFAULT_TAG_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                String typeId = rs.getString("type_id");
                String nodeLabel = rs.getString("node_label");
                String defaultTag = rs.getString("default_tag");
                if (nodeLabel == null) {
                    taskTypeDefaultTag.put(typeId, defaultTag);
                } else {
                    taskTypeDefaultTag.put(typeId + "@" + nodeLabel, defaultTag);
                }
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return taskTypeDefaultTag;
    }
}
