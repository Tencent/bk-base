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

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.JobNodeLabelDao;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.node.NodeLabel;
import com.tencent.bk.base.dataflow.jobnavi.node.NodeLabelHost;
import com.tencent.bk.base.dataflow.jobnavi.util.db.ConnectionPool;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MySqlJobNodeLabelDao implements JobNodeLabelDao {

    private static final String SQL_NODE_LABEL_ADD
            = "INSERT INTO jobnavi_node_label_info(label_name, description) VALUES(?, ?)";
    private static final String SQL_NODE_LABEL_LIST = "SELECT label_name, description FROM jobnavi_node_label_info";
    private static final String SQL_NODE_LABEL_DELETE = "DELETE FROM jobnavi_node_label_info WHERE label_name = ?";

    private static final String SQL_NODE_LABEL_HOST_ADD
            = "INSERT INTO jobnavi_node_label_host_info(host, label_name, description) VALUES(?, ?, ?)";
    private static final String SQL_NODE_LABEL_HOST_LIST
            = "SELECT label_name, host, description FROM jobnavi_node_label_host_info";
    private static final String SQL_NODE_LABEL_HOST_DELETE_BY_HOST_AND_LABEL
            = "DELETE FROM jobnavi_node_label_host_info WHERE host = ? AND label_name = ?";
    private static final String SQL_NODE_LABEL_HOST_DELETE_BY_HOST
            = "DELETE FROM jobnavi_node_label_host_info WHERE host = ?";
    private static final String SQL_NODE_LABEL_HOST_DELETE_BY_LABEL
            = "DELETE FROM jobnavi_node_label_host_info WHERE label_name = ?";

    @Override
    public void addNodeLabel(NodeLabel label) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_NODE_LABEL_ADD);
            ps.setString(1, label.getName());
            ps.setString(2, label.getDescription());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void deleteNodeLabel(String labelName) throws NaviException {
        Connection con = null;
        try {
            PreparedStatement ps = null;
            con = ConnectionPool.getConnection();
            try {
                ps = con.prepareStatement(SQL_NODE_LABEL_DELETE);
                ps.setString(1, labelName);
                ps.execute();
            } finally {
                MySqlUtil.close(ps);
            }

            try {
                ps = con.prepareStatement(SQL_NODE_LABEL_HOST_DELETE_BY_LABEL);
                ps.setString(1, labelName);
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
    public List<NodeLabel> listNodeLabel() throws NaviException {
        List<NodeLabel> nodeLabels = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_NODE_LABEL_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                NodeLabel nodeLabel = new NodeLabel();
                nodeLabel.setName(rs.getString(1));
                nodeLabel.setDescription(rs.getString(2));
                nodeLabels.add(nodeLabel);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return nodeLabels;
    }

    @Override
    public void addHostNodeLabel(NodeLabelHost labelHost) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_NODE_LABEL_HOST_ADD);
            ps.setString(1, labelHost.getHost());
            ps.setString(2, labelHost.getLabelName());
            ps.setString(3, labelHost.getDescription());
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void deleteHostLabel(String host, String labelName) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_NODE_LABEL_HOST_DELETE_BY_HOST_AND_LABEL);
            ps.setString(1, host);
            ps.setString(2, labelName);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public void deleteAllHostLabel(String host) throws NaviException {
        Connection con = null;
        PreparedStatement ps = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_NODE_LABEL_HOST_DELETE_BY_HOST);
            ps.setString(1, host);
            ps.execute();
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, null);
        }
    }

    @Override
    public List<NodeLabelHost> listNodeLabelHost() throws NaviException {
        List<NodeLabelHost> nodeLabelHosts = new ArrayList<>();
        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            con = ConnectionPool.getConnection();
            ps = con.prepareStatement(SQL_NODE_LABEL_HOST_LIST);
            rs = ps.executeQuery();
            while (rs.next()) {
                NodeLabelHost nodeLabelHost = new NodeLabelHost();
                nodeLabelHost.setLabelName(rs.getString(1));
                nodeLabelHost.setHost(rs.getString(2));
                nodeLabelHost.setDescription(rs.getString(3));
                nodeLabelHosts.add(nodeLabelHost);
            }
        } catch (SQLException e) {
            throw new NaviException(e);
        } finally {
            MySqlUtil.close(con, ps, rs);
        }
        return nodeLabelHosts;
    }
}
