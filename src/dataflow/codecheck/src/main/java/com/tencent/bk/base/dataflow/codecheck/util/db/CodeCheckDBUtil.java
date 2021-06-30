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

package com.tencent.bk.base.dataflow.codecheck.util.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CodeCheckDBUtil {

    private static List convertToList(ResultSet rs) throws SQLException {
        List list = new ArrayList();
        ResultSetMetaData md = rs.getMetaData();//获取键名
        int columnCount = md.getColumnCount();//获取行的数量
        while (rs.next()) {
            Map rowData = new HashMap();//声明Map
            for (int i = 1; i <= columnCount; i++) {
                rowData.put(md.getColumnName(i), rs.getObject(i));//获取键名及值
            }
            list.add(rowData);
        }
        return list;
    }

    /**
     * addParserGroup
     *
     * @param groupName
     * @param libDir
     * @param sourceDir
     */
    public static void addParserGroup(String groupName, String libDir, String sourceDir) {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;

        try {
            ps = con.prepareStatement(
                    "REPLACE INTO codecheck_parser_group_info SET parser_group_name=?, lib_dir=?, source_dir=?;");
            ps.setString(1, groupName);
            ps.setString(2, libDir);
            ps.setString(3, sourceDir);
            ps.execute();
        } catch (Exception e) {
            // do nothing
        } finally {
            //DatabaseUtil.close(rs);
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
    }

    /**
     * getParserGroup
     *
     * @param groupName
     * @return java.util.List
     */
    public static List getParserGroup(String groupName) {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;

        try {
            ResultSet rs = null;
            ps = con.prepareStatement(
                    "SELECT * FROM codecheck_parser_group_info WHERE parser_group_name=? LIMIT 1;");
            ps.setString(1, groupName);
            rs = ps.executeQuery();
            return convertToList(rs);
        } catch (Exception e) {
            // do nothing
        } finally {
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
        return null;
    }

    /**
     * listParserGroup
     *
     * @return java.util.List
     */
    public static List listParserGroup() {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;

        try {
            ResultSet rs = null;
            ps = con.prepareStatement(
                    "SELECT * FROM codecheck_parser_group_info;");
            rs = ps.executeQuery();
            return convertToList(rs);
        } catch (Exception e) {
            // do nothing
        } finally {
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
        return null;
    }

    /**
     * deleteParserGroup
     *
     * @param groupName
     */
    public static void deleteParserGroup(String groupName) {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(
                    "DELETE FROM codecheck_parser_group_info where parser_group_name=?;");
            ps.setString(1, groupName);
            ps.execute();
        } catch (Exception e) {
            // do nothing
        } finally {
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
    }

    /**
     * addBlacklistGroup
     *
     * @param groupName
     * @param blacklist
     */
    public static void addBlacklistGroup(String groupName, String blacklist) {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;

        try {
            ps = con.prepareStatement(
                    "REPLACE INTO codecheck_blacklist_group_info SET blacklist_group_name=?, blacklist=?;");
            ps.setString(1, groupName);
            ps.setString(2, blacklist);
            ps.execute();
        } catch (Exception e) {
            // do nothing
        } finally {
            //DatabaseUtil.close(rs);
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
    }

    /**
     * getBlacklistGroup
     *
     * @param groupName
     * @return java.util.List
     */
    public static List getBlacklistGroup(String groupName) {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;

        try {
            ResultSet rs = null;
            ps = con.prepareStatement(
                    "SELECT * FROM codecheck_blacklist_group_info WHERE blacklist_group_name=? LIMIT 1;");
            ps.setString(1, groupName);
            rs = ps.executeQuery();
            return convertToList(rs);
        } catch (Exception e) {
            // do nothing
        } finally {
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
        return null;
    }

    /**
     * listBlacklistGroup
     *
     * @return java.util.List
     */
    public static List listBlacklistGroup() {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;

        try {
            ResultSet rs = null;
            ps = con.prepareStatement(
                    "SELECT * FROM codecheck_blacklist_group_info;");
            rs = ps.executeQuery();
            return convertToList(rs);
        } catch (Exception e) {
            // do nothing
        } finally {
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
        return null;
    }

    /**
     * deleteBlacklistGroup
     *
     * @param groupName
     */
    public static void deleteBlacklistGroup(String groupName) {
        Connection con = DBConnectionUtil.getSingleton().getConnection();
        PreparedStatement ps = null;
        try {
            ps = con.prepareStatement(
                    "DELETE FROM codecheck_blacklist_group_info where blacklist_group_name=?;");
            ps.setString(1, groupName);
            ps.execute();
        } catch (Exception e) {
            // do nothing
        } finally {
            DBCloseUtil.close(ps);
            DBCloseUtil.close(con);
        }
    }
}
