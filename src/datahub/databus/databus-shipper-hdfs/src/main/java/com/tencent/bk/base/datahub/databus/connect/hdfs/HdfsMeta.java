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


package com.tencent.bk.base.datahub.databus.connect.hdfs;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.utils.ConnUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;


public class HdfsMeta {

    private static final Logger log = LoggerFactory.getLogger(HdfsMeta.class);
    private static Connection connection;

    /**
     * 提交metadata数据
     *
     * @param rtId rtId
     * @param partition 分区编号
     * @param lastDataTime 最新的时间信息
     */
    public void submitMetadata(String rtId, int partition, Map<String, List<String>> lastDataTime) {
        // TODO 将数据库密码加密
        Connection conn = getConnection();
        if (conn != null) {
            insertHdfsMetaData(conn, rtId, partition, lastDataTime);
        }
    }

    /**
     * 写入hdfs meta信息到数据库中
     *
     * @param conn 数据库连接
     * @param rtId rtId
     * @param partition 分区编号
     * @param lastDataTime 最新的时间数据
     */
    private void insertHdfsMetaData(Connection conn, String rtId, int partition,
            Map<String, List<String>> lastDataTime) {
        String now = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
        String updateString =
                "INSERT INTO hdfs_metadata (rt_id, path_time, data_time, write_time, is_rerun, path, partition_num) "
                        + "VALUES(?,?,?,?,?,?,?) "
                        + "ON DUPLICATE KEY UPDATE is_rerun=VALUES(is_rerun), data_time=VALUES(data_time), "
                        + "write_time=VALUES(write_time)";

        PreparedStatement preparedStatement = null;
        try {
            conn.setAutoCommit(false);
            preparedStatement = conn.prepareStatement(updateString);
            String parentPath;
            for (Map.Entry<String, List<String>> entry : lastDataTime.entrySet()) {
                String dataTime = entry.getValue().get(0);
                preparedStatement.setString(1, rtId);
                preparedStatement.setString(2, dataTime.replaceAll(":.*", "").replace(" ", "").replace("-", ""));
                preparedStatement.setString(3, dataTime);
                preparedStatement.setString(4, now);
                preparedStatement.setInt(5, Integer.parseInt(lastDataTime.get(entry.getKey()).get(1)));
                File file = new File(entry.getKey());
                parentPath = file.getParent() != null ? file.getParent().replaceFirst("hdfs:/", "hdfs://") : "";
                preparedStatement.setString(6, parentPath);
                preparedStatement.setInt(7, partition);

                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            try {
                LogUtils.warn(log, "SQLException, now going to roll back transaction! " + e.getMessage());
                conn.rollback();
            } catch (SQLException ignore) {
                // ignore
            }
        } finally {
            closeStatement(preparedStatement);
        }
    }

    /**
     * 关闭操作句柄
     *
     * @param preparedStatement 操作句柄
     */
    private static void closeStatement(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException ignore) {
                // ignore
            }
        }
    }

    /**
     * 获取数据库连接
     *
     * @return 数据库连接
     */
    private Connection getConnection() {
        if (!isValidConnection(connection)) {
            try {
                Map<String, String> props = BasicProps.getInstance().getConnectorProps();
                Properties connectionProps = new Properties();
                connectionProps.put("user", props.get(HdfsConsts.META_CONN_USER));
                String cipher = props.get(HdfsConsts.META_CONN_PASS);
                String pass = ConnUtils.decodePass(cipher);
                connectionProps.put("password", pass);
                LogUtils.info(log, "initial mysql connection {}", props.get(HdfsConsts.META_CONN_URL));
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection(props.get(HdfsConsts.META_CONN_URL), connectionProps);
            } catch (Exception ignore) {
                LogUtils.warn(log, "Can not connect to database {}", ignore.getMessage());
            }
        }

        return connection;
    }

    /**
     * 检查数据库连接是否有效
     *
     * @param conn 数据量连接
     * @return 数据库连接是否有效
     */
    private boolean isValidConnection(Connection conn) {
        boolean result = false;
        try {
            if (conn != null && conn.isValid(1)) {
                result = true;
            }
        } catch (SQLException ignore) {
            // ignore
        }

        return result;
    }

}
