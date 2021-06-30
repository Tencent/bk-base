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

package com.tencent.bk.base.datahub.databus.connect.jdbc.util;

import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class CachedConnectionProvider {

  private static final Logger log = LoggerFactory.getLogger(CachedConnectionProvider.class);

  private static final int VALIDITY_CHECK_TIMEOUT_SECONDS = 5;

  private final String url;
  private final String username;
  private final String password;

  private Connection connection;

  public CachedConnectionProvider(String url) {
    this(url, null, null);
  }

  public CachedConnectionProvider(String url, String username, String password) {
    this.url = url;
    this.username = username;
    this.password = password;
  }

  /**
   * 获取有效连接
   * @return Connection
   */
  public synchronized Connection getValidConnection() {
    try {
      if (connection == null) {
        newConnection();
      } else if (!connection.isValid(VALIDITY_CHECK_TIMEOUT_SECONDS)) {
        LogUtils.info(log,"The database connection is invalid. Reconnecting...");
        closeQuietly();
        newConnection();
      }
    } catch (SQLException sqle) {
      throw new ConnectException(sqle);
    }
    return connection;
  }

  private void newConnection() throws SQLException {
    LogUtils.debug(log,"Attempting to connect to {}", url);
    Properties connectionProps = new Properties();
    connectionProps.put("user", username);
    connectionProps.put("password", password);
    // 高版本mysql必须指明是否使用ssl
    connectionProps.put("useSSL", "false");
    connection = DriverManager.getConnection(url, connectionProps);
    onConnect(connection);
  }

  /**
   * 关闭连接
   */
  public synchronized void closeQuietly() {
    if (connection != null) {
      try {
        connection.close();
        connection = null;
      } catch (SQLException sqle) {
        LogUtils.warn(log,"Ignoring error closing connection", sqle);
      }
    }
  }

  protected void onConnect(Connection connection) throws SQLException {
  }

}
