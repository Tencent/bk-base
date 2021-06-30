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

package com.tencent.bk.base.datahub.iceberg;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class ScriptRunner {

    private static final Logger log = LoggerFactory.getLogger(ScriptRunner.class);
    private static final String DEFAULT_DELIMITER = ";";
    private final Connection connection;

    public ScriptRunner(Connection connection) {
        this.connection = connection;
    }

    public void runScript(Reader reader) throws IOException, SQLException {
        try {
            boolean originalAutoCommit = connection.getAutoCommit();
            try {
                if (!originalAutoCommit) {
                    connection.setAutoCommit(true);
                }

                runScript(connection, reader);
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        } catch (IOException | SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error running script: " + e.getMessage(), e);
        }
    }

    public void close() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    //@SuppressWarnings("checkstyle:CyclomaticComplexity")
    private void runScript(Connection conn, Reader reader) throws IOException, SQLException {
        StringBuilder command = null;
        try (Statement statement = conn.createStatement()) {
            LineNumberReader lineReader = new LineNumberReader(reader);
            String line = null;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuilder();
                }

                String trimmedLine = line.trim();
                if (trimmedLine.startsWith("--") || trimmedLine.startsWith("//")) {
                    log.info(trimmedLine);
                } else if (trimmedLine.endsWith(getDelimiter())) {
                    command.append(line.substring(0, line.lastIndexOf(getDelimiter())));
                    command.append(" ");
                    String cmd = command.toString();
                    log.info(cmd);
                    boolean hasResults = statement.execute(cmd);
                    conn.commit();

                    ResultSet rs = statement.getResultSet();
                    if (hasResults && rs != null) {
                        ResultSetMetaData md = rs.getMetaData();
                        int cols = md.getColumnCount();
                        StringBuilder sb = new StringBuilder();
                        for (int i = 1; i <= cols; i++) {
                            sb.append(md.getColumnLabel(i)).append("\t");
                        }

                        log.info(sb.toString());
                        while (rs.next()) {
                            sb = new StringBuilder();
                            for (int i = 1; i <= cols; i++) {
                                sb.append(rs.getString(i)).append("\t");
                            }
                            log.info(sb.toString());
                        }
                    }

                    command = null;
                    Thread.yield();
                } else {
                    command.append(line);
                    command.append(" ");
                }
            }

            conn.commit();
        } catch (SQLException | IOException e) {
            log.error("Error executing: " + command, e);
            throw e;
        } finally {
            conn.rollback();
        }
    }

    private String getDelimiter() {
        return DEFAULT_DELIMITER;
    }
}