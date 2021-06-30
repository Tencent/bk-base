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

import com.tencent.bk.base.datahub.databus.connect.jdbc.error.ConnectionBrokenException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.DataTooLongException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.DuplicateEntryException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.IncorrectStringException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderInstanceDownException;
import com.tencent.bk.base.datahub.databus.connect.jdbc.error.TspiderNoPartitionException;
import com.tencent.bk.base.datahub.databus.commons.errors.ConnectException;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcUtils {

    private static final Logger log = LoggerFactory.getLogger(JdbcUtils.class);

    // 这里配置的等待时间不宜过长，否则会导致执行进程一直sleep，及时触发task shutdown，也需要等待sleep完毕后才会执行。
    private static final int[] INTERNAL = {5000, 3000, 1000};


    /**
     * The default table types to include when listing tables if none are specified. Valid values
     * are those specified by the @{java.sql.DatabaseMetaData#getTables} method's TABLE_TYPE column.
     * The default only includes standard, user-defined tables.
     */
    public static final Set<String> DEFAULT_TABLE_TYPES = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList("TABLE", "VIEW"))
    );

    private static final int GET_TABLES_TYPE_COLUMN = 4;
    private static final int GET_TABLES_NAME_COLUMN = 3;

    private static final int GET_COLUMNS_COLUMN_NAME = 4;
    private static final int GET_COLUMNS_IS_NULLABLE = 18;
    private static final int GET_COLUMNS_IS_AUTOINCREMENT = 23;


    /**
     * 构建插入数据库的语句
     *
     * @param tableName 数据库表的表名
     * @param columnNames 字段名称数组
     * @param escape escape字符
     * @return 插入的sql语句
     */
    public static String buildInsertSql(String tableName, String[] columnNames, String escape) {
        String insert = "INSERT INTO " + escape + tableName + escape + " (##COLUMNS##) VALUES (##VALUES##)";
        return buildSql(insert, columnNames, escape);
    }

    /**
     * 构建插入数据库的sql语句
     *
     * @param dbName 数据库库名称
     * @param tableName 数据库表名称
     * @param columnNames 字段名称数组
     * @param escape escape字符
     * @return 插入的sql语句
     */
    public static String buildInsertSql(String dbName, String tableName, String[] columnNames, String escape) {
        String insert = "INSERT INTO " + escape + dbName + escape + "." + escape + tableName + escape
                + " (##COLUMNS##) VALUES (##VALUES##)";
        return buildSql(insert, columnNames, escape);
    }


    /**
     * 根据sql模板构建sql插入语句并返回
     *
     * @param sql 插入sql模板
     * @param columnNames 字段名称数组
     * @param escape escape字符
     * @return 替换后的sql语句
     */
    private static String buildSql(String sql, String[] columnNames, String escape) {
        // column名称加上escape，避免和关键字冲突
        String cols = escape + StringUtils.join(columnNames, escape + "," + escape) + escape;
        String[] vals = new String[columnNames.length];
        for (int i = 0; i < vals.length; i++) {
            vals[i] = "?";
        }
        String values = StringUtils.join(vals, ",");
        return sql.replace("##COLUMNS##", cols).replace("##VALUES##", values);
    }

    /**
     * 将数据批量插入到数据库中，如果所有重试均失败，记入到日志中，并通过更上层进行重试。
     *
     * @param connection 数据库连接
     * @param sql 插入的sql语句
     * @param records 待处理的数据列表
     * @param timeoutSeconds sql执行超时时间
     */
    public static void batchInsert(Connection connection, String sql, List<List<Object>> records, int timeoutSeconds)
            throws ConnectionBrokenException, DataTooLongException, TspiderNoPartitionException,
            TspiderInstanceDownException, DuplicateEntryException, IncorrectStringException {
        int maxRetries = 1; // 去掉重试机制，避免数据写入tspider中出现重复
        while (maxRetries > 0) {
            PreparedStatement preparedStatement = null;
            try {
                preparedStatement = connection.prepareStatement(sql); // 构建preparedStatement用于batch insert
                preparedStatement.setQueryTimeout(timeoutSeconds);
                addBatchStatement(records, preparedStatement);
                preparedStatement.executeBatch();
                records.clear();// 数据写入tspider成功后，将records置空
                return;
            } catch (SQLException e) {
                handleSQLException(e, timeoutSeconds, sql);
                maxRetries--; // 重试次数减一
                // TODO 可能这里的逻辑导致线程长时间sleep，在stop task时，超过task停止的等待时间，无法优雅shutdown task。
                try { // 等待一定时间后重试
                    Thread.sleep(INTERNAL[maxRetries] + (long) (Math.random() * 3000));
                } catch (InterruptedException ok) { // just ignore this
                }
                if (maxRetries == 0) {
                    LogUtils.reportExceptionLog(log, LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR,
                            "set task state to FAILED! failed to execute sql " + sql, e);
                    throw new ConnectException("failed to execute sql " + sql, e);
                } else {
                    LogUtils.warn(log, "failed to execute sql {}, remaining retries {}. errMsg: {}", sql, maxRetries,
                            e.getMessage());
                }
            } finally { // 关闭prepareStatement
                try {
                    if (preparedStatement != null) {
                        preparedStatement.close();
                    }
                } catch (SQLException ok) {
                    LogUtils.warn(log,
                            "failed to close the prepare statement due to SQLException! {}" + ok.getMessage());
                }
            }
        }
    }

    private static void handleSQLException(SQLException e, int timeoutSeconds, String sql)
            throws TspiderNoPartitionException, DuplicateEntryException, IncorrectStringException,
            DataTooLongException, ConnectionBrokenException, TspiderInstanceDownException {
        String errMsg = e.getMessage();
        LogUtils.warn(log, "batch insert failed: {}", errMsg);
        if (errMsg.contains("Table has no partition")) {
            // 写tspider，数据日期thedate太老或者为未来时间，存在分区不存在的异常，无法写入，需要过滤掉
            throw new TspiderNoPartitionException(e);
        } else if (errMsg.contains("Duplicate entry")) {
            // 主键冲突，数据有重复
            throw new DuplicateEntryException(e);
        } else if (errMsg.contains("Incorrect string value") || errMsg.contains("invalid byte sequence")) {
            // UTF8格式的字符串写入tspider失败，需跳过此数据
            throw new IncorrectStringException(e);
        } else if (errMsg.contains("Data too long for column") || errMsg.contains("Data truncated for column")
                || errMsg.contains("Data truncation") || errMsg.contains("cannot be null")) {
            throw new DataTooLongException(e);
        } else if (errMsg.contains("Remote MySQL server has gone away") || errMsg
                .contains("No operations allowed")
                || errMsg.contains("Communications link failure") || errMsg
                .contains("The last packet successfully received from the server")
                || errMsg.contains("Write failed")) {
            throw new ConnectionBrokenException(e);
        } else if (errMsg.contains("Unable to connect to foreign data source")) {
            throw new TspiderInstanceDownException(e);
        } else {
            LogUtils.warn(log, "{} timeout {} {}", sql, timeoutSeconds, errMsg);
        }
    }

    private static void addBatchStatement(List<List<Object>> records, PreparedStatement preparedStatement)
            throws SQLException {
        for (List<Object> objList : records) {
            for (int j = 0; j < objList.size(); j++) {
                preparedStatement.setObject(j + 1, objList.get(j));
            }
            preparedStatement.addBatch();
        }
    }

    /**
     * 删除数据库中指定的数据,保证插入数据后,数据唯一,不会重复。
     *
     * @param connection 数据库连接
     * @param dbName 数据库库名称
     * @param tableName 数据库表名称
     * @param columns 表字段列表
     * @param records 待删除的数据记录
     */
    public static void deleteRecords(Connection connection, String dbName, String tableName, String[] columns,
            List<List<Object>> records, String escape) {
        if (records.size() > 0) {
            List<String> delSql = buildDeleteSqlList(dbName, tableName, columns, records, true, escape);
            LogUtils.debug(log, "going to delete records: {}", StringUtils.join(delSql, "; "));
            String msg = execSql(connection, delSql);
            if (!msg.equals("")) {
                // sql语句执行失败，记录日志
                LogUtils.warn(log, "failed to delete records by sql {} --- {}", delSql, msg);
            }
        }
    }

    /**
     * 执行sql语句，如果执行成功，返回空字符串。如果执行失败，返回错误消息。
     *
     * @param connection 数据库连接
     * @param sqls 待执行的sql语句列表
     * @return 执行时碰到的异常提示信息，如果执行成功，返回空字符串。
     */
    private static String execSql(Connection connection, List<String> sqls) {
        String msg = "";
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            for (String sql : sqls) {
                stmt.addBatch(sql);
            }
            stmt.executeBatch();
        } catch (SQLException ignore) {
            msg = ignore.getMessage();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (Exception ignore) {
                // ignore
            }
        }

        return msg;
    }

    /**
     * 构造删除数据的sql语句并返回。
     *
     * @param dbName 数据库库名
     * @param tableName 表名
     * @param columns 字段名称
     * @param records 待删除的记录列表
     * @param needEscape 是否转义字符串中的'\字符
     * @return 删除的sql语句
     */
    private static List<String> buildDeleteSqlList(String dbName, String tableName, String[] columns,
            List<List<Object>> records, boolean needEscape, String escape) {
        List<String> result = new ArrayList<>(records.size());
        for (List<Object> record : records) {
            StringBuilder sb = new StringBuilder("DELETE FROM ");
            if (StringUtils.isNotBlank(dbName)) {
                sb.append(escape).append(dbName).append(escape).append(".");
            }
            sb.append(escape).append(tableName).append(escape).append(" WHERE ")
                    .append(buildConditions(columns, record, needEscape));

            result.add(sb.toString());
        }

        return result;
    }

    /**
     * 根据一条记录构建删除条件
     *
     * @param columns 字段名称列表
     * @param record 记录的值列表
     * @param needEscape 是否转义字符串中的'\字符
     * @return 删除条件字符串
     */
    private static String buildConditions(String[] columns, List<Object> record, boolean needEscape) {
        List<String> conds = new ArrayList<>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            Object obj = record.get(i);
            String tmp = "`" + columns[i] + "`";
            if (obj == null) {
                conds.add(tmp + " IS NULL");
            } else if (obj instanceof String) {
                // 将单引号替换为''，反斜杠替换为两个反斜杠，避免拼sql语句时出错
                String escaped =
                        needEscape ? StringUtils.replace(StringUtils.replace(obj.toString(), "'", "''"), "\\", "\\\\")
                                : obj.toString();
                conds.add(tmp + "='" + escaped + "'");
            } else {
                conds.add(tmp + "=" + obj);
            }
        }

        return StringUtils.join(conds, " AND ");
    }

    /**
     * 返回将要插入到Tspider中的数据和对应的sql语句组成的字符串
     *
     * @param recordList 数据列表
     * @return 插入的sql和数据的字符串
     */
    public static String getRecordsInString(String sql, List<List<Object>> recordList) {
        // 获取insert into xxx (x, x, x) values 这一段字符串
        String insertSql = sql.substring(0, sql.indexOf("(?"));
        StringBuilder sb = new StringBuilder().append(System.lineSeparator()).append(insertSql);

        // 将每行记录转换为sql语句
        List<String> values = new ArrayList<>(recordList.size());
        for (List<Object> objList : recordList) {
            values.add(" ()");
        }
        sb.append(StringUtils.join(values, ",")).append(";");

        return sb.toString();
    }


    private static final ThreadLocal<SimpleDateFormat> DATE_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
        }
    };

    /**
     * Get a list of tables in the database. This uses the default filters, which only include
     * user-defined tables.
     *
     * @param conn database connection
     * @return a list of tables
     * @throws SQLException e
     */
    public static List<String> getTables(Connection conn, String schemaPattern) throws SQLException {
        return getTables(conn, schemaPattern, DEFAULT_TABLE_TYPES);
    }

    /**
     * Get a list of table names in the database.
     *
     * @param conn database connection
     * @param types a set of table types that should be included in the results
     * @throws SQLException e
     */
    public static List<String> getTables(Connection conn, String schemaPattern, Set<String> types) throws SQLException {
        DatabaseMetaData metadata = conn.getMetaData();
        try (ResultSet rs = metadata.getTables(null, schemaPattern, "%", null)) {
            List<String> tableNames = new ArrayList<>();
            while (rs.next()) {
                if (types.contains(rs.getString(GET_TABLES_TYPE_COLUMN))) {
                    String colName = rs.getString(GET_TABLES_NAME_COLUMN);
                    // SQLite JDBC driver does not correctly mark these as system tables
                    if (metadata.getDatabaseProductName().equals("SQLite") && colName.startsWith("sqlite_")) {
                        continue;
                    }

                    tableNames.add(colName);
                }
            }
            return tableNames;
        }
    }

    /**
     * Look up the autoincrement column for the specified table.
     *
     * @param conn database connection
     * @param table the table to
     * @return the name of the column that is an autoincrement column, or null if there is no autoincrement column or
     *         more than one exists
     * @throws SQLException e
     */
    public static String getAutoincrementColumn(Connection conn, String schemaPattern, String table)
            throws SQLException {
        String result = null;
        int matches = 0;

        try (ResultSet rs = conn.getMetaData().getColumns(null, schemaPattern, table, "%")) {
            // Some database drivers (SQLite) don't include all the columns
            if (rs.getMetaData().getColumnCount() >= GET_COLUMNS_IS_AUTOINCREMENT) {
                while (rs.next()) {
                    if (rs.getString(GET_COLUMNS_IS_AUTOINCREMENT).equals("YES")) {
                        result = rs.getString(GET_COLUMNS_COLUMN_NAME);
                        matches++;
                    }
                }
                return (matches == 1 ? result : null);
            }
        }

        // Fallback approach is to query for a single row. This unfortunately does not work with any
        // empty table
        LogUtils.trace(log, "Falling back to SELECT detection of auto-increment column for {}:{}", conn, table);
        try (Statement stmt = conn.createStatement()) {
            String quoteString = getIdentifierQuoteString(conn);
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + quoteString + table + quoteString + " LIMIT 1");
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 1; i < rsmd.getColumnCount(); i++) {
                if (rsmd.isAutoIncrement(i)) {
                    result = rsmd.getColumnName(i);
                    matches++;
                }
            }
        }
        return (matches == 1 ? result : null);
    }

    /**
     * Get the string used for quoting identifiers in this database's SQL dialect.
     *
     * @param connection the database connection
     * @return the quote string
     * @throws SQLException e
     */
    public static String getIdentifierQuoteString(Connection connection) throws SQLException {
        String quoteString = connection.getMetaData().getIdentifierQuoteString();
        quoteString = quoteString == null ? "" : quoteString;
        return quoteString;
    }

    /**
     * Quote the given string.
     *
     * @param orig the string to quote
     * @param quote the quote character
     * @return the quoted string
     */
    public static String quoteString(String orig, String quote) {
        return quote + orig + quote;
    }

    /**
     * Return current time at the database
     *
     * @param conn Connection
     * @param cal Calendar
     * @return timestamp
     */
    public static Timestamp getCurrentTimeOnDb(Connection conn, Calendar cal) throws SQLException, ConnectException {
        String query;

        // This is ugly, but to run a function, everyone does 'select function()'
        // except Oracle that does 'select function() from dual'
        // and Derby uses either the dummy table SYSIBM.SYSDUMMY1  or values expression (I chose to use values)
        String dbProduct = conn.getMetaData().getDatabaseProductName();
        if ("Oracle".equals(dbProduct)) {
            query = "select CURRENT_TIMESTAMP from dual";
        } else if ("Apache Derby".equals(dbProduct)) {
            query = "values(CURRENT_TIMESTAMP)";
        } else {
            query = "select CURRENT_TIMESTAMP;";
        }

        try (Statement stmt = conn.createStatement()) {
            LogUtils.debug(log, "executing query " + query + " to get current time from database");
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return rs.getTimestamp(1, cal);
            } else {
                throw new ConnectException(
                        "Unable to get current time from DB using query " + query + " on database " + dbProduct);
            }
        } catch (SQLException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log,
                    "Failed to get current time from DB using query " + query + " on database " + dbProduct, e);
            throw e;
        }
    }

    /**
     * Get the real data type from database
     *
     * @param conn Connection
     * @param tableName TableName
     * @param columnName ColumnName
     * @return the type name of columnName in the database, e.g. varchar/char/datetime/timestamp
     */
    public static String getColumnTypeOnDb(Connection conn, String tableName, String columnName)
            throws SQLException, ConnectException {
        String databaseName = conn.getCatalog();
        tableName = tableName.replace("`", "");
        String query = "select data_type from information_schema.columns where table_name= '" + tableName
                + "' and table_schema= '" + databaseName + "' and COLUMN_NAME = '" + columnName + "'  ";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {

            LogUtils.debug(log, "executing query " + stmt.toString() + " to get columnType from database");
            ResultSet rs = stmt.executeQuery(query);
            if (rs.next()) {
                return rs.getString(1);
            } else {
                throw new ConnectException("Unable to get column type from DB using query " + query + " on database ");
            }
        } catch (SQLException e) {
            LogUtils.error(LogUtils.ERR_PREFIX + LogUtils.MYSQL_ERR, log,
                    "Failed to get columnType from DB using query " + query + " on database ", e);
            throw e;
        }
    }


}
