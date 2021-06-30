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

package com.tencent.bk.base.datalab.queryengine.server.querydriver;

import static com.tencent.bk.base.datalab.queryengine.server.querydriver.ClickHouseQueryDriver.ClickHouseErrorCodeEnum.FUNC_NOT_FOUND;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.meta.StorageCluster;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_CLICKHOUSE)
public class ClickHouseQueryDriver extends AbstractQueryDriver {

    public static final String TCP_TGW = "tcp_tgw";
    private static final String QUERY_URL_PATTERN = "jdbc:clickhouse://%s";
    private static final Pattern MISSING_COLUMNS_MSG_PATTERN = Pattern
            .compile(
                    "DB::ExceptionDB::Exception: Missing columns: '(.+)' while processing "
                            + "query[\\s\\S]+");
    private static final Pattern UNKNOWN_FUNCTION_MSG_PATTERN = Pattern.compile(
            "DB::ExceptionDB::Exception: Unknown function (\\w+)\\.[\\s\\S]+");
    private static final Pattern DATABASE_NOT_EXIST_MSG_PATTERN = Pattern
            .compile("DB::ExceptionDB::Exception: Database (.+) doesn't exist[\\s\\S]+");
    private static final Pattern TABLE_NOT_EXIST_MSG_PATTERN = Pattern
            .compile("DB::ExceptionDB::Exception: Table (.+) doesn't exist[\\s\\S]+");
    private static final Pattern SYNTAX_ERROR_MSG_PATTERN = Pattern
            .compile("DB::ExceptionDB::Exception: Syntax error[\\s\\S]+");
    private static final Pattern ILLEGAL_GROUP_MSG_PATTERN = Pattern
            .compile(
                    "DB::ExceptionDB::Exception: Column `(.+)` is not under aggregate function "
                            + "and not in GROUP BY[\\s\\S]+");
    private static final ImmutableMap<ClickHouseErrorCodeEnum, Pattern> ERROR_PATTERN_MAP;
    private static final ImmutableMap<ClickHouseErrorCodeEnum, ResultCodeEnum> ERROR_CODE_MAP;

    static {
        ERROR_PATTERN_MAP = ImmutableMap.<ClickHouseErrorCodeEnum, Pattern>builder()
                .put(ClickHouseErrorCodeEnum.DATABASE_NOT_FOUND,
                        DATABASE_NOT_EXIST_MSG_PATTERN)
                .put(ClickHouseErrorCodeEnum.TABLE_NOT_FOUND,
                        TABLE_NOT_EXIST_MSG_PATTERN)
                .put(ClickHouseErrorCodeEnum.COLUMN_NOT_FOUND,
                        MISSING_COLUMNS_MSG_PATTERN)
                .put(FUNC_NOT_FOUND,
                        UNKNOWN_FUNCTION_MSG_PATTERN)
                .put(ClickHouseErrorCodeEnum.SYNTAX_ERROR_MISMATCHED,
                        SYNTAX_ERROR_MSG_PATTERN)
                .put(ClickHouseErrorCodeEnum.SYNTAX_ERROR_ILLEGAL_GROUP,
                        ILLEGAL_GROUP_MSG_PATTERN).build();

        ERROR_CODE_MAP = ImmutableMap.<ClickHouseErrorCodeEnum, ResultCodeEnum>builder()
                .put(ClickHouseErrorCodeEnum.DATABASE_NOT_FOUND,
                        ResultCodeEnum.QUERY_SCHEMA_NOT_FOUND)
                .put(ClickHouseErrorCodeEnum.TABLE_NOT_FOUND,
                        ResultCodeEnum.SQL_ERROR_REAL_TABLE_NOT_FOUND)
                .put(ClickHouseErrorCodeEnum.COLUMN_NOT_FOUND,
                        ResultCodeEnum.QUERY_FILED_NOT_EXSITS)
                .put(FUNC_NOT_FOUND,
                        ResultCodeEnum.QUERY_FUNC_NOT_EXISTS).build();
    }

    @Override
    public Connection doGetConnection(QueryTaskContext queryTaskContext) throws Exception {
        StorageCluster cluster = queryTaskContext.getPickedValidStorage()
                .getStorageCluster();
        Map<String, Object> connectionMap = getConnectionMap(queryTaskContext);
        String tcpTgw = requireNonNull(connectionMap.get(TCP_TGW),
                String.format("ClickHouse tcp_tgw is null:%s", cluster.getClusterName()))
                .toString();
        return DriverManager
                .getConnection(String.format(QUERY_URL_PATTERN, tcpTgw));
    }

    /**
     * 处理执行异常
     *
     * @param queryTaskContext QueryTaskContext
     * @param e 异常实例
     */
    @Override
    public void processExecuteException(QueryTaskContext queryTaskContext, Throwable e) {
        if (!(e instanceof SQLException)) {
            return;
        }
        SQLException ex = (SQLException) e;
        log.error("QueryDriver error|queryid:{}|sql:{}|errorCode:{}|message:{}",
                queryTaskContext.getQueryTaskInfo().getQueryId(),
                queryTaskContext.getQueryTaskInfo().getSqlText(),
                ex.getErrorCode(), ex.getMessage());
        ClickHouseErrorCodeEnum code = getErrorCode(ex);
        switch (code) {
            case DATABASE_NOT_FOUND:
            case TABLE_NOT_FOUND:
            case COLUMN_NOT_FOUND:
            case FUNC_NOT_FOUND:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, ERROR_PATTERN_MAP.get(code),
                        ERROR_CODE_MAP.get(code));
                break;
            case SYNTAX_ERROR_MISMATCHED:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR,
                        ex.getMessage());
                break;
            case SYNTAX_ERROR_ILLEGAL_GROUP:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR,
                        MessageLocalizedUtil.getMessage(StorageConstants.ILLEGAL_GROUP_MSG_TEMPLATE,
                                new Object[]{QueryDriverUtil.getErrorItemFromPattern(ILLEGAL_GROUP_MSG_PATTERN,
                                        ex.getMessage(), 1)}));
                break;
            default:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        e.getMessage());
        }
    }

    /**
     * 获取异常错误码
     *
     * @param exception SQLException 实例
     * @return 异常错误码
     */
    private ClickHouseErrorCodeEnum getErrorCode(SQLException exception) {
        AtomicReference<ClickHouseErrorCodeEnum> errorCode = new AtomicReference<>();
        ERROR_PATTERN_MAP.entrySet().stream()
                .filter(entry -> entry.getValue().matcher(exception.getMessage()).matches())
                .findFirst()
                .ifPresent(entry -> errorCode.set(entry.getKey()));
        if (errorCode.get() == null) {
            errorCode.set(ClickHouseErrorCodeEnum.OTHER);
        }
        return errorCode.get();
    }

    /**
     * ErrorCode 列表
     */
    public enum ClickHouseErrorCodeEnum {
        SYNTAX_ERROR_MISMATCHED("syntax_error_mismatched"),
        SYNTAX_ERROR_ILLEGAL_GROUP("syntax_error_illegal_group"),
        DATABASE_NOT_FOUND("database_not_found"),
        TABLE_NOT_FOUND("table_not_found"),
        COLUMN_NOT_FOUND("column_not_found"),
        FUNC_NOT_FOUND("func_not_found"),
        OTHER("other");
        public final String code;

        ClickHouseErrorCodeEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }
}
