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

import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_MYSQL)
public class MySqlQueryDriver extends AbstractQueryDriver {

    private static final String QUERY_URL_PATTERN = "jdbc:mysql://%s:%d?useUnicode=true"
            + "&characterEncoding=utf-8&autoReconnect=true&failOverReadOnly=false";
    private static final Pattern COLUMN_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("Unknown column '(.+)' in 'field "
                    + "list");
    private static final Pattern TABLE_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("Table '(.+)' doesn't "
                    + "exist");
    private static final Pattern FUNC_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("FUNCTION (.+) does not exist");
    private static final Pattern COMMUNICATIONS_LINK_FAILURE_PATTERN = Pattern
            .compile("Communications link failure[\\s\\S]*");
    private static final String CONNECT_ERROR_CODE = "0";

    @Override
    public Connection doGetConnection(QueryTaskContext queryTaskContext) throws Exception {
        ConnectionMeta connectionMeta = getConnectionMeta(queryTaskContext);
        return DriverManager
                .getConnection(String.format(QUERY_URL_PATTERN, connectionMeta.getHost(), connectionMeta.getPort()),
                        connectionMeta.getUser(), connectionMeta.getPassWord());
    }

    @Override
    public void processExecuteException(QueryTaskContext queryTaskContext, Throwable e) {
        if (!(e instanceof SQLException)) {
            return;
        }
        SQLException ex = (SQLException) e;
        log.error("QueryDriver error|queryId:{}|sql:{}|errorCode:{}|message:{}",
                queryTaskContext.getQueryTaskInfo().getQueryId(),
                queryTaskContext.getQueryTaskInfo().getSqlText(),
                ex.getErrorCode(), ex.getMessage());
        MySqlErrorCodeEnum code = getErrorCode(ex);
        String errorInfo;
        switch (code) {
            case COLUMN_NOT_FOUND:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, COLUMN_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.QUERY_FILED_NOT_EXSITS);
                break;
            case FUNC_NOT_FOUND:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, FUNC_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.QUERY_FUNC_NOT_EXISTS);
                break;
            case TABLE_NOT_FOUND:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, TABLE_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.SQL_ERROR_REAL_TABLE_NOT_FOUND);
                break;
            case SYNTAX_ERROR:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR,
                        ex.getMessage());
                break;
            case COMMUNICATIONS_LINK_FAILURE:
            case ER_NET_READ_ERROR:
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询超时，建议优化SQL减少数据集大小或切换为HDFS存储");
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        errorInfo);
                break;
            case ER_DERIVED_MUST_HAVE_ALIAS:
                errorInfo = MessageLocalizedUtil
                        .getMessage("子查询必须设置别名");
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR,
                        errorInfo);
                break;
            case ER_NET_READ_INTERRUPTED:
                errorInfo = MessageLocalizedUtil
                        .getMessage("数据库连接异常中断");
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        errorInfo);
                break;
            default:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        ex.getMessage());
        }
    }

    /**
     * 获取异常错误码
     *
     * @param ex SQLException 实例
     * @return 异常错误码
     */
    private MySqlErrorCodeEnum getErrorCode(SQLException ex) {
        String exCode = String.valueOf(ex.getErrorCode());
        MySqlErrorCodeEnum code = MySqlErrorCodeEnum.OTHER;
        if (CONNECT_ERROR_CODE.equals(exCode)) {
            Matcher matcher = COMMUNICATIONS_LINK_FAILURE_PATTERN
                    .matcher(ex.getMessage());
            if (matcher.matches()) {
                return MySqlErrorCodeEnum.COMMUNICATIONS_LINK_FAILURE;
            } else {
                return code;
            }
        }
        for (MySqlErrorCodeEnum ec : MySqlErrorCodeEnum.values()) {
            if (ec.code.equalsIgnoreCase(exCode)) {
                code = ec;
                break;
            }
        }
        return code;
    }

    /**
     * ErrorCode 列表
     */
    public enum MySqlErrorCodeEnum {
        COMMUNICATIONS_LINK_FAILURE("0"),
        SYNTAX_ERROR("1064"),
        TABLE_NOT_FOUND("1146"),
        COLUMN_NOT_FOUND("1054"),
        ER_DERIVED_MUST_HAVE_ALIAS("1248"),
        FUNC_NOT_FOUND("1305"),
        ER_NET_READ_ERROR("1158"),
        ER_NET_READ_INTERRUPTED("1159"),
        OTHER("other");
        public final String code;

        MySqlErrorCodeEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }
}
