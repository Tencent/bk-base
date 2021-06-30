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

import com.tencent.bk.base.datalab.meta.StorageCluster;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.util.ErrorMessageItem;
import com.tencent.bk.base.datalab.queryengine.server.util.FillQueryTaskStageUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_POSTGRESQL)
public class PgQueryDriver extends AbstractQueryDriver {

    private static final String URL_PATTERN = "jdbc:postgresql://%s:%d/%s?ssl=true";
    private static final Pattern COLUMN_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("column (.+) does not exist");
    private static final Pattern FUNC_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("function (.+) does not exist");
    private static final Pattern TABLE_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("relation (.+) does not exist");

    @Override
    public Connection doGetConnection(QueryTaskContext queryTaskContext) throws Exception {
        ConnectionMeta connectionMeta = getConnectionMeta(queryTaskContext);
        return DriverManager
                .getConnection(String.format(URL_PATTERN, connectionMeta.getHost(), connectionMeta.getPort(),
                        getDbName(queryTaskContext)),
                        connectionMeta.getUser(), connectionMeta.getPassWord());
    }

    /**
     * 获取 dbname
     *
     * @param queryTaskContext 查询上下文
     * @return dbname
     */
    private String getDbName(QueryTaskContext queryTaskContext) {
        StorageCluster cluster = queryTaskContext.getPickedValidStorage()
                .getStorageCluster();
        Map<String, Object> connectionInfo = JacksonUtil
                .convertJson2Map(cluster.getConnectionInfo());
        if (connectionInfo == null || !connectionInfo.containsKey(StorageConstants.CONNECTION_DB)) {
            String errorInfo = "Failed to extractDb";
            ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_OTHER_ERROR.code(),
                    ResultCodeEnum.QUERY_OTHER_ERROR.message(), errorInfo);
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB,
                    JacksonUtil.object2Json(item));
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR, errorInfo);
        }
        return connectionInfo.get(StorageConstants.CONNECTION_DB)
                .toString();
    }

    /**
     * 处理执行异常
     *
     * @param queryTaskContext 查询上下文
     * @param e 异常实例
     */
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
        String errorInfo;
        PgErrorCodeEnum code = getErrorCode(ex);
        switch (code) {
            case CONNECTION_FAILURE:
                errorInfo = String.format("Communication failure：%s", ex.getMessage());
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_CONNECT_OTHER_ERROR, errorInfo);
                break;
            case INVALID_PARAM_TYPE_ERROR:
                errorInfo = String.format("Invalid parameter type：%s", ex.getMessage());
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, errorInfo);
                break;
            case UNDEFINED_COLUMN:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, COLUMN_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.QUERY_FILED_NOT_EXSITS);
                break;
            case UNDEFINED_FUNCTION:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, FUNC_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.QUERY_FUNC_NOT_EXISTS);
                break;
            case UNDEFINED_RELATION:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, TABLE_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.SQL_ERROR_REAL_TABLE_NOT_FOUND);
                break;
            case SYNTAX_ERROR:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR,
                        ex.getMessage());
                break;
            default:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_OTHER_ERROR, ex.getMessage());
        }
    }

    /**
     * 获取异常错误码
     *
     * @param ex SQLException 实例
     * @return 异常错误码
     */
    private PgErrorCodeEnum getErrorCode(SQLException ex) {
        String exCode = String.valueOf(ex.getSQLState());
        PgErrorCodeEnum code = PgErrorCodeEnum.OTHER;
        for (PgErrorCodeEnum ec : PgErrorCodeEnum.values()) {
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
    public enum PgErrorCodeEnum {
        INVALID_PARAM_TYPE_ERROR("07006"),
        CONNECTION_FAILURE("08006"),
        SYNTAX_ERROR("42601"),
        UNDEFINED_COLUMN("42703"),
        UNDEFINED_FUNCTION("42883"),
        UNDEFINED_RELATION("42P01"),
        OTHER("other");
        public final String code;

        PgErrorCodeEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }
}
