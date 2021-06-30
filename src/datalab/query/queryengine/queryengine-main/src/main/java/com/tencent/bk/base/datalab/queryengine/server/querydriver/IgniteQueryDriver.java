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
import org.apache.commons.lang.StringUtils;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_IGNITE)
public class IgniteQueryDriver extends AbstractQueryDriver {

    private static final String QUERY_URL_PATTERN = "jdbc:ignite:thin://%s?useUnicode=true"
            + "&characterEncoding=utf-8&lazy=true";

    private static final String TABLE_NOT_FOUND_MSG_PATTERN = "Table .+ not found";

    @Override
    public Connection doGetConnection(QueryTaskContext queryTaskContext) throws Exception {
        ConnectionMeta connectionMeta = getConnectionMeta(queryTaskContext);
        String endPoints = buildEndPoints(connectionMeta.getHost(), connectionMeta.getPort());
        return DriverManager
                .getConnection(String.format(QUERY_URL_PATTERN, endPoints), connectionMeta.getUser(),
                        connectionMeta.getPassWord());
    }

    /**
     * 构建 Ignite endPoints
     *
     * @param host host 列表
     * @param port 端口
     * @return host:port 列表字符串
     */
    private String buildEndPoints(String host, int port) {
        StringBuilder endPointSb = new StringBuilder();
        if (StringUtils.isNotBlank(host) && port > 0) {
            String[] hostArray = host.split(",");
            boolean isHead = true;
            for (int i = 0; i < hostArray.length; i++) {
                if (isHead) {
                    endPointSb.append(hostArray[i])
                            .append(":")
                            .append(port);
                    isHead = false;
                } else {
                    endPointSb.append(",")
                            .append(hostArray[i])
                            .append(":")
                            .append(port);
                }
            }
        }
        return endPointSb.toString();
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
        log.error("QueryDriver error|queryid:{}|sql:{}|errorCode:{}|message:{}",
                queryTaskContext.getQueryTaskInfo().getQueryId(),
                queryTaskContext.getQueryTaskInfo().getSqlText(),
                ex.getErrorCode(), ex.getMessage());
        String errorInfo;
        IgniteErrorCodeEnum code = getErrorCode(ex);
        switch (code) {
            case CONVERSION_ERROR:
                errorInfo = String.format("Type convertion error：%s", ex.getMessage());
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, errorInfo);
                break;
            case CONNECT_REJECT_ERROR:
                errorInfo = String.format("Connect reject error：%s", ex.getMessage());
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_CONNECT_OTHER_ERROR, errorInfo);
                break;
            case UNSUPPORTED_PARAM_TYPE_ERROR:
                errorInfo = String.format("UnSupported parameter Type：%s", ex.getMessage());
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, errorInfo);
                break;
            case QUERY_PARSE_ERROR:
                Matcher matcher = Pattern.compile(TABLE_NOT_FOUND_MSG_PATTERN)
                        .matcher(ex.getMessage());
                if (matcher.find()) {
                    errorInfo = MessageLocalizedUtil.getMessage("查询异常，物理表不存在，入库任务没有启动或没有正常运行");
                    QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.SQL_ERROR_REAL_TABLE_NOT_FOUND,
                            errorInfo);
                } else {
                    errorInfo = String.format("Ignite syntax error：%s", ex.getMessage());
                    QueryDriverUtil
                            .handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, errorInfo);
                }
                break;
            case INTERNAL_ERROR:
                errorInfo = String.format("Ignite inner error：%s", ex.getMessage());
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR, errorInfo);
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
    private IgniteErrorCodeEnum getErrorCode(SQLException ex) {
        String exCode = String.valueOf(ex.getSQLState());
        IgniteErrorCodeEnum code = IgniteErrorCodeEnum.OTHER;
        for (IgniteErrorCodeEnum ec : IgniteErrorCodeEnum.values()) {
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
    public enum IgniteErrorCodeEnum {
        CONVERSION_ERROR("0700B"),
        CONNECT_REJECT_ERROR("08004"),
        UNSUPPORTED_PARAM_TYPE_ERROR("22023"),
        QUERY_PARSE_ERROR("42000"),
        INTERNAL_ERROR("50000"),
        OTHER("other");
        public final String code;

        IgniteErrorCodeEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }
}
