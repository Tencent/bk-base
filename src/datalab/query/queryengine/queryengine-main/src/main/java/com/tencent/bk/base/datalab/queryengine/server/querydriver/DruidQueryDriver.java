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

import static java.util.Objects.requireNonNull;

import com.tencent.bk.base.datalab.meta.StorageCluster;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.apache.commons.lang3.math.NumberUtils;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_DRUID)
public class DruidQueryDriver extends AbstractQueryDriver {

    private static final Map<String, String> COMMON_HEADER = new HashMap<>();
    private static final String VALIDATION_EXCEPTION = "org.apache.calcite.tools"
            + ".ValidationException";
    private static final String SQLPARSE_EXCEPTION = "org.apache.calcite.sql.parser"
            + ".SqlParseException";
    private static final String QUERY_URL_PATTERN = "jdbc:avatica:remote:url=http://%s:%d/druid"
            + "/v2/sql/avatica/";
    private static final Pattern COLUMN_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("Column '(.+)' not found in any table "
                    + "-> ValidationException");
    private static final Pattern TABLE_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("Object '(.+)' not found "
                    + "-> ValidationException");
    private static final Pattern FUNC_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("No match found for function signature (.+) "
                    + "-> ValidationException");

    @Override
    public Connection doGetConnection(QueryTaskContext queryTaskContext) throws Exception {
        StorageCluster cluster = queryTaskContext.getPickedValidStorage()
                .getStorageCluster();
        Map<String, Object> connectionMap = getConnectionMap(queryTaskContext);
        String host = requireNonNull(connectionMap.get("host"),
                String.format("Connection host is null:%s", cluster.getClusterName())).toString();
        int port = NumberUtils.toInt(requireNonNull(connectionMap.get("port"),
                String.format("Connection port is null:%s", cluster.getClusterName())).toString());
        return DriverManager
                .getConnection(String.format(QUERY_URL_PATTERN, host, port));
    }

    /**
     * 处理执行异常
     *
     * @param queryTaskContext QueryTaskContext
     * @param e 异常实例
     */
    @Override
    public void processExecuteException(QueryTaskContext queryTaskContext, Throwable e) {
        if (!(e instanceof AvaticaSqlException)) {
            return;
        }
        AvaticaSqlException ex = (AvaticaSqlException) e;
        log.error("QueryDriver error|queryid:{}|sql:{}|errorCode:{}|message:{}",
                queryTaskContext.getQueryTaskInfo().getQueryId(),
                queryTaskContext.getQueryTaskInfo().getSqlText(),
                ex.getErrorCode(), ex.getMessage());
        DruidErrorCodeEnum code = getErrorCode(ex);
        switch (code) {
            case TABLE_NOT_FOUND:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, TABLE_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.QUERY_NO_DATA_WRITTEN);
                break;
            case COLUMN_NOT_FOUND:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, COLUMN_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.QUERY_FILED_NOT_EXSITS);
                break;
            case FUNC_NOT_FOUND:
                QueryDriverUtil.handleNotFound(queryTaskContext, ex, FUNC_NOT_FOUND_MSG_PATTERN,
                        ResultCodeEnum.QUERY_FUNC_NOT_EXISTS);
                break;
            default:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        ex.getMessage());
        }
    }


    /**
     * 根据异常消息获取 ErrorCode
     *
     * @param exception SQLException 实例
     * @return 异常错误码
     */
    private DruidErrorCodeEnum getErrorCode(AvaticaSqlException exception) {
        DruidErrorCodeEnum errorCode;
        String message = exception.getMessage();
        Matcher tableMatcher = TABLE_NOT_FOUND_MSG_PATTERN.matcher(message);
        Matcher columnMatcher = COLUMN_NOT_FOUND_MSG_PATTERN.matcher(message);
        Matcher funcMatcher = FUNC_NOT_FOUND_MSG_PATTERN.matcher(message);
        if (tableMatcher.find()) {
            errorCode = DruidErrorCodeEnum.TABLE_NOT_FOUND;
        } else if (columnMatcher.find()) {
            errorCode = DruidErrorCodeEnum.COLUMN_NOT_FOUND;
        } else if (funcMatcher.find()) {
            errorCode = DruidErrorCodeEnum.FUNC_NOT_FOUND;
        } else {
            errorCode = DruidErrorCodeEnum.OTHER;
        }
        return errorCode;
    }

    /**
     * ErrorCode 列表
     */
    public enum DruidErrorCodeEnum {
        TABLE_NOT_FOUND("Object not found"),
        COLUMN_NOT_FOUND("Column not found"),
        FUNC_NOT_FOUND("No match found for function"),
        OTHER("other");
        public final String code;

        DruidErrorCodeEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }
}
