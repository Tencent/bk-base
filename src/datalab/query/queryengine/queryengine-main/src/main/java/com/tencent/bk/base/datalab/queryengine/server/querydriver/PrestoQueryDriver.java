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

import static com.tencent.bk.base.datalab.queryengine.server.querydriver.PrestoQueryDriver.PrestoErrorCodeEnum.CATALOG_NOT_FOUND;
import static com.tencent.bk.base.datalab.queryengine.server.querydriver.PrestoQueryDriver.PrestoErrorCodeEnum.COLUMN_NOT_FOUND;
import static com.tencent.bk.base.datalab.queryengine.server.querydriver.PrestoQueryDriver.PrestoErrorCodeEnum.FUNC_NOT_FOUND;
import static com.tencent.bk.base.datalab.queryengine.server.querydriver.PrestoQueryDriver.PrestoErrorCodeEnum.SCHEMA_NOT_FOUND;
import static com.tencent.bk.base.datalab.queryengine.server.querydriver.PrestoQueryDriver.PrestoErrorCodeEnum.TABLE_NOT_FOUND;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.datalab.queryengine.server.configure.PrestoClusterConfig;
import com.tencent.bk.base.datalab.queryengine.server.configure.PrestoConfig;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.third.MetaApiService;
import com.tencent.bk.base.datalab.queryengine.server.util.FillQueryTaskStageUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ResultTableUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import io.prestosql.jdbc.PrestoConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqlTimeoutException;
import org.apache.commons.lang.StringUtils;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_HDFS)
public class PrestoQueryDriver extends AbstractQueryDriver {

    public static final String CONNECTION_CLIENT_TAGS = "ClientTags";
    private static final String QUERY_URL_PATTERN = "jdbc:presto://%s:%d";
    private static final Pattern COLUMN_NOT_FOUND_MSG_PATTERN = Pattern.compile("Query failed "
            + "\\(#\\w+\\): line \\d+:\\d+: Column '(.+)' cannot be resolved");
    private static final Pattern FUNC_NOT_FOUND_MSG_PATTERN = Pattern.compile("Query failed "
            + "\\(#\\w+\\): line \\d+:\\d+: Function (.+) not registered");
    private static final Pattern CATALOG_NOT_FOUND_MSG_PATTERN = Pattern.compile("Query failed "
            + "\\(#\\w+\\): line \\d+:\\d+: Catalog (.+) does not exist");
    private static final Pattern SCHEMA_NOT_FOUND_MSG_PATTERN = Pattern.compile("Query failed "
            + "\\(#\\w+\\): line \\d+:\\d+: Schema (.+) does not exist");
    private static final Pattern TABLE_NOT_FOUND_MSG_PATTERN = Pattern.compile("Query failed "
            + "\\(#\\w+\\): line \\d+:\\d+: Table (.+) does not exist");
    private static final Pattern SYNTAX_ERROR_MISMATCHED_MSG_PATTERN = Pattern
            .compile("Query failed "
                    + "\\(#\\w+\\): line \\d+:\\d+: mismatched input '(.+)'. Expecting: .+");
    private static final Pattern SYNTAX_ERROR_ILLEGAL_GROUP_MSG_PATTERN = Pattern
            .compile("Query failed "
                    + "\\(#\\w+\\): line \\d+:\\d+: '(.+)' must be an aggregate expression or "
                    + "appear in GROUP BY clause");
    private static final Pattern QUERY_TIMEOUT_MSG_PATTERN = Pattern
            .compile("Query failed "
                    + "\\(#\\w+\\): Query exceeded maximum time limit of .+");
    private static final Pattern TYPE_MISMATCH_MSG_PATTERN = Pattern.compile("Query failed "
            + "\\(#\\w+\\): line \\d+:\\d+: Cannot apply operator: (.+)");
    private static final Pattern FILE_NOT_EXIST_MSG_PATTERN = Pattern
            .compile("Query failed "
                    + "\\(#\\w+\\): File does not exist: (.+\\.parquet)[\\s\\S]*");
    private static final ImmutableMap<PrestoErrorCodeEnum, Pattern> ERROR_PATTERN_MAP;
    private static final ImmutableMap<PrestoErrorCodeEnum, ResultCodeEnum> ERROR_CODE_MAP;
    private static final String BIZ_PREFIX = "biz_";
    private static final String DEFAULT_CLUSTER = "default";
    private static final String FILE_NOT_EXIST_MSG_TEMPLATE = "文件查找失败：{0}";
    private static final String QUERY_TIMEOUT_MSG = "Hdfs查询超时，建议优化SQL减少数据集大小";
    /**
     * 内部保留字段，需要从返回结果集中过滤掉
     */
    private static final List<String> INTERVAL_COLUMN_LIST;

    static {
        ERROR_PATTERN_MAP =
                ImmutableMap.<PrestoErrorCodeEnum, Pattern>builder()
                        .put(CATALOG_NOT_FOUND, CATALOG_NOT_FOUND_MSG_PATTERN)
                        .put(SCHEMA_NOT_FOUND, SCHEMA_NOT_FOUND_MSG_PATTERN)
                        .put(PrestoErrorCodeEnum.TABLE_NOT_FOUND, TABLE_NOT_FOUND_MSG_PATTERN)
                        .put(COLUMN_NOT_FOUND, COLUMN_NOT_FOUND_MSG_PATTERN)
                        .put(PrestoErrorCodeEnum.FUNC_NOT_FOUND, FUNC_NOT_FOUND_MSG_PATTERN)
                        .put(PrestoErrorCodeEnum.SYNTAX_ERROR_MISMATCHED,
                                SYNTAX_ERROR_MISMATCHED_MSG_PATTERN)
                        .put(PrestoErrorCodeEnum.SYNTAX_ERROR_ILLEGAL_GROUP,
                                SYNTAX_ERROR_ILLEGAL_GROUP_MSG_PATTERN)
                        .put(PrestoErrorCodeEnum.QUERY_TIMEOUT,
                                QUERY_TIMEOUT_MSG_PATTERN)
                        .put(PrestoErrorCodeEnum.TYPE_MISMATCH,
                                TYPE_MISMATCH_MSG_PATTERN)
                        .put(PrestoErrorCodeEnum.FILE_NOT_EXIST,
                                FILE_NOT_EXIST_MSG_PATTERN)
                        .build();

        ERROR_CODE_MAP =
                ImmutableMap.<PrestoErrorCodeEnum, ResultCodeEnum>builder()
                        .put(CATALOG_NOT_FOUND, ResultCodeEnum.QUERY_CATALOG_NOT_FOUND)
                        .put(SCHEMA_NOT_FOUND, ResultCodeEnum.QUERY_SCHEMA_NOT_FOUND)
                        .put(TABLE_NOT_FOUND, ResultCodeEnum.SQL_ERROR_REAL_TABLE_NOT_FOUND)
                        .put(COLUMN_NOT_FOUND, ResultCodeEnum.QUERY_FILED_NOT_EXSITS)
                        .put(FUNC_NOT_FOUND, ResultCodeEnum.QUERY_FUNC_NOT_EXISTS)
                        .build();

        INTERVAL_COLUMN_LIST = ImmutableList.of("____et");
    }

    @Override
    public Connection getConnection(QueryTaskContext queryTaskContext) {
        Connection connection = null;
        try {
            final String bizId = ResultTableUtil
                    .extractBizId(queryTaskContext.getResultTableIdList().get(0));
            List<String> rtList = queryTaskContext.getResultTableIdList();
            String rtId = rtList.get(0);
            MetaApiService metaApiService = SpringBeanUtil.getBean(MetaApiService.class);
            String areaCode = metaApiService.fetchCodeArea(rtId);
            PrestoConfig prestoConfig = SpringBeanUtil.getBean(PrestoConfig.class);
            Map<String, Map<String, PrestoClusterConfig>> prestoClusterAreaMap = prestoConfig
                    .getClusterMap();
            Map<String, PrestoClusterConfig> prestoClusterBizMap;
            PrestoClusterConfig prestoClusterConfig;
            if (prestoClusterAreaMap != null) {
                //地域标签对应的Presto集群信息不存在时使用默认presto集群(INLAND)
                prestoClusterBizMap = Optional
                        .ofNullable(prestoClusterAreaMap.get(StringUtils.upperCase(areaCode)))
                        .orElse(prestoClusterAreaMap.get(BkDataConstants.AREA_TAG_INLAND));
                prestoClusterConfig = Optional
                        .ofNullable(prestoClusterBizMap.get(BIZ_PREFIX + bizId))
                        .orElse(prestoClusterBizMap.get(DEFAULT_CLUSTER));
                final String host = prestoClusterConfig.getHost();
                final int port = prestoClusterConfig.getPort();
                Properties properties = new Properties();
                properties.setProperty("user", prestoClusterConfig.getUsername());
                properties.setProperty("password", prestoClusterConfig.getPassword());
                properties.setProperty("SSL", prestoClusterConfig.getSsl());
                connection = DriverManager
                        .getConnection(String.format(QUERY_URL_PATTERN, host, port), properties);
                connection.unwrap(PrestoConnection.class)
                        .setClientInfo(CONNECTION_CLIENT_TAGS, bizId);
                FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB);
                FillQueryTaskStageUtil
                        .initQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB_SEQ, CommonConstants.QUERY_DB,
                                System.currentTimeMillis());
                return connection;
            } else {
                throw new QueryDetailException(ResultCodeEnum.SYSTEM_INNER_ERROR);
            }
        } catch (SqlTimeoutException e) {
            super.handleJdbcConnectTimeOutException(queryTaskContext, e);
        } catch (Throwable e) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                    log.error(ex.getMessage(), e);
                }
            }
            super.handleOtherJdbcConnectError(queryTaskContext, e);
        }
        return null;
    }

    @Override
    public boolean isInternalColumn(String columnName) {
        return INTERVAL_COLUMN_LIST.contains(columnName.toLowerCase());
    }

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
        PrestoErrorCodeEnum code = getErrorCode(ex);
        String errorMsg = ex.getMessage();
        switch (code) {
            case CATALOG_NOT_FOUND:
            case FUNC_NOT_FOUND:
            case TABLE_NOT_FOUND:
            case SCHEMA_NOT_FOUND:
            case COLUMN_NOT_FOUND:
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
                                new Object[]{QueryDriverUtil.getErrorItemFromPattern(
                                        SYNTAX_ERROR_ILLEGAL_GROUP_MSG_PATTERN, errorMsg, 1)}));
                break;
            case QUERY_TIMEOUT:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        MessageLocalizedUtil.getMessage(QUERY_TIMEOUT_MSG));
                break;
            case TYPE_MISMATCH:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        MessageLocalizedUtil.getMessage(StorageConstants.TYPE_MISMATCH_MSG_TEMPLATE,
                                new Object[]{QueryDriverUtil.getErrorItemFromPattern(TYPE_MISMATCH_MSG_PATTERN,
                                        errorMsg, 1)}));
                break;
            case FILE_NOT_EXIST:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        MessageLocalizedUtil.getMessage(FILE_NOT_EXIST_MSG_TEMPLATE,
                                new Object[]{QueryDriverUtil.getErrorItemFromPattern(FILE_NOT_EXIST_MSG_PATTERN,
                                        errorMsg, 1)}));
                break;
            default:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR,
                        errorMsg);
        }
    }

    /**
     * 根据异常消息获取 ErrorCode
     *
     * @param exception SQLException 实例
     * @return 异常错误码
     */
    private PrestoErrorCodeEnum getErrorCode(SQLException exception) {
        AtomicReference<PrestoErrorCodeEnum> errorCode = new AtomicReference<>();
        ERROR_PATTERN_MAP.entrySet().stream()
                .filter(entry -> entry.getValue().matcher(exception.getMessage()).matches())
                .findFirst()
                .ifPresent(entry -> errorCode.set(entry.getKey()));
        if (errorCode.get() == null) {
            errorCode.set(PrestoErrorCodeEnum.OTHER);
        }
        return errorCode.get();
    }

    /**
     * ErrorCode 列表
     */
    public enum PrestoErrorCodeEnum {
        SYNTAX_ERROR_MISMATCHED("1"),
        SYNTAX_ERROR_ILLEGAL_GROUP("1"),
        CATALOG_NOT_FOUND("44"),
        SCHEMA_NOT_FOUND("45"),
        TABLE_NOT_FOUND("46"),
        COLUMN_NOT_FOUND("47"),
        FUNC_NOT_FOUND("6"),
        QUERY_TIMEOUT("131075"),
        TYPE_MISMATCH("58"),
        FILE_NOT_EXIST("0"),
        OTHER("other");
        public final String code;

        PrestoErrorCodeEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }
}
