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

import com.beust.jcommander.internal.Sets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.tencent.bk.base.common.crypt.Crypt;
import com.tencent.bk.base.common.crypt.CryptException;
import com.tencent.bk.base.datahub.iceberg.BkTable;
import com.tencent.bk.base.datahub.iceberg.DataBuffer;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.meta.StorageCluster;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.configure.ApplicationConfig;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultTableFiledTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.meta.FieldMeta;
import com.tencent.bk.base.datalab.queryengine.server.util.DataLakeUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ErrorMessageItem;
import com.tencent.bk.base.datalab.queryengine.server.util.FillQueryTaskStageUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.JdbcUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.JolUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ResultTableUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqlTimeoutException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;

@Slf4j
public abstract class AbstractQueryDriver implements QueryDriver {

    public static final String RECORD_SCHEMA = "record_schema";
    public static final int DATA_BUFFER_FLUSH_INTERVAL = 100;
    public static final int DATA_BUFFER_LIMIT = 100_000;
    public static final int DATA_BUFFER_FIRST_COMMIT_DELAY_MS = 3_000;
    public static final long CHECK_BATCH_SIZE;

    static {
        CHECK_BATCH_SIZE = SpringBeanUtil.getBean(ApplicationConfig.class).getCheckBatchSize();
    }

    /**
     * 获 取connectionMap
     *
     * @param queryTaskContext 查询上下文
     * @return connectionMap
     */
    protected static Map<String, Object> getConnectionMap(QueryTaskContext queryTaskContext) {
        StorageCluster cluster = queryTaskContext.getPickedValidStorage()
                .getStorageCluster();
        String connectionStr = cluster.getConnectionInfo();
        if (StringUtils.isBlank(connectionStr)) {
            String errorInfo = MessageLocalizedUtil
                    .getMessage("未配置集群地址 {0}", new Object[]{cluster.getClusterName()});
            ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_OTHER_ERROR.code(),
                    ResultCodeEnum.QUERY_OTHER_ERROR.message(), errorInfo);
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB,
                    JacksonUtil.object2Json(item));
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR, errorInfo);
        }

        Map<String, Object> connectionMap = JacksonUtil.convertJson2Map(connectionStr);
        if (connectionMap == null || connectionMap.size() == 0) {
            String errorInfo = MessageLocalizedUtil
                    .getMessage("集群配置信息有误 {0}", new Object[]{cluster.getClusterName()});
            ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_OTHER_ERROR.code(),
                    ResultCodeEnum.QUERY_OTHER_ERROR.message(), errorInfo);
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB,
                    JacksonUtil.object2Json(item));
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR, errorInfo);
        }
        return connectionMap;
    }

    @Override
    public void query(QueryTaskContext queryTaskContext) throws Exception {
        FillQueryTaskStageUtil
                .initQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB_SEQ, CommonConstants.CONNECT_DB,
                        System.currentTimeMillis());
        executeQuery(queryTaskContext);
        QueryDriverUtil.addDataSetInfo(queryTaskContext);
        FillQueryTaskStageUtil
                .initQueryTaskStage(queryTaskContext, CommonConstants.WRITE_CACHE_SEQ, CommonConstants.WRITE_CACHE,
                        System.currentTimeMillis());
        FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.WRITE_CACHE);
    }

    @Override
    public void executeQuery(QueryTaskContext queryTaskContext) throws Exception {
        String realSql = queryTaskContext.getRealSql();
        try (Connection conn = getConnection(queryTaskContext);
                Statement st = conn.createStatement();
                ResultSet rs = st.executeQuery(realSql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            if (metaData != null) {
                String queryMethod = queryTaskContext.getQueryTaskInfo()
                        .getQueryMethod();
                if (StringUtils.equals(queryMethod, CommonConstants.SYNC)) {
                    processSyncResult(queryTaskContext, rs);
                } else {
                    processAsyncResult(queryTaskContext, rs);
                }
            }
            QueryDriverUtil.fillColumnOrder(queryTaskContext, metaData);
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB);
        } catch (QueryDetailException e) {
            log.error(e.getMessage(), e);
            ErrorMessageItem item = new ErrorMessageItem(e.getResultCode().code(),
                    e.getResultCode().message(), e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB,
                    JacksonUtil.object2Json(item));
            throw e;
        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            processExecuteException(queryTaskContext, e);
            ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_DEVICE_ERROR.code(),
                    ResultCodeEnum.QUERY_DEVICE_ERROR.message(), e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB,
                    JacksonUtil.object2Json(item));
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    @Override
    public Connection getConnection(QueryTaskContext queryTaskContext) throws Exception {
        try {
            Connection connection = doGetConnection(queryTaskContext);
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB);
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB_SEQ, CommonConstants.QUERY_DB,
                            System.currentTimeMillis());
            return connection;
        } catch (SqlTimeoutException e) {
            handleJdbcConnectTimeOutException(queryTaskContext, e);
        } catch (Throwable e) {
            handleOtherJdbcConnectError(queryTaskContext, e);
        }
        return null;
    }

    protected Connection doGetConnection(QueryTaskContext queryTaskContext) throws Exception {
        return null;
    }

    /**
     * 处理异步结果集
     *
     * @param queryTaskContext 查询上下文
     * @param rs 结果集
     * @throws IOException when IOException occurs
     * @throws SQLException when SQLException occurs
     */
    protected void processAsyncResult(QueryTaskContext queryTaskContext, ResultSet rs)
            throws Exception {
        if (rs == null) {
            return;
        }
        queryTaskContext.setSelectFields(getSelectFieldsMeta(queryTaskContext.getStatementType(),
                rs.getMetaData()));
        try {
            initCreateTableName(queryTaskContext);
            QueryDriverUtil.registerResultTable(queryTaskContext);
            writeToDataLake(queryTaskContext, rs);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    /**
     * 异步返回处理
     *
     * @param queryTaskContext 查询上下文
     * @param data 查询返回结果集
     */
    protected void processAsyncResult(QueryTaskContext queryTaskContext,
            List<Map<String, Object>> data) {
        if (CollectionUtils.isEmpty(data)) {
            return;
        }
        queryTaskContext
                .setSelectFields(getSelectFieldsMeta(queryTaskContext.getStatementType(), data));
        try {
            initCreateTableName(queryTaskContext);
            QueryDriverUtil.registerResultTable(queryTaskContext);
            writeToDataLake(queryTaskContext, data);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    /**
     * 处理同步结果集
     *
     * @param queryTaskContext 查询上下文
     * @param rs 结果集
     * @throws SQLException when SQLException occurs
     */
    protected void processSyncResult(QueryTaskContext queryTaskContext, ResultSet rs)
            throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        List<Map<String, Object>> list = Lists.newArrayList();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
            Map<String, Object> rowData = new HashMap<>(16);
            for (int i = 1; i <= columnCount; ++i) {
                rowData.put(JdbcUtil.getColumnName(metaData, i), JdbcUtil.getColumnValue(metaData, i, rs));
            }
            list.add(rowData);
            if (list.size() % CHECK_BATCH_SIZE == 0) {
                checkResponseSize(list);
            }
        }
        queryTaskContext.setResultData(list);
        queryTaskContext.setTotalRecords(list.size());
    }

    /**
     * 检查返回结果集大小
     *
     * @param list 返回结果集集合
     */
    protected void checkResponseSize(List<Map<String, Object>> list) {
        long deepSize = JolUtil.deepSize(list);
        long deepSizeMb = deepSize / 1024 / 1024;
        long maxQuerySize = SpringBeanUtil.getBean(ApplicationConfig.class).getMaxQuerySize();
        if (deepSizeMb > maxQuerySize) {
            String message = MessageLocalizedUtil
                    .getMessage("当前结果集大小超过最大允许结果集大小({0}MB)，请减少返回条数或字段数",
                            new Object[]{maxQuerySize});
            throw new QueryDetailException(ResultCodeEnum.QUERY_EXCEED_MAX_RESPONSE_SIZE, message);
        }
    }

    /**
     * 获取 select 字段列表
     *
     * @param type 语句类型
     * @param metaData ResultSetMetaData 实例
     * @return select 字段列表
     */
    protected List<FieldMeta> getSelectFieldsMeta(StatementTypeEnum type,
            ResultSetMetaData metaData) throws Exception {
        List<FieldMeta> fieldMetaList = Lists.newArrayList();
        if (metaData != null) {
            for (int i = 1, columnSize = metaData.getColumnCount(); i <= columnSize; i++) {
                String columnName = JdbcUtil.getColumnName(metaData, i);
                FieldMeta.FieldMetaBuilder fb = FieldMeta.builder()
                        .fieldIndex(i - 1)
                        .fieldName(QueryDriverUtil.convertFieldName(type, columnName, i - 1))
                        .fieldAlias(columnName);
                int columnType = metaData.getColumnType(i);
                ResultTableFiledTypeEnum rtType;
                switch (columnType) {
                    case Types
                            .TIMESTAMP:
                    case Types
                            .BIGINT:
                        rtType = ResultTableFiledTypeEnum.LONG;
                        break;
                    case Types
                            .TINYINT:
                    case Types
                            .SMALLINT:
                    case Types
                            .INTEGER:
                    case Types
                            .BOOLEAN:
                        rtType = ResultTableFiledTypeEnum.INT;
                        break;
                    case Types
                            .FLOAT:
                    case Types
                            .REAL:
                        rtType = ResultTableFiledTypeEnum.FLOAT;
                        break;
                    case Types
                            .DOUBLE:
                    case Types
                            .DECIMAL:
                        rtType = ResultTableFiledTypeEnum.DOUBLE;
                        break;
                    default:
                        rtType = ResultTableFiledTypeEnum.STRING;
                }
                fb.fieldType(rtType.toString());
                fieldMetaList.add(fb.build());
            }
            return fieldMetaList;
        }
        return null;
    }

    /**
     * 获取 select 字段列表
     *
     * @param type 语句类型
     * @param data 查询结果集
     * @return select 字段列表
     */
    protected List<FieldMeta> getSelectFieldsMeta(StatementTypeEnum type,
            List<Map<String, Object>> data) {
        if (CollectionUtils.isEmpty(data)) {
            return null;
        }
        List<String> columnOrder = Lists
                .newArrayList(Optional.ofNullable(data.get(0)).map(Map::keySet).orElse(
                        Sets.newHashSet()));
        List<FieldMeta> result = Lists.newArrayList();
        try {
            for (int i = 0, columnSize = columnOrder.size(); i < columnSize; i++) {
                String columnName = columnOrder.get(i);
                FieldMeta.FieldMetaBuilder fb = FieldMeta.builder()
                        .fieldIndex(i)
                        .fieldName(QueryDriverUtil.convertFieldName(type, columnName, i))
                        .fieldAlias(columnName)
                        .fieldType(ResultTableFiledTypeEnum.STRING.toString());
                result.add(fb.build());
            }
            return result;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR);
        }
    }

    /**
     * 处理 jdbc 连接超时
     *
     * @param queryTaskContext 查询上下文
     * @param e SqlTimeoutException异常实例
     */
    protected void handleJdbcConnectTimeOutException(QueryTaskContext queryTaskContext,
            SqlTimeoutException e) {
        log.error(ResultCodeEnum.QUERY_CONNECT_TIMEOUT_ERROR.message(), e);
        ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_CONNECT_TIMEOUT_ERROR.code(),
                ResultCodeEnum.QUERY_CONNECT_TIMEOUT_ERROR.message(), e.getMessage());
        FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB,
                JacksonUtil.object2Json(item));
        throw new QueryDetailException(ResultCodeEnum.QUERY_CONNECT_TIMEOUT_ERROR, e.getMessage());
    }

    /**
     * 处理其他 jdbc 连接错误
     *
     * @param queryTaskContext 查询上下文
     * @param e Throwable实例
     */
    protected void handleOtherJdbcConnectError(QueryTaskContext queryTaskContext, Throwable e) {
        log.error(ResultCodeEnum.QUERY_CONNECT_OTHER_ERROR.message(), e);
        ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_CONNECT_OTHER_ERROR.code(),
                ResultCodeEnum.QUERY_CONNECT_OTHER_ERROR.message(), e.getMessage());
        FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB,
                JacksonUtil.object2Json(item));
        throw new QueryDetailException(ResultCodeEnum.QUERY_CONNECT_OTHER_ERROR, e.getMessage());
    }

    /**
     * 生成 parquet 元数据
     *
     * @param columnSet column信息
     * @return parquet 元数据
     */
    protected MessageType generateParquetSchema(List<String> columnSet) {
        Preconditions.checkNotNull(columnSet, "columnSet can not be null!");
        MessageType schema;
        org.apache.parquet.schema.Types.MessageTypeBuilder typeBuilder =
                org.apache.parquet.schema.Types
                        .buildMessage();
        for (String columnName : columnSet) {
            typeBuilder.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                    .as(OriginalType.UTF8)
                    .named(columnName);
        }
        schema = typeBuilder.named(RECORD_SCHEMA);
        return schema;
    }

    /**
     * 填充查询的列名
     *
     * @param data 查询的结果数据
     * @return columnSet 列集合
     */
    protected List<String> generateColumnList(List<Map<String, Object>> data,
            QueryTaskContext queryTaskContext) throws Exception {
        BKSqlService bkSqlService = SpringBeanUtil.getBean(BKSqlService.class);
        List<String> columns = (List<String>) bkSqlService
                .convert(BkSqlContants.BKSQL_PROTOCOL_SELECT_COLUMNS,
                        new ParsingContext(queryTaskContext.getQueryTaskInfo()
                                .getSqlText(), queryTaskContext.getQueryTaskInfo()
                                .getProperties()));
        if (columns.contains(CommonConstants.WILDCARD_SEPERATOR)) {
            columns.clear();
            for (Map.Entry<String, Object> entry : data.get(0).entrySet()) {
                if (entry.getValue() instanceof Map) {
                    columns.addAll(((Map<String, Object>) entry.getValue()).keySet());
                } else {
                    columns.add(entry.getKey());
                }
            }
        }
        return columns;
    }

    /**
     * 初始化 query set 类型的结果表 Id
     *
     * @param queryTaskContext 查询上下文
     */
    protected void initCreateTableName(QueryTaskContext queryTaskContext) {
        String createTableName = queryTaskContext.getCreateTableName();
        if (StringUtils.isBlank(createTableName)) {
            String bizId = ResultTableUtil
                    .extractBizId(queryTaskContext.getResultTableIdList()
                            .get(0));
            String queryId = queryTaskContext.getQueryTaskInfo()
                    .getQueryId();
            createTableName = String.format("%s_%s", bizId,
                    queryId);
            queryTaskContext.setCreateTableName(createTableName);
        }
    }

    /**
     * 同步查询结果集到数据湖
     *
     * @param queryTaskContext 查询上下文
     * @param rs ResultSet 查询结果集
     */
    protected void writeToDataLake(QueryTaskContext queryTaskContext, ResultSet rs) {
        final String regResultTableId = queryTaskContext.getCreateTableName();
        DataBuffer dataBuffer = null;
        int rowCount = 0;
        try {
            if (!rs.next()) {
                return;
            }
            ResultSetMetaData metaData = rs.getMetaData();
            Record record;
            BkTable table = DataLakeUtil.getBkTable(regResultTableId);
            table.loadTable();
            dataBuffer = new DataBuffer(table, DATA_BUFFER_FLUSH_INTERVAL,
                    DATA_BUFFER_LIMIT, DATA_BUFFER_FIRST_COMMIT_DELAY_MS);
            List<Record> batchData = new ArrayList<>();
            do {
                rowCount++;
                record = GenericRecord.create(table.schema());
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    record.setField(StringUtils
                                    .lowerCase(QueryDriverUtil.getFieldNameByAlias(queryTaskContext,
                                            JdbcUtil.getColumnName(metaData, i))),
                            JdbcUtil.getColumnValue(metaData, i, rs));
                }
                batchData.add(record);
                if (rowCount % DATA_BUFFER_LIMIT == 0) {
                    dataBuffer.add(batchData,
                            ImmutableMap.of(regResultTableId, DATA_BUFFER_LIMIT + ""));
                    batchData = new ArrayList<>();
                }
            } while (rs.next());
            dataBuffer.add(batchData,
                    ImmutableMap.of(regResultTableId, DATA_BUFFER_LIMIT + ""));
            queryTaskContext.setTotalRecords(rowCount);
        } catch (Exception e) {
            log.error("Data writing to the data lake failed queryId:{} message:{}, exception:{}",
                    queryTaskContext.getQueryTaskInfo().getQueryId(), e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR,
                    String.format("Data writing to the data lake failed due to the following reasons:%s",
                            e.getMessage()));
        } finally {
            if (dataBuffer != null) {
                dataBuffer.close();
            }
        }
    }

    /**
     * 同步查询结果集到数据湖
     *
     * @param queryTaskContext 查询上下文
     * @param data 查询结果集
     */
    protected void writeToDataLake(QueryTaskContext queryTaskContext,
            List<Map<String, Object>> data) {
        String regResultTableId = queryTaskContext.getCreateTableName();
        BkTable table = DataLakeUtil.getBkTable(regResultTableId);
        table.loadTable();
        try (DataBuffer dataBuffer = new DataBuffer(table, DATA_BUFFER_FLUSH_INTERVAL,
                DATA_BUFFER_LIMIT, DATA_BUFFER_FIRST_COMMIT_DELAY_MS)) {
            List<Record> batchData = new ArrayList<>();
            int rowCount = 0;
            GenericRecord record;
            for (Map<String, Object> rowData : data) {
                rowCount++;
                record = GenericRecord.create(table.schema());
                for (Map.Entry<String, Object> entry : rowData.entrySet()) {
                    if (entry.getValue() instanceof Map) {
                        for (Map.Entry<String, Object> groupByData :
                                ((Map<String, Object>) entry.getValue()).entrySet()) {
                            record.setField(StringUtils.lowerCase(
                                    QueryDriverUtil.getFieldNameByAlias(queryTaskContext, groupByData.getKey())),
                                    groupByData.getValue().toString());
                        }
                    } else {
                        record.setField(StringUtils.lowerCase(
                                QueryDriverUtil.getFieldNameByAlias(queryTaskContext, entry.getKey())),
                                Optional.ofNullable(entry.getValue()).orElse("")
                                        .toString());
                    }
                }
                batchData.add(record);
                if (rowCount % DATA_BUFFER_LIMIT == 0) {
                    dataBuffer.add(batchData,
                            ImmutableMap.of(regResultTableId, DATA_BUFFER_LIMIT + ""));
                    batchData = new ArrayList<>();
                }
            }
            dataBuffer.add(batchData,
                    ImmutableMap.of(regResultTableId, DATA_BUFFER_LIMIT + ""));
            queryTaskContext.setTotalRecords(rowCount);
        } catch (Exception e) {
            log.error("Data writing to the data lake failed queryId:{} message:{}, exception:{}",
                    queryTaskContext.getQueryTaskInfo().getQueryId(), e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR,
                    String.format("Data writing to the data lake failed due to the following reasons:%s",
                            e.getMessage()));
        }
    }

    /**
     * 同步查询结果集到数据湖
     *
     * @param queryTaskContext 查询上下文
     * @param columnList 字段列表
     * @param dataList 查询结果集
     */
    protected void writeToDataLake(QueryTaskContext queryTaskContext, List<String> columnList,
            List<List<String>> dataList) {
        String regResultTableId = queryTaskContext.getCreateTableName();
        BkTable table = DataLakeUtil.getBkTable(regResultTableId);
        table.loadTable();
        try (DataBuffer buffer = new DataBuffer(table, DATA_BUFFER_FLUSH_INTERVAL,
                DATA_BUFFER_LIMIT, DATA_BUFFER_FIRST_COMMIT_DELAY_MS)) {
            List<Record> batchData = new ArrayList<>();
            int lines = 0;
            GenericRecord record;
            for (List<String> valueList : dataList) {
                lines++;
                record = GenericRecord.create(table.schema());
                for (int i = 0; i < valueList.size(); i++) {
                    record.setField(
                            StringUtils.lowerCase(
                                    QueryDriverUtil.getFieldNameByAlias(queryTaskContext, columnList.get(i))),
                            valueList.get(i));
                }
                batchData.add(record);
                if (lines % DATA_BUFFER_LIMIT == 0) {
                    buffer.add(batchData,
                            ImmutableMap.of(regResultTableId, DATA_BUFFER_LIMIT + ""));
                    batchData = new ArrayList<>();
                }
            }
            buffer.add(batchData,
                    ImmutableMap.of(regResultTableId, DATA_BUFFER_LIMIT + ""));
            queryTaskContext.setTotalRecords(lines);
        } catch (Exception e) {
            log.error("Data writing to the data lake failed queryId:{} message:{}, exception:{}",
                    queryTaskContext.getQueryTaskInfo().getQueryId(), e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR,
                    String.format("Data writing to the data lake failed due to the following reasons:%s",
                            e.getMessage()));
        }
    }

    /**
     * 获取连接信息
     *
     * @param queryTaskContext 查询上下文
     * @return ConnectionMeta 连接信息
     */
    protected ConnectionMeta getConnectionMeta(QueryTaskContext queryTaskContext) throws CryptException {
        StorageCluster cluster = queryTaskContext.getPickedValidStorage()
                .getStorageCluster();
        Map<String, Object> connectionMap = getConnectionMap(queryTaskContext);
        String host = requireNonNull(connectionMap.get(StorageConstants.CONNECTION_HOST),
                String.format("Connection host is null:%s", cluster.getClusterName())).toString();
        int port = NumberUtils.toInt(requireNonNull(connectionMap.get(StorageConstants.CONNECTION_PORT),
                String.format("Connection port is null:%s", cluster.getClusterName())).toString());
        String user = requireNonNull(connectionMap.get(StorageConstants.CONNECTION_USER),
                String.format("Connection userName is null:%s", cluster.getClusterName()))
                .toString();
        String passWord = new String(Crypt.decrypt(requireNonNull(connectionMap.get(StorageConstants.CONNECTION_PWD),
                String.format("Connection password is null:%s", cluster.getClusterName()))
                        .toString(), BkDataConstants.BKDATA_ROOT_KEY, BkDataConstants.BKDATA_ROOT_IV,
                BkDataConstants.BKDATA_INSTANCE_KEY), StandardCharsets.UTF_8);
        return ConnectionMeta.builder().host(host).port(port).user(user).passWord(passWord).build();
    }
}
