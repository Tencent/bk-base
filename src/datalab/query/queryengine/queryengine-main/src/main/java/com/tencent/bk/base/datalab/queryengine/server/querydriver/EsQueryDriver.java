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
import static javax.ws.rs.HttpMethod.GET;
import static javax.ws.rs.HttpMethod.POST;

import com.beust.jcommander.internal.Sets;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.common.crypt.Crypt;
import com.tencent.bk.base.datahub.iceberg.BkTable;
import com.tencent.bk.base.datahub.iceberg.DataBuffer;
import com.tencent.bk.base.datalab.meta.StorageCluster;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.common.time.DateUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.enums.ElasticSearchDataTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultTableFiledTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.meta.FieldMeta;
import com.tencent.bk.base.datalab.queryengine.server.third.StoreKitApiService;
import com.tencent.bk.base.datalab.queryengine.server.util.DataLakeUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ErrorMessageItem;
import com.tencent.bk.base.datalab.queryengine.server.util.FillQueryTaskStageUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.QueryDriverUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.SpringBeanUtil;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

@Slf4j
@QueryDriverDesc(name = StorageConstants.DEVICE_TYPE_ES)
public class EsQueryDriver extends AbstractQueryDriver {

    public static final String PREFERENCE = "preference";
    public static final String IGNORE_UNAVAILABLE = "ignore_unavailable";
    public static final String PRIMARY_FIRST = "_primary_first";
    public static final String HITS = "hits";
    public static final String SOURCE = "_source";
    public static final String TOTAL = "total";
    public static final String VALUE = "value";
    public static final String QUERY = "query";
    public static final String BOOL = "bool";
    public static final String FILTER = "filter";
    public static final String RANGE = "range";
    public static final String GTE = "gte";
    public static final String LTE = "lte";
    public static final String NAME = "name";
    public static final String TYPE = "type";
    private static final Map<String, String> COMMON_HEADER = new HashMap<>();
    private static final int CONNECT_TIMEOUT = 10 * 1000;
    private static final int SOCKET_TIMEOUT = 5 * 60 * 1000;
    private static final int MAX_RETRY_TIMEOUT = 5 * 60 * 1000;
    private static final String DT_EVENT_TIMESTAMP = "dtEventTimeStamp";
    private static final String EPOCH_TIMES = "epoch_millis";
    private static final long LOWEST_TIMESTAMP = 4023446400L;
    private static final String DATEFORMAT_YYYYMMDD = "yyyyMMdd";
    private static final Joiner COMMON_JSONNER = Joiner.on(CommonConstants.COMMA_SEPERATOR);
    private static final Splitter COMMON_SPLITTER = Splitter.on(CommonConstants.COMMA_SEPERATOR);
    private static final List<String> RESERVED_FIELDS;
    private static final String ES_6_X_X = "6.0.0";
    private static final String ES_7_X_X = "7.0.0";
    private static final String SQL_OLD_ENDPOINT = "/_xpack/sql";
    private static final String SQL_NEW_ENDPOINT = "/_sql?format=json&pretty";
    private static final String DSL_MAPPING_ENDPOINT = "/%s/_mapping";
    private static final String DSL_SEARCH_ENDPOINT = "/%s/_search";
    private static final String INDEX_PATTERN = "(\\w+)_(\\d{8}|\\*|\\d{4})";
    private static final Pattern COLUMN_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("Unknown column \\[([^ ]+)]");
    private static final Pattern TABLE_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("Unknown index \\[([^ ]+)]");
    private static final Pattern FUNC_NOT_FOUND_MSG_PATTERN = Pattern
            .compile("Unknown function \\[([^ ]+)]");
    private static final Map<EsErrorCodeEnum, Pattern> ERROR_PATTERN_MAP;

    static {
        COMMON_HEADER.put("Content-type", "application/json");
        RESERVED_FIELDS = Lists.newArrayList("_iteration_idx");
        ERROR_PATTERN_MAP = Maps.newHashMap();
        ERROR_PATTERN_MAP
                .putIfAbsent(EsErrorCodeEnum.COLUMN_NOT_FOUND, COLUMN_NOT_FOUND_MSG_PATTERN);
        ERROR_PATTERN_MAP.putIfAbsent(EsErrorCodeEnum.TABLE_NOT_FOUND, TABLE_NOT_FOUND_MSG_PATTERN);
        ERROR_PATTERN_MAP.putIfAbsent(EsErrorCodeEnum.FUNC_NOT_FOUND, FUNC_NOT_FOUND_MSG_PATTERN);
    }

    @Override
    public void executeQuery(QueryTaskContext queryTaskContext) throws Exception {
        try {
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CONNECT_DB);
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB_SEQ, CommonConstants.QUERY_DB,
                            System.currentTimeMillis());
            StorageCluster cluster = queryTaskContext.getPickedValidStorage()
                    .getStorageCluster();
            Map<String, Object> connectionMap = getConnectionMap(queryTaskContext);
            String host = requireNonNull(connectionMap.get(StorageConstants.CONNECTION_HOST),
                    String.format("Connection host is null:%s", cluster.getClusterName()))
                    .toString();
            int port = NumberUtils.toInt(requireNonNull(connectionMap.get(StorageConstants.CONNECTION_PORT),
                    String.format("Connection port is null:%s", cluster.getClusterName()))
                    .toString());
            String user = requireNonNull(connectionMap.get(StorageConstants.CONNECTION_USER),
                    String.format("Connection user is null:%s", cluster.getClusterName()))
                    .toString();
            String passWord = new String(
                    Crypt.decrypt(requireNonNull(connectionMap.get(StorageConstants.CONNECTION_PWD),
                            String.format("Connection password is null:%s", cluster.getClusterName()))
                                    .toString(), BkDataConstants.BKDATA_ROOT_KEY, BkDataConstants.BKDATA_ROOT_IV,
                            BkDataConstants.BKDATA_INSTANCE_KEY), StandardCharsets.UTF_8);
            RestClient restClient = getEsClient(host, port, user, passWord);
            String realSql = queryTaskContext.getRealSql();
            if (JacksonUtil.readTree(realSql) != null) {
                EsDslQueryEntity queryEntity = extractQueryParams(queryTaskContext);
                if (queryEntity == null) {
                    throw new QueryDetailException(ResultCodeEnum.PARAM_IS_INVALID,
                            "Elasticsearch查询Dsl格式错误，请检查Dsl格式");
                }
                queryElasticByDsl(queryTaskContext, queryEntity, restClient);
            } else {
                queryElasticBySql(queryTaskContext, restClient);
            }
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB);
        } catch (QueryDetailException e) {
            QueryDriverUtil.updateQueryStage(queryTaskContext, CommonConstants.QUERY_DB, e, e.getResultCode());
            throw e;
        } catch (Throwable e) {
            QueryDriverUtil.updateQueryStage(queryTaskContext, CommonConstants.QUERY_DB, e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    @Override
    public void processExecuteException(QueryTaskContext queryTaskContext, Throwable e) {
        if (!(e instanceof ResponseException)) {
            return;
        }
        ResponseException ex = (ResponseException) e;
        log.error("EsQueryError|sql:{}|StatusLine:{}|message():{}",
                queryTaskContext.getQueryTaskInfo().getSqlText(),
                ex.getResponse().getStatusLine(), ex.getMessage());
        EsErrorCodeEnum code = getErrorCode(ex);
        String errorInfo = ex.getMessage();
        String lostObject = "";
        switch (code) {
            case COLUMN_NOT_FOUND:
                Matcher columnMatcher = COLUMN_NOT_FOUND_MSG_PATTERN.matcher(errorInfo);
                if (columnMatcher.find()) {
                    lostObject = columnMatcher.group(1);
                }
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询失败，字段 {0} 在表中不存在", new Object[]{lostObject});
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_FILED_NOT_EXSITS, errorInfo);
                break;
            case TABLE_NOT_FOUND:
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询失败，物理表不存在，入库任务没有启动或没有正常运行");
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_FUNC_NOT_EXISTS, errorInfo);
                break;
            case FUNC_NOT_FOUND:
                Matcher funcMatcher = FUNC_NOT_FOUND_MSG_PATTERN.matcher(errorInfo);
                if (funcMatcher.find()) {
                    lostObject = funcMatcher.group(1);
                }
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询失败，函数 {0} 不存在", new Object[]{lostObject});
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_FUNC_NOT_EXISTS, errorInfo);
                break;
            default:
                QueryDriverUtil.handleQueryError(queryTaskContext, ResultCodeEnum.QUERY_DEVICE_ERROR, ex.getMessage());
        }
    }

    /**
     * 根据异常消息获取 ErrorCode
     *
     * @param exception SQLException 实例
     * @return 异常错误码
     */
    private EsErrorCodeEnum getErrorCode(ResponseException exception) {
        AtomicReference<EsErrorCodeEnum> errorCode = new AtomicReference<>();
        String message = exception.getMessage();
        ERROR_PATTERN_MAP.entrySet().forEach(entry -> {
            Matcher matcher = entry.getValue().matcher(message);
            if (matcher.find()) {
                errorCode.set(entry.getKey());
                return;
            }
        });
        if (errorCode.get() == null) {
            errorCode.set(EsErrorCodeEnum.OTHER);
        }
        return errorCode.get();
    }

    /**
     * 执行 Es 查询
     *
     * @param restClient RestClient 实例
     */
    private void queryElasticBySql(QueryTaskContext queryTaskContext, RestClient restClient) {
        List<FieldMeta> columnsList = Lists.newArrayList();
        int status;
        Response response;
        String endPoint = SQL_NEW_ENDPOINT;
        String responseBody;
        try {
            String version = queryTaskContext.getPickedValidStorage()
                    .getStorageCluster()
                    .getVersion();
            if (ES_6_X_X.compareTo(version) > 0) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_NOT_SUPPORT,
                        MessageLocalizedUtil
                                .getMessage("ES当前版本 {0} 不支持SQL检索", new Object[]{version}));
            }
            if (ES_6_X_X.compareTo(version) < 0 && ES_7_X_X.compareTo(version) > 0) {
                endPoint = SQL_OLD_ENDPOINT;
            }
            Map<String, String> params = Maps.newHashMap();
            Map<String, String> queryBody = Maps.newHashMap();
            String realSql = queryTaskContext.getRealSql();
            queryBody.put("query", realSql);
            HttpEntity entity = new NStringEntity(JacksonUtil.object2Json(queryBody),
                    ContentType.APPLICATION_JSON);
            response = restClient.performRequest(POST, endPoint, params, entity);
            status = response.getStatusLine()
                    .getStatusCode();
            responseBody = EntityUtils.toString(response.getEntity());
            if (status == HttpStatus.SC_OK) {
                JsonNode responseNode = JacksonUtil.readTree(responseBody);
                JsonNode columnsNode = responseNode != null ? responseNode.get("columns") : null;
                JsonNode rowsNode = responseNode != null ? responseNode.get("rows") : null;
                if (columnsNode != null) {
                    fillColumnOrder(columnsNode, queryTaskContext, columnsList);
                }
                if (rowsNode != null) {
                    String queryMethod = queryTaskContext.getQueryTaskInfo()
                            .getQueryMethod();
                    if (StringUtils.equals(queryMethod, CommonConstants.SYNC)) {
                        processSyncResult(columnsList, rowsNode, queryTaskContext);
                    } else {
                        processAsyncResult(columnsList, rowsNode, queryTaskContext);
                    }
                }
            }
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB);
        } catch (QueryDetailException e) {
            log.error(e.getMessage(), e);
            throw e;
        } catch (Throwable e) {
            processExecuteException(queryTaskContext, e);
            log.error(e.getMessage(), e);
            ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_DEVICE_ERROR.code(),
                    ResultCodeEnum.QUERY_DEVICE_ERROR.message(), e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB,
                    JacksonUtil.object2Json(item));
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        } finally {
            closeResource(restClient);
        }
    }

    /**
     * 处理异步结果
     *
     * @param columnsList 结果字段列表
     * @param rowsNode 返回的数据节点
     * @param queryTaskContext 查询上下文
     */
    private void processAsyncResult(List<FieldMeta> columnsList,
            JsonNode rowsNode, QueryTaskContext queryTaskContext) {
        if (columnsList == null || rowsNode == null) {
            return;
        }
        queryTaskContext.setSelectFields(columnsList);
        try {
            initCreateTableName(queryTaskContext);
            QueryDriverUtil.registerResultTable(queryTaskContext);
            writeToDataLake(queryTaskContext, rowsNode.iterator());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    /**
     * 查询结果集同步到数据湖
     *
     * @param queryTaskContext 查询上下文
     * @param rows 查询结果集
     */
    private void writeToDataLake(QueryTaskContext queryTaskContext, Iterator<JsonNode> rows) {
        if (rows == null) {
            return;
        }
        String regResultTableId = queryTaskContext.getCreateTableName();
        BkTable table = DataLakeUtil.getBkTable(regResultTableId);
        table.loadTable();
        int rowCount = 0;
        Record record;
        try (DataBuffer dataBuffer = new DataBuffer(table, DATA_BUFFER_FLUSH_INTERVAL,
                DATA_BUFFER_LIMIT, DATA_BUFFER_FIRST_COMMIT_DELAY_MS)) {
            List<Record> batchData = new ArrayList<>();
            while (rows.hasNext()) {
                rowCount++;
                record = GenericRecord.create(table.schema());
                transEsRowToDataLakeRecord(record, queryTaskContext.getSelectFields(),
                        rows.next());
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
            log.error("Faild to write data to datalake queryId:{} message:{}, exception:{}",
                    queryTaskContext.getQueryTaskInfo().getQueryId(), e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR,
                    "Faild to write data to datalake");
        }
    }

    /**
     * 处理同步结果
     *
     * @param columnsList 结果字段列表
     * @param rowsNode 返回的数据节点
     * @param queryTaskContext 查询上下文
     */
    private void processSyncResult(List<FieldMeta> columnsList,
            JsonNode rowsNode, QueryTaskContext queryTaskContext) {
        if (columnsList == null || rowsNode == null) {
            return;
        }
        try {
            Iterator<JsonNode> rows = rowsNode.iterator();
            List<Map<String, Object>> list = Lists.newArrayList();
            while (rows.hasNext()) {
                JsonNode row = rows.next();
                Map<String, Object> rowData = new HashMap<>(16);
                transEsRowToRowData(rowData, columnsList, row);
                list.add(rowData);
            }
            queryTaskContext.setResultData(list);
            queryTaskContext.setTotalRecords(list.size());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    /**
     * 将 es 返回的 JsonNode 数据类型转换为 Map 数据类型
     *
     * @param rowData 转换后的Map
     * @param columnsList 字段列表
     * @param row 转换前的JsonNode
     */
    private void transEsRowToRowData(Map<String, Object> rowData, List<FieldMeta> columnsList,
            JsonNode row) {
        if (rowData == null || columnsList == null || row == null) {
            return;
        }
        try {
            for (int i = 0; i < columnsList.size(); i++) {
                FieldMeta column = columnsList.get(i);
                String columnLabel = column.getFieldName();
                String columnType = column.getFieldType();
                ElasticSearchDataTypeEnum dataType = ElasticSearchDataTypeEnum
                        .valueOf(columnType.toUpperCase(Locale.ENGLISH));
                switch (dataType) {
                    case BYTE:
                    case INTEGER:
                    case SHORT:
                        if (row.get(i) != null) {
                            rowData.put(columnLabel, row.get(i)
                                    .asInt());
                        }
                        break;
                    case LONG:
                        if (row.get(i) != null) {
                            rowData.put(columnLabel, row.get(i)
                                    .asLong());
                        }
                        break;
                    case HALF_FLOAT:
                    case DOUBLE:
                    case FLOAT:
                    case SCALED_FLOAT:
                        if (row.get(i) != null) {
                            rowData.put(columnLabel, row.get(i)
                                    .asDouble());
                        }
                        break;
                    default:
                        rowData.put(columnLabel, Optional.ofNullable(row.get(i)
                                .asText(""))
                                .orElse(""));
                }
            }
        } catch (QueryDetailException e) {
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR, e.getMessage());
        }
    }

    /**
     * 将 es 返回的 JsonNode 数据类型转换为 Record 数据类型
     *
     * @param record 转换后的Record
     * @param columnsList 字段列表
     * @param row 转换前的JsonNode
     */
    private void transEsRowToDataLakeRecord(Record record, List<FieldMeta> columnsList,
            JsonNode row) {
        try {
            for (int i = 0; i < columnsList.size(); i++) {
                FieldMeta column = columnsList.get(i);
                String columnLabel = column.getFieldName();
                String columnType = column.getFieldType();
                ElasticSearchDataTypeEnum dataType = ElasticSearchDataTypeEnum
                        .valueOf(columnType.toUpperCase(Locale.ENGLISH));
                switch (dataType) {
                    case BYTE:
                    case INTEGER:
                    case SHORT:
                        if (row.get(i) != null) {
                            record.setField(columnLabel.toLowerCase(), row.get(i)
                                    .asInt());
                        }
                        break;
                    case LONG:
                        if (row.get(i) != null) {
                            record.setField(columnLabel.toLowerCase(), row.get(i)
                                    .asLong());
                        }
                        break;
                    case HALF_FLOAT:
                    case DOUBLE:
                    case FLOAT:
                    case SCALED_FLOAT:
                        if (row.get(i) != null) {
                            record.setField(columnLabel.toLowerCase(), row.get(i)
                                    .asDouble());
                        }
                        break;
                    default:
                        record.setField(columnLabel.toLowerCase(), Optional.ofNullable(row.get(i)
                                .asText(""))
                                .orElse(""));
                }
            }
        } catch (QueryDetailException e) {
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_OTHER_ERROR, e.getMessage());
        }
    }


    /**
     * 获取 Es 连接
     *
     * @param host 主机名
     * @param port 端口
     * @param user 用户名
     * @param passWord 密码
     * @return Es RestClient 实例
     */
    private RestClient getEsClient(String host, int port, String user, String passWord) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(host), "ElasticSearch host is null or empty");
        Preconditions.checkArgument(port > 0, "ElasticSearch port can not be negative");
        Preconditions
                .checkArgument(StringUtils.isNotBlank(user), "ElasticSearch user is null or empty");
        Preconditions.checkArgument(StringUtils.isNotBlank(passWord),
                "ElasticSearch passWord is null or empty");
        final CredentialsProvider credentialsProvider =
                new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, passWord));
        RestClientBuilder builder = RestClient.builder(
                new HttpHost(host, port));
        builder.setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(CONNECT_TIMEOUT)
                        .setSocketTimeout(SOCKET_TIMEOUT));
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider));
        builder.setMaxRetryTimeoutMillis(MAX_RETRY_TIMEOUT);
        return builder.build();
    }

    private String getRtFieldType(String columnType) {
        ResultTableFiledTypeEnum type;
        ElasticSearchDataTypeEnum dataType = ElasticSearchDataTypeEnum
                .valueOf(columnType.toUpperCase(Locale.ENGLISH));
        switch (dataType) {
            case BYTE:
            case INTEGER:
            case SHORT:
                type = ResultTableFiledTypeEnum.INT;
                break;
            case LONG:
                type = ResultTableFiledTypeEnum.LONG;
                break;
            case HALF_FLOAT:
            case SCALED_FLOAT:
            case FLOAT:
            case DOUBLE:
                type = ResultTableFiledTypeEnum.DOUBLE;
                break;
            default:
                type = ResultTableFiledTypeEnum.STRING;
        }
        return type.toString();
    }

    /**
     * 执行 Es 查询
     *
     * @param restClient RestClient 实例
     */
    private void queryElasticByDsl(QueryTaskContext queryTaskContext, EsDslQueryEntity queryEntity,
            RestClient restClient) {
        try {
            if (queryEntity.isMapping()) {
                esMappingQuery(queryTaskContext, queryEntity, restClient);
            } else {
                esNormalQuery(queryTaskContext, queryEntity, restClient);
            }
        } catch (QueryDetailException e) {
            QueryDriverUtil.updateQueryStage(queryTaskContext, CommonConstants.QUERY_DB, e, e.getResultCode());
            throw e;
        } catch (Throwable e) {
            QueryDriverUtil.updateQueryStage(queryTaskContext, CommonConstants.QUERY_DB, e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        } finally {
            closeResource(restClient);
        }
    }

    /**
     * 执行 Es mapping 查询
     *
     * @param queryTaskContext 查询上下文
     * @param queryEntity EsDslQueryEntity 请求体
     * @param restClient RestClient 实例
     * @throws IOException IOException
     */
    private void esMappingQuery(QueryTaskContext queryTaskContext, EsDslQueryEntity queryEntity,
            RestClient restClient) throws IOException {
        String endPoint = String.format(DSL_MAPPING_ENDPOINT, queryEntity.getQueryIndex());
        Response response = restClient.performRequest(GET, endPoint);
        int status = response.getStatusLine()
                .getStatusCode();
        String responseBody = EntityUtils.toString(response.getEntity());
        if (status == HttpStatus.SC_OK) {
            Map<String, Object> responseMap = JacksonUtil.convertJson2Map(responseBody);
            if (responseMap != null) {
                queryTaskContext.setResultData(responseMap);
                queryTaskContext.setTotalRecords(responseMap.size());
            }
        }
    }

    /**
     * 执行 Es 常规查询
     *
     * @param queryTaskContext 查询上下文
     * @param queryEntity EsDslQueryEntity 请求体
     * @param restClient RestClient 实例
     * @throws IOException IOException
     */
    private void esNormalQuery(QueryTaskContext queryTaskContext, EsDslQueryEntity queryEntity,
            RestClient restClient) throws IOException {
        String endPoint = String.format(DSL_SEARCH_ENDPOINT, queryEntity.getQueryIndex());
        Map<String, String> params = Maps.newHashMap();
        String version = queryTaskContext.getPickedValidStorage()
                .getStorageCluster()
                .getVersion();
        if (ES_7_X_X.compareTo(version) > 0) {
            params.put(PREFERENCE, PRIMARY_FIRST);
        }
        params.put(IGNORE_UNAVAILABLE, "true");
        HttpEntity entity = new NStringEntity(queryEntity.getBodyNode()
                .toString(), ContentType.APPLICATION_JSON);
        Response response = restClient.performRequest(POST, endPoint, params, entity);
        int status = response.getStatusLine()
                .getStatusCode();
        String responseBody = EntityUtils.toString(response.getEntity());
        if (status == HttpStatus.SC_OK) {
            JsonNode responseNode = JacksonUtil.readTree(responseBody);
            JsonNode hitsNode = responseNode != null ? responseNode.get(HITS) : null;
            JsonNode subHitsNode = hitsNode != null ? hitsNode.get(HITS) : null;
            if (subHitsNode != null && subHitsNode.size() > 0) {
                fillColumnOrder(queryTaskContext, subHitsNode.get(0)
                        .get(SOURCE)
                        .fieldNames());
                int totalRecords = 0;
                JsonNode totalNode = hitsNode.get(TOTAL);
                if (totalNode != null) {
                    JsonNode value = totalNode.get(VALUE);
                    totalRecords = value != null ? value.asInt() : totalNode.asInt();
                }
                queryTaskContext.setTotalRecords(totalRecords);
            }
            queryTaskContext.setResultData(JacksonUtil.convertJson2Map(responseBody));
        }
    }

    /**
     * 连接资源释放
     *
     * @param restClient RestClient 实例
     */
    private void closeResource(RestClient restClient) {
        if (restClient == null) {
            return;
        }
        try {
            restClient.close();
        } catch (IOException e) {
            log.error("Failed to close Es RestClient", e);
        }
    }

    /**
     * 获取请求参数
     *
     * @param queryTaskContext 查询上下文
     * @return EsDslQueryEntity dsl 请求对象
     */
    private EsDslQueryEntity extractQueryParams(QueryTaskContext queryTaskContext) {
        JsonNode dslNode = JacksonUtil.readTree(queryTaskContext.getRealSql());
        if (dslNode == null) {
            return null;
        }
        boolean mapping =
                dslNode.has(StorageConstants.ES_MAPPING) && dslNode
                        .get(StorageConstants.ES_MAPPING)
                        .asBoolean(false);
        JsonNode bodyNode = dslNode.get(StorageConstants.ES_BODY);
        String queryIndex = dslNode.has(StorageConstants.ES_INDEX) ? dslNode.get(StorageConstants.ES_INDEX)
                .asText("")
                .toLowerCase() : "";
        queryIndex = mapping ? queryIndex
                : getQueryIndexByTimeStamp(queryTaskContext, queryIndex, bodyNode);
        return EsDslQueryEntity.of(queryIndex, bodyNode, mapping);
    }

    /**
     * 根据查询时间范围获取 index 列表
     *
     * @param queryTaskContext 查询上下文
     * @param index dsl 中的原始 index
     * @param bodyNode dsl 请求体
     * @return 处理后的 index
     */
    private String getQueryIndexByTimeStamp(QueryTaskContext queryTaskContext, String
            index,
            JsonNode bodyNode) {
        Preconditions
                .checkArgument(StringUtils.isNotBlank(index),
                        "es index can not be null or empty");
        Preconditions.checkArgument(bodyNode != null, "es body can not be null");
        final String alias = getIndexAlias(index);
        final StoragesProperty sp = queryTaskContext.getPickedStorageMap()
                .values()
                .stream()
                .filter(st ->
                        st.getPhysicalTableName()
                                .equalsIgnoreCase(alias))
                .findFirst()
                .orElse(new StoragesProperty());
        if (StringUtils.isNotBlank(alias)) {
            String query = queryTaskContext.getRealSql();
            Set<String> indexSet = Sets.newHashSet();
            if (query.contains(DT_EVENT_TIMESTAMP) && query.contains(EPOCH_TIMES)) {
                try {
                    Iterator<JsonNode> filterNodeIter = bodyNode.get(QUERY)
                            .get(BOOL)
                            .get(FILTER)
                            .elements();
                    while (filterNodeIter.hasNext()) {
                        JsonNode filter = filterNodeIter.next();
                        if (filter.has(RANGE)) {
                            JsonNode rangeNode = filter.get(RANGE)
                                    .get(DT_EVENT_TIMESTAMP);
                            long beginTs = rangeNode.get(GTE)
                                    .asLong();
                            long endTs = rangeNode.get(LTE)
                                    .asLong();
                            beginTs =
                                    beginTs < LOWEST_TIMESTAMP ? beginTs * 1000L : beginTs;
                            endTs = endTs < LOWEST_TIMESTAMP ? endTs * 1000L : endTs;
                            Date beginDate = DateUtil.millsUnix2Date(beginTs);
                            Date endDate = DateUtil.millsUnix2Date(endTs);
                            int dayDiff = DateUtil.getDayDiffBetween(beginDate, endDate);
                            dayDiff = Math.max(dayDiff, 0);
                            for (int i = 0; i < dayDiff + 1; i++) {
                                String tmpIndex = alias + "_"
                                        + DateUtil
                                        .getSpecialDateBeforeDay(
                                                DATEFORMAT_YYYYMMDD,
                                                beginDate, i);
                                indexSet.add(tmpIndex);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.error(
                            "Failed to match_index_by_ts|index={}|query={}|error={}", index,
                            query,
                            e);
                }
            }
            index += CommonConstants.COMMA_SEPERATOR + alias;
            indexSet.addAll(Lists.newArrayList(COMMON_SPLITTER.split(index)));
            String previousClusterName = sp.getPreviousClusterName();
            if (StringUtils.isBlank(previousClusterName)) {
                return COMMON_JSONNER.join(indexSet);
            }
            String finalPreviousClusterName = SpringBeanUtil.getBean(StoreKitApiService.class)
                    .getEsClusterName(previousClusterName);
            //跨集群查询，拼接clusterName
            List<String> concatIndex = indexSet.stream()
                    .map(tmpIndex -> new StringBuffer(tmpIndex)
                            .append(CommonConstants.COMMA_SEPERATOR)
                            .append(finalPreviousClusterName)
                            .append(CommonConstants.COLON_SEPERATOR)
                            .append(tmpIndex)
                            .toString()
                    )
                    .collect(Collectors.toList());
            return COMMON_JSONNER.join(concatIndex);
        }
        return index;
    }

    /**
     * 获取 es 索引别名
     *
     * @param index es 索引
     * @return 索引别名
     */
    private String getIndexAlias(String index) {
        Preconditions.checkArgument(index != null, "ElasticSearch index can not be null");
        Pattern pattern = Pattern.compile(INDEX_PATTERN);
        Matcher matcher = pattern.matcher(index);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return index;
    }

    /**
     * 生成元数据信息
     *
     * @param queryTaskContext 查询上下文
     * @param filedNames 原始字段元信息
     */
    private void fillColumnOrder(QueryTaskContext
            queryTaskContext, Iterator<String> filedNames) {
        if (filedNames == null) {
            return;
        }
        List<String> orderColumns = Lists.newArrayList();
        try {
            while (filedNames.hasNext()) {
                String filedName = filedNames.next();
                if (!RESERVED_FIELDS.contains(filedName)) {
                    orderColumns.add(filedName);
                }
            }
            Collections.sort(orderColumns);
            orderColumns.addAll(0, RESERVED_FIELDS);
            queryTaskContext.setColumnOrder(orderColumns);
        } catch (Exception e) {
            ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_DEVICE_ERROR.code(),
                    ResultCodeEnum.QUERY_DEVICE_ERROR.message(), e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB,
                    JacksonUtil.object2Json(item));
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR);
        }
    }

    /**
     * 生成元数据信息
     *
     * @param columnsNode 列元数据(列名+类型)
     * @param queryTaskContext 查询上下文
     * @param columnsList 生成的元数据信息
     */
    private void fillColumnOrder(JsonNode columnsNode, QueryTaskContext queryTaskContext,
            List<FieldMeta> columnsList) {
        if (columnsNode == null || columnsNode.size() == 0) {
            return;
        }
        Iterator<JsonNode> columns = columnsNode.iterator();
        final StatementTypeEnum statementType = queryTaskContext.getStatementType();
        List<String> orderColumns = Lists.newArrayList();
        try {
            int index = 0;
            while (columns.hasNext()) {
                JsonNode column = columns.next();
                String filedName = column.get(NAME)
                        .asText();
                String filedType = column.get(TYPE)
                        .asText();
                columnsList.add(FieldMeta.builder()
                        .fieldIndex(index)
                        .fieldName(QueryDriverUtil.convertFieldName(statementType,
                                filedName, index))
                        .fieldAlias(filedName)
                        .fieldType(getRtFieldType(filedType))
                        .build());
                orderColumns.add(filedName);
                index++;
            }
            queryTaskContext.setColumnOrder(orderColumns);
        } catch (Exception e) {
            ErrorMessageItem item = new ErrorMessageItem(ResultCodeEnum.QUERY_DEVICE_ERROR.code(),
                    ResultCodeEnum.QUERY_DEVICE_ERROR.message(), e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB,
                    JacksonUtil.object2Json(item));
            log.error(e.getMessage(), e);
            throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_ERROR, e.getMessage());
        }
    }


    /**
     * ErrorCode 列表
     */
    public enum EsErrorCodeEnum {
        COLUMN_NOT_FOUND("Unknown column"),
        TABLE_NOT_FOUND("Unknown index"),
        FUNC_NOT_FOUND("Unknown function"),
        OTHER("other");
        public final String code;

        EsErrorCodeEnum(String code) {
            this.code = code;
        }

        public String getCode() {
            return code;
        }
    }
}
