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

package com.tencent.bk.base.datalab.queryengine.server.util;

import static com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants.AUTH_METHOD_USERNAME;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.FAILED;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.QUERY_DB;
import static com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants.SYNC;
import static com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants.DEVICE_TYPE_HDFS;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.CELL_ID;
import static com.tencent.bk.base.datalab.queryengine.server.constant.ThirdApiConstants.NOTEBOOK_ID;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.INTERFACE_LABAPI_INVOKE_ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.QUERY_DEVICE_ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum.SYSTEM_INNER_ERROR;
import static com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum.DML_SELECT;
import static com.tencent.bk.base.datalab.queryengine.server.util.JdbcUtil.getColumnName;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.dto.DataProcessing;
import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTable;
import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTableProperty;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultTableGenerateTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.exception.BaseException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.meta.FieldMeta;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskDataSet;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskStage;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskDataSetService;
import com.tencent.bk.base.datalab.queryengine.server.third.DataLabApiService;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryDriverUtil {

    public static final String CONVERT_FIELD_NAME_PREFIX = "__c";
    public static final String HDFS_DATA_PATH = "path";
    public static final String DATA_FORMAT = "format";
    public static final String COMPRESS = "compress";
    public static final String PARQUET_TYPE = "parquet";
    public static final String COMPRESS_NONE = "none";

    /**
     * 根据语句类型获取新的字段名
     *
     * @param type 语句类型
     * @param filedName 原始字段名
     * @param index 字段位置索引
     * @return 转换后的字段名
     */
    public static String convertFieldName(StatementTypeEnum type, String filedName, int index) {
        String name = filedName;
        if (type == DML_SELECT) {
            name = CONVERT_FIELD_NAME_PREFIX + index;
        }
        return name;
    }

    /**
     * 填充字段列表
     *
     * @param metaData 返回结果元数据
     */
    public static void fillColumnOrder(QueryTaskContext queryTaskContext,
            ResultSetMetaData metaData) {
        if (metaData == null) {
            return;
        }
        List<String> columnOrder = Lists.newArrayList();
        try {
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; ++i) {
                columnOrder.add(getColumnName(metaData, i));
            }
            queryTaskContext.setColumnOrder(columnOrder);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(QUERY_DEVICE_ERROR, e.getMessage());
        }
    }

    /**
     * 添加结果集元数据
     *
     * @param queryTaskContext 查询上下文
     */
    public static void addDataSetInfo(QueryTaskContext queryTaskContext) {
        try {
            String queryMethod = queryTaskContext.getQueryTaskInfo()
                    .getQueryMethod();
            if (SYNC.equals(queryMethod)) {
                return;
            }
            QueryTaskDataSet qds = new QueryTaskDataSet();
            qds.setQueryId(queryTaskContext.getQueryTaskInfo()
                    .getQueryId());
            qds.setResultTableId(queryTaskContext.getCreateTableName());
            ResultTableGenerateTypeEnum resultTableGenerateType = queryTaskContext
                    .getStatementType() == DML_SELECT
                    ? ResultTableGenerateTypeEnum.SYSTEM : ResultTableGenerateTypeEnum.USER;
            qds.setResultTableGenerateType(resultTableGenerateType.toString());
            qds.setSchemaInfo(JacksonUtil.object2Json(queryTaskContext.getSelectFields()));
            qds.setTotalRecords(queryTaskContext.getTotalRecords());
            qds.setCreatedBy(queryTaskContext.getQueryTaskInfo()
                    .getCreatedBy());
            Map<String, String> connectionInfo = Maps.newHashMap();
            connectionInfo.put(HDFS_DATA_PATH, queryTaskContext.getDataPath());
            connectionInfo.put(DATA_FORMAT, PARQUET_TYPE);
            connectionInfo.put(COMPRESS, COMPRESS_NONE);
            qds.setConnectionInfo(JacksonUtil.object2Json(connectionInfo));
            qds.setDescription("");
            QueryTaskDataSetService queryTaskDataSetService = SpringBeanUtil
                    .getBean(QueryTaskDataSetService.class);
            queryTaskDataSetService.insert(qds);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new QueryDetailException(SYSTEM_INNER_ERROR);
        }
    }

    /**
     * 同步结果集元数据
     *
     * @param queryTaskContext 查询上下文
     */
    public static void registerResultTable(QueryTaskContext queryTaskContext) {
        try {
            String queryMethod = queryTaskContext.getQueryTaskInfo()
                    .getQueryMethod();
            if (SYNC.equals(queryMethod)) {
                return;
            }
            String createTableId = queryTaskContext.getCreateTableName();
            //构建ResultTable
            ResultTable resultTable = new ResultTable();
            resultTable.setResultTableId(createTableId);
            resultTable.setFields(queryTaskContext.getSelectFields());
            resultTable.setStorages(
                    ImmutableMap.of(DEVICE_TYPE_HDFS, ImmutableMap.of(DataLakeUtil.DATA_TYPE, DataLakeUtil.ICEBERG)));
            //构建ResultTableProperty
            ResultTableProperty rtProperty = new ResultTableProperty();
            rtProperty.setUserName(queryTaskContext.getQueryTaskInfo()
                    .getCreatedBy());
            rtProperty.setAuthMethod(AUTH_METHOD_USERNAME);
            rtProperty.setResultTableList(ImmutableList.of(resultTable));
            if (queryTaskContext.getStatementType() == DML_SELECT) {
                rtProperty.setGenerateType(ResultTableGenerateTypeEnum.SYSTEM.toString());
            } else {
                //构造DataProcessing
                DataProcessing dp = new DataProcessing();
                dp.setProcessingId(createTableId);
                dp.setInputs(queryTaskContext.getResultTableIdList());
                dp.setOutputs(ImmutableList.of(createTableId));
                rtProperty.setDataProcessings(ImmutableList.of(dp));
            }
            DataLabApiService dataLabApiService = SpringBeanUtil
                    .getBean(DataLabApiService.class);
            Map<String, Object> queryProperties = Optional
                    .ofNullable(queryTaskContext.getQueryTaskInfo()
                            .getProperties())
                    .orElse(Maps.newHashMap());
            String noteBookId = (String) queryProperties.get(NOTEBOOK_ID);
            String cellId = (String) queryProperties.get(CELL_ID);
            boolean regResult = dataLabApiService
                    .registerResultTable(noteBookId, cellId, rtProperty);
            if (!regResult) {
                log.error("Failed to register resultTableId:{}", createTableId);
                throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR,
                        MessageLocalizedUtil.getMessage("结果表 {0} 注册失败，请检查输入参数是否正确或结果表是否已注册",
                                new Object[]{createTableId}));
            }
        } catch (QueryDetailException e) {
            updateQueryStage(queryTaskContext, QUERY_DB, e);
            throw e;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            updateQueryStage(queryTaskContext, QUERY_DB, e);
            throw new QueryDetailException(INTERFACE_LABAPI_INVOKE_ERROR,
                    "Failed to register ResultTable");
        }
    }


    /**
     * 更新 QUERY_DB 阶段的状态
     *
     * @param queryTaskContext 查询上下文
     * @param stage 查询阶段
     * @param e Exception
     */
    public static void updateQueryStage(QueryTaskContext queryTaskContext, String stage,
            Throwable e) {
        ResultCodeEnum resultCode = SYSTEM_INNER_ERROR;
        if (e instanceof BaseException) {
            resultCode = ((BaseException) e).getResultCode();
        }
        updateQueryStage(queryTaskContext, stage, e, resultCode);
    }

    /**
     * 更新 QUERY_DB 阶段的状态
     *
     * @param queryTaskContext 查询上下文
     * @param stage 查询阶段
     * @param e Exception 异常实例
     * @param code ResultCode 错误码
     */
    public static void updateQueryStage(QueryTaskContext queryTaskContext, String stage,
            Throwable e, ResultCodeEnum code) {
        QueryTaskStage queryTaskStage = queryTaskContext.getQueryTaskStageMap()
                .get(stage);
        if (!FAILED.equals(queryTaskStage.getStageStatus())) {
            ErrorMessageItem item = new ErrorMessageItem(code.code(),
                    code.message(),
                    e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, stage, JacksonUtil.object2Json(item));
        }
    }

    /**
     * 处理查询异常
     *
     * @param queryTaskContext 查询上下文
     * @param errorCode ResultCode 错误码
     * @param errorInfo 异常提示信息
     */
    public static void handleQueryError(QueryTaskContext queryTaskContext, ResultCodeEnum errorCode,
            String errorInfo) {
        ErrorMessageItem item = new ErrorMessageItem(errorCode.code(),
                errorCode.message(),
                errorInfo);
        FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, QUERY_DB,
                JacksonUtil.object2Json(item));
        throw new QueryDetailException(errorCode, ImmutableMap.of("error", errorInfo));
    }

    /**
     * 处理物理字段/函数/表/catalog/schema 不存在的异常
     *
     * @param queryTaskContext 查询上下文
     * @param e SQLException 异常实例
     */
    public static void handleNotFound(QueryTaskContext queryTaskContext,
            SQLException e, Pattern errorPattern, ResultCodeEnum code) {
        Matcher matcher = errorPattern
                .matcher(e.getMessage());
        String errorInfo = e.getMessage();
        String lostObject = "";
        if (matcher.find()) {
            lostObject = matcher.group(1);
        }
        switch (code) {
            case QUERY_FILED_NOT_EXSITS:
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询失败，字段 {0} 在表中不存在", new Object[]{lostObject});
                break;
            case QUERY_FUNC_NOT_EXISTS:
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询失败，函数 {0} 不存在", new Object[]{lostObject});
                break;
            case SQL_ERROR_REAL_TABLE_NOT_FOUND:
                errorInfo = MessageLocalizedUtil.getMessage(
                        "查询失败，物理表不存在，入库任务没有启动或没有正常运行");
                break;
            case QUERY_CATALOG_NOT_FOUND:
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询失败，数据源 {0} 不存在", new Object[]{lostObject});
                break;
            case QUERY_SCHEMA_NOT_FOUND:
                errorInfo = MessageLocalizedUtil
                        .getMessage("查询失败，数据库 {0} 不存在", new Object[]{lostObject});
                break;
            case QUERY_NO_DATA_WRITTEN:
                errorInfo = MessageLocalizedUtil
                        .getMessage("结果表对应物理表 {0} 不存在，请确认是否有数据入库", new Object[]{lostObject});
                break;
            default:
                handleQueryError(queryTaskContext, QUERY_DEVICE_ERROR,
                        MessageLocalizedUtil.getMessage(e.getMessage()));
        }
        handleQueryError(queryTaskContext, code, errorInfo);
    }

    /**
     * 根据字段 fieldAlias 获取 fieldName
     *
     * @param queryTaskContext 查询上下文
     * @param fieldAlias 字段的 fieldAlias
     * @return fieldName
     */
    public static String getFieldNameByAlias(QueryTaskContext queryTaskContext, String fieldAlias) {
        List<FieldMeta> fields = queryTaskContext.getSelectFields();
        Optional<FieldMeta> fieldMeta = fields.stream()
                .filter(f -> f.getFieldAlias().equalsIgnoreCase(fieldAlias)).findFirst();
        if (fieldMeta.isPresent()) {
            return fieldMeta.get().getFieldName();
        }
        return "";
    }

    /**
     * 获取异常分组内容
     *
     * @param pattern 异常消息正则 Pattern
     * @param errorMsg 异常消息
     * @param index 匹配分组索引
     * @return 匹配分组内容
     */
    public static String getErrorItemFromPattern(Pattern pattern, String errorMsg, int index) {
        String errorItem = "";
        Matcher matcher = pattern.matcher(errorMsg);
        if (matcher.find()) {
            errorItem = matcher.group(index);
        }
        return errorItem;
    }

}
