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

package com.tencent.bk.base.datalab.queryengine.server.service.impl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.exception.TokenMgrException;
import com.tencent.bk.base.datalab.bksql.parser.calcite.ParserHelper;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import com.tencent.bk.base.datalab.meta.Field;
import com.tencent.bk.base.datalab.meta.StorageCluster;
import com.tencent.bk.base.datalab.meta.Storages;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.meta.TableMeta;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkDataConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.BkSqlContants;
import com.tencent.bk.base.datalab.queryengine.server.constant.CommonConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.ResponseConstants;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContext;
import com.tencent.bk.base.datalab.queryengine.server.context.BkAuthContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContext;
import com.tencent.bk.base.datalab.queryengine.server.context.ResultTableContextHolder;
import com.tencent.bk.base.datalab.queryengine.server.enums.ResultCodeEnum;
import com.tencent.bk.base.datalab.queryengine.server.enums.StatementTypeEnum;
import com.tencent.bk.base.datalab.queryengine.server.eval.BaseQueryEvaluator;
import com.tencent.bk.base.datalab.queryengine.server.eval.QueryCost;
import com.tencent.bk.base.datalab.queryengine.server.eval.QueryEvaluatorFactory;
import com.tencent.bk.base.datalab.queryengine.server.exception.PermissionForbiddenException;
import com.tencent.bk.base.datalab.queryengine.server.exception.QueryDetailException;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskInfo;
import com.tencent.bk.base.datalab.queryengine.server.model.QueryTaskResultTable;
import com.tencent.bk.base.datalab.queryengine.server.querydriver.QueryDriver;
import com.tencent.bk.base.datalab.queryengine.server.querydriver.QueryDriverFactory;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskInfoService;
import com.tencent.bk.base.datalab.queryengine.server.service.QueryTaskResultTableService;
import com.tencent.bk.base.datalab.queryengine.server.service.SqlParseService;
import com.tencent.bk.base.datalab.queryengine.server.third.AuthApiService;
import com.tencent.bk.base.datalab.queryengine.server.third.MetaApiService;
import com.tencent.bk.base.datalab.queryengine.server.third.StorageState;
import com.tencent.bk.base.datalab.queryengine.server.third.StoreKitApiService;
import com.tencent.bk.base.datalab.queryengine.server.util.BkSqlUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ErrorMessageItem;
import com.tencent.bk.base.datalab.queryengine.server.util.FillQueryTaskStageUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.FunctionFactory;
import com.tencent.bk.base.datalab.queryengine.server.util.MessageLocalizedUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.ResultTableUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.SchemaUtil;
import com.tencent.bk.base.datalab.queryengine.server.util.TypeFactory;
import com.tencent.bk.base.datalab.queryengine.server.wrapper.QueryTaskContext;
import io.prestosql.jdbc.$internal.guava.collect.Sets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class BaseQueryServiceImpl implements QueryService {

    @Autowired
    private AuthApiService authApiService;

    @Autowired
    private BKSqlService bkSqlService;

    @Autowired
    private MetaApiService metaApiService;

    @Autowired
    private StoreKitApiService storeKitApiService;

    @Autowired
    private SqlParseService sqlParseService;

    @Autowired
    private QueryTaskInfoService queryTaskInfoService;

    @Autowired
    private QueryTaskResultTableService queryTaskResultTableService;

    /**
     * 获取 rootSchema
     *
     * @param schema SchemaPlus 实例
     * @return rootSchema
     */
    private static SchemaPlus rootSchema(SchemaPlus schema) {
        for (; ; ) {
            if (schema.getParentSchema() == null) {
                return schema;
            }
            schema = schema.getParentSchema();
        }
    }

    @Override
    public void query(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        Preconditions.checkNotNull(queryTaskContext, "queryTaskContext can not be null");
        try {
            if (CommonConstants.ASYNC.equals(queryTaskContext.getQueryTaskInfo()
                    .getQueryMethod())) {
                queryTaskInfoService.insert(queryTaskContext.getQueryTaskInfo());
            }
            doQuery(queryTaskContext);
        } catch (QueryDetailException e) {
            throw e;
        } catch (Exception e) {
            throw e;
        } finally {
            String queryStorage = ObjectUtils.firstNonNull(queryTaskContext.getQueryTaskInfo()
                    .getPreferStorage(), queryTaskContext.getPickedClusterType(), "badStorage");
            log.debug(
                    "[Query-Finished]|queryType={}|queryId={}|origin_sql={}|convert_sql"
                            + "={}|server_elapsed_time={}",
                    queryStorage, queryTaskContext.getQueryTaskInfo()
                            .getQueryId(), queryTaskContext.getQueryTaskInfo()
                            .getSqlText(), queryTaskContext.getRealSql(),
                    (System.currentTimeMillis() - beginTime));
        }
    }

    @Override
    public void checkQuerySyntax(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.CHECK_QUERY_SYNTAX_SEQ,
                            CommonConstants.CHECK_QUERY_SYNTAX,
                            beginTime);
            String sql = queryTaskContext.getQueryTaskInfo()
                    .getSqlText();
            String preferStorage = queryTaskContext.getQueryTaskInfo()
                    .getPreferStorage();
            boolean isEsDsl =
                    BkSqlUtil.isEsDsl(sql, preferStorage);
            queryTaskContext.setEsDsl(isEsDsl);
            if (!isEsDsl) {
                sqlParseService.checkSyntax(sql);
                queryTaskContext.setSourceRtRangeMap(getSourceRtRange(queryTaskContext));
            }
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CHECK_QUERY_SYNTAX);
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            log.error(errorMsg, e);
            ResultCodeEnum code = (e instanceof TokenMgrException) ? ResultCodeEnum.QUERY_SQL_TOKEN_ERROR
                    : ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR;
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CHECK_QUERY_SYNTAX, JacksonUtil
                            .object2Json(new ErrorMessageItem(code.code(),
                                    code.message(), errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.QUERY_SQL_SYNTAX_ERROR, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg, CommonConstants.CHECK_QUERY_SYNTAX_SEQ,
                    CommonConstants.CHECK_QUERY_SYNTAX);
        }
    }

    @Override
    public void checkPermission(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.CHECK_PERMISSION_SEQ,
                            CommonConstants.CHECK_PERMISSION, beginTime);
            setCreateTableNameAndQuerySql(queryTaskContext);
            Set<String> rtSet = getResultTables(queryTaskContext);
            rtSet.forEach(rt -> {
                boolean isValid = ResultTableUtil.validResultTable(rt);
                if (!isValid) {
                    throw new QueryDetailException(ResultCodeEnum.SQL_ERROR_TABLE_NOT_FOUND,
                            MessageLocalizedUtil.getMessage("结果表 {0} 表名不符合规范",
                                    new Object[]{rt}));
                }
            });
            authApiService.checkAuth(rtSet.toArray(new String[]{}), BkDataConstants.PIZZA_AUTH_ACTION_QUERY_RT);
            queryTaskContext.setResultTableIdList(Lists.newArrayList(rtSet));
            ResultTableContextHolder
                    .set(ResultTableContext.builder()
                            .resultTableId(rtSet.toString())
                            .resultTableIdList(Lists.newArrayList(rtSet))
                            .sourceRtRangeMap(queryTaskContext.getSourceRtRangeMap())
                            .build());
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CHECK_PERMISSION);
        } catch (QueryDetailException | PermissionForbiddenException e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CHECK_PERMISSION,
                    JacksonUtil.object2Json(new ErrorMessageItem(e.getResultCode()
                            .code(),
                            MessageLocalizedUtil.getMessage(e.getResultCode()
                                    .message()),
                            errorMsg)));
            throw e;
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            FillQueryTaskStageUtil.updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CHECK_PERMISSION,
                    JacksonUtil.object2Json(new ErrorMessageItem(ResultCodeEnum.QUERY_PERMISSION_INVALID.code(),
                            ResultCodeEnum.QUERY_PERMISSION_INVALID.message(),
                            errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.QUERY_PERMISSION_INVALID, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg, CommonConstants.CHECK_PERMISSION_SEQ,
                    CommonConstants.CHECK_PERMISSION);
        }
    }

    @Override
    public void pickValidStorage(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.PICK_VALID_STORAGE_SEQ,
                            CommonConstants.PICK_VALID_STORAGE,
                            beginTime);
            setStorageMapAndSqlType(queryTaskContext);
            setResultTables(queryTaskContext);
            setPreferStorage(queryTaskContext);
            setClusterInfo(queryTaskContext);
            ResultTableContextHolder.get().setClusterName(queryTaskContext.getPickedClusterName());
            ResultTableContextHolder.get().setClusterType(queryTaskContext.getPickedClusterType());
            QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
            queryTaskInfo.setCostTime(0);
            queryTaskInfo.setQueryState(CommonConstants.RUNNING);
            queryTaskInfo.setTotalRecords(0L);
            queryTaskInfo.setUpdatedBy(queryTaskInfo.getCreatedBy());
            queryTaskInfoService.update(queryTaskContext.getQueryTaskInfo());
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.PICK_VALID_STORAGE);
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            ResultCodeEnum code =
                    (e instanceof QueryDetailException) ? ((QueryDetailException) e).getResultCode()
                            : ResultCodeEnum.SYSTEM_INNER_ERROR;
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.PICK_VALID_STORAGE, JacksonUtil
                            .object2Json(new ErrorMessageItem(code.code(),
                                    code.message(), errorMsg)));
            throw new QueryDetailException(code, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg, CommonConstants.PICK_VALID_STORAGE_SEQ,
                    CommonConstants.PICK_VALID_STORAGE);
        }
    }

    @Override
    public void matchQueryForbiddenConfig(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.MATCH_QUERY_FORBIDDEN_CONFIG_SEQ,
                            CommonConstants.MATCH_QUERY_FORBIDDEN_CONFIG, beginTime);
            checkIfOnMigrate(queryTaskContext);
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.MATCH_QUERY_FORBIDDEN_CONFIG);
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            log.error(errorMsg, e);
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.MATCH_QUERY_FORBIDDEN_CONFIG,
                            JacksonUtil
                                    .object2Json(new ErrorMessageItem(ResultCodeEnum.SYSTEM_INNER_ERROR.code(),
                                            ResultCodeEnum.SYSTEM_INNER_ERROR.message(),
                                            errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.SYSTEM_INNER_ERROR, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg,
                    CommonConstants.MATCH_QUERY_FORBIDDEN_CONFIG_SEQ, CommonConstants.MATCH_QUERY_FORBIDDEN_CONFIG);
        }
    }

    @Override
    public void checkQuerySemantic(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.CHECK_QUERY_SEMANTIC_SEQ,
                            CommonConstants.CHECK_QUERY_SEMANTIC,
                            beginTime);
            String sql = queryTaskContext.getQueryTaskInfo()
                    .getSqlText();
            String preferStorage = queryTaskContext.getQueryTaskInfo()
                    .getPreferStorage();
            boolean isEsDsl =
                    JacksonUtil.readTree(sql) != null && StorageConstants.DEVICE_TYPE_ES.equals(preferStorage);
            if (!isEsDsl) {
                sql = sql.replaceAll(BkSqlContants.PATTERN_STORAGE, "");
                result = semanticValidate(queryTaskContext, sql);
            }
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CHECK_QUERY_SEMANTIC);
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            log.error(errorMsg, e);
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CHECK_QUERY_SEMANTIC, JacksonUtil
                            .object2Json(new ErrorMessageItem(ResultCodeEnum.QUERY_SEMANTIC_ERROR.code(),
                                    ResultCodeEnum.QUERY_SEMANTIC_ERROR.message(),
                                    errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.QUERY_SEMANTIC_ERROR, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg, CommonConstants.CHECK_QUERY_SEMANTIC_SEQ,
                    CommonConstants.CHECK_QUERY_SEMANTIC);
        }
    }

    @Override
    public void matchQueryRoutingRule(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil.initQueryTaskStage(queryTaskContext, CommonConstants.MATCH_QUERY_ROUTING_RULE_SEQ,
                    CommonConstants.MATCH_QUERY_ROUTING_RULE, beginTime);
            boolean skipMatch = shouldSkipRoute(queryTaskContext);
            if (!skipMatch) {
                getStorageLoad(queryTaskContext);
                calQueryExecCost(queryTaskContext);
                executeQueryRule(queryTaskContext);
            }
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.MATCH_QUERY_ROUTING_RULE);
            queryTaskInfoService.update(queryTaskContext.getQueryTaskInfo());
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            log.error(errorMsg, e);
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.MATCH_QUERY_ROUTING_RULE, JacksonUtil
                            .object2Json(new ErrorMessageItem(ResultCodeEnum.SYSTEM_INNER_ERROR.code(),
                                    ResultCodeEnum.SYSTEM_INNER_ERROR.message(),
                                    errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.SYSTEM_INNER_ERROR, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg,
                    CommonConstants.MATCH_QUERY_ROUTING_RULE_SEQ, CommonConstants.MATCH_QUERY_ROUTING_RULE);
        }
    }

    @Override
    public void convertQueryStatement(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil.initQueryTaskStage(queryTaskContext, CommonConstants.CONVERT_QUERY_STATEMENT_SEQ,
                    CommonConstants.CONVERT_QUERY_STATEMENT, beginTime);
            String sqlText = queryTaskContext.getQueryTaskInfo()
                    .getSqlText();
            String preferStorage = queryTaskContext.getQueryTaskInfo()
                    .getPreferStorage();
            boolean isEsDsl =
                    JacksonUtil.readTree(sqlText) != null && StorageConstants.DEVICE_TYPE_ES.equals(preferStorage);
            boolean isBatchRoute = CommonConstants.ROUTING_TYPE_BATCH
                    .equalsIgnoreCase(queryTaskContext.getRoutingType());
            boolean skipConvert = isEsDsl || isBatchRoute;
            if (!skipConvert) {
                sqlText = convertStatementWithBkSql(queryTaskContext);
            }
            queryTaskContext.setRealSql(sqlText);
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.CONVERT_QUERY_STATEMENT);
            QueryTaskInfo queryTaskInfo = queryTaskContext.getQueryTaskInfo();
            queryTaskInfo.setConvertedSqlText(queryTaskContext.getRealSql());
            queryTaskInfoService.update(queryTaskContext.getQueryTaskInfo());
        } catch (Exception e) {
            log.error(errorMsg, e);
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.CONVERT_QUERY_STATEMENT, JacksonUtil
                            .object2Json(new ErrorMessageItem(ResultCodeEnum.INTERFACE_BKSQL_INVOKE_ERROR.code(),
                                    ResultCodeEnum.INTERFACE_BKSQL_INVOKE_ERROR.message(),
                                    errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.INTERFACE_BKSQL_INVOKE_ERROR, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg, CommonConstants.CONVERT_QUERY_STATEMENT_SEQ,
                    CommonConstants.CONVERT_QUERY_STATEMENT);
        }
    }

    @Override
    public void getQueryDriver(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.GET_QUERY_DRIVER_SEQ,
                            CommonConstants.GET_QUERY_DRIVER, beginTime);
            String pickedStorage = queryTaskContext.getPickedClusterType();
            String routingType = queryTaskContext.getRoutingType();
            pickedStorage = getQueryDriverName(pickedStorage, routingType);
            QueryDriver queryDriver = QueryDriverFactory.getQueryDriver(pickedStorage);
            queryTaskContext.setQueryDriver(queryDriver);
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.GET_QUERY_DRIVER);
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            log.error(errorMsg, e);
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.GET_QUERY_DRIVER,
                            JacksonUtil.object2Json(
                                    new ErrorMessageItem(ResultCodeEnum.SYSTEM_INNER_ERROR.code(),
                                            ResultCodeEnum.SYSTEM_INNER_ERROR.message(),
                                            errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.SYSTEM_INNER_ERROR, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg, CommonConstants.GET_QUERY_DRIVER_SEQ,
                    CommonConstants.GET_QUERY_DRIVER);
        }
    }

    @Override
    public void executeQuery(QueryTaskContext queryTaskContext) throws Exception {
        long beginTime = System.currentTimeMillis();
        boolean result = true;
        String errorMsg = "";
        try {
            FillQueryTaskStageUtil
                    .initQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB_SEQ, CommonConstants.QUERY_DB,
                            beginTime);
            QueryDriver queryDriver = queryTaskContext.getQueryDriver();
            queryDriver.query(queryTaskContext);
            FillQueryTaskStageUtil.updateQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB);
        } catch (QueryDetailException e) {
            throw e;
        } catch (Exception e) {
            result = false;
            errorMsg = MessageLocalizedUtil.getMessage(e.getMessage());
            log.error(errorMsg, e);
            FillQueryTaskStageUtil
                    .updateFailedQueryTaskStage(queryTaskContext, CommonConstants.QUERY_DB, JacksonUtil.object2Json(
                            new ErrorMessageItem(ResultCodeEnum.SYSTEM_INNER_ERROR.code(),
                                    ResultCodeEnum.SYSTEM_INNER_ERROR.message(),
                                    errorMsg)));
            throw new QueryDetailException(ResultCodeEnum.SYSTEM_INNER_ERROR, ImmutableMap
                    .of(ResponseConstants.ERROR, errorMsg, ResponseConstants.QUERY_ID,
                            queryTaskContext.getQueryTaskInfo()
                                    .getQueryId()));
        } finally {
            logStageInfo(queryTaskContext, beginTime, result, errorMsg, CommonConstants.QUERY_DB_SEQ,
                    CommonConstants.QUERY_DB);
        }
    }

    @Override
    public void getStorageLoad(QueryTaskContext queryTaskContext) throws Exception {
        StoragesProperty pickedStorageProperty = queryTaskContext.getPickedValidStorage();
        int storageConfigId = 0;
        if (pickedStorageProperty != null) {
            storageConfigId = pickedStorageProperty.getId();
        }
        StorageState storageState = storeKitApiService.getStorageState(storageConfigId);
        queryTaskContext.setStorageState(storageState);
    }

    @Override
    public void calQueryExecCost(QueryTaskContext queryTaskContext) throws Exception {
        String preferStorage = queryTaskContext.getQueryTaskInfo()
                .getPreferStorage();
        //联邦查询按照hdfs计算成本
        if (StorageConstants.DEVICE_TYPE_FEDERATION.equals(preferStorage)) {
            preferStorage = StorageConstants.DEVICE_TYPE_HDFS;
        }
        QueryCost evalCost = Optional.ofNullable(QueryEvaluatorFactory.getQueryEvaluator(preferStorage))
                .orElse(new BaseQueryEvaluator()).eval(queryTaskContext);
        queryTaskContext.setQueryCost(evalCost);
        log.debug("calQueryExecCost|queryId:{}|sql:{}|preferStorage:{}|queryCost:{}",
                queryTaskContext.getQueryTaskInfo().getQueryId(),
                queryTaskContext.getQueryTaskInfo().getSqlText(),
                preferStorage,
                evalCost);
    }

    @Override
    public void executeQueryRule(QueryTaskContext queryTaskContext) throws Exception {
        double rowCount = 0;
        try {
            String preferStorage = queryTaskContext.getQueryTaskInfo()
                    .getPreferStorage();
            boolean isDirect = (!StorageConstants.DEVICE_TYPE_FEDERATION.equalsIgnoreCase(preferStorage)
                    && !StorageConstants.DEVICE_TYPE_HDFS.equalsIgnoreCase(preferStorage));
            //普通直连查询路由到各自存储QueryDriver
            if (isDirect) {
                queryTaskContext.setRoutingType(CommonConstants.ROUTING_TYPE_DIRECT);
                return;
            }
            //hdfs查询且是ctas语句路由到batch
            if (isHdfsClusterType(queryTaskContext) && StatementTypeEnum.DDL_CTAS == queryTaskContext
                    .getStatementType()) {
                queryTaskContext.setRoutingType(CommonConstants.ROUTING_TYPE_BATCH);
                return;
            }
            //联邦查询路由到Presto
            if (StorageConstants.DEVICE_TYPE_FEDERATION.equalsIgnoreCase(preferStorage)) {
                queryTaskContext.setRoutingType(CommonConstants.ROUTING_TYPE_PRESTO);
                return;
            }
            queryTaskContext.setRoutingType(CommonConstants.ROUTING_TYPE_BATCH);
        } catch (Exception e) {
            throw e;
        } finally {
            log.debug("execute_query_rule|queryId:{}|sql:{}|rowCount:{}|routingType:{}",
                    queryTaskContext.getQueryTaskInfo().getQueryId(),
                    queryTaskContext.getQueryTaskInfo().getSqlText(),
                    rowCount,
                    queryTaskContext.getRoutingType());
        }
    }

    /**
     * 调用 BkSql 接口进行语句转换
     *
     * @param queryTaskContext 查询上下文
     * @return 转换后的 sql 语句
     * @throws Exception when exception occurs
     */
    private String convertStatementWithBkSql(QueryTaskContext queryTaskContext) throws Exception {
        String convertProtocol = getConvertProtocol(queryTaskContext);
        if (StringUtils.isBlank(convertProtocol)) {
            throw new QueryDetailException(ResultCodeEnum.INTERFACE_BKSQL_INVOKE_ERROR,
                    "Failed to Call bkSql interface caused by protocol is null or empty");
        }
        Map<String, Object> queryTaskProperties = Optional
                .ofNullable(queryTaskContext.getQueryTaskInfo()
                        .getProperties()).orElse(Maps.newHashMap());
        queryTaskProperties
                .put(ResponseConstants.PREFER_STORAGE, queryTaskContext.getPreferStorage());
        return (String) bkSqlService
                .convert(convertProtocol, new ParsingContext(queryTaskContext.getQueryTaskInfo()
                        .getSqlText(), queryTaskProperties));
    }

    /**
     * 获取 BkSql 转换协议
     *
     * @param queryTaskContext 查询上下文
     * @return BkSql 转换协议
     */
    private String getConvertProtocol(QueryTaskContext queryTaskContext) {
        final String pickedStorage = queryTaskContext.getPickedClusterType();
        if (StorageConstants.DEVICE_TYPE_FEDERATION.equalsIgnoreCase(pickedStorage)) {
            return BkSqlContants.BKSQL_PROTOCOL_FEDERATION;
        }
        final StoragesProperty pickedStorageProps = queryTaskContext.getPickedValidStorage();
        final String version = Optional.ofNullable(pickedStorageProps)
                .map(StoragesProperty::getStorageCluster).map(StorageCluster::getVersion)
                .orElse("");
        final String dataType = pickedStorageProps.getDataType();
        String convertProtocol;
        boolean isNonPartitionTable =
                queryTaskContext.getResultTableIdList().stream()
                        .anyMatch(ResultTableUtil::isNonPartitionTable);
        if (StorageConstants.DEVICE_TYPE_HDFS.equalsIgnoreCase(pickedStorage)) {
            convertProtocol = getHdfsConvertProtocol(dataType, isNonPartitionTable);
        } else if (
                StorageConstants.DEVICE_TYPE_DRUID.equalsIgnoreCase(pickedStorage)
                        && StorageConstants.DRUID_VERSION_0_11
                        .equals(version)) {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_DRUID_V0_11;
        } else if (
                StorageConstants.DEVICE_TYPE_HERMES.equalsIgnoreCase(pickedStorage)
                        && StorageConstants.HERMES_VERSION_1_0_0
                        .equals(version)) {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_HERMES_V1_0_0;
        } else if (StorageConstants.DEVICE_TYPE_CLICKHOUSE.equalsIgnoreCase(pickedStorage) && isNonPartitionTable) {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_CLICKHOUSE_WITHOUT_PARTITION;
        } else if (BkSqlContants.REDIRECT_FEDERATION_LIST.contains(pickedStorage)) {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_FEDERATION;
        } else {
            convertProtocol = pickedStorage;
        }
        return convertProtocol;
    }


    /**
     * 获取 hdfs 查询 BkSql 转换协议
     *
     * @param dataType 结果表类型
     * @param isNonPartitionTable 是否是非分区表
     * @return BkSql 转换协议
     */
    private String getHdfsConvertProtocol(String dataType, boolean isNonPartitionTable) {
        String convertProtocol;
        if (isNonPartitionTable && StorageConstants.DATA_TYPE_ICEBERG.equalsIgnoreCase(dataType)) {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_PRESTO_ICEBERG_WITHOUT_PARTITION;
        } else if (!isNonPartitionTable && StorageConstants.DATA_TYPE_ICEBERG.equalsIgnoreCase(dataType)) {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_PRESTO_ICEBERG;
        } else if (isNonPartitionTable && !StorageConstants.DATA_TYPE_ICEBERG.equalsIgnoreCase(dataType)) {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_PRESTO_WITHOUT_PARTITION;
        } else {
            convertProtocol = BkSqlContants.BKSQL_PROTOCOL_PRESTO;
        }
        return convertProtocol;
    }

    /**
     * sql 语义校验
     *
     * @param queryTaskContext 查询上下文
     * @param sql 需要检验 的sql 语句
     * @return 校验后转换成的关系表达式
     * @throws SqlParseException sql 解析异常
     * @throws ValidationException sql 校验异常
     * @throws RelConversionException 关系表达式转换异常
     */
    private boolean semanticValidate(QueryTaskContext queryTaskContext, String sql)
            throws Exception {
        boolean checkResult = false;
        CalciteCatalogReader catalogReader = getCalciteCatalogReader(queryTaskContext);
        SqlValidatorImpl validatorImpl = getSqlValidator(catalogReader,
                queryTaskContext.getSqlType());
        SqlNode originNode = ParserHelper.parse(sql);
        final SqlNode validatedNode = validatorImpl.validate(originNode);
        if (validatedNode != null) {
            checkResult = true;
        }
        return checkResult;
    }

    /**
     * 获取 SqlValidator 实例
     *
     * @param catalogReader CalciteCatalogReader
     * @param sqlType oneSql/nativeSql
     * @return SqlValidatorImpl 实例
     */
    @NotNull
    private SqlValidatorImpl getSqlValidator(CalciteCatalogReader catalogReader, String sqlType) {
        SqlOperatorTable sqlOperatorTable = ChainedSqlOperatorTable
                .of(SqlStdOperatorTable.instance(), catalogReader);
        SqlValidatorImpl validatorImpl = (SqlValidatorImpl) SqlValidatorUtil
                .newValidator(sqlOperatorTable, catalogReader,
                        new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT),
                        SqlConformanceEnum.LENIENT);
        validatorImpl.setSkipFunctionValid(true);
        return validatorImpl;
    }

    /**
     * 获取 CatalogReader 实例
     *
     * @param queryTaskContext 查询上下文
     * @return CalciteCatalogReader 实例
     */
    @NotNull
    private CalciteCatalogReader getCalciteCatalogReader(QueryTaskContext queryTaskContext) {
        String pickedStorage = queryTaskContext.getPickedClusterType();
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        ResultTableContext resultTableContext = ResultTableContextHolder.get();
        for (String rt : queryTaskContext.getResultTableIdList()) {
            TableMeta tableMeta = metaApiService.fetchTableFields(rt);
            List<Field> fields = tableMeta.getFields();
            List<Field> localFields = Lists.newArrayList(fields);
            for (Field af : StorageConstants.getAddtionalColumns(pickedStorage)) {
                String fName = af.getFieldName();
                boolean hasExists = false;
                for (Field lf : localFields) {
                    if (lf.getFieldName()
                            .equalsIgnoreCase(fName)) {
                        hasExists = true;
                        break;
                    }
                }
                if (!hasExists) {
                    localFields.add(af);
                }
            }
            rootSchema.add(rt, SchemaUtil.generateCalciteSchema(localFields, new SqlTypeFactoryImpl(
                    RelDataTypeSystem.DEFAULT), RelDataTypeSystem.DEFAULT));
            resultTableContext.setBizId(tableMeta.getBkBizId());
            resultTableContext.setProjectId(tableMeta.getProjectId());
        }
        String sqlType = queryTaskContext.getSqlType();
        if (StringUtils.equals(sqlType, CommonConstants.SQLTYPE_ONESQL)) {
            pickedStorage = sqlType;
        }
        if (StringUtils.endsWithIgnoreCase(StorageConstants.DEVICE_TYPE_MYSQL, pickedStorage)) {
            pickedStorage = StorageConstants.DEVICE_TYPE_MYSQL;
        }
        SchemaPlus schemaPlus = rootSchema.plus();
        //加载函数库
        loadUdfByStorage(pickedStorage, schemaPlus);
        //加载类型库
        loadUdtByStorage(pickedStorage, schemaPlus);
        return new CalciteCatalogReader(
                CalciteSchema.from(rootSchema(schemaPlus)),
                CalciteSchema.from(rootSchema(schemaPlus))
                        .path(null),
                new SqlTypeFactoryImpl(
                        RelDataTypeSystem.DEFAULT),
                null);
    }

    /**
     * 加载存储自定义函数库
     *
     * @param pickedStorage 存储类型
     * @param schemaPlus schemaPlus
     */
    private void loadUdfByStorage(String pickedStorage, SchemaPlus schemaPlus) {
        List<Map<String, ScalarFunction>> funcList = FunctionFactory.getRepository(pickedStorage);
        if (funcList == null || funcList.size() == 0) {
            return;
        }
        for (Map<String, ScalarFunction> udfMap : funcList) {
            for (Map.Entry<String, ScalarFunction> udf : udfMap.entrySet()) {
                schemaPlus.add(udf.getKey(), udf.getValue());
            }
        }
    }

    /**
     * 加载存储库自定义类型库
     *
     * @param pickedStorage 存储类型
     * @param schemaPlus schemaPlus
     */
    private void loadUdtByStorage(String pickedStorage, SchemaPlus schemaPlus) {
        //加载默认的自定义类型
        List<Map<String, RelProtoDataType>> defaultTypeList = TypeFactory
                .getRepository(CommonConstants.SQLTYPE_BKSQL);
        if (CollectionUtils.isEmpty(defaultTypeList)) {
            return;
        }
        defaultTypeList.forEach(udfMap -> udfMap
                .forEach(schemaPlus::add));
        //加载制定存储的自定义类型
        List<Map<String, RelProtoDataType>> storageTypeList = TypeFactory
                .getRepository(pickedStorage);
        if (CollectionUtils.isEmpty(storageTypeList)) {
            return;
        }
        storageTypeList.forEach(udfMap -> udfMap
                .forEach(schemaPlus::add));
    }

    /**
     * 获取 DSL 或者 SQL 语句里的结果表列表
     *
     * @param queryTaskContext 查询上下文
     * @return 结果表列表
     * @throws Exception 运行时异常
     */
    private Set<String> getResultTables(QueryTaskContext queryTaskContext) throws Exception {
        Preconditions.checkNotNull(queryTaskContext, "queryTaskContext can not be null");
        String preferStorage = queryTaskContext.getQueryTaskInfo()
                .getPreferStorage();
        String sql = queryTaskContext.getQueryTaskInfo()
                .getSqlText();
        boolean isEsDsl =
                JacksonUtil.readTree(sql) != null && StorageConstants.DEVICE_TYPE_ES.equals(preferStorage);
        if (isEsDsl) {
            return Sets.newHashSet(ResultTableUtil.getEsResultTableId(sql));
        }
        return BkSqlUtil.getResultTableIds(sql);
    }

    /**
     * 获取 create 语句里的需要创建的表名
     *
     * @param queryTaskContext 查询上下文
     * @return create 表名
     * @throws Exception 运行时异常
     */
    private String getCreateTableName(QueryTaskContext queryTaskContext) throws Exception {
        String preferStorage = queryTaskContext.getQueryTaskInfo()
                .getPreferStorage();
        String sql = queryTaskContext.getQueryTaskInfo()
                .getSqlText();
        boolean isEsDsl =
                JacksonUtil.readTree(sql) != null && StorageConstants.DEVICE_TYPE_ES.equals(preferStorage);
        if (isEsDsl) {
            return "";
        }
        return BkSqlUtil.getCreateTableName(sql);
    }

    /**
     * 获取 DSL 或者 SQL 语句里的结果表和存储类型映射 Map
     *
     * @param queryTaskContext 查询上下文
     * @return 结果表和存储类型映射 Map
     * @throws Exception 运行时异常
     */
    private Map<String, String> getResultTableWithStorage(QueryTaskContext queryTaskContext)
            throws Exception {
        String sql = queryTaskContext.getQueryTaskInfo()
                .getSqlText();
        return BkSqlUtil.getResultTableWithStorage(sql);
    }

    /**
     * 设置查询关联的结果表
     *
     * @param queryTaskContext 查询上下文
     */
    private void setResultTables(QueryTaskContext queryTaskContext) {
        Map<String, StoragesProperty> pickedStorageMap = queryTaskContext.getPickedStorageMap();
        if (pickedStorageMap != null && !pickedStorageMap.isEmpty()) {
            for (Map.Entry<String, StoragesProperty> entry : pickedStorageMap.entrySet()) {
                addTaskResultTable(queryTaskContext, entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 设置查询集群名称和集群类型
     *
     * @param queryTaskContext 查询上下文
     */
    private void setClusterInfo(QueryTaskContext queryTaskContext) {
        String pickedClusterType = queryTaskContext.getQueryTaskInfo()
                .getPreferStorage();
        queryTaskContext.setPickedClusterType(pickedClusterType);
        if (StringUtils.equalsIgnoreCase(StorageConstants.DEVICE_TYPE_FEDERATION, pickedClusterType)) {
            queryTaskContext.setPickedClusterName(StorageConstants.DEVICE_TYPE_FEDERATION);
        } else {
            for (StoragesProperty storagesProperty : queryTaskContext.getPickedStorageMap()
                    .values()) {
                queryTaskContext.setPickedValidStorage(storagesProperty);
                queryTaskContext.setPickedClusterName(storagesProperty.getStorageCluster()
                        .getClusterName());
                break;
            }
        }
    }

    /**
     * 获取 StorageMap 以及 SqlType
     *
     * @param queryTaskContext 查询上下文
     * @throws Exception when Exception occurs
     */
    private void setStorageMapAndSqlType(QueryTaskContext queryTaskContext) throws Exception {
        String preferStorage = queryTaskContext.getQueryTaskInfo()
                .getPreferStorage();
        String sql = queryTaskContext.getQueryTaskInfo()
                .getSqlText();
        String queryId = queryTaskContext.getQueryTaskInfo()
                .getQueryId();
        boolean isEsDsl =
                JacksonUtil.readTree(sql) != null && StorageConstants.DEVICE_TYPE_ES.equals(preferStorage);
        if (isEsDsl) {
            //SqlType is nativeSql
            queryTaskContext.setSqlType(CommonConstants.SQLTYPE_NATIVESQL);
            findStorageMapWithPreferStorage(queryTaskContext, queryId, preferStorage);
        } else {
            Map<String, String> resultTableMap = getResultTableWithStorage(queryTaskContext);
            if (resultTableMap == null || resultTableMap.isEmpty()) {
                if (StringUtils.isBlank(preferStorage)) {
                    //SqlType is oneSql
                    queryTaskContext.setSqlType(CommonConstants.SQLTYPE_ONESQL);
                    findStorageMapWithOutPreferStorage(queryTaskContext, queryId);
                } else {
                    //SqlType is nativeSql
                    queryTaskContext.setSqlType(CommonConstants.SQLTYPE_NATIVESQL);
                    findStorageMapWithPreferStorage(queryTaskContext, queryId, preferStorage);
                }
            } else {
                //SqlType is nativeSql
                queryTaskContext.setSqlType(CommonConstants.SQLTYPE_NATIVESQL);
                if (StringUtils.isBlank(preferStorage)) {
                    findStorageMapWithStorageSuffix(queryTaskContext, queryId, resultTableMap);
                } else {
                    throw new QueryDetailException(ResultCodeEnum.PARAM_IS_INVALID,
                            MessageLocalizedUtil.getMessage("参数preferStorage和sql中的结果表后存储参数不能同时存在"));
                }
            }
        }
    }

    /**
     * 获取存储类型 联邦/单存储
     *
     * @param queryTaskContext 查询上下文
     * @return 存储类型
     */
    private String findStorageType(QueryTaskContext queryTaskContext) {
        Map<String, StoragesProperty> storagesPropertyMap = queryTaskContext.getPickedStorageMap();
        String tmpClusterType = "";
        String tmpClusterName = "";
        String tmpDataType = "";
        for (Map.Entry<String, StoragesProperty> entry : storagesPropertyMap.entrySet()) {
            String clusterType = entry.getValue()
                    .getStorageCluster()
                    .getClusterType();
            String clusterName = entry.getValue()
                    .getStorageCluster()
                    .getClusterName();
            String dataType = entry.getValue()
                    .getDataType();
            if (StringUtils.isBlank(tmpClusterType)) {
                tmpClusterType = clusterType;
                tmpClusterName = clusterName;
                tmpDataType = dataType;
            }
            boolean isHdfsClusterType =
                    StringUtils.equalsIgnoreCase(tmpClusterType, clusterType) && StorageConstants.DEVICE_TYPE_HDFS
                            .equalsIgnoreCase(clusterType);
            boolean isDifferentClusterType = !StringUtils
                    .equalsIgnoreCase(tmpClusterType, clusterType);
            boolean isDifferentClusterName =
                    !isDifferentClusterType && !StringUtils
                            .equalsIgnoreCase(tmpClusterName, clusterName);
            boolean isDifferentDataType = !StringUtils
                    .equalsIgnoreCase(tmpDataType, dataType);
            boolean isFederation =
                    isDifferentClusterType || (isDifferentClusterName && !isHdfsClusterType) || (
                            isDifferentDataType && isHdfsClusterType);
            if (isFederation) {
                tmpClusterType = StorageConstants.DEVICE_TYPE_FEDERATION;
                break;
            }
        }
        return tmpClusterType;
    }

    /**
     * 查找结果表对应的存储信息
     *
     * @param queryTaskContext 查询上下文
     * @param queryId 查询 Id
     */
    private void findStorageMapWithOutPreferStorage(QueryTaskContext queryTaskContext,
            String queryId) {
        List<String> resultTableIds = queryTaskContext.getResultTableIdList();
        for (String resultTableId : resultTableIds) {
            Storages storages = metaApiService.fetchTableStorages(resultTableId)
                    .getStorages();
            if (storages == null) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_NO_STORAGES_ERROR,
                        MessageLocalizedUtil
                                .getMessage("结果表 {0} 未配置存储入库，请先配置入库", new Object[]{resultTableId}),
                        ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, queryId)
                                .build());
            }
            Map<String, StoragesProperty> storagesPropertyMap = storages.getAdditionalProperties();
            boolean supportQuery = false;
            for (String device : StorageConstants.SUPPORTED_QUERY_DEVICE) {
                if (storagesPropertyMap
                        .containsKey(device)) {
                    supportQuery = true;
                    queryTaskContext.getPickedStorageMap()
                            .putIfAbsent(resultTableId, storagesPropertyMap.get(device));
                    break;
                }
            }
            if (!supportQuery) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_NO_STORAGES_ERROR,
                        MessageLocalizedUtil
                                .getMessage("结果表 {0} 未配置存储入库，请先配置入库", new Object[]{resultTableId}),
                        ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, queryId)
                                .build());
            }
        }
    }

    /**
     * 查找结果表对应的存储信息
     *
     * @param queryTaskContext 查询上下文
     * @param queryId 查询 Id
     */
    private void findStorageMapWithPreferStorage(QueryTaskContext queryTaskContext, String queryId,
            String preferStorage) {
        List<String> resultTableIds = queryTaskContext.getResultTableIdList();
        for (String resultTableId : resultTableIds) {
            Storages storages = metaApiService.fetchTableStorages(resultTableId)
                    .getStorages();
            if (storages == null) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_NO_STORAGES_ERROR,
                        MessageLocalizedUtil
                                .getMessage("结果表 {0} 未配置存储入库，请先配置入库", new Object[]{resultTableId}),
                        ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, queryId)
                                .build());
            }
            Map<String, StoragesProperty> storagesPropertyMap = storages.getAdditionalProperties();
            if (StorageConstants.DEVICE_TYPE_TSPIDER_V2.equals(preferStorage)) {
                preferStorage = StorageConstants.DEVICE_TYPE_TSPIDER;
                queryTaskContext.getQueryTaskInfo()
                        .setPreferStorage(preferStorage);
            }
            StoragesProperty sp = storagesPropertyMap.get(preferStorage);
            if (sp == null) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_NOT_SUPPORT,
                        MessageLocalizedUtil.getMessage("结果表 {0} 未配置 {1} 存储入库，请先配置入库",
                                new Object[]{resultTableId, preferStorage}),
                        ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, queryId)
                                .build());
            } else {
                queryTaskContext.getPickedStorageMap()
                        .putIfAbsent(resultTableId, sp);
            }
        }
    }

    /**
     * 获取查询语句中结果表关联的存储
     *
     * @param queryTaskContext 查询上下文
     * @param resultTableMap 结果表与存储映射表
     */
    private void findStorageMapWithStorageSuffix(QueryTaskContext queryTaskContext, String queryId,
            Map<String, String> resultTableMap) {
        for (Map.Entry<String, String> rtEntry : resultTableMap.entrySet()) {
            final String resultTableId = rtEntry.getKey();
            final String resultTableStorage = rtEntry.getValue();
            Storages storages = metaApiService.fetchTableStorages(resultTableId)
                    .getStorages();
            if (storages == null) {
                throw new QueryDetailException(ResultCodeEnum.QUERY_NO_STORAGES_ERROR,
                        MessageLocalizedUtil
                                .getMessage("结果表 {0} 未配置存储入库，请先配置入库", new Object[]{resultTableId}),
                        ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, queryId)
                                .build());
            }
            Map<String, StoragesProperty> storagesPropertyMap = storages.getAdditionalProperties();
            if (StringUtils.isNotBlank(resultTableStorage)) {
                if (!storagesPropertyMap
                        .containsKey(resultTableStorage)) {
                    throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_NOT_SUPPORT,
                            MessageLocalizedUtil.getMessage("结果表 {0} 未配置 {1} 存储入库，请先配置入库",
                                    new Object[]{resultTableId, resultTableStorage}),
                            ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID, queryId)
                                    .build());
                } else {
                    queryTaskContext.getPickedStorageMap()
                            .putIfAbsent(resultTableId,
                                    storagesPropertyMap.get(resultTableStorage));
                }
            }
        }
    }

    /**
     * 添加结果表元数据记录
     *
     * @param resultTableId 结果表 id
     * @param storagesProperty 存储相关元数据
     */
    private void addTaskResultTable(QueryTaskContext queryTaskContext,
            String resultTableId, StoragesProperty storagesProperty) {
        QueryTaskResultTable resultTable = new QueryTaskResultTable();
        resultTable.setQueryId(queryTaskContext.getQueryTaskInfo()
                .getQueryId());
        resultTable.setResultTableId(resultTableId);
        resultTable.setStorageClusterConfigId(storagesProperty.getStorageCluster()
                .getStorageClusterConfigId());
        resultTable.setPhysicalTableName(storagesProperty.getPhysicalTableName());
        resultTable.setClusterName(storagesProperty.getStorageCluster()
                .getClusterName());
        resultTable.setClusterType(storagesProperty.getStorageCluster()
                .getClusterType());
        resultTable.setClusterGroup(storagesProperty.getStorageCluster()
                .getClusterGroup());
        resultTable.setConnectionInfo(storagesProperty.getStorageCluster()
                .getConnectionInfo());
        resultTable.setPriority(storagesProperty.getStorageCluster()
                .getPriority());
        resultTable.setVersion(storagesProperty.getStorageCluster()
                .getVersion());
        resultTable.setBelongsTo(storagesProperty.getStorageCluster()
                .getBelongsTo());
        resultTable.setStorageConfig(storagesProperty.getStorageConfig());
        BkAuthContext authContext = BkAuthContextHolder.get();
        String userName = "";
        if (authContext != null) {
            if (StringUtils.isNotEmpty(authContext.getBkUserName())) {
                userName = authContext.getBkUserName();
            } else {
                userName = authContext.getBkAppCode();
            }
        }
        resultTable.setCreatedBy(userName);
        resultTable.setDescription("");
        if (queryTaskContext.getQueryTaskResultTableList() != null) {
            queryTaskContext.getQueryTaskResultTableList()
                    .add(resultTable);
        } else {
            queryTaskContext.setQueryTaskResultTableList(Lists.newArrayList(resultTable));
        }
        if (CommonConstants.ASYNC.equals(queryTaskContext.getQueryTaskInfo()
                .getQueryMethod())) {
            queryTaskResultTableService.insert(resultTable);
        }
    }

    /**
     * 记录各个阶段日志
     *
     * @param queryTaskContext 查询上下文
     * @param beginTime 阶段开始时间
     * @param result 处理结果
     * @param errorMsg 异常消息
     * @param seq 阶段序号
     * @param stage 阶段类型
     */
    protected void logStageInfo(QueryTaskContext queryTaskContext, long beginTime, boolean result,
            String errorMsg, int seq, String stage) {
        if (log.isTraceEnabled()) {
            log.trace(
                    "[Stage{}-{}]|result={}|queryType={}|queryId={}|origin_sql={}|convert_sql"
                            + "={}|cost_time={}|errorMsg={}",
                    seq, stage, result, queryTaskContext.getQueryTaskInfo()
                            .getPreferStorage(), queryTaskContext.getQueryTaskInfo()
                            .getQueryId(), queryTaskContext.getQueryTaskInfo()
                            .getSqlText(), queryTaskContext.getRealSql(),
                    (System.currentTimeMillis() - beginTime), errorMsg);
        }
    }

    /**
     * 获取 SQL 里数据源表与查询时间范围的映射 Map
     *
     * @param queryTaskContext 查询上下文
     * @return 数据源表与查询时间范围的映射 Map
     */
    private Map<String, Map<String, String>> getSourceRtRange(QueryTaskContext queryTaskContext) {
        String originSql = queryTaskContext.getQueryTaskInfo()
                .getSqlText();
        //获取batch输入RT数据时间范围
        Map<String, Map<String, String>> inputRtRangeMap;
        try {
            inputRtRangeMap = BkSqlUtil
                    .getResultTablePartition(originSql);
        } catch (Exception e) {
            return Maps.newHashMap();
        }
        return inputRtRangeMap;
    }

    /**
     * 判断是否需要跳过查询路由校验
     *
     * @param queryTaskContext 查询上下文
     * @return 是否需要跳过查询路由校验
     */
    private boolean shouldSkipRoute(QueryTaskContext queryTaskContext) {
        String queryMethod = queryTaskContext.getQueryTaskInfo()
                .getQueryMethod();
        boolean isEsQuery = StorageConstants.DEVICE_TYPE_ES
                .equals(queryTaskContext.getQueryTaskInfo()
                        .getPreferStorage());
        boolean isSyncQuery = CommonConstants.SYNC.equals(queryMethod);
        boolean isCtasStatement = StringUtils
                .isNotBlank(queryTaskContext.getCreateTableName());
        return isEsQuery || isSyncQuery || !isCtasStatement;
    }

    /**
     * 设置 CTAS 表名和查询语句
     *
     * @param queryTaskContext 查询上下文
     * @throws Exception 执行异常
     */
    private void setCreateTableNameAndQuerySql(QueryTaskContext queryTaskContext) throws Exception {
        String createTableName = getCreateTableName(queryTaskContext);
        if (StringUtils.isNotBlank(createTableName)) {
            String sqlText = BkSqlUtil
                    .getQueryInCreateAs(queryTaskContext.getQueryTaskInfo()
                            .getSqlText());
            queryTaskContext.getQueryTaskInfo()
                    .setSqlText(sqlText);
            queryTaskContext.setCreateTableName(createTableName);
        }
    }

    /**
     * 设置 preferStorage
     *
     * @param queryTaskContext 查询上下文
     */
    private void setPreferStorage(QueryTaskContext queryTaskContext) {
        Map<String, StoragesProperty> pickedStorageMap = queryTaskContext.getPickedStorageMap();
        if (pickedStorageMap != null && !pickedStorageMap.isEmpty()) {
            queryTaskContext.getQueryTaskInfo()
                    .setPreferStorage(findStorageType(queryTaskContext));
        }
    }

    /**
     * 检查当前查询关联的结果表是否正在 iceberg 表进行迁移
     *
     * @param queryTaskContext 查询上下文
     */
    private void checkIfOnMigrate(QueryTaskContext queryTaskContext) {
        queryTaskContext
                .getQueryTaskResultTableList().stream()
                .filter(rt -> StorageConstants.DEVICE_TYPE_HDFS.equalsIgnoreCase(rt.getClusterType()))
                .forEach(rt -> {
                    String resultTableId = rt.getResultTableId();
                    List<String> onMigResultTables = storeKitApiService
                            .fetchOnMigrateResultTables();
                    if (onMigResultTables != null && onMigResultTables.contains(resultTableId)) {
                        throw new QueryDetailException(ResultCodeEnum.QUERY_DEVICE_NOT_SUPPORT,
                                MessageLocalizedUtil.getMessage("结果表 {0} 正在进行迁移，当前HDFS查询不可用",
                                        new Object[]{resultTableId}),
                                ImmutableMap.<String, String>builder().put(ResponseConstants.QUERY_ID,
                                        queryTaskContext.getQueryTaskInfo().getQueryId()).build());
                    }
                });
    }

    /**
     * 判断结果表是否是属于 hdfs 存储类型
     *
     * @param queryTaskContext 查询上下文
     * @return true：属于hdfs存储类型，false：不属于hdfs存储类型
     */
    private boolean isHdfsClusterType(QueryTaskContext queryTaskContext) {
        Map<String, StoragesProperty> storagesPropertyMap = queryTaskContext.getPickedStorageMap();
        String tmpClusterType = "";
        for (Map.Entry<String, StoragesProperty> entry : storagesPropertyMap.entrySet()) {
            String clusterType = entry.getValue()
                    .getStorageCluster()
                    .getClusterType();
            if (StringUtils.isBlank(tmpClusterType)) {
                tmpClusterType = clusterType;
            }
            boolean isHdfsClusterType =
                    StringUtils.equalsIgnoreCase(tmpClusterType, clusterType) && StorageConstants.DEVICE_TYPE_HDFS
                            .equalsIgnoreCase(clusterType);
            if (!isHdfsClusterType) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取 QueryDriver 名称
     *
     * @param pickedStorage 选择的存储/计算引擎
     * @param routingType 查询路由
     * @return QueryDriver 名称
     */
    private String getQueryDriverName(String pickedStorage, String routingType) {
        if (CommonConstants.ROUTING_TYPE_BATCH.equalsIgnoreCase(routingType)) {
            return StorageConstants.DEVICE_TYPE_BATCH;
        }
        if (BkSqlContants.REDIRECT_FEDERATION_LIST.contains(pickedStorage)) {
            return StorageConstants.DEVICE_TYPE_FEDERATION;
        }
        if (StorageConstants.DEVICE_TYPE_TPG.equalsIgnoreCase(pickedStorage)) {
            return StorageConstants.DEVICE_TYPE_POSTGRESQL;
        }
        if (StorageConstants.DEVICE_TYPE_FEDERATION.equalsIgnoreCase(pickedStorage)) {
            return StorageConstants.DEVICE_TYPE_HDFS;
        }
        if (StorageConstants.DEVICE_TYPE_TSPIDER.equalsIgnoreCase(pickedStorage)) {
            return StorageConstants.DEVICE_TYPE_MYSQL;
        }
        return pickedStorage;
    }
}
