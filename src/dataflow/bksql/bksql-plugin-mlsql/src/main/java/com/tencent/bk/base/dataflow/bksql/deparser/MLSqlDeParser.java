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

package com.tencent.bk.base.dataflow.bksql.deparser;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlRel;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlCommonTask;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlDDLOperation;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTableNameAccess;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask.AbstractField;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskGroup;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTransformType;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlWriteMode;
import com.tencent.bk.base.dataflow.bksql.util.MLSqlModelUtils;
import com.tencent.bk.base.datalab.bksql.deparser.DeParser;
import com.tencent.bk.base.dataflow.bksql.rest.exception.MessageLocalizedExceptionV1;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.MLSqlLocalizedException;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.BlueKingAlgorithmFactory;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.MLSqlFunctionConstants;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.MLSqlOperatorTable;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.RelParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.planner.MLSqlPlannerFactory;
import com.tencent.bk.base.dataflow.bksql.mlsql.schema.MLSqlSchema;
import com.tencent.bk.base.dataflow.bksql.mlsql.table.MLSqlTable;
import com.tencent.bk.base.dataflow.bksql.mlsql.table.MLSqlTableRegistry;
import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCreateTableFromModel;
import org.apache.calcite.sql.SqlDropModels;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlShow;
import org.apache.calcite.sql.SqlShowSql;
import org.apache.calcite.sql.SqlTrainModel;
import org.apache.calcite.sql.SqlTruncateTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.tools.Planner;
import org.apache.commons.lang.StringUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MLSqlDeParser implements DeParser {

    public static final String PROPERTY_PREFIX = "mlsql";
    public static final String PROPERTY_ONLY_PARSE_TABLE = "only_parse_table";
    public static final String PROPERTY_HAS_SUB_QUERY = "has_sub_query";
    public static final String PROPERTY_SUB_TABLE_NAME = "sub_table_name";
    public static final String PROPERTY_SUB_TABLE_FIELDS = "sub_table_fields";
    public static final String PROPERTY_PARENT_TABLES = "parent_tables";
    private static final Logger logger = LoggerFactory.getLogger(MLSqlDeParser.class);
    protected final Config config;
    protected final boolean onlyParseTable;
    private final String trtTableMetadataUrlPattern;
    private final String modelMetaUrlPattern;
    private final String algorithmMetaUrlPattern;

    //sql执行需要的context与schema对象
    private MLSqlTaskContext context;
    private MLSqlSchema schema;
    private MLSqlTableRegistry tableRegistry;
    private boolean hasSubQuery = false;
    private String subTableName;
    private List<Map<String, Object>> subTableFields;
    private List<String> parentTables = new ArrayList<>();

    public MLSqlDeParser(
            @JacksonInject("properties") Config properties,
            @JsonProperty(value = "trtTableMetadataUrlPattern", required = true)
                    String trtTableMetadataUrlPattern,
            @JsonProperty(value = "modelMetaUrlPattern", required = true)
                    String modelMetaUrlPattern,
            @JsonProperty(value = "algorithmMetaUrlPattern", required = true)
                    String algorithmMetaUrlPattern) {
        this.config = properties.getConfig(PROPERTY_PREFIX);
        this.trtTableMetadataUrlPattern = trtTableMetadataUrlPattern;
        this.modelMetaUrlPattern = modelMetaUrlPattern;
        this.algorithmMetaUrlPattern = algorithmMetaUrlPattern;
        onlyParseTable = config.getBoolean(PROPERTY_ONLY_PARSE_TABLE);
        if (config.hasPath(PROPERTY_HAS_SUB_QUERY)) {
            this.hasSubQuery = config.getBoolean(PROPERTY_HAS_SUB_QUERY);
        }
        if (this.hasSubQuery) {
            this.subTableName = config.getString(PROPERTY_SUB_TABLE_NAME);
            this.subTableFields = (List<Map<String, Object>>) config.getAnyRef(PROPERTY_SUB_TABLE_FIELDS);
        }
    }

    public MLSqlDeParser(String trtTableMetadataUrlPattern, String modelMetaUrlPattern,
            String algorithmMetaUrlPattern) {
        this.config = ConfigFactory.empty();
        this.trtTableMetadataUrlPattern = trtTableMetadataUrlPattern;
        this.modelMetaUrlPattern = modelMetaUrlPattern;
        this.algorithmMetaUrlPattern = algorithmMetaUrlPattern;
        this.onlyParseTable = false;
    }

    public static void main(String[] args) {

    }

    @Override
    public Object deParse(SqlNode statement) {
        try {
            Resources.setThreadLocale(LocaleHolder.instance().get());
            return deParseV1(statement);
        } catch (MessageLocalizedExceptionV1 e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            Resources.setThreadLocale(null);
        }
    }

    /**
     * 针对insert语句优化的sqlNode
     */
    public SqlNode refineValidatedNode(SqlNode sqlNode, SqlNode defaultValidated) throws Exception {
        if (sqlNode instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) sqlNode;
            return insert.getSource();
        } else {
            return defaultValidated;
        }
    }

    /**
     * 针对insert，获取目标输出需要的一些信息
     *
     * @param planner
     * @param validated
     * @param schema 当前语句涉及到相关表的schema信息
     * @param sqlNode
     * @param targetFieldList 存储最终出列的相关类型，用于后续进行校验，因为在insert中
     *         源表与目标表的类型需要保持一致，否则会报错
     * @return 最终写入表的名称，如果不是insert，则返回为null
     */
    public String getTargetInfo(Planner planner,
            SqlNode validated,
            Schema schema,
            SqlNode sqlNode,
            List<RelDataTypeField> targetFieldList) throws Exception {
        SqlInsert insert = null;
        if (!(sqlNode instanceof SqlInsert)) {
            return null;
        }
        insert = (SqlInsert) sqlNode;

        // 获取insert的目标列，即要写入的表的哪几列
        List<SqlNode> targetNodeList = insert.getTargetColumnList().getList();
        List<String> targetColumNameList = new ArrayList<>();
        for (SqlNode node : targetNodeList) {
            SqlIdentifier nodeIdentifier = (SqlIdentifier) node;
            targetColumNameList.add(nodeIdentifier.getSimple());
        }
        // 获取insert词法
        try {
            RelRoot rel = planner.rel(validated);
            RelNode insertRel = rel.rel;
            //使用insertRel获取insert操作的信息： 目标表
            LogicalTableModify modify = (LogicalTableModify) insertRel;
            String tableName = modify.getTable().getQualifiedName().get(1);

            //获取源表的列信息
            Table table = schema.getTable(tableName);
            if (table != null) {
                MLSqlTable mlSqlTable = (MLSqlTable) table;
                RelDataType recordType = mlSqlTable.getRowType(planner.getTypeFactory());
                List<RelDataTypeField> fieldList = recordType.getFieldList();
                Map<String, RelDataTypeField> fieldTypeMap = new HashMap<>();
                fieldList.forEach(type -> {
                    fieldTypeMap.put(type.getName(), type);
                });

                //输出目标列的类型信息
                targetColumNameList.forEach(column -> targetFieldList.add(fieldTypeMap.get(column)));
                return modify.getTable().getQualifiedName().get(1);
            } else {
                throw new Exception("table does not exist");
            }
        } catch (Exception e) {
            throw new MessageLocalizedExceptionV1(
                    "syntax.error",
                    new Object[]{e.getMessage()},
                    MLSqlDeParser.class,
                    ErrorCode.SYNTAX_ERROR
            );
        }
    }

    /**
     * 生成优化后的RelNode
     */
    public MLSqlRel generateOptimizedRelNode(Planner planner, SqlNode validated) {
        RelRoot selectRel = null;
        try {
            selectRel = planner.rel(validated);
        } catch (Exception e) {
            throw new MessageLocalizedExceptionV1("semantic.error",
                    new Object[]{e.getMessage()},
                    MLSqlDeParser.class,
                    ErrorCode.SYMANTIC_ERROR);
        }
        //后面过程与select过程的验证完全一致
        // optimize (logical plan -> physical plan)
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        traitSet = traitSet.plus(MLSqlRel.CONVENTION);
        RelNode optimized = selectRel.rel;
        try {
            //查询优化
            optimized = planner.transform(0, traitSet, selectRel.rel);
        } catch (Exception e) {
            //优化失败，不优化
            e.printStackTrace();
        }
        MLSqlRel mlsqlRel = (MLSqlRel) optimized;
        return mlsqlRel;
    }

    /**
     * 生成task
     */
    public MLSqlTableNameAccess generateTask(MLSqlRel mlsqlRel) throws Exception {
        MLSqlTaskGroup taskGroup = new MLSqlTaskGroup();
        MLSqlTableNameAccess buildTask = mlsqlRel.translateToTask(context.withRoot(mlsqlRel), taskGroup, mlsqlRel);
        return buildTask;
    }

    /**
     * 对于insert语句，需要对生成的task重新进行额外的优化，包括：
     * 1. 源和目标列的类型需要一致，如不一致需要报错
     * 2. 最终的输出列名，需要将源表列名切换为目标列名，比如insert into a(c1, c2) from b(d1,d2)，需要将d1->c1,d2->c2
     * 3. 其它信息填充
     *
     * @param targetFieldList 最终需要生成的目标列类型信息，用于进行校验
     */
    public void refineTask(MLSqlTask task,
            List<RelDataTypeField> targetFieldList,
            String targetTableName,
            MLSqlRel mlsqlRel,
            MLSqlTaskContext context) {
        //拿到计算结果对应的类型，应该与原表的类型整合，如果不相符则要报错
        List<AbstractField> buildFieldList = task.getFields();
        for (int ind = 0; ind < buildFieldList.size(); ind++) {
            AbstractField buildField = buildFieldList.get(ind);
            RelDataTypeField tableField = targetFieldList.get(ind);
            if (buildField.getType().equalsIgnoreCase(tableField.getType().getSqlTypeName().getName())) {
                //to do 抛出异常
            } else {
                //类型匹配，直接变更名称即可
                buildField.setField(tableField.getName());
            }
        }
        //在builder的基础上重新设置一些属性
        task.setTableName(targetTableName);
        task.setTableNeedCreate(false);
        task.setType(MLSqlTransformType.data);
        task.setName(task.getName() + "_run");
        task.setId(task.getName());
    }

    /**
     * 从sqlnode中解析later function的相关信息
     */
    public JSONObject extractDynamicFuncsFromNode(SqlNode sqlNode) {
        try {
            SqlSelect select;
            switch (sqlNode.getKind()) {
                case SELECT:
                    select = (SqlSelect) sqlNode;
                    break;
                case INSERT:
                    select = (SqlSelect) ((SqlInsert) sqlNode).getSource();
                    break;
                default:
                    select = null;
            }
            if (select == null) {
                return null;
            }
            //lateral 实质为一个join操作，如果不是join,则无需要解析
            SqlJoin join = (SqlJoin) select.getFrom();
            SqlCall as = (SqlCall) join.getRight();
            SqlCall lateral = (SqlCall) as.getOperandList().get(0);
            SqlCall lateralFunc = (SqlCall) lateral.getOperandList().get(0);
            SqlBasicCall call2 = (SqlBasicCall) lateralFunc.getOperandList().get(0);
            List<SqlNode> nodeList = call2.getOperandList();
            for (SqlNode node : nodeList) {
                SqlCall basicCall = (SqlCall) node;
                List<SqlNode> basicNodeList = basicCall.getOperandList();
                if (basicNodeList.get(0) instanceof SqlLiteral) {
                    SqlLiteral value = (SqlLiteral) basicNodeList.get(0);
                    SqlLiteral name = (SqlLiteral) basicNodeList.get(1);
                    if (RelParser.MLSQL_MODEL_NAME.equalsIgnoreCase(name.getValueAs(String.class))) {
                        String modelName = value.getValueAs(String.class);
                        MLSqlModel model = MLSqlModelUtils.getMLSqlModel(modelName);
                        if (model != null) {
                            String algorithmName = model.getAlgorithmName();
                            MLSqlModelFunction function = (MLSqlModelFunction) MLSqlOperatorTable
                                    .getFunctionByName(algorithmName);
                            if (function != null) {
                                JSONObject lateralFunctionObject = new JSONObject();
                                lateralFunctionObject
                                        .put(MLSqlFunctionConstants.LATERAL_FUNCTION_NAME,
                                                call2.getOperator().getName());
                                lateralFunctionObject
                                        .put(MLSqlFunctionConstants.LATERAL_RETURN_SIZE, function.getOutputs().size());
                                lateralFunctionObject.put(RelParser.MLSQL_MODEL_NAME, modelName);
                                return lateralFunctionObject;
                            } else {
                                return null;
                            }
                        } else {
                            return null;
                        }
                    }
                }
            }
            return null;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 获取run语句中的生成表
     */
    public String parseTargetTable(SqlNode sqlNode) {
        if (sqlNode.getKind() != SqlKind.SELECT) {
            return null;
        } else {
            return MLSqlModelUtils.parseTargetTable((SqlSelect) sqlNode);
        }
    }

    /**
     * 获取语句中使用到的模型（生成的或是使用的）
     */
    public String parseModel(SqlNode sqlNode) {
        if (sqlNode.getKind() == SqlKind.INSERT) {
            return MLSqlModelUtils.parseModel(((SqlInsert) sqlNode).getSource());
        } else {
            return MLSqlModelUtils.parseModel((SqlSelect) sqlNode);
        }
    }

    /**
     * 根据sqlNode的类型，确定最终任务的writeMode
     */
    public void setTaskWriteMode(MLSqlTask task, SqlNode sqlNode) {
        if (sqlNode.getKind() == SqlKind.INSERT) {
            SqlInsert insert = (SqlInsert) sqlNode;
            if (!insert.isOverwrite()) {
                task.setWriteMode(MLSqlWriteMode.APPEND);
            }
        }
    }

    /**
     * 处理Show语句
     *
     * @param sqlNode
     * @return 解析后的结果，如果有正则表达式则一起返回
     */
    public MLSqlCommonTask showModels(SqlNode sqlNode) {
        SqlShow show = (SqlShow) sqlNode;
        MLSqlDDLOperation operation = new MLSqlDDLOperation();
        if (show.isShowModels()) {
            operation.setType("show_models");
        } else {
            operation.setType("show_tables");
        }
        operation.setData(show.getRegex());
        MLSqlCommonTask task = new MLSqlCommonTask(true, operation);
        return task;
    }

    /**
     * 处理Show Train语句
     *
     * @param sqlNode
     * @return 解析后的结果，如果有正则表达式则一起返回
     */
    public MLSqlCommonTask showModelSql(SqlNode sqlNode) {
        SqlShowSql show = (SqlShowSql) sqlNode;
        MLSqlDDLOperation operation = new MLSqlDDLOperation();
        if (show.isShowModel()) {
            operation.setType("show_train_model");
        } else {
            operation.setType("show_create_table");
        }
        operation.setData(show.getName().getSimple());
        MLSqlCommonTask task = new MLSqlCommonTask(true, operation);
        return task;
    }

    /**
     * 处理Drop语句
     *
     * @param sqlNode
     * @return 返回drop的相关信息
     */
    public MLSqlCommonTask dropModels(SqlNode sqlNode) {
        SqlDropModels drop = (SqlDropModels) sqlNode;
        SqlIdentifier name = (SqlIdentifier) drop.getOperandList().get(0);
        MLSqlDDLOperation operation = new MLSqlDDLOperation();
        if (drop.isDropModels()) {
            operation.setType("drop_model");
            operation.setData(name.getSimple());
            operation.setIfExists(drop.isIfExists());
            MLSqlCommonTask task = new MLSqlCommonTask(true, operation);
            return task;
        } else {
            //drop tables
            operation.setType("drop_table");
            operation.setData(name.getSimple());
            operation.setIfExists(drop.isIfExists());
            MLSqlCommonTask task = new MLSqlCommonTask(true, operation);
            return task;
        }
    }

    /**
     * 处理truncate语句
     *
     * @param sqlNode
     * @return 返回truncate操作的相关信息
     */
    public MLSqlCommonTask truncateTable(SqlNode sqlNode) {
        SqlTruncateTable truncate = (SqlTruncateTable) sqlNode;
        SqlIdentifier name = (SqlIdentifier) truncate.getOperandList().get(0);
        MLSqlDDLOperation operation = new MLSqlDDLOperation();
        operation.setType("truncate");
        operation.setData(name.getSimple());
        MLSqlCommonTask task = new MLSqlCommonTask(true, operation);
        return task;
    }

    /**
     * 对于MLSQL语句，每个语句均会对应一个查询子句：
     * 对train,create语句，其包括一个query（SqlSelect），将模型，算法等信息进行保存（即用|隔开的内容），以便于后续解析
     * 对insert语句，仅使用一个query无法完成这种表达，因为有目标表的信息，所以其查询子句是一个完整的insert
     * 此函数即返回查询子句
     *
     * @param sqlNode 原始语句解析生成的SqlNode
     * @return 查询子句
     */
    public SqlNode getQueryNode(SqlNode sqlNode) throws Exception {
        switch (sqlNode.getKind()) {
            case TRAIN:
                SqlTrainModel train = (SqlTrainModel) sqlNode;
                return train.operand(2);
            case CREATE_TABLE:
                SqlCreateTableFromModel create = (SqlCreateTableFromModel) sqlNode;
                return create.operand(2);
            case INSERT:
                return sqlNode;
            default:
                throw new Exception("Unsupported operation:" + sqlNode.getKind());
        }
    }

    /**
     * 初始化解析语句需要的信息，包括注册函数，注册表， 同时生成解析需要的Planner
     *
     * @param queryNode
     */
    private Planner initAndGetPlanner(SqlNode queryNode) {
        this.context = createContext(config);
        this.schema = new MLSqlSchema();
        MLSqlModelUtils.initMetadataUrl(this.modelMetaUrlPattern);
        BlueKingAlgorithmFactory.init(this.algorithmMetaUrlPattern);
        JSONObject dynamicFunctions = extractDynamicFuncsFromNode(queryNode);
        Planner planner = MLSqlPlannerFactory.get()
                .withDefaultSchema(schema, dynamicFunctions);
        tableRegistry = new MLSqlTableRegistry(
                this.trtTableMetadataUrlPattern,
                schema,
                planner.getTypeFactory(),
                context);
        for (String table : this.parentTables) {
            try {
                tableRegistry.register(table);
            } catch (IllegalArgumentException e) {
                throw new MLSqlLocalizedException("table.not.found.error",
                        new Object[]{table},
                        MLSqlDeParser.class,
                        ErrorCode.TABLE_NOT_FOUND_ERR);
            }
        }
        return planner;
    }

    /**
     * 解析MLSQL语句本身的相关信息，包括：算法与模型信息，最终输出字段信息等
     * 如果没有子查询，此函数的返回即为整个MLSQL完整的执行逻辑
     *
     * @param planner 查询计划对应的Planner
     * @param queryNode MLSQL语句的查询子句
     * @return 完整的任务执行逻辑
     */
    public MLSqlTask parseMainQurery(Planner planner, SqlNode queryNode) throws Exception {
        //语法验证
        SqlNode validated = planner.validate(queryNode);
        //获取目标补充信息
        List<RelDataTypeField> targetFiledList = new ArrayList<>();
        String targetTableName = getTargetInfo(planner, validated, schema, queryNode, targetFiledList);

        //对于Insert，重新获取validated
        validated = refineValidatedNode(queryNode, validated);

        //sql优化
        MLSqlRel mlSqlRel = generateOptimizedRelNode(planner, validated);

        //生成task
        MLSqlTableNameAccess task = generateTask(mlSqlRel);

        //对于insert语句，需要进行优化
        if (!StringUtils.isBlank(targetTableName) && task != null) {
            refineTask((MLSqlTask) task, targetFiledList, targetTableName, mlSqlRel, context);
            setTaskWriteMode((MLSqlTask) task, queryNode);
        }
        return (MLSqlTask) task;
    }

    /**
     * 处理仅解析输入输出模型的相关请求
     *
     * @param queryNode 用户输入的sql转换而成的SqlNode
     * @return CommonTask
     */
    private MLSqlCommonTask parseInputAndOutput(SqlNode queryNode) {
        Map<String, List<String>> readObject = new HashMap<>();
        Map<String, List<String>> writeObject = new HashMap<>();
        List<String> tableReadList = new ArrayList<>();
        List<String> tableWriteList = new ArrayList<>();
        List<String> modelReadList = new ArrayList<>();
        readObject.put("result_table", tableReadList);
        readObject.put("model", modelReadList);
        writeObject.put("result_table", tableWriteList);
        List<String> modelWriteList = new ArrayList<>();
        writeObject.put("model", modelWriteList);
        String modelName = parseModel(queryNode);
        if (queryNode.getKind() == SqlKind.INSERT) {
            tableReadList.addAll(this.parentTables.subList(0, this.parentTables.size() - 1));
            tableWriteList.add(this.parentTables.get(this.parentTables.size() - 1));
            modelReadList.add(modelName);
        } else {
            tableReadList.addAll(this.parentTables);
            String targetTable = parseTargetTable(queryNode);
            if (!StringUtils.isBlank(targetTable)) {
                //create
                tableWriteList.add(targetTable);
                modelReadList.add(modelName);
            } else {
                //train
                modelWriteList.add(modelName);
            }
        }
        Map<String, Map<String, List<String>>> resultMap = new HashMap<>();
        resultMap.put("read", readObject);
        resultMap.put("write", writeObject);
        return new MLSqlCommonTask(true, resultMap);
    }

    public void mergeTasks(MLSqlTask task, MLSqlTask subTask) {
        task.clearAndSetParents(subTask.getId());
    }

    /**
     * 对于有子查询的情况，在解析MLSQL语句之前，需要先对原有的查询进行优化，目前主要的优化为：
     * 将子查询替换为一个临时表（可能不存在）
     * 原因是：对于子查询可能是很复杂的（join ,uninon等）,不可能在MLSQL里增加相关的配置，这些应该交给spark-sql去处理
     * 而MLSQL只需要认为是一个临时表即可，这样就不需要在MLSQL加入复杂的普通SQL规则
     */
    public void refineQueryNode(SqlNode queryNode) throws Exception {
        SqlIdentifier identifier = new SqlIdentifier(this.subTableName, SqlParserPos.ZERO);
        switch (queryNode.getKind()) {
            case SELECT:
                SqlSelect select = (SqlSelect) queryNode;
                SqlNode from = select.getFrom();
                if (from.getKind() == SqlKind.JOIN) {
                    // lateral table
                    SqlJoin join = (SqlJoin) from;
                    join.setLeft(identifier);
                } else {
                    select.setFrom(identifier);
                }
                break;
            case INSERT:
                SqlSelect source = (SqlSelect) ((SqlInsert) queryNode).getSource();
                source.setFrom(identifier);
                break;
            default:
                throw new Exception("Unsupported refine operator type:" + queryNode.getKind());
        }
    }

    private Object deParseV1(SqlNode sqlNode) throws Exception {
        try {
            if (sqlNode instanceof SqlShow) {
                //调用接口返回结果
                return showModels(sqlNode);
            } else if (sqlNode instanceof SqlShowSql) {
                return showModelSql(sqlNode);
            } else if (sqlNode instanceof SqlDropModels) {
                return dropModels(sqlNode);
            } else if (sqlNode instanceof SqlTruncateTable) {
                //truncate
                return truncateTable(sqlNode);
            } else {
                MLSqlTableNamesDeParser tableNamesDeParser = new MLSqlTableNamesDeParser();
                this.parentTables = (List<String>) tableNamesDeParser.deParse(sqlNode);
                SqlNode queryNode = this.getQueryNode(sqlNode);
                if (this.onlyParseTable) {
                    return this.parseInputAndOutput(queryNode);
                }
                //初始化
                Planner planner = this.initAndGetPlanner(queryNode);
                if (this.hasSubQuery) {
                    List<AbstractField> fieldList = new ArrayList<>();
                    this.subTableFields.forEach(item -> {
                        fieldList.add(new MLSqlTask.ColumnField(
                                item.get("field").toString(),
                                item.get("type").toString(),
                                item.get("description").toString(),
                                Collections.singletonList("")));
                    });
                    logger.info("sub table fields:" + this.subTableFields);
                    logger.info("field list:" + fieldList);
                    this.refineQueryNode(queryNode);
                    this.tableRegistry.registerVirtualTable(this.subTableName, fieldList);
                }

                //解析主sql
                logger.info("query node:" + queryNode);
                MLSqlTask task = this.parseMainQurery(planner, queryNode);
                MLSqlTaskGroup taskGroup = new MLSqlTaskGroup();
                taskGroup.add(task);
                logger.info(taskGroup.toString());

                return taskGroup;
            }
        } catch (MLSqlLocalizedException e) {
            logger.error("parse and validate sql error", e);
            MLSqlCommonTask task = new MLSqlCommonTask(false, e.getMessage(), e.getCode());
            return task;
        } catch (Exception e) {
            logger.error("uexpected error", e);
            MLSqlCommonTask task = new MLSqlCommonTask(false, e.getMessage(), ErrorCode.DEFAULT_ERR);
            return task;
        }
    }

    private MLSqlTaskContext createContext(Config config) {
        MLSqlTaskContext.Builder builder = MLSqlTaskContext.builder();
        return builder.create();
    }
}
