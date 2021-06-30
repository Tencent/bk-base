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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.dataflow.bksql.validator.optimizer.SparkSqlIpLibOptimizer;
import com.tencent.bk.base.dataflow.bksql.validator.optimizer.SparkSqlIpLibOptimizer.IpLibMeta;
import com.tencent.blueking.bksql.deparser.SimpleListenerBasedDeParser;
import com.tencent.blueking.bksql.exception.FailedOnDeParserException;
import com.tencent.blueking.bksql.function.udf.BkdataUdfMetadataConnector;
import com.tencent.blueking.bksql.function.udf.UdfArgs;
import com.tencent.blueking.bksql.function.udf.UdfMetadata;
import com.tencent.blueking.bksql.function.udf.UdfParameterMetadata;
import com.tencent.blueking.bksql.table.BlueKingTrtTableMetadataConnector;
import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.tencent.blueking.bksql.table.ColumnMetadataImpl;
import com.tencent.blueking.bksql.table.TableMetadata;
import com.tencent.blueking.bksql.util.BlueKingDataTypeMapper;
import com.tencent.blueking.bksql.util.DataType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Select;
import org.apache.spark.sql.BkSparkSqlAnalyzer;
import org.apache.spark.sql.BkSparkUdf;
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry;
import org.apache.spark.sql.catalyst.catalog.CatalogFunction;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.codehaus.janino.SimpleCompiler;
import org.codehaus.janino.util.ClassFile;
import org.apache.spark.sql.BkDataType$;
import scala.Tuple3;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.regex.Pattern;

public class SparkSqlDeParser extends SimpleListenerBasedDeParser {

    public static final String PROPERTY_PREFIX = "spark";
    public static final String PROPERTY_KEY_BIZ_ID = "bk_biz_id";
    public static final String PROPERTY_KEY_RESULT_TABLE_NAME = "result_table_name";
    public static final String PROPERTY_KEY_INPUT_RESULT_TABLE = "input_result_table";
    public static final String PROPERTY_KEY_INPUT_RESULT_TABLE_WITH_SCHEMA = "input_result_table_with_schema";
    public static final String PROPERTY_KEY_DATAFLOW_UDF_ENV = "dataflow_udf_env";
    public static final String PROPERTY_KEY_DATAFLOW_UDF_ENV_DEFAULT_VAULE = "product";
    public static final String PROPERTY_KEY_DATAFLOW_UDF_FUNCTION_NAME = "dataflow_udf_function_name";
    public static final String PROPERTY_KEY_DATAFLOW_SELFDEPENDENCY_NAME = "self_dependency_config";
    private static final Pattern NUMERIC_PREFIX_PATTERN = Pattern.compile("\\d+_.+");
    private static final Pattern NUMERIC_SUFFIX_PATTERN = Pattern.compile(".+_\\d+");

    protected final Config config;
    protected final String bizID;
    protected final String outputTableName;
    private final BlueKingTrtTableMetadataConnector generalTableMetadataConnector;
    private final BkdataUdfMetadataConnector bkdataUdfMetadataConnector;
    private final Map<String, CatalogTable> tableRegistry = new HashMap<>();
    private final HashSet<String> virtualIpTableRegistry = new HashSet<>();
    private final List<SparkSqlUdfEntity> udfEntities = new ArrayList<>();
    private final Map<String, String> sqlUsedTables = new HashMap<>();
    private List<SparkSqlTableEntity> results = new ArrayList<>();
    private String selfDependencyTableName;
    private SelfDependencyParser selfDependencyParser = null;

    private Map<String, String> nameToAlias = new HashMap();

    /**
     * 解析SQL并生成最终字段信息
     *
     * @param properties 配置参数
     * @param trtTableMetadataUrlPattern 结果表元数据信息接口
     * @param dataflowUDFUrlPattern UDF信息接口
     * @param udfList udf列表
     * @param udafList udaf列表
     * @param udtfList udtf列表
     */
    @JsonCreator
    public SparkSqlDeParser(
            @JacksonInject("properties")
                    Config properties,
            @JsonProperty(value = "trtTableMetadataUrlPattern", required = true)
                    String trtTableMetadataUrlPattern,
            @JsonProperty(value = "dataflowUDFUrlPattern", required = true)
                    String dataflowUDFUrlPattern,
            @JsonProperty(value = "udf-list", required = true)
                    List<String> udfList,
            @JsonProperty(value = "udaf-list", required = true)
                    List<String> udafList,
            @JsonProperty(value = "udtf-list", required = true)
                    List<String> udtfList) {
        config = properties.getConfig(PROPERTY_PREFIX);
        generalTableMetadataConnector = BlueKingTrtTableMetadataConnector.forUrl(trtTableMetadataUrlPattern);
        bkdataUdfMetadataConnector = BkdataUdfMetadataConnector.forUrl(dataflowUDFUrlPattern);
        bizID = config.getString(PROPERTY_KEY_BIZ_ID);
        outputTableName = config.getString(PROPERTY_KEY_RESULT_TABLE_NAME);
        List<String> inPutTables = config.getStringList(PROPERTY_KEY_INPUT_RESULT_TABLE);
        for (String tableName : inPutTables) {
            String formatedTableName = moveNumericPrefixToTail(tableName.toLowerCase());
            if (isVirtualIpTable(formatedTableName)) {
                virtualIpTableRegistry.add(formatedTableName);
            } else {
                List<ColumnMetadata> columnMetaList = fetchGeneralMetadata(tableName).listColumns();
                registerTable(formatedTableName, columnMetaList);
                for (ColumnMetadata columnMeta : columnMetaList) {
                    nameToAlias.put(columnMeta.getColumnName(), columnMeta.getAlias());
                }
            }
        }

        if (config.hasPath(PROPERTY_KEY_DATAFLOW_SELFDEPENDENCY_NAME)) {
            selfDependencyParser =
                    new SelfDependencyParser(config.getConfig(PROPERTY_KEY_DATAFLOW_SELFDEPENDENCY_NAME));
            if (selfDependencyParser.isReady()) {
                selfDependencyTableName =
                        moveNumericPrefixToTail(selfDependencyParser.getSelfDependencyTableName().toLowerCase());
                registerTable(selfDependencyTableName,
                        selfDependencyParser.getSelfDependTableMetaList());
            }
        }
        // 如果传入schema使用传入schema
        parseCustomizedTableWithSchema();
        addUdfs(udfList, SparkSqlUdfEntity.BkFuncType.UDF);
        addUdfs(udafList, SparkSqlUdfEntity.BkFuncType.UDAF);
        addUdfs(udtfList, SparkSqlUdfEntity.BkFuncType.UDTF);
    }

    private boolean isVirtualIpTable(String tableName) {
        for (IpLibMeta iplibMeta : SparkSqlIpLibOptimizer.IP_LIB_METAS) {
            if (iplibMeta.getLogicalIpTableName().toLowerCase().equals(tableName)) {
                return true;
            }
        }
        return false;
    }

    //检查是否被使用的iplib有被注册
    private void checkIfUsedIpLibRegistered() {
        HashSet<String> tmpUsedIpLibHashSet = SparkSqlIpLibOptimizer.USED_IP_LIB_HASH_SET.get();
        for (String ipLib : tmpUsedIpLibHashSet) {
            if (!virtualIpTableRegistry.contains(ipLib)) {
                SparkSqlIpLibOptimizer.USED_IP_LIB_HASH_SET.remove();
                throw new FailedOnDeParserException("ip.lib.no.register.error", new Object[]{ipLib},
                        SparkSqlDeParser.class);
            }
        }
        SparkSqlIpLibOptimizer.USED_IP_LIB_HASH_SET.remove();
    }

    private void addUdfs(List<String> udfs, SparkSqlUdfEntity.BkFuncType functionType) {
        for (String udf : udfs) {
            boolean isError = false;
            String[] strs1 = udf.split("\\(", 2);
            String functionName = strs1[0].trim();
            String[] strs2 = strs1[1].split("\\)=", 2);
            String returnType = strs2[1].trim();
            String[] args = strs2[0].split(",");
            int noNeedArgs = 0;
            List<String> inputTypes = new ArrayList<>();
            for (String arg : args) {
                arg = arg.trim();
                if (arg.startsWith("[") && arg.endsWith("]")) {
                    noNeedArgs += 1;
                    arg = arg.substring(1, arg.length() - 1);
                } else {
                    if (noNeedArgs > 0) {
                        isError = true;
                    }
                }
                if (!"".equals(arg)) {
                    inputTypes.add(arg);
                }
            }

            String[] returnTypes;
            if (returnType.startsWith("[") && returnType.endsWith("]")) {
                returnTypes = returnType
                        .substring(returnType.indexOf("[") + 1, returnType.lastIndexOf("]"))
                        .split("\\s*,\\s*");
            } else {
                returnTypes = new String[]{returnType};
            }

            if (!isError) {
                String[] inputs = inputTypes.toArray(new String[inputTypes.size()]);
                int requiredArgNum = inputs.length - noNeedArgs;
                udfEntities.add(new SparkSqlUdfEntity(functionName,
                        returnTypes,
                        inputs,
                        requiredArgNum,
                        functionType));
            }
        }
    }

    private void parseCustomizedTableWithSchema() {
        if (config.hasPath(PROPERTY_KEY_INPUT_RESULT_TABLE_WITH_SCHEMA)) {
            Config schemaConfig = config.getConfig(PROPERTY_KEY_INPUT_RESULT_TABLE_WITH_SCHEMA);
            BlueKingDataTypeMapper dataTypeMapper = new BlueKingDataTypeMapper();
            List<ColumnMetadata> metaDatas = new ArrayList<>();

            for (Map.Entry<String, ConfigValue> item : schemaConfig.entrySet()) {
                String resultTableId = item.getKey();
                if (item.getKey().startsWith("\"")) {
                    resultTableId = resultTableId.substring(1, resultTableId.length() - 1);
                }
                List<? extends Config> tableFieldsConfig = schemaConfig.getConfigList(resultTableId);
                for (Config fieldConfig : tableFieldsConfig) {
                    ColumnMetadata columnMetadata =
                            new ColumnMetadataImpl(
                                    fieldConfig.getString("field_name"),
                                    dataTypeMapper.toBKSqlType(fieldConfig.getString("field_type").toLowerCase()),
                                    fieldConfig.getString("field_name"));
                    metaDatas.add(columnMetadata);
                }
                registerTable(moveNumericPrefixToTail(resultTableId.toLowerCase()),
                        metaDatas);
            }
        }
    }

    @Override
    public void enterNode(Table table) {
        super.enterNode(table);
        String tableName = table.getName();
        if (tableName == null) {
            return;
        }
        sqlUsedTables.put(tableName.toLowerCase(), tableName);

    }

    @Override
    public void exitNode(Select select) {
        super.exitNode(select);
        // 校验表是否被使用
        this.checkIfTableIsUsed();
        this.checkIfUsedIpLibRegistered();
        //
        String sql = select.toString();
        Seq<CatalogTable> tables = JavaConverters.asScalaIteratorConverter(
                tableRegistry.values().iterator()).asScala().toSeq();
        // 获取用户自定义函数
        udfEntities.addAll(getDataflowUDFFunctions());

        scala.Tuple3 tuple3 = BkSparkUdf.register(udfEntities);
        // 注册Spark ScalaUDF函数
        FunctionRegistry functionRegistry = (FunctionRegistry) tuple3._1();
        // 注册函数(Hive)
        Seq<CatalogFunction> hiveUdfFunctions = (Seq<CatalogFunction>) tuple3._2();
        // 动态生成的hive函数类
        ClassLoader hiveUdfClassLoader = getHiveUdfClassLoader(tuple3);

        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        LogicalPlan logicalPlan = null;
        try {
            Thread.currentThread().setContextClassLoader(hiveUdfClassLoader);
            logicalPlan = BkSparkSqlAnalyzer.parserSQL(sql,
                    tables,
                    functionRegistry,
                    hiveUdfFunctions);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }

        SparkSqlTableEntity.Builder tableEntityBuilder = SparkSqlTableEntity.builder()
                .id(outputTableName)
                .name(outputTableName)
                .sql(sql);
        Iterable<SparkSqlTableEntity.Field> fields = JavaConverters.asJavaIterableConverter(
                BkSparkSqlAnalyzer.getOutFields(logicalPlan)).asJava();
        for (SparkSqlTableEntity.Field field : fields) {
            if (nameToAlias.containsKey(field.getField())) {
                String tmpAlias = nameToAlias.get(field.getField());
                if (tmpAlias != null) {
                    field.setAlias(tmpAlias);
                }
            }

            if (selfDependencyParser != null && selfDependencyParser.isReady()) {
                String tmpAlias = selfDependencyParser.getColumnAlias(field.getField());
                if (tmpAlias != null) {
                    field.setAlias(tmpAlias);
                }
            }
            tableEntityBuilder.addField(field);
        }

        SparkSqlTableEntity tableEntity = tableEntityBuilder.create();
        if (selfDependencyParser != null && selfDependencyParser.isReady()) {
            checkBKSQLFieldEquality(selfDependencyParser.getFields(), tableEntity.getFields());
        }
        results.add(tableEntity);
    }

    private void checkIfTableIsUsed() {
        for (String tabName : tableRegistry.keySet()) {
            if (!sqlUsedTables.containsKey(tabName.toLowerCase())
                    && !SparkSqlIpLibOptimizer.USED_IP_LIB_HASH_SET.get().contains(tabName.toLowerCase())) {
                if (selfDependencyTableName != null && tabName.toLowerCase()
                        .equals(selfDependencyTableName.toLowerCase())) {
                    throw new FailedOnDeParserException(
                            "self.table.no.used.error", new Object[]{moveNumericSuffixToStart(tabName)},
                            SparkSqlDeParser.class);
                }
                throw new FailedOnDeParserException(
                        "table.no.used.error", new Object[]{moveNumericSuffixToStart(tabName)}, SparkSqlDeParser.class);
            }
        }
    }

    private void checkBKSQLFieldEquality(
            List<SelfDependencyParser.SelfDependencyField> inputFields,
            List<SparkSqlTableEntity.Field> outputFields) {
        List<SelfDependencyParser.SelfDependencyField> copyInputFields =
                new ArrayList<SelfDependencyParser.SelfDependencyField>(inputFields);
        BlueKingDataTypeMapper dataTypeMapper = new BlueKingDataTypeMapper();
        for (SparkSqlTableEntity.Field outfield : outputFields) {
            boolean findField = false;
            for (SelfDependencyParser.SelfDependencyField inputField : copyInputFields) {
                if (outfield.getField().toLowerCase().equals(inputField.getFieldName().toLowerCase())) {
                    findField = true;
                    if (!outfield.getType().equals(
                            BkDataType$.MODULE$.dataType2String(
                                    BkDataType$.MODULE$.str2DataType(
                                            dataTypeMapper.toBKSqlType(
                                                    inputField.getFieldType()).name()), inputField.getFieldName()))) {
                        throw new FailedOnDeParserException(
                                "self.table.column.type.fault",
                                new Object[]{inputField.getFieldName(), inputField.getFieldType(), outfield.getType()},
                                SparkSqlDeParser.class);
                    }
                    copyInputFields.remove(inputField);
                    break;
                }
            }
            if (!findField) {
                throw new FailedOnDeParserException(
                        "self.table.column.input.no.find", new Object[]{outfield.getField()}, SparkSqlDeParser.class);
            }
        }

        if (copyInputFields.size() != 0) {
            StringBuilder missFieldBuilder = new StringBuilder();
            for (SelfDependencyParser.SelfDependencyField inputField : copyInputFields) {
                missFieldBuilder.append(inputField.getFieldName() + ",");
            }

            throw new FailedOnDeParserException(
                    "self.table.column.output.no.find", new Object[]{missFieldBuilder.toString()},
                    SparkSqlDeParser.class);
        }
    }

    private ClassLoader getHiveUdfClassLoader(Tuple3 tuple3) {
        ClassFile[] classFiles = (ClassFile[]) tuple3._3();
        Map<String, byte[]> udtfClass = new HashMap<>();
        SimpleCompiler compiler = new SimpleCompiler();
        for (ClassFile cf : classFiles) {
            udtfClass.put(cf.getThisClassName(), cf.toByteArray());
        }
        compiler.cook(udtfClass);
        return compiler.getClassLoader();
    }

    protected void registerTable(String tableName, List<ColumnMetadata> metadataList) {
        if (tableRegistry.containsKey(tableName)) {
            return;
        }
        // 是否添加保留字段
        addReservedFields(metadataList);

        CatalogTable table = BkSparkSqlAnalyzer.createTableDesc(tableName,
                JavaConverters.asScalaIteratorConverter(metadataList.iterator()).asScala().toSeq());
        tableRegistry.put(tableName, table);
    }

    private void addReservedFields(List<ColumnMetadata> metadataList) {
        // 添加保留字段
        HashMap<String, ColumnMetadata> reservedFields = new HashMap<>();
        reservedFields.put("dtEventTime".toLowerCase(),
                new ColumnMetadataImpl("dtEventTime", DataType.STRING, "", "dtEventTime"));
        reservedFields.put("dtEventTimeStamp".toLowerCase(),
                new ColumnMetadataImpl("dtEventTimeStamp", DataType.LONG, "", "dtEventTimeStamp"));
        reservedFields.put("localTime".toLowerCase(),
                new ColumnMetadataImpl("localTime", DataType.STRING, "", "localTime"));
        reservedFields.put("thedate".toLowerCase(),
                new ColumnMetadataImpl("thedate", DataType.INTEGER, "", "thedate"));
        for (ColumnMetadata columnMetadata : metadataList) {
            String name = columnMetadata.getColumnName().toLowerCase();
            if (reservedFields.containsKey(name)) {
                reservedFields.remove(name);
            }
        }
        metadataList.addAll(reservedFields.values());
    }

    private List<SparkSqlUdfEntity> getDataflowUDFFunctions() {
        List<SparkSqlUdfEntity> sqlUdfEntities = new ArrayList<>();
        String env = config.hasPath(PROPERTY_KEY_DATAFLOW_UDF_ENV) ? config.getString(PROPERTY_KEY_DATAFLOW_UDF_ENV) :
                PROPERTY_KEY_DATAFLOW_UDF_ENV_DEFAULT_VAULE;
        String functionName = null;
        if (config.hasPath(PROPERTY_KEY_DATAFLOW_UDF_FUNCTION_NAME)) {
            functionName = config.getString(PROPERTY_KEY_DATAFLOW_UDF_FUNCTION_NAME);
        }
        List<UdfMetadata> udfMetadatas = new ArrayList<>();
        if (functionName != null) {
            udfMetadatas = bkdataUdfMetadataConnector.fetchUdfMetaData(new UdfArgs(env, functionName));
        }
        for (UdfMetadata udfMetadata : udfMetadatas) {
            UdfParameterMetadata parameterMetadata = udfMetadata.getParameterMetadataList().get(0);
            String[] returnTypes =
                    parameterMetadata.getOutputTypes().toArray(new String[parameterMetadata.getOutputTypes().size()]);
            String[] inputTypes =
                    parameterMetadata.getInputTypes().toArray(new String[parameterMetadata.getInputTypes().size()]);
            int requiredArgNum = parameterMetadata.getInputTypes().size();
            SparkSqlUdfEntity.BkFuncType functionType =
                    SparkSqlUdfEntity.BkFuncType.valueOf(udfMetadata.getUdfType().toString());
            SparkSqlUdfEntity entity = new SparkSqlUdfEntity(
                    udfMetadata.getFunctionName(), returnTypes, inputTypes, requiredArgNum, functionType);
            sqlUdfEntities.add(entity);
        }
        return sqlUdfEntities;
    }

    private TableMetadata<ColumnMetadata> fetchGeneralMetadata(String tableName) {
        return generalTableMetadataConnector.fetchTableMetadata(bkTableIDConvention(tableName));
    }

    protected String bkTableIDConvention(String tableName) {
        return tableName;
    }

    /**
     * 移动业务ID放到表后面
     *
     * @param tableName
     * @return
     */
    protected String moveNumericPrefixToTail(String tableName) {
        if (NUMERIC_PREFIX_PATTERN.matcher(tableName).matches()) {
            int idx = tableName.indexOf("_");
            return tableName.substring(idx + 1) + "_" + tableName.substring(0, idx);
        }
        return tableName;
    }

    protected String moveNumericSuffixToStart(String tableName) {
        if (NUMERIC_SUFFIX_PATTERN.matcher(tableName).matches()) {
            int idx = tableName.lastIndexOf("_");
            return tableName.substring(idx + 1) + "_" + tableName.substring(0, idx);
        }
        return tableName;
    }


    @Override
    protected Object getRetObj() {
        return results;
    }
}
