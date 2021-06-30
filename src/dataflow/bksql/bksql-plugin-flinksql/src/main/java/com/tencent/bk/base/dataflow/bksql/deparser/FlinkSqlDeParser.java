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

import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserJoin.findFromMultiStaticTable;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserJoin.findJoinKeys;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserJoin.findStaticJoinKeys;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserJoin.getJoinType;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserJoin.isJoinContainsAlias;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserJoin.isJoinQuery;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserUdf.escape;
import static com.tencent.bk.base.dataflow.bksql.deparser.FlinkSqlDeParserUdf.getUDTFAliasName;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.ACCUMULATE_WINDOW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.GENERATE_STATIC_JOIN_TABLE_SUFFIX;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.HOP_END;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.HOP_START;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROCESSOR_TYPE_JOIN_TRANSFORM;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROCESSOR_TYPE_STATIC_JOIN_TRANSFORM;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_ALLOWED_LATENESS;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_BIZ_ID;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_COUNT_FREQ;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_EXPIRED_TIME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_IS_CURRENT_RT_NEW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_IS_REUSE_TIME_FIELD;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_LATENESS_COUNT_FREQ;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_LATENESS_TIME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_MODE;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_RESULT_TABLE_NAME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_SESSION_GAP;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_SOURCE_DATA;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_SYSTEM_FIELDS;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_WAITING_TIME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_WINDOW_LENGTH;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_KEY_WINDOW_TYPE;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_PREFIX;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_VALUE_MODE_AGGRESSIVE;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.PROPERTY_VALUE_MODE_TYPICAL;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.SESSION_END;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.SESSION_START;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.SESSION_WINDOW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.SLIDING_WINDOW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.SUBQUERY;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.TIME_ATTRIBUTE;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.TIME_ATTRIBUTE_OUTPUT_NAME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.TUMBLE_END;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.TUMBLE_START;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.TUMBLING_WINDOW;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME;
import static com.tencent.bk.base.dataflow.bksql.util.Constants.WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.tencent.blueking.bksql.deparser.SimpleListenerBasedDeParser;
import com.tencent.blueking.bksql.exception.FailedOnDeParserException;
import com.tencent.blueking.bksql.function.udf.BkdataUdfMetadataConnector;
import com.tencent.blueking.bksql.rest.error.LocaleHolder;
import com.tencent.blueking.bksql.table.BlueKingStaticTableMetadataConnector;
import com.tencent.blueking.bksql.table.BlueKingTrtTableMetadataConnector;
import com.tencent.blueking.bksql.table.ColumnMetadata;
import com.tencent.blueking.bksql.table.ColumnMetadataImpl;
import com.tencent.blueking.bksql.table.TableMetadata;
import com.tencent.blueking.bksql.util.AggregationExpressionDetector;
import com.tencent.blueking.bksql.util.BaseASTreeVisitor;
import com.tencent.blueking.bksql.util.BlueKingDataTypeMapper;
import com.tencent.bk.base.dataflow.bksql.util.ColumnWrapper;
import com.tencent.blueking.bksql.util.DataType;
import com.tencent.bk.base.dataflow.bksql.util.FlinkDataTypeMapper;
import com.tencent.bk.base.dataflow.bksql.util.GeneratedSchema;
import com.tencent.blueking.bksql.util.QuoteUtil;
import com.tencent.blueking.bksql.util.SimpleListener;
import com.tencent.bk.base.dataflow.bksql.util.TableEntity;
import com.tencent.bk.base.dataflow.bksql.util.TableEntity.Field;
import com.tencent.bk.base.dataflow.flink.streaming.function.base.FlinkFunctionFactory;
import com.typesafe.config.Config;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.calcite.runtime.Resources;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * todo list:
 * 1. i18n
 * 2. output table name settings
 * 3. window settings
 * 4. other settings
 * 5. more unit tests
 */
public class FlinkSqlDeParser extends SimpleListenerBasedDeParser {

    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("(\\d+)_(\\w+)");
    // flink sql的常量时间字段，如 LOCALTIMESTAMP
    private static final Set<String> FLINK_TIMESTAMP_CONSTANT_COLUMNS = Stream.of("LOCALTIMESTAMP")
            .collect(Collectors.toSet());

    protected final String bizID;
    protected final String outputTableName;
    protected final Config config;
    private final List<String> sourceData;
    private final BlueKingTrtTableMetadataConnector generalTableMetadataConnector;
    private final BlueKingStaticTableMetadataConnector staticTableMetadataConnector;
    private final BkdataUdfMetadataConnector udfMetadataConnector;
    private final StreamExecutionEnvironment env;
    private final Map<String, GeneratedSchema> tableRegistry = new HashMap<>();
    private final List<TableEntity> tableEntities = new ArrayList<>();
    private final ObjectMapper jsonMapper = new ObjectMapper();
    private final BlueKingDataTypeMapper bkDataTypeMapper = new BlueKingDataTypeMapper();
    private final FlinkDataTypeMapper flinkDataTypeMapper = new FlinkDataTypeMapper();
    private final Set<String> defaultNonOutputColumns;
    private final Set<String> defaultNonLoadedColumns;
    private final List<ColumnMetadata> defaultLoadedColumns;
    private final List<String> defaultSelectedColumns;
    private final List<ColumnMetadata> defaultJoinOutputColumns;
    private final AggregationExpressionDetector aggregationExpressionDetector;
    private final AtomicInteger nextID = new AtomicInteger(0);
    private final FlinkSqlDeParserUdf deParserUdf;
    private final FlinkSqlDeParserValidate validate;
    private Set<String> functions = new HashSet<>();
    private Set<String> aggregationCallSet;

    // cache table/field/column info for diff join clauses.
    private String cacheVirtualStaticJoinName = null;
    private List<Field> cacheJoinTableFields = null;
    private List<Field> cacheFactTableFields = null;
    private List<ColumnMetadata> cacheColumnMetas = null;
    private Map<Map.Entry<String, String>, String> cacheGeneratedColumnNames = new HashMap<>();

    /**
     * FlinkSQL解析类
     *
     * @param properties properties
     * @param trtTableMetadataUrlPattern trtTableMetadataUrlPattern
     * @param staticTableMetadataUrlPattern staticTableMetadataUrlPattern
     * @param udfMetadataUrlPattern udfMetadataUrlPattern
     * @param aggregationCallRefs aggregationCallRefs
     */
    @JsonCreator
    public FlinkSqlDeParser(
            @JacksonInject("properties")
                    Config properties,
            @JsonProperty(value = "trtTableMetadataUrlPattern", required = true)
                    String trtTableMetadataUrlPattern,
            @JsonProperty(value = "staticTableMetadataUrlPattern", required = true)
                    String staticTableMetadataUrlPattern,
            @JsonProperty(value = "udfMetadataUrlPattern", required = true)
                    String udfMetadataUrlPattern,
            @JsonProperty(value = "aggregationCallRefs", required = true)
                    List<String> aggregationCallRefs
    ) {
        config = properties.getConfig(PROPERTY_PREFIX);
        generalTableMetadataConnector = BlueKingTrtTableMetadataConnector.forUrl(trtTableMetadataUrlPattern);
        staticTableMetadataConnector = BlueKingStaticTableMetadataConnector.forUrl(staticTableMetadataUrlPattern);
        aggregationCallSet = new HashSet<>(aggregationCallRefs);

        udfMetadataConnector = BkdataUdfMetadataConnector.forUrl(udfMetadataUrlPattern);
        bizID = config.getString(PROPERTY_KEY_BIZ_ID);
        outputTableName = config.getString(PROPERTY_KEY_RESULT_TABLE_NAME);
        sourceData = config.hasPath(PROPERTY_KEY_SOURCE_DATA)
                ? config.getStringList(PROPERTY_KEY_SOURCE_DATA) : new ArrayList<>();
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        defaultNonOutputColumns = initDefaultNonOutputColumns();
        defaultNonLoadedColumns = initDefaultNonLoadedColumns();
        defaultLoadedColumns = initDefaultLoadedColumns();
        defaultSelectedColumns = initDefaultSelectedColumns();
        defaultJoinOutputColumns = initDefaultJoinOutputColumns();
        // init udf info
        deParserUdf = new FlinkSqlDeParserUdf().initUdfInfo(config, aggregationCallSet, udfMetadataConnector);
        // collect aggregation function
        aggregationExpressionDetector = new AggregationExpressionDetector(aggregationCallSet, true);
        validate = new FlinkSqlDeParserValidate();
    }

    public static String getColumnName(Column column, boolean trimQuotes) {
        if (trimQuotes) {
            return QuoteUtil.trimQuotes(column.getColumnName());
        }
        return column.getColumnName();
    }

    public static String getColumnName(Column column) {
        return getColumnName(column, true);
    }

    public static String getTableName(Table table) {
        return getTableName(table, false);
    }

    public static String getTableName(Table table, boolean nullable) {
        Preconditions.checkNotNull(table, "no table found");
        if (!nullable) {
            Preconditions.checkNotNull(table.getName(), "empty table name");
        }
        return table.getName();
    }

    public static String getAliasName(Table table) {
        Alias alias = table.getAlias();
        // 当两个表 join，表没有别名时，默认取表名为别名
        if (alias == null || alias.getName() == null) {
            return table.getName();
        }
        return alias.getName();
    }

    private Set<String> initDefaultNonOutputColumns() {
        return new HashSet<>(ImmutableList.of(
                TIME_ATTRIBUTE,
                "offset" // previous version compatible: the old offset column
        ));
    }

    private Set<String> initDefaultNonLoadedColumns() { // todo rename to non transferred ?
        Set<String> retVal = new HashSet<>();
        retVal.add(TIME_ATTRIBUTE);
        if (config.hasPath(PROPERTY_KEY_SYSTEM_FIELDS)) {
            List<? extends Config> defaultColumns = this.config.getConfigList(PROPERTY_KEY_SYSTEM_FIELDS);
            for (Config column : defaultColumns) {
                String name = column.getString("field");
                retVal.add(name);
            }
        }
        return retVal;
    }

    private List<ColumnMetadata> initDefaultLoadedColumns() {
        List<ColumnMetadata> list = new ArrayList<>();
        list.add(new ColumnMetadataImpl(TIME_ATTRIBUTE, DataType.TIME_INDICATOR));
        if (config.hasPath(PROPERTY_KEY_SYSTEM_FIELDS)) {
            List<? extends Config> defaultColumns = this.config.getConfigList(PROPERTY_KEY_SYSTEM_FIELDS);
            for (Config column : defaultColumns) {
                String name = column.getString("field");
                String type = column.getString("type");
                list.add(new ColumnMetadataImpl(name, bkDataTypeMapper.toBKSqlType(type)));
            }
        }
        return list;
    }

    private List<String> initDefaultSelectedColumns() {
        List<String> list = new ArrayList<>();
        if (config.hasPath(PROPERTY_KEY_SYSTEM_FIELDS)) {
            List<? extends Config> defaultColumns = this.config.getConfigList(PROPERTY_KEY_SYSTEM_FIELDS);
            for (Config column : defaultColumns) {
                String name = column.getString("field");
                list.add(name);
            }
        }
        return list;
    }

    private List<ColumnMetadata> initDefaultJoinOutputColumns() {
        List<ColumnMetadata> list = new ArrayList<>();
        if (config.hasPath(PROPERTY_KEY_SYSTEM_FIELDS)) {
            List<? extends Config> defaultColumns = this.config.getConfigList(PROPERTY_KEY_SYSTEM_FIELDS);
            for (Config column : defaultColumns) {
                String name = column.getString("field");
                String type = column.getString("type");
                list.add(new ColumnMetadataImpl(name, bkDataTypeMapper.toBKSqlType(type)));
            }
        }
        return list;
    }

    @Override
    public void enterNode(WithItem withItem) {
        super.enterNode(withItem);
        throw new IllegalArgumentException("illegal with clause");
    }

    @Override
    public void enterNode(SetOperationList setOpList) {
        super.enterNode(setOpList);
        throw new IllegalArgumentException("illegal set operation");
    }

    @Override
    public void enterNode(SubJoin subJoin) {
        super.enterNode(subJoin);
        throw new IllegalArgumentException("illegal sub join");
    }

    /**
     * Collection functions in the sql.
     *
     * @param function function
     */
    @Override
    public void enterNode(Function function) {
        super.enterNode(function);
        functions.add(function.getName().toLowerCase());
    }

    @Override
    public void exitNode(Select select) {
        super.exitNode(select);
        if (select.getSelectBody() instanceof PlainSelect) {
            makeEntity((PlainSelect) select.getSelectBody(), globalOutputTableName());
        } else {
            throw new UnsupportedOperationException(select.getSelectBody().getClass().toString());
        }
    }

    @Override
    public void exitNode(PlainSelect select) {
        super.exitNode(select);
        // each of mentioned tables is supposed to be a physical table
        registerPhysicalTables(select);
        checkParseMode(select);
        if (isJoinQuery(select)) {
            // 校验静态关联是否含有窗口
            validate.checkIsLegalStaticWindowType(sourceData, config);
            onJoin(select);
        } else {
            // 校验窗口类型与SQL是否匹配
            validate.checkIsLegalWindowType(select, sourceData, config, aggregationExpressionDetector);
        }

        if (isAggregationQuery(select)) {
            onGroupBy(select);
        } else {
            // add default selected columns
            if (noSelectAllIncluded(select)) {
                reloadItems(select);
            }
        }
    }

    private void reloadItems(PlainSelect select) {
        List<SelectItem> defaultItems = defaultSelectedColumns.stream()
                .map(c -> new SelectExpressionItem(new Column(c)))
                .collect(Collectors.toList());
        SelectExpressionItem startTimeItem;
        SelectExpressionItem endTimeItem;
        // multi-table select should generate new start and end time
        if (isReuseTimeField() && !validate.isHasMoreStreamTable(sourceData, config)) {
            startTimeItem = new SelectExpressionItem(new Column(WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME));
            endTimeItem = new SelectExpressionItem(new Column(WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME));
        } else {
            startTimeItem = new SelectExpressionItem(warpUtcFunction(new Column(TIME_ATTRIBUTE_OUTPUT_NAME)));
            endTimeItem = new SelectExpressionItem(warpUtcFunction(new Column(TIME_ATTRIBUTE_OUTPUT_NAME)));
            startTimeItem.setAlias(new Alias(WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME, true));
            endTimeItem.setAlias(new Alias(WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME, true));
        }
        List<SelectItem> newSelectItems = new ArrayList<>();
        newSelectItems.addAll(defaultItems);
        if (isNewCurrentTable()) {
            newSelectItems.add(startTimeItem);
            newSelectItems.add(endTimeItem);
        }
        newSelectItems.addAll(select.getSelectItems());
        select.setSelectItems(newSelectItems);
    }

    private Function warpUtcFunction(Expression expression) {
        Function func = new Function();
        func.setName("utc_to_local");
        func.setParameters(new ExpressionList(Arrays.asList(expression)));
        return func;
    }

    private boolean isAggregationQuery(PlainSelect select) {
        if (CollectionUtils.isNotEmpty(select.getGroupByColumnReferences())) {
            return true;
        }
        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SelectExpressionItem) {
                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                if (aggregationExpressionDetector.isAggregationExpression(expression)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isNewCurrentTable() {
        if (config.hasPath(PROPERTY_KEY_IS_CURRENT_RT_NEW)
                && config.getBoolean(PROPERTY_KEY_IS_CURRENT_RT_NEW)) {
            return true;
        }
        return false;
    }

    private boolean isReuseTimeField() {
        if (config.hasPath(PROPERTY_KEY_IS_REUSE_TIME_FIELD)
                && config.getBoolean(PROPERTY_KEY_IS_REUSE_TIME_FIELD)) {
            return true;
        }
        return false;
    }

    private boolean noSelectAllIncluded(PlainSelect select) {
        for (SelectItem selectItem : select.getSelectItems()) {
            if (selectItem instanceof AllColumns) {
                return false;
            }
        }
        return true;
    }

    private void checkParseMode(PlainSelect select) {
        if (config.hasPath(PROPERTY_KEY_MODE)) {
            String mode = config.getString(PROPERTY_KEY_MODE);
            switch (mode) {
                case PROPERTY_VALUE_MODE_AGGRESSIVE:
                    flatten(select);
                    break;
                case PROPERTY_VALUE_MODE_TYPICAL:
                    break;
                default:
                    throw new IllegalArgumentException("illegal parse mode: " + mode);
            }
        }
    }


    private void onGroupBy(PlainSelect select) {
        Function selFunc = new Function();
        Function startTimeSelFunc = new Function();
        Function endTimeSelFunc = new Function();
        Function groupByFunc = new Function();
        String windowType = config.getString(PROPERTY_KEY_WINDOW_TYPE);
        int countFreq = config.getInt(PROPERTY_KEY_COUNT_FREQ);
        // 根据不同的窗口类型增加窗口开始结束时间及dtEventTime字段
        wrapFunctionByWindowType(selFunc, startTimeSelFunc, endTimeSelFunc, groupByFunc, windowType, countFreq);
        List<SelectItem> newSelectItems = new ArrayList<>();
        SelectExpressionItem sei = new SelectExpressionItem();
        sei.setExpression(selFunc);
        sei.setAlias(new Alias(TIME_ATTRIBUTE_OUTPUT_NAME, true));
        newSelectItems.add(sei);
        //add startTime/endTime field
        if (isNewCurrentTable()) {
            // set startTime
            SelectExpressionItem startTimeSei = new SelectExpressionItem();
            startTimeSei.setExpression(warpUtcFunction(startTimeSelFunc));
            startTimeSei.setAlias(new Alias(WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME, true));
            newSelectItems.add(startTimeSei);
            // set endTime
            SelectExpressionItem endTimeSei = new SelectExpressionItem();
            endTimeSei.setExpression(warpUtcFunction(endTimeSelFunc));
            endTimeSei.setAlias(new Alias(WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME, true));
            newSelectItems.add(endTimeSei);
        }
        newSelectItems.addAll(select.getSelectItems());
        select.setSelectItems(newSelectItems);

        List<Expression> groupByColumnReferences = select.getGroupByColumnReferences();
        List<Expression> newGroupByColumnReferences = new ArrayList<>();
        newGroupByColumnReferences.add(groupByFunc);
        if (CollectionUtils.isNotEmpty(groupByColumnReferences)) {
            // handle non-group by case (null group by refs)
            newGroupByColumnReferences.addAll(groupByColumnReferences);
        }
        select.setGroupByColumnReferences(newGroupByColumnReferences);
    }

    /**
     * 根据不同的窗口类型增加窗口开始结束时间及dtEventTime字段
     *
     * @param selFunc
     * @param startTimeSelFunc
     * @param endTimeSelFunc
     * @param groupByFunc
     * @param windowType
     * @param countFreq
     */
    private void wrapFunctionByWindowType(Function selFunc,
                                          Function startTimeSelFunc,
                                          Function endTimeSelFunc,
                                          Function groupByFunc,
                                          String windowType,
                                          int countFreq) {
        switch (windowType) {
            case TUMBLING_WINDOW: {
                selFunc.setName("TUMBLE_START");
                selFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(countFreq)
                )));
                // 增加窗口开始/结束时间
                addSelStartOrEndFunc(startTimeSelFunc, TUMBLE_START, countFreq, 0);
                addSelStartOrEndFunc(endTimeSelFunc, TUMBLE_END, countFreq, 0);
                groupByFunc.setName("TUMBLE");
                groupByFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(countFreq)
                )));
                break;
            }
            case SLIDING_WINDOW: {
                int windowLength = config.getInt(PROPERTY_KEY_WINDOW_LENGTH);
                selFunc.setName("HOP_START");
                selFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(countFreq),
                        toIntervalExpression(windowLength)
                )));
                // 增加窗口开始/结束时间
                addSelStartOrEndFunc(startTimeSelFunc, HOP_START, countFreq, windowLength);
                addSelStartOrEndFunc(endTimeSelFunc, HOP_END, countFreq, windowLength);
                groupByFunc.setName("HOP");
                groupByFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(countFreq),
                        toIntervalExpression(windowLength)
                )));
                break;
            }
            case ACCUMULATE_WINDOW: {
                int windowLength = config.getInt(PROPERTY_KEY_WINDOW_LENGTH);
                selFunc.setName("HOP_END");
                selFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(countFreq),
                        toIntervalExpression(windowLength)
                )));
                // 增加窗口开始/结束时间
                addSelStartOrEndFunc(startTimeSelFunc, HOP_START, countFreq, windowLength);
                addSelStartOrEndFunc(endTimeSelFunc, HOP_END, countFreq, windowLength);
                groupByFunc.setName("HOP");
                groupByFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(countFreq),
                        toIntervalExpression(windowLength)
                )));
                break;
            }
            case SESSION_WINDOW: {
                int sessionGap = config.getInt(PROPERTY_KEY_SESSION_GAP);
                selFunc.setName("SESSION_END");
                selFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(sessionGap)
                )));
                // 增加窗口开始/结束时间
                addSelStartOrEndFunc(startTimeSelFunc, SESSION_START, sessionGap, 0);
                addSelStartOrEndFunc(endTimeSelFunc, SESSION_END, sessionGap, 0);
                groupByFunc.setName("SESSION");
                groupByFunc.setParameters(new ExpressionList(Arrays.asList(
                        new Column(TIME_ATTRIBUTE),
                        toIntervalExpression(sessionGap)
                )));
                break;
            }
            default:
                throw new IllegalArgumentException("unrecognizable window type: " + windowType);
        }
    }

    /**
     * 增加开始时间和结束时间
     *
     * @param selFunc
     * @param windowFuncName
     * @param countFreq
     * @param windowLength
     * @return
     */
    private Function addSelStartOrEndFunc(Function selFunc, String windowFuncName, int countFreq, int windowLength) {
        List<Expression> expressions;
        if (windowLength == 0) {
            expressions = Arrays.asList(new Column(TIME_ATTRIBUTE), toIntervalExpression(countFreq));
        } else {
            expressions = Arrays.asList(
                    new Column(TIME_ATTRIBUTE),
                    toIntervalExpression(countFreq),
                    toIntervalExpression(windowLength));
        }
        selFunc.setName(windowFuncName);
        selFunc.setParameters(new ExpressionList(expressions));
        return selFunc;
    }

    /**
     * The main LRD traversal process of parse tree.
     * <p/>
     * reference:
     * <p>
     * SELECT: a select clause
     * JOIN: a join clause (with exact 2 from items)
     * TABLE: a physical table item
     * RTABLE: an intermediate virtual table, which has been registered to flink env
     * </p>
     * <p>
     * 1.
     * ROOT
     * /
     * SELECT
     * /          \
     * JOIN        SELECT
     * /      \         \
     * SELECT    SELECT   TABLE
     * /         \
     * TABLE        TABLE
     * </p>
     * generated entities:
     * (empty)
     * <p>
     * 2.
     * ROOT
     * /
     * SELECT
     * /          \
     * JOIN        SELECT
     * /      \         \
     * => SELECT    SELECT   TABLE
     * /         \
     * RTABLE        TABLE
     * </p>
     * generated entities:
     * (empty)
     * <p/>
     * <p>
     * 3.
     * ROOT
     * /
     * SELECT
     * /          \
     * JOIN        SELECT
     * /      \         \
     * SELECT => SELECT   TABLE
     * /         \
     * RTABLE        RTABLE
     * </p>
     * generated entities:
     * (empty)
     * <p/>
     * <p>
     * 4.
     * ROOT
     * /
     * SELECT
     * /          \
     * => JOIN        SELECT
     * /      \         \
     * RTABLE    RTABLE   TABLE
     * </p>
     * generated entities:
     * 1. common_transform
     * 2. common_transform
     * <p/>
     * <p>
     * 5.
     * ROOT
     * /
     * SELECT
     * /          \
     * => SELECT     SELECT
     * /                \
     * RTABLE             TABLE
     * </p>
     * generated entities:
     * 1. common_transform
     * 2. common_transform
     * 3. join_transform
     * <p/>
     * <p>
     * 6.
     * ROOT
     * /
     * SELECT
     * /          \
     * SELECT  => SELECT
     * /                \
     * RTABLE             RTABLE
     * </p>
     * generated entities:
     * 1. common_transform
     * 2. common_transform
     * 3. join_transform
     * <p/>
     * <p>
     * 7.
     * => ROOT
     * /
     * RTABLE
     * </p>
     * generated entities:
     * 1. common_transform
     * 2. common_transform
     * 3. join_transform
     * 4. common_transform
     *
     * @param select node of plain select
     */
    private void onJoin(PlainSelect select) {
        validate.checkJoinClauses(select, config);
        flatten(select);
        if (isWindowJoin(select)) {
            doWindowJoin(select);
        } else {
            doStaticJoin(select);
        }
    }

    /**
     * 判断是否是含窗口的两个事实表关联
     *
     * @param select node of plain select
     * @return true 是含窗口的两个事实表关联
     */
    private boolean isWindowJoin(PlainSelect select) {
        FromItem fromItem = select.getFromItem();
        if (!(fromItem instanceof Table)) {
            throw new IllegalStateException("from item is supposed to be replaced yet: " + fromItem);
        }
        Table tableLeft = (Table) fromItem;
        Join join = select.getJoins().get(0);
        Table tableRight = (Table) join.getRightItem();
        String leftTableName = getTableName(tableLeft);
        String rightTableName = getTableName(tableRight);
        boolean isfactJoin = (!validate.isStaticTable(leftTableName, config)
                && !validate.isStaticTable(rightTableName, config));
        boolean isOneJoin = (select.getJoins().size() == 1);
        if (isfactJoin && isOneJoin) {
            return true;
        }
        return false;
    }

    /**
     * 执行含有窗口的
     *
     * @param select node of plain select
     */
    private void doWindowJoin(PlainSelect select) {
        FromItem fromItem = select.getFromItem();
        Table tableLeft = (Table) fromItem;
        Join join = select.getJoins().get(0);
        Table tableRight = (Table) join.getRightItem();
        final String leftTableName = getTableName(tableLeft);
        final String leftAliasName = getAliasName(tableLeft);
        final String rightTableName = getTableName(tableRight);
        final String rightAliasName = getAliasName(tableRight);
        Expression onExpression = join.getOnExpression();
        String joinProcessorArgs = generateJoinProcessorArgs(getJoinType(join), onExpression,
                leftTableName, leftAliasName, rightTableName, rightAliasName);
        String joinedTableName = generateTableName(globalOutputTableName(), "Join");
        String configWindowType = config.getString(PROPERTY_KEY_WINDOW_TYPE);
        String windowType = config.hasPath(PROPERTY_KEY_WINDOW_TYPE) ? configWindowType : null;
        if (!"tumbling".equalsIgnoreCase(windowType)) {
            // 流关联只支持滚动窗口
            throw new FailedOnDeParserException("only.tumbling.window.for.stream.join.error",
                    new Object[0], FlinkSqlDeParser.class);
        }
        TableEntity.Builder builder = TableEntity.builder();
        addWindowJoinBasicInfo(builder, joinedTableName, joinProcessorArgs, leftTableName, rightTableName);
        addWindowSettings(builder);
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        Map<Map.Entry<String, String>, String> generatedColumnNames = new HashMap<>();
        addSystemColumns(builder);
        defaultLoadedColumns.add(new ColumnMetadataImpl("_startTime_", DataType.STRING));
        defaultLoadedColumns.add(new ColumnMetadataImpl("_endTime_", DataType.STRING));
        for (ColumnWrapper columnWrapper : collectColumns(select)) {
            Column column = columnWrapper.getColumn();
            String fromTableName = findFromTable(leftTableName, leftAliasName, rightTableName, rightAliasName, column);
            GeneratedSchema tableSchema = tableRegistry.get(fromTableName);
            if (null == tableSchema) {
                throw new FailedOnDeParserException("table.not.found.error",
                        new Object[]{fromTableName}, FlinkSqlDeParser.class);
            }
            Map<String, ColumnMetadata> schemaMap = tableSchema.asMap();
            ColumnMetadata schema = schemaMap.get(getColumnName(column));
            if (null == schema) {
                validate.checkColumnTypo(schemaMap.keySet(), column);
                throw new FailedOnDeParserException("column.not.found.error",
                        new Object[]{getColumnName(column)}, FlinkSqlDeParser.class);
            }
            Preconditions.checkNotNull(schema, "schema not found for column: " + getColumnName(column));
            if (generatedColumnNames
                    .containsKey(new AbstractMap.SimpleEntry<>(fromTableName, getColumnName(column)))) {
                continue;
            }
            String replacedColumnName = generateColumnName(column, fromTableName);
            generatedColumnNames.put(new AbstractMap.SimpleEntry<>(fromTableName,
                            getColumnName(column)),
                    replacedColumnName);
            builder.addField(new TableEntity.Field(
                    replacedColumnName,
                    bkDataTypeMapper.toExternalType(schema.getDataType()),
                    bkTableIDConvention(fromTableName) + ":" + getColumnName(column),
                    "",
                    false
            ));
            columnMetadataList.add(new ColumnMetadataImpl(replacedColumnName,
                    schema.getDataType(),
                    schema.getDescription()));
        }
        tableEntities.add(builder.create());
        registerTable(joinedTableName, columnMetadataList); // register the joined table.

        // change from selecting original table to selecting joined table
        replaceColumnToGenName(select, leftTableName, leftAliasName, rightTableName,
                rightAliasName, generatedColumnNames);
        select.setJoins(null);
        select.setFromItem(new Table(joinedTableName));
    }

    /**
     * 遍历替换列名为新生成的列名
     *
     * @param select
     * @param leftTableName
     * @param leftAliasName
     * @param rightTableName
     * @param rightAliasName
     * @param generatedColumnNames
     */
    private void replaceColumnToGenName(PlainSelect select,
                                        String leftTableName,
                                        String leftAliasName,
                                        String rightTableName,
                                        String rightAliasName,
                                        Map<Map.Entry<String, String>, String> generatedColumnNames) {
        select.accept(new BaseASTreeVisitor(new SimpleListener() {
            @Override
            public void enterNode(PlainSelect plainSelect) {
                super.enterNode(plainSelect);
                List<SelectItem> selectItems = plainSelect.getSelectItems();
                for (SelectItem selectItem : selectItems) {
                    if (selectItem instanceof SelectExpressionItem) {
                        Expression expr = ((SelectExpressionItem) selectItem).getExpression();
                        if (expr instanceof Column) {
                            if (((SelectExpressionItem) selectItem).getAlias() == null) {
                                ((SelectExpressionItem) selectItem)
                                        .setAlias(new Alias(getColumnName((Column) expr, false)));
                            }
                        }
                    }
                }
            }

            @Override
            public void enterNode(Column tableColumn) {
                super.enterNode(tableColumn);
                // 当字段是 flink sql 常量时间变量时，不进行字段转换
                if (!FLINK_TIMESTAMP_CONSTANT_COLUMNS.contains(tableColumn.getColumnName().toUpperCase())) {
                    String fromTableName = findFromTable(leftTableName,
                            leftAliasName,
                            rightTableName,
                            rightAliasName,
                            tableColumn);
                    tableColumn.setTable(null);
                    String generatedColumnName = generatedColumnNames
                            .get(new AbstractMap.SimpleEntry<>(fromTableName, getColumnName(tableColumn)));
                    Preconditions.checkNotNull(generatedColumnName,
                            "missing generated column: " + generatedColumnName);
                    tableColumn.setColumnName(generatedColumnName);
                }
            }
        }));
    }

    /**
     * 执行静态关联
     *
     * 超过1个以上的多Join语句必须右表都是静态表
     * 处理多维表步骤：
     * 第1个JOIN操作：
     * 1. 将SELECT中的（左侧事实表字段、本次JOIN的右侧维度表字段）放入fields
     * 2. 将ON中的（事实表字段、本次JOIN的右侧维度表字段）放入fields
     * 3. 生成的field list 缓存下给下一个JOIN使用
     *
     * 第2～N个JOIN操作：
     * 1. 将SELECT中的（本次JOIN的右侧维度表字段）放入fields
     * 2. 将ON中的    （本次JOIN的右侧维度表字段）放入fields
     * 3. 替换fields中origin属性值前缀stream（如：topic）/static表(如：static_join_transform)名称为
     * 上个JOIN产生的虚拟表名称(如：xxx_FlinkSqlStaticJoin_1)
     * 4. 将processor_args中的join_keys里的stream使用的字段替换成fields里新的field字段
     * 2. 生成的field list 缓存下给下一个JOIN使用
     *
     * 第N个JOIN操作之后：
     * 3. 产生common_transform
     *
     * @param select
     */
    private void doStaticJoin(PlainSelect select) {
        List<Join> joins = select.getJoins();
        // all right static table Map（tableName->tableAlias）
        Map<String, String> staticTableAliaMap = joins.stream()
                .map(join -> (Table) join.getRightItem())
                .collect(Collectors.toMap(table ->
                        getTableName(table), table -> getAliasName(table), (e1, e2) -> e2));

        pushDownOnExpression(select);
        Join join = joins.get(0);
        //第1个JOIN操作
        doMajorJoin(select, join, staticTableAliaMap);
        //第2～N个JOIN操作
        doMultiJoin(select, joins, staticTableAliaMap);
        //第N个JOIN操作之后生成common_transfrom
        changeOriginSelect(select,
                staticTableAliaMap,
                this.cacheGeneratedColumnNames,
                this.cacheVirtualStaticJoinName);
    }

    /**
     * 调整 ON 语句的位置，将写在最后一个 JOIN 末尾的 ON 语句内多个 AND 连接的表达式下推到各个 JOIN 语句中
     *
     * @param select
     */
    private void pushDownOnExpression(PlainSelect select) {
        FromItem fromItem = select.getFromItem();
        String factAliasName = getAliasName((Table) fromItem);

        List<Join> joins = select.getJoins();
        List<Join> joinOnClauses = joins
                .stream()
                .filter(join -> join.getOnExpression() != null)
                .collect(Collectors.toList());

        if (joins.size() == joinOnClauses.size()) {
            return;
        }
        // 抽取ON语句中全部的AND表达式
        List<EqualsTo> equals = Lists.newArrayList();
        joinOnClauses.forEach(join -> {
            Expression onExpression = join.getOnExpression();
            extraEqualExpression(onExpression, equals);
        });
        // AND下推到各个JOIN中
        joins.forEach(join -> {
            Table rightTable = (Table) (join.getRightItem());
            String staticAliasName = getAliasName(rightTable);
            List<Expression> equalsToListTemp = equals.stream()
                    .filter(equalsTo -> isJoinContainsAlias(factAliasName, staticAliasName, equalsTo))
                    .collect(Collectors.toList());
            if (equalsToListTemp.size() == 1) {
                join.setOnExpression(equalsToListTemp.get(0));
            } else if (equalsToListTemp.size() > 1) {
                Expression finalAndExpression = equalsToListTemp.stream()
                        .reduce((left, right) -> new AndExpression(left, right))
                        .get();
                join.setOnExpression(finalAndExpression);
            }
        });
    }

    /**
     * 递归抽取ON语句中的Equal表达式
     *
     * @param expression
     * @param equals
     */
    private void extraEqualExpression(Expression expression, List<EqualsTo> equals) {
        if (expression instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) expression;
            Expression leftExpression = andExpression.getLeftExpression();
            Expression rightExpression = andExpression.getRightExpression();
            if (rightExpression instanceof EqualsTo) {
                equals.add((EqualsTo) rightExpression);
            }
            extraEqualExpression(leftExpression, equals);
        } else if (expression instanceof EqualsTo) {
            equals.add((EqualsTo) expression);
        }
    }

    /**
     * 单独处理多维度表关联的第一个join操作
     *
     * @param select select clause
     * @param join first join of multi static join
     * @param staticTableAliaMap 静态表与静态表别名的映射
     */
    private void doMajorJoin(PlainSelect select, Join join, Map<String, String> staticTableAliaMap) {
        FromItem fromItem = select.getFromItem();
        // only one left fact table
        Table leftFactTable = (Table) fromItem;
        String factTableName = getTableName(leftFactTable);
        String factAliasName = getAliasName(leftFactTable);
        // first of multi right static table
        final Table tableRight = (Table) join.getRightItem();
        final String rightTableName = getTableName(tableRight);
        final String rightAliasName = getAliasName(tableRight);

        final Expression onExpression = join.getOnExpression();

        TableEntity.Builder builder = TableEntity.builder();
        // generate joined table name. eg: test_node_FlinkSqlStaticJoin_1
        String joinedTableName = generateTableName(globalOutputTableName(), GENERATE_STATIC_JOIN_TABLE_SUFFIX);
        String joinProcessorArgs = genStaticJoinProcessorArgs(getJoinType(join),
                onExpression, rightTableName, rightAliasName, factTableName, factAliasName);
        String parentTableName = bkTableIDConvention(factTableName);

        addStaticJoinBasicInfo(builder, joinedTableName, joinProcessorArgs, parentTableName);
        addSystemColumns(builder);
        addWindowSettings(builder);
        // 保存当前虚拟表供下个JOIN迭代使用
        this.cacheVirtualStaticJoinName = bkTableIDConvention(joinedTableName);

        this.cacheColumnMetas = new ArrayList<>();
        for (ColumnWrapper columnWrapper : collectColumns(select)) {
            Column column = columnWrapper.getColumn();
            String fromTableName = findFromMultiStaticTable(factTableName,
                    factAliasName,
                    staticTableAliaMap,
                    column,
                    tableRegistry,
                    validate);
            // 只将SELECT中的（左侧事实表字段、本次JOIN的右侧维度表字段）放入fields，不属于本次JOIN右表的SELECT字段跳过
            if (!fromTableName.equals(factTableName) && !fromTableName.equals(rightTableName)) {
                continue;
            }
            addStaticIntoField(fromTableName, column, builder);
        }
        TableEntity tableEntity = builder.create();
        tableEntities.add(tableEntity);
        //（第一次不用换）后续的JOIN里的 joinKeys 里的stream 的key 替换成为上一次的rt对应。即对于stream，第一次建立一个映射。原始字段-> 变后字段
        this.cacheFactTableFields = tableEntity.getFields();
        this.cacheJoinTableFields = tableEntity.getFields();

        List<ColumnMetadata> systemColumns = Lists.newArrayList();
        systemColumns.add(new ColumnMetadataImpl(WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME, DataType.STRING));
        systemColumns.add(new ColumnMetadataImpl(WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME, DataType.STRING));
        registerTable(joinedTableName, this.cacheColumnMetas, systemColumns); // register the joined table.
    }

    /**
     * 单独处理多维度表关联的第2~N个join操作
     *
     * @param select select clause
     * @param joins multi static joins
     * @param staticTableAliaMap 静态表与静态表别名的映射
     */
    private void doMultiJoin(PlainSelect select, List<Join> joins, Map<String, String> staticTableAliaMap) {
        FromItem fromItem = select.getFromItem();
        // only one left fact table
        Table leftFactTable = (Table) fromItem;
        String factTableName = getTableName(leftFactTable);
        String factAliasName = getAliasName(leftFactTable);

        for (int joinId = 1; joinId < joins.size(); joinId++) {
            Join join = joins.get(joinId);
            final Table tableRight = (Table) join.getRightItem();
            final String rightTableName = getTableName(tableRight);
            final String rightAliasName = getAliasName(tableRight);

            final Expression onExpression = join.getOnExpression();

            TableEntity.Builder builder = TableEntity.builder();
            // generate joined table name. eg: test_node_FlinkSqlStaticJoin_1
            String joinedTableName = generateTableName(globalOutputTableName(), GENERATE_STATIC_JOIN_TABLE_SUFFIX);
            String joinProcessorArgs = genMultiStaticJoinProcessorArgs(getJoinType(join),
                    onExpression, rightTableName, rightAliasName, factTableName, factAliasName);

            addStaticJoinBasicInfo(builder, joinedTableName, joinProcessorArgs, this.cacheVirtualStaticJoinName);
            addWindowSettings(builder);

            /*第二次JOIN使用上次缓存
            替换fields中origin属性值前缀stream（如：topic）/static表(如：static_join_transform)名称为
            上个JOIN产生的虚拟表名称(如：xxx_FlinkSqlStaticJoin_1)*/
            this.cacheJoinTableFields.stream().forEach(field -> {
                builder.addField(new TableEntity.Field(
                        field.getField(),
                        field.getType(),
                        this.cacheVirtualStaticJoinName + ":" + field.getField(),
                        "",
                        false
                ));
            });
            // 保存当前虚拟表供下个JOIN迭代使用
            this.cacheVirtualStaticJoinName = bkTableIDConvention(joinedTableName);

            for (ColumnWrapper columnWrapper : collectColumns(select)) {
                Column column = columnWrapper.getColumn();
                String fromTableName = findFromMultiStaticTable(factTableName,
                        factAliasName,
                        staticTableAliaMap,
                        column,
                        tableRegistry,
                        validate);
                // 将SELECT中的（本次JOIN的右侧维度表字段）放入fields
                if (!fromTableName.equals(rightTableName)) {
                    continue;
                }
                addStaticIntoField(fromTableName, column, builder);
            }
            TableEntity tableEntity = builder.create();
            tableEntities.add(tableEntity);
            //  生成的field缓存下给下
            this.cacheJoinTableFields = tableEntity.getFields();

            List<ColumnMetadata> systemColumns = Lists.newArrayList();
            systemColumns.add(new ColumnMetadataImpl(WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME, DataType.STRING));
            systemColumns.add(new ColumnMetadataImpl(WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME, DataType.STRING));
            registerTable(joinedTableName, this.cacheColumnMetas, systemColumns); // register the joined table.
        }
    }

    private void addStaticIntoField(String fromTableName, Column column, TableEntity.Builder builder) {
        GeneratedSchema tableSchema = tableRegistry.get(fromTableName);
        if (null == tableSchema) {
            throw new FailedOnDeParserException("table.not.found.error",
                    new Object[]{fromTableName}, FlinkSqlDeParser.class);
        }
        Map<String, ColumnMetadata> schemaMap = tableSchema.asMap();
        String columnName = getColumnName(column);
        ColumnMetadata schema = schemaMap.get(columnName);
        if (schema == null) {
            validate.checkColumnNameTypo(schemaMap.keySet(), columnName);
        }
        if (!this.cacheGeneratedColumnNames
                .containsKey(new AbstractMap.SimpleEntry<>(fromTableName, columnName))) {
            DataType dataType = schema.getDataType();
            String dataDesc = schema.getDescription();
            // generate col name. eg: col_123_test_node_col1_e7c03f
            String replacedColumnName = generateColumnName(column, fromTableName);
            addTableColumns(builder, replacedColumnName, dataType, fromTableName, columnName);
            this.cacheGeneratedColumnNames.put(new AbstractMap.SimpleEntry<>(fromTableName, columnName),
                    replacedColumnName);
            this.cacheColumnMetas.add(new ColumnMetadataImpl(replacedColumnName, dataType, dataDesc));
        }
    }

    /**
     * 最后一个JOIN操作之后生成common_transfrom
     *
     * @param select select clause
     * @param staticTableAliaMap 静态表与静态表别名的映射
     * @param generatedColumnNames 缓存的生成列
     * @param joinedTableName 缓存的生成表
     */
    private void changeOriginSelect(PlainSelect select, Map<String, String> staticTableAliaMap,
                                    Map<Map.Entry<String, String>, String> generatedColumnNames,
                                    String joinedTableName) {

        FromItem fromItem = select.getFromItem();
        // only one left fact table
        Table leftFactTable = (Table) fromItem;
        String factTableName = getTableName(leftFactTable);
        String factAliasName = getAliasName(leftFactTable);

        select.accept(new BaseASTreeVisitor(new SimpleListener() {
            @Override
            public void enterNode(PlainSelect plainSelect) {
                super.enterNode(plainSelect);
                List<SelectItem> selectItems = plainSelect.getSelectItems();
                for (SelectItem selectItem : selectItems) {
                    if (selectItem instanceof SelectExpressionItem) {
                        Expression expr = ((SelectExpressionItem) selectItem).getExpression();
                        if (expr instanceof Column) {
                            if (((SelectExpressionItem) selectItem).getAlias() == null) {
                                ((SelectExpressionItem) selectItem)
                                        .setAlias(new Alias(getColumnName((Column) expr, false)));
                            }
                        }
                    }
                }
            }

            @Override
            public void enterNode(Column tableColumn) {
                super.enterNode(tableColumn);
                // 当字段是 flink sql 常量时间变量时，不进行字段转换
                if (!FLINK_TIMESTAMP_CONSTANT_COLUMNS.contains(tableColumn.getColumnName().toUpperCase())) {
                    String fromTableName = findFromMultiStaticTable(factTableName,
                            factAliasName,
                            staticTableAliaMap,
                            tableColumn,
                            tableRegistry,
                            validate);
                    tableColumn.setTable(null);
                    String generatedColumnName = generatedColumnNames.get(
                            new AbstractMap.SimpleEntry<>(fromTableName, getColumnName(tableColumn)));
                    Preconditions.checkNotNull(generatedColumnName,
                            "missing generated column: " + generatedColumnName);
                    tableColumn.setColumnName(generatedColumnName);
                }
            }
        }));
        select.setJoins(null);
        select.setFromItem(new Table(joinedTableName));
    }


    private String findFromTable(String leftTableName,
                                 String leftAliasName,
                                 String rightTableName,
                                 String rightAliasName,
                                 Column column) {
        String fromTableName;
        String alias = getTableName(column.getTable(), true);
        if (alias == null) {
            GeneratedSchema leftSchema = tableRegistry.get(leftTableName);
            GeneratedSchema rightSchema = tableRegistry.get(rightTableName);
            if (!leftSchema.asMap().containsKey(getColumnName(column))
                    && !rightSchema.asMap().containsKey(getColumnName(column))) {
                validate.checkColumnTypo(leftSchema.asMap().keySet(), column);
                validate.checkColumnTypo(rightSchema.asMap().keySet(), column);
                throw new FailedOnDeParserException("column.not.found.error",
                        new Object[]{getColumnName(column)}, FlinkSqlDeParser.class);
            }
            if (leftSchema.asMap().containsKey(getColumnName(column))
                    && rightSchema.asMap().containsKey(getColumnName(column))) {
                throw new IllegalArgumentException("column " + getColumnName(column)
                        + " duplicated in table " + leftTableName
                        + " and table " + rightTableName + ", alias definition needed");
            }
            if (leftSchema.asMap().containsKey(getColumnName(column))) {
                return leftTableName;
            }
            return rightTableName;
        }
        if (alias.equalsIgnoreCase(leftAliasName)) {
            fromTableName = leftTableName;
        } else if (alias.equalsIgnoreCase(rightAliasName)) {
            fromTableName = rightTableName;
        } else {
            throw new IllegalArgumentException("illegal alias name: " + alias);
        }
        return fromTableName;
    }

    /**
     * 构造builder添加staticJoin基础信息(表名称\处理参数\父表)
     *
     * @param builder builder
     * @param joinedTableName JOIN表名称
     * @param joinProcessorArgs JOIN处理参数
     * @param parentTableName 父表名称
     */
    private void addStaticJoinBasicInfo(TableEntity.Builder builder,
                                        String joinedTableName,
                                        String joinProcessorArgs,
                                        String parentTableName) {
        builder.id(bkTableIDConvention(joinedTableName));
        builder.name(bkTableNameConvention(joinedTableName));
        builder.processor(new TableEntity.Processor(PROCESSOR_TYPE_STATIC_JOIN_TRANSFORM, joinProcessorArgs));
        // 设置parent为真实流表
        builder.addParent(parentTableName);
    }

    /**
     * 构造builder添加windowJoin基础信息(表名称\处理参数\父表)
     *
     * @param builder builder
     * @param joinedTableName JOIN表名称
     * @param joinProcessorArgs JOIN处理参数
     * @param leftParentTableName 第一个父表名称
     * @param rightParentTableName 第二个父表名称
     */
    private void addWindowJoinBasicInfo(TableEntity.Builder builder,
                                        String joinedTableName,
                                        String joinProcessorArgs,
                                        String leftParentTableName,
                                        String rightParentTableName) {
        builder.id(bkTableIDConvention(joinedTableName));
        builder.name(bkTableNameConvention(joinedTableName));
        builder.processor(new TableEntity.Processor(PROCESSOR_TYPE_JOIN_TRANSFORM, joinProcessorArgs));
        // 设置parent为真实流表
        builder.addParent(bkTableIDConvention(leftParentTableName));
        builder.addParent(bkTableIDConvention(rightParentTableName));
    }

    /**
     * 构造builder添加static字段信息
     *
     * @param builder builder
     * @param replacedColumnName replacedColumnName
     * @param dataType dataType
     * @param fromTableName fromTableName
     * @param columnName columnName
     */
    private void addTableColumns(TableEntity.Builder builder,
                                 String replacedColumnName,
                                 DataType dataType,
                                 String fromTableName,
                                 String columnName) {
        String prefix = validate.isStaticTable(fromTableName, config)
                ? PROCESSOR_TYPE_STATIC_JOIN_TRANSFORM : bkTableIDConvention(fromTableName);
        String origin = prefix + ":" + columnName;
        builder.addField(new TableEntity.Field(
                replacedColumnName,
                bkDataTypeMapper.toExternalType(dataType),
                origin,
                "",
                false
        ));
    }

    /**
     * 构造builder添加系统字段信息
     *
     * @param builder builder
     */
    private void addSystemColumns(TableEntity.Builder builder) {
        defaultJoinOutputColumns.stream().forEach(columnMeta -> {
            builder.addField(new TableEntity.Field(
                    columnMeta.getColumnName(),
                    bkDataTypeMapper.toExternalType(columnMeta.getDataType()),
                    "",
                    "",
                    false
            ));
        });
        //add extra system columns.
        builder.addField(new TableEntity.Field(
                WINDOW_START_TIME_ATTRIBUTE_OUTPUT_NAME,
                bkDataTypeMapper.toExternalType(DataType.STRING),
                "",
                "",
                false
        ));
        builder.addField(new TableEntity.Field(
                WINDOW_END_TIME_ATTRIBUTE_OUTPUT_NAME,
                bkDataTypeMapper.toExternalType(DataType.STRING),
                "",
                "",
                false
        ));
    }

    /**
     * 构造builder添加窗口配置信息
     *
     * @param builder
     */
    private void addWindowSettings(TableEntity.Builder builder) {
        TableEntity.Window.Builder windowBuilder = builder.window();
        Map<String, Object> configMap = config.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey(),
                        entry -> (entry.getValue().unwrapped()),
                        (e1, e2) -> e2));
        windowBuilder.type((String) configMap.getOrDefault(PROPERTY_KEY_WINDOW_TYPE, null));
        windowBuilder.countFreq(Integer.parseInt(configMap.getOrDefault(PROPERTY_KEY_COUNT_FREQ, "0").toString()));
        windowBuilder.waitingTime(Integer.parseInt(configMap.getOrDefault(PROPERTY_KEY_WAITING_TIME, "0").toString()));
        windowBuilder.length(Integer.parseInt(configMap.getOrDefault(PROPERTY_KEY_WINDOW_LENGTH, "0").toString()));
        windowBuilder.sessionGap(Integer.parseInt(configMap.getOrDefault(PROPERTY_KEY_SESSION_GAP, "0").toString()));
        windowBuilder.expiredTime(Integer.parseInt(configMap.getOrDefault(PROPERTY_KEY_EXPIRED_TIME, "0").toString()));

        boolean lateness = Boolean
                .parseBoolean(configMap.getOrDefault(PROPERTY_KEY_ALLOWED_LATENESS, "false").toString());
        windowBuilder.allowedLateness(lateness);
        int latenessTime = Integer
                .parseInt(configMap.getOrDefault(PROPERTY_KEY_LATENESS_TIME, "0").toString());
        windowBuilder.latenessTime(latenessTime);
        int latenessCountFreq = Integer
                .parseInt(configMap.getOrDefault(PROPERTY_KEY_LATENESS_COUNT_FREQ, "0").toString());
        windowBuilder.latenessCountFreq(latenessCountFreq);
        windowBuilder.create();
    }

    private IntervalExpression toIntervalExpression(long countFreq) {
        IntervalExpression intervalExpression = new IntervalExpression();
        if (countFreq > 60 && countFreq < 6000) {
            intervalExpression.setParameter(String.format("'%d'", Duration.ofSeconds(countFreq).toMinutes()));
            intervalExpression.setIntervalType("MINUTE");
        } else if (countFreq >= 6000) {
            intervalExpression.setParameter(String.format("'%d'", Duration.ofSeconds(countFreq).toHours()));
            intervalExpression.setIntervalType("HOUR");
        } else {
            intervalExpression.setParameter(String.format("'%d'", Duration.ofSeconds(countFreq).getSeconds()));
            intervalExpression.setIntervalType("SECOND");
        }
        return intervalExpression;
    }

    /**
     * Register physical tables appeared from from clauses or join clauses
     *
     * @param select node of plain select
     */
    private void registerPhysicalTables(PlainSelect select) {
        if (select.getFromItem() instanceof Table) {
            String tableName = ((Table) select.getFromItem()).getName();
            Preconditions.checkNotNull(tableName, "table name");
            validate.checkInputTable(tableName, sourceData);
            registerPhysicalTable(tableName);
        }
        if (isJoinQuery(select)) {
            validate.checkJoinClauses(select, config);
            // 支持多关联
            select.getJoins().stream()
                    .filter(join -> (join.getRightItem() instanceof Table))
                    .forEach(join -> {
                        String tableName = ((Table) join.getRightItem()).getName();
                        Preconditions.checkNotNull(tableName, "table name");
                        validate.checkInputTable(tableName, sourceData);
                        registerPhysicalTable(tableName);
                    });
        }
    }

    private Set<ColumnWrapper> collectColumns(PlainSelect select) {
        final Set<ColumnWrapper> columns = new HashSet<>();
        select.accept(new BaseASTreeVisitor(new SimpleListener() {
            @Override
            public void enterNode(Column tableColumn) {
                super.enterNode(tableColumn);
                // 当字段是 flink sql 常量时间变量时，不进行字段转换
                if (!FLINK_TIMESTAMP_CONSTANT_COLUMNS.contains(tableColumn.getColumnName().toUpperCase())) {
                    columns.add(new ColumnWrapper(tableColumn));
                }
            }
        }));
        return columns;
    }

    /**
     * Flatten: extract out and register table for every SubSelect children of the input node, as expected, the select
     * node itself is supposed to become none-nested and join-ready.
     * <p>
     * E.g. SELECT a.col FROM (SELECT * FROM tab) a =>  SELECT a.col
     * FROM __table_xxxxxx a | (where __table_xxxxxx is registered as A 'common_transform')
     * </p>
     * E.g. SELECT a.col FROM (SELECT * FROM tab_1) a join (SELECT * FROM tab_1) b
     * ON a.id = b.id => SELECT a.col FROM __table_xxxxxx a join __table_xxxxxx b ON a.id = b.id
     *
     * @param select node of plain select
     */
    private void flatten(PlainSelect select) {
        if (select.getFromItem() instanceof SubSelect) {
            PlainSelect subSelect = (PlainSelect) ((SubSelect) select.getFromItem()).getSelectBody();
            String tableName = generateTableName(globalOutputTableName(), SUBQUERY);
            makeEntity(subSelect, tableName, true);
            Table table = new Table(tableName);
            table.setAlias(select.getFromItem().getAlias());
            select.setFromItem(table);
        }

        if (isJoinQuery(select)) {
            validate.checkJoinClauses(select, config);
            // this is the only one join clause
            Join join = select.getJoins().get(0);
            if (join.getRightItem() instanceof SubSelect) {
                PlainSelect subSelect = (PlainSelect) ((SubSelect) join.getRightItem()).getSelectBody();
                String tableName = generateTableName(globalOutputTableName(), SUBQUERY);
                makeEntity(subSelect, tableName, true);
                Table table = new Table(tableName);
                table.setAlias(join.getRightItem().getAlias());
                join.setRightItem(table);
            }
        }
    }

    private void registerPhysicalTable(String tableName) {
        if (validate.isStaticTable(tableName, config)) {
            // fixme what if a table name belongs to a static table and a general table at the same time
            registerTable(tableName, fetchStaticMetadata(getStaticDataID(tableName)).listColumns());
        } else {
            registerTable(tableName, fetchGeneralMetadata(tableName).listColumns());
        }
    }

    /**
     * make an entity that does not contain any join clauses
     * query is not sub query by default
     *
     * @param select node of plain select
     * @param outputTableName output table name
     */
    private void makeEntity(PlainSelect select, String outputTableName) {
        makeEntity(select, outputTableName, false);
    }

    /**
     * make an entity that does not contain any join clauses
     *
     * @param select node of plain select
     * @param outputTableName output table name
     * @param isFlatten select is sub query
     */
    private void makeEntity(PlainSelect select, String outputTableName, boolean isFlatten) {
        final TableEntity.Builder builder = TableEntity.builder();
        builder.id(bkTableIDConvention(outputTableName));
        builder.name(bkTableNameConvention(outputTableName));
        String processorType;
        if (isAggregationQuery(select)) {
            processorType = "event_time_window";
        } else {
            processorType = "common_transform";
        }
        addWindowSettings(builder);
        final List<String> upstreamTableNames = findTableNames(select);
        for (String t : upstreamTableNames) {
            builder.addParent(bkTableIDConvention(t));
        }
        List<ColumnMetadata> metadataList = new ArrayList<>();
        String sql = prepareSql(select, outputTableName);
        builder.processor(new TableEntity.Processor(processorType, sql));
        // TableSchema schema = preExecuteSql(sql, upstreamTableNames, outputTableName);

        // 在非子查询的情况下，检查配置的实时数据源是否都被使用到
        if (!isFlatten) {
            validate.checkSourceTableUse(builder.getParents(), sourceData, config);
        }
        org.apache.flink.table.api.Table flinkTable = preExecuteSql(sql, upstreamTableNames, outputTableName);
        TableSchema schema = flinkTable.getSchema();
        for (int i = 0; i < schema.getFieldNames().length; i++) {
            String column = schema.getFieldNames()[i];
            if (!defaultNonOutputColumns.contains(column)) {
                // for entity output
                TypeInformation<?> type1 = schema.getFieldType(column).get();
                builder.addField(
                        new TableEntity.Field(column,
                                bkDataTypeMapper.toExternalType(flinkDataTypeMapper.toBKSqlType(type1)),
                                "", "", false));
            }
            // for intermediate flink env table register
            TypeInformation<?> type = schema.getFieldType(column).get();
            metadataList.add(new ColumnMetadataImpl(column, flinkDataTypeMapper.toBKSqlType(type)));
        }
        tableEntities.add(builder.create());
        registerTable(outputTableName, metadataList);
    }

    /**
     * 获取聚合结果列
     *
     * @param select
     * @return
     */
    private Map<Integer, Boolean> getAggFieldsMap(PlainSelect select) {
        List<SelectItem> selectItems = select.getSelectItems();
        Map<Integer, Boolean> aggFieldsMap = new HashMap<>();
        for (int i = 0; i < selectItems.size(); i++) {
            SelectItem selectItem = selectItems.get(i);
            if (selectItem instanceof SelectExpressionItem) {
                Expression expression = ((SelectExpressionItem) selectItem).getExpression();
                if (aggregationExpressionDetector.isAggregationExpression(expression)) {
                    aggFieldsMap.put(i, true);
                }
            }
        }
        return aggFieldsMap;
    }

    protected String bkTableNameConvention(String tableName) {
        Matcher matcher = TABLE_NAME_PATTERN.matcher(tableName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(tableName);
        }
        return matcher.group(2);
    }

    private String reverseTableName(String original) {
        Matcher matcher = TABLE_NAME_PATTERN.matcher(original);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(original);
        }
        String bizID = matcher.group(1);
        String simpleName = matcher.group(2);
        return simpleName + "_" + bizID;
    }

    private List<String> findTableNames(PlainSelect select) {
        final List<String> tableNames = new ArrayList<>();
        select.accept(new BaseASTreeVisitor(new SimpleListener() {

            @Override
            public void enterNode(PlainSelect plainSelect) {
                super.enterNode(plainSelect);
                if (plainSelect.getFromItem() instanceof Table) {
                    Table table = (Table) plainSelect.getFromItem();
                    tableNames.add(getTableName(table));
                }
            }
        }));
        return tableNames;
    }

    private String prepareSql(PlainSelect plainSelect, String outputTableName) {
        final java.util.function.Function<String, String> escape = FlinkSqlDeParserUdf::escape;
        ExpressionDeParser expressionVisitor = new ExpressionDeParser() {
            @Override
            public void visit(Column tableColumn) {
                // note: [select table.col from table] is not supported
                // note: use of [select a.col from table a] is recommended
                Column column = new Column(renameTable(
                        tableColumn.getTable(),
                        escape
                ), escape(getColumnName(tableColumn)));
                super.visit(column);
            }
        };
        SelectDeParser d = new SelectDeParser() {

            @Override
            public void visit(SelectExpressionItem selectExpressionItem) {

                if (selectExpressionItem.getAlias() != null) {
                    String name = selectExpressionItem.getAlias().getName();
                    selectExpressionItem.getAlias().setName(escape(name));
                }
                super.visit(selectExpressionItem);
            }

            @Override
            public void visit(Table tableName) {
                Table table = renameTable(
                        tableName,
                        escape.compose(n -> mixTableNames(n, outputTableName))
                );
                if (table != null) {
                    super.visit(table);
                }
            }

            /**
             * UDTF 中字段和别名增加 `
             * 如对 LATERAL TABLE(zip(log, '3')) AS x(re) 转变为 LATERAL TABLE(zip(`log`, '3')) AS x(`re`)
             *
             * @param tableFunction table function
             */
            @Override
            public void visit(net.sf.jsqlparser.statement.select.TableFunction tableFunction) {
                if (tableFunction.isLiteral()) {
                    for (Expression expression : tableFunction.getFunction().getParameters().getExpressions()) {
                        if (expression instanceof Column
                                && ((Column) expression).getExpressionType().equals("column")) {
                            String columnName = ((Column) expression).getColumnName();
                            ((Column) expression).setColumnName(escape(columnName));
                        }
                    }

                    if (null != tableFunction.getAlias()) {
                        String aliasName = getUDTFAliasName(tableFunction.getAlias().getName());
                        tableFunction.getAlias().setName(MessageFormat.format("T({0})", aliasName));
                    }
                }
                super.visit(tableFunction);
            }
        };
        StringBuilder buffer = new StringBuilder();
        expressionVisitor.setBuffer(buffer);
        d.setBuffer(buffer);
        expressionVisitor.setSelectVisitor(d);
        d.setExpressionVisitor(expressionVisitor);
        plainSelect.accept(d);
        return d.getBuffer().toString();
    }

    private org.apache.flink.table.api.Table preExecuteSql(String sql,
                                                           List<String> upstreamTableNames,
                                                           String outputTableName) {
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        new FlinkFunctionFactory(tEnv).registerAll();
        //注册UDF
        this.deParserUdf.registerUdf(tEnv, functions);
        for (String upstreamTableName : upstreamTableNames) {
            if (!tableRegistry.containsKey(upstreamTableName)) {
                throw new IllegalStateException("upstream table not registered: " + upstreamTableName);
            }
            GeneratedSchema schema = tableRegistry.get(upstreamTableName);
            TypeInformation<Row> typeInfo = toRowTypeInfo(schema.getColumnMetadata());
            DataStream<Row> t = env.fromCollection(Collections.emptyList(), typeInfo);
            tEnv.registerDataStream(mixTableNames(upstreamTableName, outputTableName), t);
        }
        try {
            Resources.setThreadLocale(LocaleHolder.instance().get());
            return tEnv.sqlQuery(sql);
        } catch (ValidationException e) {
            // format ValidationException error message
            throw new FailedOnDeParserException("custom.error.message",
                    new Object[]{e.getMessage()}, FlinkSqlDeParser.class);
        } finally {
            Resources.setThreadLocale(null);
        }
    }

    protected String generateColumnName(Column column, String fromTable) {
        return "col_" + String.format("%s_%s_%s",
                fromTable,
                getColumnName(column),
                UUID.randomUUID().toString().substring(0, 6));
    }

    protected String generateTableName(String fromTable, String suffix) {
        return String.format("%s_%s_%d", fromTable, "FlinkSql" + suffix, getNextID());
    }

    private String generateJoinProcessorArgs(JoinType joinType,
                                             Expression onExpression,
                                             String leftTableName,
                                             String leftAliasName,
                                             String rightTableName,
                                             String rightAliasName) {

        JsonNodeFactory factory = JsonNodeFactory.instance;
        ArrayNode array = factory.arrayNode();
        findJoinKeys(onExpression, array, leftAliasName, rightAliasName);

        try {
            return jsonMapper.writeValueAsString(factory.objectNode()
                    .put("first", bkTableIDConvention(leftTableName))
                    .put("second", bkTableIDConvention(rightTableName))
                    .put("type", joinType.getName())
                    .set("join_keys", array));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String genStaticJoinProcessorArgs(JoinType joinType,
                                              Expression onExpression,
                                              String staticTableName,
                                              String staticAliasName,
                                              String streamTableName,
                                              String streamAliasName) {

        List<Map<String, String>> sortedJoinKeys = new ArrayList<>();
        List<String> staticTableKeys = fetchStaticTableKeys(getStaticDataID(staticTableName));
        staticTableKeys.forEach(x -> sortedJoinKeys.add(null));
        findStaticJoinKeys(onExpression, sortedJoinKeys, staticAliasName, streamAliasName, staticTableKeys);
        // 静态表中的key必须全部进行关联
        validate.checkStaticKey(sortedJoinKeys, staticTableName, staticTableKeys);
        return buildStaticJoinArgs(sortedJoinKeys, staticTableName, joinType);
    }

    //（第一次不用换）后续的JOIN里的 joinKeys 里的stream 的key 替换成为上一次的rt对应。即对于stream，第一次建立一个映射。原始字段-> 变后字段
    private String genMultiStaticJoinProcessorArgs(JoinType joinType,
                                                   Expression onExpression,
                                                   String staticTableName,
                                                   String staticAliasName,
                                                   String streamTableName,
                                                   String streamAliasName) {

        List<Map<String, String>> sortedJoinKeys = new ArrayList<>();
        List<String> staticTableKeys = fetchStaticTableKeys(getStaticDataID(staticTableName));
        staticTableKeys.forEach(x -> sortedJoinKeys.add(null));
        findStaticJoinKeys(onExpression, sortedJoinKeys, staticAliasName, streamAliasName, staticTableKeys);
        // 静态表中的key必须全部进行关联
        validate.checkStaticKey(sortedJoinKeys, staticTableName, staticTableKeys);
        // 替换stream join key为
        sortedJoinKeys.forEach(joinKeyMap -> {
            String streamKey = joinKeyMap.get("stream");
            this.cacheFactTableFields.stream().forEach(factField -> {
                String factFieldOrigin = factField.getOrigin();
                String factFieldName = factField.getField();
                if (!factFieldOrigin.contains(PROCESSOR_TYPE_STATIC_JOIN_TRANSFORM) && factFieldOrigin.contains(":")
                        && factFieldOrigin.split(":")[1].equals(streamKey)) {

                    joinKeyMap.put("stream", factFieldName);
                }
            });
        });
        return buildStaticJoinArgs(sortedJoinKeys, staticTableName, joinType);
    }

    private String buildStaticJoinArgs(List<Map<String, String>> sortedJoinKeys,
                                       String staticTableName,
                                       JoinType joinType) {
        JsonNodeFactory factory = JsonNodeFactory.instance;
        ArrayNode array = factory.arrayNode();
        sortedJoinKeys.forEach(joinKeyMap -> {
            ObjectNode jsonNodes = array.addObject();
            joinKeyMap.forEach((tableTag, joinKey) -> jsonNodes.put(tableTag, joinKey));
        });

        try {
            return jsonMapper.writeValueAsString(factory.objectNode()
                    .putPOJO("storage_info", fetchStaticStorageInfo(getStaticDataID(staticTableName)))
                    .put("table_name", bkTableNameConvention(staticTableName)) // todo
                    .put("biz_id", fetchStaticBizID(getStaticDataID(staticTableName)))
                    .put("type", joinType.getName())
                    .set("join_keys", array));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void registerTable(String tableName,
                               List<ColumnMetadata> metadataList) {
        if (tableRegistry.containsKey(tableName)) {
            return;
        }
        List<ColumnMetadata> rowsToRegister = Stream.concat(
                defaultLoadedColumns.stream(),
                metadataList.stream()
                        .filter(columnMetadata ->
                                !defaultNonLoadedColumns.contains(columnMetadata.getColumnName()))
        )
                .collect(Collectors.toList());
        GeneratedSchema schema = new GeneratedSchema(rowsToRegister);
        tableRegistry.put(tableName, schema);
    }

    private void registerTable(String tableName,
                               List<ColumnMetadata> metadataList,
                               List<ColumnMetadata> systemColumnList) {
        if (tableRegistry.containsKey(tableName)) {
            return;
        }
        List<ColumnMetadata> rowsToRegister = Stream.concat(
                defaultLoadedColumns.stream(),
                metadataList.stream()
                        .filter(columnMetadata ->
                                !defaultNonLoadedColumns.contains(columnMetadata.getColumnName()))
        )
                .collect(Collectors.toList());
        rowsToRegister.addAll(systemColumnList);
        GeneratedSchema schema = new GeneratedSchema(rowsToRegister);
        tableRegistry.put(tableName, schema);
    }

    private Table renameTable(Table inputTable, java.util.function.Function<String, String> renaming) {
        if (inputTable == null) {
            return null;
        }
        if (inputTable.getName() == null) {
            return null;
        }
        Table table = new Table(inputTable.getDatabase(),
                inputTable.getSchemaName(),
                renaming.apply(inputTable.getName()));
        table.setHint(inputTable.getIndexHint());
        table.setAlias(inputTable.getAlias());
        return table;
    }

    private TableMetadata<ColumnMetadata> fetchGeneralMetadata(String tableName) {
        try {
            return generalTableMetadataConnector.fetchTableMetadata(bkTableIDConvention(tableName));
        } catch (Exception e) {
            throw new FailedOnDeParserException("fetch.meta.data.error", new Object[0], FlinkSqlDeParser.class);
        }
    }

    private TableMetadata<ColumnMetadata> fetchStaticMetadata(String dataID) {
        try {
            return staticTableMetadataConnector.fetchTableMetadata(dataID);
        } catch (Exception e) {
            throw new FailedOnDeParserException("fetch.static.meta.data.error", new Object[0], FlinkSqlDeParser.class);
        }
    }

    private Map<String, Object> fetchStaticStorageInfo(String dataID) {
        try {
            return staticTableMetadataConnector.fetchStorageInfo(dataID);
        } catch (Exception e) {
            throw new FailedOnDeParserException("fetch.static.meta.data.error", new Object[0], FlinkSqlDeParser.class);
        }
    }


    private String fetchStaticBizID(String dataID) {
        try {
            return staticTableMetadataConnector.fetchBizID(dataID);
        } catch (Exception e) {
            throw new FailedOnDeParserException("fetch.static.meta.data.error", new Object[0], FlinkSqlDeParser.class);
        }
    }

    protected String getStaticDataID(String staticTableName) {
        return staticTableName;
    }


    private List<String> fetchStaticTableKeys(String staticTableName) {
        try {
            return staticTableMetadataConnector.fetchKeys(staticTableName);
        } catch (Exception e) {
            throw new FailedOnDeParserException("fetch.static.meta.data.error", new Object[0], FlinkSqlDeParser.class);
        }
    }

    private TypeInformation<Row> toRowTypeInfo(List<ColumnMetadata> columnMetadata) {
        Set<String> columnNames = new LinkedHashSet<>();
        List<TypeInformation<?>> types = new ArrayList<>();

        for (ColumnMetadata c : columnMetadata) {
            columnNames.add(c.getColumnName());
            types.add(flinkDataTypeMapper.toExternalType(c.getDataType()));
        }

        return Types.ROW_NAMED(columnNames.toArray(new String[columnNames.size()]),
                types.toArray(new TypeInformation<?>[types.size()]));
    }

    protected String mixTableNames(String upstreamTableName, String downstreamTableName) {
        // fixme a possible name conflict: a + 4 + b4c = a4b4c | a4b + 4 + c = a4b4c
        return reverseTableName(downstreamTableName) + "___" + reverseTableName(upstreamTableName);
    }

    protected String globalOutputTableName() {
        return bizID + "_" + outputTableName;
    }

    protected String bkTableIDConvention(String tableName) {
        return tableName;
    }

    @Override
    protected Object getRetObj() {
        return tableEntities;
    }

    protected int getNextID() {
        return nextID.incrementAndGet();
    }

    public enum JoinType {
        LEFT("left"),
        INNER("inner"),
        RIGHT("right");

        private final String name;

        JoinType(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
