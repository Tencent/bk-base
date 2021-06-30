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

package com.tencent.bk.base.dataflow.bksql.mlsql.rel;

import com.google.common.collect.ImmutableMap;
import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.rest.exception.MessageLocalizedExceptionV1;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.RelParser;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlProcessor;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTableNameAccess;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskGroup;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTransformType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.util.ImmutableBitSet;
import org.json.JSONArray;
import org.json.JSONObject;

public class MLSqlCorrelate extends Correlate implements MLSqlRel {

    public static final String ZIP = "ZIP";
    public static final String SPLIT_FIELD_TO_RECORDS = "SPLIT_FIELD_TO_RECORDS";
    public static final String ADD_OS = "ADD_OS";

    public MLSqlCorrelate(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right,
            CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        super(cluster, traits, left, right, correlationId, requiredColumns, joinType);
    }

    @Override
    public Correlate copy(RelTraitSet traits, RelNode left, RelNode right, CorrelationId correlationId,
            ImmutableBitSet requiredColumns, JoinRelType joinType) {
        return new MLSqlCorrelate(getCluster(), traits, left, right, correlationId, requiredColumns, joinType);
    }

    /**
     * 处理Lateral Join的情况，在此情况下Model的使用是作为join右侧的表存在的
     *
     * @param tableFunctionScan 语句中表的信息，可以通过其找到lateral中的相关调用信息
     * @param streamTable later语句中的具体表信息（也可能是语句生成的临时表信息）
     * @param mlsqlRel 语法信息
     * @param taskGroup 存储最终的任务信息
     * @param context 语法上下文信息
     * @param builder 生成task的Builder
     */
    public void dealWithLateralJoin(MLSqlTableFunctionScan tableFunctionScan,
            MLSqlRel streamTable, MLSqlRel mlsqlRel, MLSqlTaskGroup taskGroup,
            MLSqlTaskContext context, MLSqlTask.Builder builder) throws Exception {
        RelDataType streamType = streamTable.getRowType();
        List<String> streamFieldNames = MLSqlRels.mlsqlFieldNames(streamType);
        RexCall call = (RexCall) tableFunctionScan.getCall();
        MLSqlProcessor processor = new MLSqlProcessor();
        Map<String, Object> result = RelParser.parseFunctionNode(call, streamFieldNames, processor);
        MLSqlModelFunction function = (MLSqlModelFunction) result.get(RelParser.FUNCTION_KEY);
        if (function == null) {
            throw new MessageLocalizedExceptionV1(
                    "algorithm.not.found.error",
                    new Object[]{},
                    MLSqlDeParser.class,
                    ErrorCode.MODEL_OR_ALG_NOT_FOUNT_ERR
            );
        }
        MLSqlModel model = null;
        if (result.containsKey(RelParser.MODEL_OBJECT_KEY)) {
            model = (MLSqlModel) result.get(RelParser.MODEL_OBJECT_KEY);
            builder.model(model);
        }

        Map<String, String> evaluateMap = null;
        if (result.containsKey(RelParser.EVALUATE_MAP_KEY)) {
            evaluateMap = (Map<String, String>) result.get(RelParser.EVALUATE_MAP_KEY);
        }
        Map<String, RelDataType> outputMap = function.getOutputs();
        JSONArray jsonArray = new JSONArray();
        for (Map.Entry<String, RelDataType> entry : outputMap.entrySet()) {
            String name = entry.getKey();
            RelDataType type = entry.getValue();
            if (type instanceof ArraySqlType) {
                type = (ArraySqlType) type;
                type = type.getComponentType();
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("name", name);
            jsonObject.put("type", type);
            jsonArray.put(jsonObject);
        }
        processor.getArgs().put(RelParser.LATERAL_OUTPUT_KEY, jsonArray.toString());
        function.checkMustInput(processor.getArgs().keySet());
        function.checkParameters(processor.getArgs(), streamType.getFieldList());
        String modelName = (String) result.get(RelParser.MODEL_NAME_KEY);
        MLSqlTableNameAccess inTask = streamTable.translateToTask(context, taskGroup, mlsqlRel);
        if (inTask != null) {
            String newModelName = RelParser
                    .buildTransformParams(inTask, false, processor, function, modelName, builder);
            RelParser.buildBasicParams(streamTable.getTable().getQualifiedName().get(1), newModelName, context,
                    model, evaluateMap, mlsqlRel, builder);
        }
    }

    @Override
    public MLSqlTableNameAccess translateToTask(MLSqlTaskContext context, MLSqlTaskGroup taskGroup, MLSqlRel mlsqlRel)
            throws Exception {
        MLSqlTableFunctionScan tableFunctionScan = null;
        MLSqlRel streamTable = null;

        boolean isLeftTableFunctionScan;
        if (getLeft() instanceof MLSqlTableFunctionScan) {
            tableFunctionScan = (MLSqlTableFunctionScan) getLeft();
            streamTable = (MLSqlRel) getRight();
            isLeftTableFunctionScan = true;
        } else {
            tableFunctionScan = (MLSqlTableFunctionScan) getRight();
            streamTable = (MLSqlRel) getLeft();
            mlsqlRel = streamTable;
            isLeftTableFunctionScan = false;
        }

        MLSqlTask.Builder builder = MLSqlTask.builder();
        builder.type(MLSqlTransformType.data);
        // output fields
        if (!isLeftTableFunctionScan) {
            dealWithLateralJoin(tableFunctionScan, streamTable, mlsqlRel, taskGroup, context, builder);
        }

        MLSqlTask task = builder.create();
        taskGroup.add(task);
        return task;
    }

    private String processorOf(RexCall rexCall) {
        switch (rexCall.op.getName()) {
            case ADD_OS:
                return ADD_OS.toLowerCase();
            case SPLIT_FIELD_TO_RECORDS:
                return SPLIT_FIELD_TO_RECORDS.toLowerCase();
            case ZIP:
                return ZIP.toLowerCase();
            default:
                throw new UnsupportedOperationException("unimplemented transform function: " + rexCall.op);
        }
    }

    private Map<String, Object> processorArgsOf(RexCall rexCall) {
        switch (rexCall.op.getName()) {
            case SPLIT_FIELD_TO_RECORDS:
                List<RexNode> operands = rexCall.getOperands();
                String originField = ((RexFieldAccess) operands.get(0)).getField().getName();
                String separator = ((RexLiteral) operands.get(1)).getValue2().toString();
                return ImmutableMap.<String, Object>builder()
                        .put("separator", separator)
                        .put("origin_field", originField)
                        .build();
            case ZIP:
                List<RexNode> operands0 = rexCall.getOperands();
                List<String> fields = new ArrayList<>();
                List<String> separator0 = new ArrayList<>();
                String concatation = null;
                for (int i = 0; i < operands0.size(); i++) {
                    if (i == 0) {
                        concatation = ((RexLiteral) operands0.get(i)).getValue2().toString();
                    } else if (i % 2 != 0) {
                        fields.add(((RexFieldAccess) operands0.get(i)).getField().getName());
                    } else {
                        separator0.add(((RexLiteral) operands0.get(i)).getValue2().toString());
                    }
                }
                return ImmutableMap.<String, Object>builder()
                        .put("fields", fields)
                        .put("concatation", concatation)
                        .put("separator", separator0)
                        .build();
            default:
                return null;
        }
    }

    private List<String> tableFunctionOriginOf(RexCall rexCall) {
        return new ArrayList<>(rexCall.getOperands().stream()
                .map(rexNode -> rexNode instanceof RexFieldAccess ? ((RexFieldAccess) rexNode).getField().getName()
                        : null)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }
}
