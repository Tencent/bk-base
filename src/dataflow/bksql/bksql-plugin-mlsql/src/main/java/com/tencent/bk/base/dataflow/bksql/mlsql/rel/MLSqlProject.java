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

import com.tencent.bk.base.dataflow.bksql.mlsql.rel.params.MLSqlRelParameters;
import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.rest.exception.MessageLocalizedExceptionV1;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.MLSqlOperatorTable;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.RelParser;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlProcessor;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTableNameAccess;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskGroup;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTransformType;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.json.JSONArray;
import org.json.JSONObject;

public class MLSqlProject extends Project implements MLSqlRel {


    public MLSqlProject(RelOptCluster cluster, RelTraitSet traits, RelNode input, List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, input, projects, rowType);
    }

    @Override
    public Project copy(RelTraitSet traits, RelNode input, List<RexNode> projects, RelDataType rowType) {
        return new MLSqlProject(getCluster(), traits, input, projects, rowType);
    }

    /**
     * 将语法信息转换为具体可识别的任务信息
     */
    public MLSqlTableNameAccess translateToTask(MLSqlTaskContext context, MLSqlTaskGroup taskGroup, MLSqlRel mlSqlRel)
            throws Exception {
        MLSqlRel input = (MLSqlRel) getInput();
        MLSqlTableNameAccess inTask = input.translateToTask(context, taskGroup, input);
        boolean isLateral = false;
        if (input instanceof MLSqlCorrelate) {
            isLateral = true;
        }

        // inType 记录input的字段类型 字段名称
        RelDataType inType = input.getRowType();
        // input的字段名称list
        List<String> inFieldNames = MLSqlRels.mlsqlFieldNames(inType);
        //变量声明
        MLSqlRelParameters relParameters = new MLSqlRelParameters();
        Map<String, Object> args = new HashMap<>();
        MLSqlProcessor processor = new MLSqlProcessor();
        MLSqlTask.Builder builder = MLSqlTask.builder();

        /*
         * 不管是哪种操作（train,create or insert），解析出的语法树，分为三种类型：
         * 1. RexCall：此操作可进一步向下解析，比如为函数调用，运算符使用等，典型的就是我们用于计算的函数都是需要进一步解析
         * 2. RexInputRef:直接使用表的字段，如'select age from student'中的age即为此类型
         * 3. RexLiteral:直接使用常量字段，如'select "a" from student'中的"a"即为此类型
         * 对于后两种情况，出现的内容就是最终输出的一部分，所以直接取出来做为builder的field即可
         */
        for (Pair<RexNode, RelDataTypeField> pair : Pair.zip(getProjects(), getRowType().getFieldList())) {
            RexNode node = pair.left;
            RelDataTypeField right = pair.right;
            if (node instanceof RexCall) {
                Map<String, Object> result = dealWithRexCall((RexCall) node, processor, inFieldNames, args, right,
                        builder);
                this.parseRexCall(result, relParameters);

            } else if (node instanceof RexInputRef) {
                RexInputRef inputRef = (RexInputRef) node;
                this.parseRexInputRef(inputRef, inFieldNames, right, isLateral, relParameters, inTask, builder);
            } else if (node instanceof RexLiteral) {
                this.parseRexLiteral(right, relParameters, builder);
            }
        }
        if (!isLateral) {
            //如果是lateral操作，对函数的校验是在MLSqlCorrelate里进行的
            //如果非lateral操作，则需要在上述解析完成之后进行
            this.checkInputParameters(processor.getArgs(), inType, relParameters);
        }

        //format builder
        String newModelName = RelParser
                .buildTransformParams(inTask, isLateral, processor, relParameters.getFunction(),
                        relParameters.getModelName(), builder);
        RelParser.buildBasicParams(relParameters.getTargetTableName(), newModelName, context, null,
                relParameters.getEvaluateMap(), this,
                builder);
        //get task
        MLSqlTask task = builder.create();
        taskGroup.add(task);
        return task;
    }

    /**
     * 从列名获取lateral结果列的索引
     */
    private int getLateralColumnIndex(String origin) {
        String regex = "column_([0-9]+)";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(origin);
        if (!matcher.find()) {
            return -1;
        } else {
            return Integer.valueOf(matcher.group(1));
        }
    }

    /**
     * 处理所有函数调用类型的节点
     */
    public Map<String, Object> dealWithRexCall(RexCall leftCall,
            MLSqlProcessor processor,
            List<String> inFieldNames,
            Map<String, Object> args,
            RelDataTypeField right,
            MLSqlTask.Builder builder) throws Exception {
        MLSqlModelFunction function = null;
        Map<String, Object> result = new HashMap<>();
        if (leftCall.getOperator() == SqlStdOperatorTable.CONCAT) {
            //如果是 a||b 这种格式，由于我们的实现方式，部分参数信息是靠这种格式解析出来的
            //这里相当于 a as b，a为具体值，b为别名
            //这里为什么不直接用AS操作，因为在语法树解析中，在最终的结果，AS后半部分会被优化掉
            List<RexNode> list = leftCall.getOperands();
            RexLiteral name = (RexLiteral) list.get(1);
            RexNode value = list.get(0);
            String attributeName = name.getValueAs(String.class);
            if (attributeName.equalsIgnoreCase(RelParser.MLSQL_CREATE_TARGET_TABLE)) {
                //处理保留字之一：create_target_table，在create与insert时用到
                result.put(RelParser.TARGET_TABLE_KEY, ((RexLiteral) value).getValueAs(String.class));
            } else {
                Map<String, Object> resultMap = RelParser.parseTrainParameters(attributeName, value,
                        processor, inFieldNames, args, builder);
                if (resultMap.containsKey(RelParser.MODEL_NAME_KEY)) {
                    result.put(RelParser.MODEL_NAME_KEY, (String) resultMap.get("modelName"));
                }
                if (resultMap.containsKey(RelParser.FUNCTION_KEY)) {
                    result.put(RelParser.FUNCTION_KEY, (MLSqlModelFunction) resultMap.get("function"));
                }
            }
        } else if (leftCall.getOperator() == MLSqlOperatorTable.MODEL_FUNC
                || leftCall.getOperator() == MLSqlOperatorTable.LATERAL_MODEL_FUNC) {
            /*
             * 1. 在具体预测使用模型时，后端在解析的时候会将用户使用的:model_name(params)
             *     替换为 (lateral_)model_func(model_name=${model_name},params)
             * 2. 这样做主要是为了规避每次模型生成都动态注册为一个函数，这样不合理，并且目前机制下做不到。
             * 3. 这种替换将模型的名称作为一个参数处理，传入我们预先注册好的两个参数（model_fun与lateral_model_func）即可
             */
            Map<String, Object> resultMap = RelParser.parseFunctionNode(leftCall, inFieldNames, processor);
            function = (MLSqlModelFunction) resultMap.get(RelParser.FUNCTION_KEY);
            result.put(RelParser.FUNCTION_KEY, function);
            result.put(RelParser.MODEL_NAME_KEY, (String) resultMap.get(RelParser.MODEL_NAME_KEY));
            if (resultMap.containsKey(RelParser.EVALUATE_MAP_KEY)) {
                ((Map<String, String>) resultMap.get(RelParser.EVALUATE_MAP_KEY))
                        .put(RelParser.PREDICT_LABEL_KEY, right.getName());
                result.put(RelParser.EVALUATE_MAP_KEY, (Map<String, String>) resultMap.get(RelParser.EVALUATE_MAP_KEY));
            }
            builder.type(MLSqlTransformType.data);
            if (resultMap.containsKey(RelParser.MODEL_OBJECT_KEY)) {
                MLSqlModel model = (MLSqlModel) resultMap.get(RelParser.MODEL_OBJECT_KEY);
                builder.model(model);
            }
            if (leftCall.getOperator() == MLSqlOperatorTable.MODEL_FUNC) {
                //insert或是非lateral-create的使用方式下，需要将函数的返回类型作为最终任务的field，lateral-create是不需要的
                RelDataType type = builder.saveOutputArgs(right.getName(), function);

                if (type == null) {
                    type = right.getType();
                }
                builder.field(new MLSqlTask.ColumnField(
                        right.getName(),
                        MLSqlRels.toMLSqlType(type),
                        null,
                        Collections.singletonList("")));
                builder.saveOutputArgs(right.getName(), function);
            }
        }
        return result;
    }

    /**
     * 将对RexCall的解析分类放入存储relParameters
     *
     * @param result 对RexCall节点解析的结果
     * @param relParameters 用于存储结果的参数对象
     */
    private void parseRexCall(Map<String, Object> result, MLSqlRelParameters relParameters) {
        if (result.containsKey(RelParser.MODEL_NAME_KEY)) {
            relParameters.setModelName((String) result.get(RelParser.MODEL_NAME_KEY));
        }
        if (result.containsKey(RelParser.FUNCTION_KEY)) {
            relParameters.setFunction((MLSqlModelFunction) result.get(RelParser.FUNCTION_KEY));
        }
        if (result.containsKey(RelParser.TARGET_TABLE_KEY)) {
            relParameters.setTargetTableName((String) result.get(RelParser.TARGET_TABLE_KEY));
        }
        if (result.containsKey(RelParser.EVALUATE_MAP_KEY)) {
            relParameters.setEvaluateMap((Map<String, String>) result.get(RelParser.EVALUATE_MAP_KEY));
        }
    }

    /**
     * 解析RexInputRef对象，用于获取MLSQL语句中对现有表中列的访问情况
     *
     * @param inputRef InputRef对象，用于获取对表中列的存储情况
     * @param inFieldNames 列名列表，用于根据索引获取具体的列表名称
     * @param right 用户操作中涉及到的列名及类型信息
     * @param isLateral 是否为literal操作
     * @param relParameters 用于存储解析结果的参数对象
     * @param inTask 根据用户输入的sql转换成的任务信息
     * @param builder builder对象
     */
    private void parseRexInputRef(RexInputRef inputRef, List<String> inFieldNames,
            RelDataTypeField right, boolean isLateral, MLSqlRelParameters relParameters,
            MLSqlTableNameAccess inTask, MLSqlTask.Builder builder) {
        String origin = inFieldNames.get(inputRef.getIndex());
        /*
         * 以下两个分支分别处理多个返回值与单一返回值的情况
         *    1:如果某个函数或模型预测有多列返回值，那模型的使用一定要是lateral且仅有lateral一种
         *    2:如果其仅有单独一列返回值，那就一定不是lateral
         *  以上两种情况就是通过“input“的类型确定的
         *     1： 对于lateral的操作，其会MLSqlCorrelate，并在那里进行函数的解析，解析后函数的相关信息需要借助args中的
         *         LATERAL_OUTPUT_KEY返回来
         *     2：如果为非lateral操作，则会进入MLSqlTableScan，直接在本函数的第一个if里进行函数的解析
         * */
        if (isLateral) {
            int index = getLateralColumnIndex(origin);
            MLSqlTask mlSqlTask = (MLSqlTask) inTask;
            Map<String, Object> argsMap = mlSqlTask.getProcessor().getArgs();
            JSONArray array = new JSONArray(argsMap.get(RelParser.LATERAL_OUTPUT_KEY).toString());
            String type = MLSqlRels.toMLSqlType(right.getType());
            if (index >= 0) {
                JSONObject object = array.getJSONObject(index);
                String name = object.getString("name");
                mlSqlTask.getProcessor().getArgs().put(name, right.getName());
                type = MLSqlRels.toMLSqlType(SqlTypeName.get(object.getString("type")));
            }
            builder.field(new MLSqlTask.ColumnField(
                    right.getName(),
                    type,
                    null,
                    Collections.singletonList(origin)));
        } else {
            RelDataType type = builder.saveOutputArgs(right.getName(), relParameters.getFunction());
            if (type == null) {
                type = right.getType();
            }
            builder.field(new MLSqlTask.ColumnField(
                    right.getName(),
                    MLSqlRels.toMLSqlType(type),
                    null,
                    Collections.singletonList(origin)));
        }
    }

    /**
     * 解析RexLiteral对象
     *
     * @param right 用户操作中涉及到的列名及类型信息
     * @param relParameters 用于存储解析结果的参数对象
     * @param builder builder对象
     */
    public void parseRexLiteral(RelDataTypeField right, MLSqlRelParameters relParameters, MLSqlTask.Builder builder) {
        RelDataType type = builder.saveOutputArgs(right.getName(), relParameters.getFunction());
        if (type == null) {
            type = right.getType();
        }
        builder.field(new MLSqlTask.ColumnField(
                right.getName(),
                MLSqlRels.toMLSqlType(type),
                null,
                Collections.singletonList("")));
    }

    /**
     * 检查参数的输入是否合法
     *
     * @param args 用户输入参数
     * @param inType 输入参数类型
     * @param relParameters 参数对象
     */
    public void checkInputParameters(Map<String, Object> args, RelDataType inType,
            MLSqlRelParameters relParameters) throws Exception {
        if (relParameters.getFunction() == null) {
            throw new MessageLocalizedExceptionV1(
                    "algorithm.not.found.error",
                    new Object[]{},
                    MLSqlDeParser.class,
                    ErrorCode.MODEL_OR_ALG_NOT_FOUNT_ERR
            );
        }
        //check parameters and types
        relParameters.getFunction().checkMustInput(args.keySet());
        relParameters.getFunction().checkParameters(args, inType.getFieldList());
    }
}
