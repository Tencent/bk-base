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

package com.tencent.bk.base.dataflow.bksql.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.dataflow.bksql.task.params.TaskBasicParams;
import com.tencent.bk.base.dataflow.bksql.task.params.TaskModelParams;
import com.tencent.bk.base.dataflow.bksql.task.params.TaskTableParams;
import com.tencent.bk.base.dataflow.bksql.task.params.TaskTransformerParams;
import com.tencent.bk.base.dataflow.bksql.MLSqlTaskType;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.parser.RelParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.ArraySqlType;

public final class MLSqlTask implements MLSqlTableNameAccess {


    private final List<AbstractField> fields;
    private final List<String> parents;
    private Map<String, MLSqlInterpreterValue> interpreter;
    private MLSqlProcessor processor;
    private String id;
    private String name;
    private String tableName;
    private MLSqlTransformType type;
    private String description;
    private MLSqlModel model;
    private String modelName;
    private boolean tableNeedCreate;
    private String writeMode = MLSqlWriteMode.OVERWRITE.getValue();
    private String taskType = MLSqlTaskType.MLSQL_QUERY.getValue();
    private Map<String, String> evaluateMap = new HashMap<>();

    private MLSqlTask(TaskBasicParams basicParams, TaskTableParams tableParams,
            TaskTransformerParams transformerParams, TaskModelParams modelParams) {
        this.id = basicParams.getId();
        this.name = basicParams.getName();
        this.tableName = tableParams.getTableName();
        this.type = transformerParams.getType();
        this.fields = tableParams.getFields();
        this.interpreter = transformerParams.getInterpreter();
        this.processor = transformerParams.getProcessor();
        this.parents = basicParams.getParents();
        this.description = basicParams.getDescription();
        this.modelName = modelParams.getModelName();
        this.tableNeedCreate = tableParams.isTableNeedCreate();
        this.model = modelParams.getModel();
        this.writeMode = tableParams.getWriteMode();
        this.taskType = basicParams.getTaskType();
        this.evaluateMap = basicParams.getEvaluateMap();
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty("id")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    @JsonProperty("table_name")
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonProperty("type")
    public MLSqlTransformType getType() {
        return type;
    }

    public void setType(MLSqlTransformType type) {
        this.type = type;
    }

    @JsonProperty("interpreter")
    public Map<String, MLSqlInterpreterValue> getInterpreter() {
        return interpreter;
    }

    @JsonProperty("processor")
    public MLSqlProcessor getProcessor() {
        return processor;
    }

    @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("parents")
    public List<String> getParents() {
        return parents;
    }

    @JsonProperty("model_info")
    public MLSqlModel getModel() {
        return model;
    }

    @JsonProperty("model_name")
    public String getModelName() {
        return modelName;
    }

    @JsonProperty("table_need_create")
    public boolean getTableNeedCreate() {
        return tableNeedCreate;
    }

    public void setTableNeedCreate(boolean tableNeedCreate) {
        this.tableNeedCreate = tableNeedCreate;
    }

    public void clearAndSetParents(String parent) {
        this.parents.clear();
        this.parents.add(parent);
    }

    @JsonProperty("fields")
    public List<AbstractField> getFields() {
        return fields;
    }

    @JsonProperty("write_mode")
    public String getWriteMode() {
        return writeMode;
    }

    public void setWriteMode(MLSqlWriteMode writeMode) {
        this.writeMode = writeMode.getValue();
    }

    @JsonProperty("task_type")
    public String getTaskType() {
        return taskType;
    }

    @JsonProperty("evaluate_map")
    public Map<String, String> getEvaluateMap() {
        return this.evaluateMap;
    }

    @Override
    public String toString() {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            return "";
        }
    }

    public static final class Builder {

        private final List<AbstractField> fields = new ArrayList<>();
        private final List<String> parents = new ArrayList<>();
        private final Map<String, Object> outputArgsMap = new HashMap<>();
        private String id = null;
        private String name = null;
        private String tableName = null;
        private MLSqlTransformType type = null;
        private Map<String, MLSqlInterpreterValue> interpreter = new HashMap<>();
        private MLSqlProcessor processor;
        private String description;
        private MLSqlModel model;
        private String modelName;
        private boolean tableNeedCreate = true;
        private String writeMode = MLSqlWriteMode.OVERWRITE.getValue();
        private String taskType = MLSqlTaskType.MLSQL_QUERY.getValue();
        private Map<String, String> evaluateMap = new HashMap<>();


        private Builder() {
        }

        public Builder addParent(String parent) {
            parents.add(parent);
            return this;
        }


        public Builder field(AbstractField field) {
            fields.add(field);
            return this;
        }

        public Builder addInterpreter(String name, MLSqlInterpreterValue value) {
            interpreter.put(name, value);
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder type(MLSqlTransformType type) {
            this.type = type;
            return this;
        }

        public Builder processor(MLSqlProcessor processor) {
            this.processor = processor;
            for (Map.Entry<String, Object> entry : outputArgsMap.entrySet()) {
                processor.getArgs().put(entry.getKey(), entry.getValue());
            }
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public Builder model(MLSqlModel model) {
            this.model = model;
            return this;
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder modelName(String modelName) {
            this.modelName = modelName;
            return this;
        }

        public Builder tableNeedCreate(boolean tableNeedCreate) {
            this.tableNeedCreate = tableNeedCreate;
            return this;
        }

        public Builder writeMode(MLSqlWriteMode writeMode) {
            this.writeMode = writeMode.getValue();
            return this;
        }

        public Builder taskType(MLSqlTaskType taskType) {
            this.taskType = taskType.getValue();
            return this;
        }

        public Builder evaluateMap(Map<String, String> evaluateMap) {
            this.evaluateMap = evaluateMap;
            return this;
        }

        /**
         * 此函数与同名函数（下方）功能类似，但处理逻辑更简单，主要用于处理lateral的时候
         * 这个时候单个参数都是一般参数，不存在array这种情况，只需要将所有参数放在一个list中即可
         */
        public void saveOutputArgs(String name, String item) {
            if (!outputArgsMap.containsKey(name)) {
                List<String> valueList = new ArrayList<>();
                valueList.add(item);
                outputArgsMap.put(name, valueList);
            } else {
                ((List) outputArgsMap.get(name)).add(item);
            }
        }

        /**
         * 对函数的输出做转换
         * 当某个模型或函数直接进行应用时会有如下的语句：func(input_cols=a,other_params=b) as result
         * 这种情况下，用户指定的函数参数里只有input_col和other_params，并没有output_col，这时需要将as后面的result作为output传递给最终的解析结果
         * 对于上述例子，result即对应此函数中的name,func即为参数中的function，我们需要从function的属性outputs中拿到其输出属性名（如output_col），
         * 然后与result对应起来,形成与input_cols，other_params同级的参数output_col=result
         * 此函数就是先形成这种对应关系保存至outputArgsMap中，然后在形成最终的processor的时候，作为Processor的args与其它参数并列
         * 这里有一种特殊情况，即用户输入的是func(input_cols=a,other_params=b,output_col=c)，即用户直接以参数的形式指定了output_col而没有后面的as
         * 那这个时候在实现上我们仍旧能够得到一个as，但其后面的内容为default值，那么就没必要再保存入outputArgsMap，直接使用户输入即可
         *
         * @param name 参数名
         * @param function 函数对象
         * @return 最终返回结果的数据类型
         */
        public RelDataType saveOutputArgs(String name, MLSqlModelFunction function) {
            if (function != null) {
                Map<String, RelDataType> outputMap = function.getOutputs();
                if (RelParser.DEFAULT_FUNCTION_OUTPUT.equalsIgnoreCase(name)) {
                    RelDataType type = outputMap.get(function.getOutputColName());
                    if (type instanceof ArraySqlType) {
                        return type.getComponentType();
                    } else {
                        return type;
                    }
                }
                //todo:目前还仅支持一个输出（可以是向量），如果有多个输出列，需要使用lateral的结构来获取
                String outputName = function.getOutputColName();
                RelDataType type = outputMap.get(outputName);
                if (type instanceof ArraySqlType) {
                    if (!outputArgsMap.containsKey(outputName)) {
                        List<String> valueList = new ArrayList<>();
                        valueList.add(name);
                        outputArgsMap.put(outputName, valueList);
                    } else {
                        ((List) outputArgsMap.get(outputName)).add(name);
                    }
                    return type.getComponentType();
                } else {
                    if (!outputArgsMap.containsKey(outputName)) {
                        outputArgsMap.put(outputName, name);
                        return type;
                    }
                }

            }
            return null;
        }

        public MLSqlTask create() {
            TaskBasicParams basicParams = new TaskBasicParams(id, name, description, taskType,
                    evaluateMap, parents);
            TaskModelParams modelParams = new TaskModelParams(modelName, model);
            TaskTableParams tableParams = new TaskTableParams(tableName, fields, tableNeedCreate, writeMode);
            TaskTransformerParams transformerParams = new TaskTransformerParams(type, interpreter, processor);
            return new MLSqlTask(basicParams, tableParams, transformerParams, modelParams);
        }

    }

    public abstract static class AbstractField {

        private final String type;
        private final String description;
        private String field;

        public AbstractField(String field, String type, String description) {
            this.field = field;
            this.type = type;
            this.description = description;
        }

        public String getField() {
            return field;
        }

        public void setField(String field) {
            this.field = field;
        }

        public String getType() {
            return type;
        }

        public String getDescription() {
            return description;
        }
    }

    public static class ColumnField extends AbstractField {

        private final List<String> origins;

        public ColumnField(String field, String type, String description, List<String> origins) {
            super(field, type, description);
            this.origins = origins;
        }

        public List<String> getOrigins() {
            return origins;
        }
    }

    /**
     * e.g.
     * {
     * "description": null,
     * "origins": [
     * "vn_ios_online"
     * ],
     * "field": "arg_1",
     * "processor_args": {
     * "expr": "(double)(${vn_ios_online}.intValue())"
     * },
     * "type": "double",
     * "processor": "transformer"
     * }
     */
    public static class TransformerField extends AbstractField {

        private final Object processorArgs;
        private final List<String> origins;
        private Boolean isDimension;
        private String processor;

        public TransformerField(String field, String type, String description, Object processorArgs,
                List<String> origins) {
            super(field, type, description);
            this.processorArgs = processorArgs;
            this.origins = origins;
            this.processor = "transformer";
            this.isDimension = false;
        }

        public String getProcessor() {
            return processor;
        }

        @JsonProperty("processor_args")
        public Object getProcessorArgs() {
            return processorArgs;
        }

        public List<String> getOrigins() {
            return origins;
        }

        @JsonProperty("is_dimension")
        public Boolean isDimension() {
            return isDimension;
        }
    }

    public static class FilterField extends AbstractField {

        private final String filter;

        public FilterField(String field, String type, String description, String filter) {
            super(field, type, description);
            this.filter = filter;
        }

        public String getFilter() {
            return filter;
        }
    }

    public static class TimestampField extends AbstractField {

        public TimestampField() {
            super("timestamp", "timestamp", "timestamp");
        }
    }

    public static class WindowedTimestampField extends TimestampField {

        private final String processor;
        private final Object processorArgs;

        public WindowedTimestampField(String processor, Map<String, Object> processorArgs) {
            super();
            this.processor = processor;
            this.processorArgs = processorArgs;
        }

        public String getProcessor() {
            return processor;
        }

        @JsonProperty("processor_args")
        public Object getProcessorArgs() {
            return processorArgs;
        }
    }

    public static class JoinTimestampField extends WindowedTimestampField {

        public JoinTimestampField(Map<String, Object> processorArgs) {
            super("join_aggregator", processorArgs);
        }
    }

    public static class DimensionField extends ColumnField {

        private final Boolean isDimension = true;

        public DimensionField(String field, String type, String description, List<String> origins) {
            super(field, type, description, origins);
        }

        @JsonProperty("is_dimension")
        public Boolean getDimension() {
            return isDimension;
        }
    }

    /**
     * {
     * "description": null,
     * "origins": "arg_1",
     * "field": "create_suc",
     * "type": "double",
     * "processor": "sum"
     * }
     */
    public static class AggCallField extends AbstractField {

        private final List<String> origins;
        private final String processor;
        private final Boolean isDimension = false;

        public AggCallField(String field, String type, String description, List<String> origins, String processor) {
            super(field, type, description);
            this.origins = origins;
            this.processor = processor;
        }

        public List<String> getOrigins() {
            return origins;
        }

        public String getProcessor() {
            return processor;
        }

        @JsonProperty("is_dimension")
        public Boolean getDimension() {
            return isDimension;
        }
    }
}
