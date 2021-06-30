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

package com.tencent.bk.base.dataflow.bksql.mlsql.parser;

import com.tencent.bk.base.dataflow.bksql.deparser.MLSqlDeParser;
import com.tencent.bk.base.dataflow.bksql.mlsql.exceptions.ErrorCode;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.MLSqlOperatorTable;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlRel;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlRels;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlInterpreterValue;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlProcessor;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlProcessorType;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTableNameAccess;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTask.Builder;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTransformType;
import com.tencent.bk.base.dataflow.bksql.util.MLSqlModelUtils;
import com.tencent.bk.base.dataflow.bksql.rest.exception.MessageLocalizedExceptionV1;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.MLSqlModel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.lang.StringUtils;

/**
 * 此类用于解析语法树中各个元素，以得到计算时需要的模型及其参数信息
 */
public class RelParser {

    public static final String MLSQL_MODEL_NAME = "model_name";
    public static final String MLSQL_ALGORITHM_NAME = "algorithm";
    public static final String MLSQL_CREATE_TARGET_TABLE = "create_target_table";
    public static final String MLSQL_EVALUATE_LABEL = "evaluate_label";
    public static final String MLSQL_EVALUATE_FUNCTION = "evaluate_function";
    public static final String MLSQL_EVALUATE_TYPE = "evaluate_type";
    public static final String PARAM_TYPE_ARRAY = "array";
    public static final String PARAM_TYPE_VECTOR = "vector";
    public static final Integer INTERPRETE_ONLY_PARENT = -2;
    public static final Integer INTERPRETE_ALL_MEMBER = -1;
    public static final String DEFAULT_FUNCTION_OUTPUT = "default_function_output";
    public static final Pattern MLSQL_ALGORITHM_PARAM_PATTERN = Pattern
            .compile("(([a-z]|[A-Z]|[0-9]|_)+)_(ARRAY|VECTOR)_INDEX_([0-9]+)(_(ARRAY|VECTOR)_([0-9]+))*");
    public static String MODEL_NAME_KEY = "modelName";
    public static String FUNCTION_KEY = "function";
    public static String EVALUATE_MAP_KEY = "evaluate_map";
    public static String EVALUATE_FUNCTION_KEY = "evaluate_function";
    public static String EVALUATE_LABEL_KEY = "evaluate_label";
    public static String PREDICT_LABEL_KEY = "predict_label";
    public static String TARGET_TABLE_KEY = "targetTable";
    public static String MODEL_OBJECT_KEY = "model_object";
    public static String LATERAL_OUTPUT_KEY = "lateral_function_output";

    /**
     * 解析模型函数
     * 模型函数poc_odel(feature=[age,name],spark_engine=gender,executornum=4)
     * 经过优化后其具体格式为:
     * MODEL_FUNC(||(CAST($cor0.age):VARCHAR CHARACTER SET "UTF-8", _UTF-8'feature_INDEX_0'),
     * ||($cor0.name, _UTF-8'feature_INDEX_1'),
     * ||(_UTF-8'poc_model', _UTF-8'model_name'),
     * ||(CAST($cor0.gender):VARCHAR CHARACTER SET "UTF-8", _UTF-8'spark_engine')
     * ||(CAST(4):VARCHAR CHARACTER SET "UTF-8" NOT NULL, _UTF-8'executornum')
     * 说明：
     * 1. 由于calcite不支持k=[a,b,c]格式参数解析，所以在解析中，将list拆分为带有INDEX的单个元素；
     * 2. 对于feature,spark_engine,executornum这类参数名，即需要保留到语法树的最终结果，同时又不能对其按列名的校验，所以需要将其转换为常量参数，
     * 同时使用“||”运行符连接以保证其不会丢失（对于普通常量语法优化后会被抛弃掉）(这里的处理后续需要优化，目前没有找到更好的方法，只能先用concat，
     * 用concat有一个副作用，就是可能会引入一个隐藏的cast过程，这个在解析的时候需要注意)
     *
     * @Parameter call
     * @Parameter args
     * @Parameter inFieldNames
     */
    public static Map<String, Object> parseFunctionNode(RexCall call, List<String> inFieldNames,
            MLSqlProcessor processor) {
        Map<String, Object> args = new LinkedHashMap<>();
        FunctionParserParams functionParserParams = new FunctionParserParams();
        for (RexNode rexNode : call.getOperands()) {
            Object attributeValue = null;
            RexCall rexCall = (RexCall) rexNode;
            if (rexCall.getOperator() == SqlStdOperatorTable.CONCAT) {
                RexNode name = rexCall.getOperands().get(1);
                RexNode value = rexCall.getOperands().get(0);
                String attributeName = ((RexLiteral) name).getValueAs(String.class);
                if (value instanceof RexLiteral) {
                    attributeValue = ((RexLiteral) value).getValueAs(String.class);
                    parseRexLiteral(attributeName, attributeValue, functionParserParams);
                } else if (value instanceof RexInputRef) {
                    attributeValue = inFieldNames.get(((RexInputRef) value).getIndex());
                } else if (value instanceof RexFieldAccess) {
                    attributeValue = ((RexFieldAccess) value).getField().getName();
                } else if (value instanceof RexCall) {
                    RexCall callValue = (RexCall) value;
                    attributeValue = parseRexCall(attributeName, callValue, inFieldNames, functionParserParams);
                }
                if (!(MLSQL_MODEL_NAME.equalsIgnoreCase(attributeName)
                        || MLSQL_ALGORITHM_NAME.equalsIgnoreCase(attributeName)
                        || MLSQL_EVALUATE_FUNCTION.equalsIgnoreCase(attributeName)
                        || MLSQL_EVALUATE_LABEL.equalsIgnoreCase(attributeName))) {
                    dealWithRegex(args, attributeName, attributeValue);
                }
            }
        }
        processor.setName(functionParserParams.getAlgorithmName());
        processor.setArgs(args);
        processor.setType(functionParserParams.getType().getValue());
        return getFunctionParamsResult(functionParserParams);
    }

    /**
     * 使用正则表达式解析用户输入参数中的向量（数组）以及多重向量（数组）
     *
     * @param args 已经解析出的数据列表，解析出的向量值需要存入此map
     * @param attributeName 属性名称
     * @param attributeValue 属性值，如[feature_ARRAY_INDEX_1_VECTOR_1=1]，表示：
     *         属性feature是一个数组，其第一个元素为一个vector，该vector的第一个元素值为1，
     *         解析后即为：feature=[<1,>,]
     */
    public static void dealWithRegex(Map<String, Object> args,
            String attributeName,
            Object attributeValue) {
        Matcher matcher = MLSQL_ALGORITHM_PARAM_PATTERN.matcher(attributeName);
        if (!matcher.find()) {
            args.put(attributeName, attributeValue);
        } else {
            String realAttributeName = matcher.group(1);
            String type = matcher.group(3);
            if (matcher.group(5) != null) {
                //解析双重数组或是向量
                String subType = matcher.group(6);
                //从正则中解析第一层的index
                int index = Integer.valueOf(matcher.group(4)).intValue();
                formatMultiLayerParams(args, realAttributeName, attributeValue, index, subType);
            } else {
                //解析单层的向量或数组
                formatSingleLayerParams(args, realAttributeName, attributeValue, type);
            }
        }
    }

    /**
     * 解析多层数组或向量的参数，如feature=[[a,b]]这种格式，经过扁平化处理为
     * feature_array_1_array_1=a,feature_array_1_array_2=b
     *
     * @param args 存储解析结果的Map
     * @param attributeName 参数名称，如上述例子中的feature
     * @param attributeValue 参数值，如上述例子中的a,b,c
     * @param elementIndex 当前值在参数中的索引值
     * @param subType 子元素的类型，Array或是Vector
     */
    public static void formatMultiLayerParams(Map<String, Object> args,
            String attributeName, Object attributeValue,
            int elementIndex, String subType) {
        if (args.containsKey(attributeName)) {
            //参数列表中已经有了此参数
            //获取已有列表
            if (PARAM_TYPE_VECTOR.equalsIgnoreCase(subType)) {
                Vector valueList = (Vector) args.get(attributeName);
                addToExistVectorParams(valueList, elementIndex, attributeValue);
            } else {
                ArrayList valueList = (ArrayList) args.get(attributeName);
                addToExistArrayParams(valueList, elementIndex, attributeValue);
            }
        } else {
            //此值在已解析的参数列表里还没有，表示是全新的一个参数
            Collection valueList = formatNewParams(subType, attributeValue);
            args.put(attributeName, valueList);
        }
    }

    /**
     * 将解析出的attibuteValue添加至已有的向量类型的参数值列表，如目前已解析出feature为向量[[a,b,c]],
     * 现又有参数feature_array_1_array_4=d，解析后，feature的值应该为[[a,b,c,d]]
     *
     * @param existValueList 已有参数值列表
     * @param elementIndex 当前元素索引
     * @param attributeValue 具体的参数值
     */
    public static void addToExistVectorParams(Vector existValueList, int elementIndex, Object attributeValue) {
        if (existValueList.size() == elementIndex) {
            //当前列表已有n个元素（其index最大为n-1）,解析中又现index值为n的，则表示此解析元素为列表的新的元素
            //需要追加至列表尾
            Vector subValueList = new Vector();
            subValueList.add(attributeValue);
            existValueList.add(subValueList);
        } else {
            //解析出的为已有列表的新的值，则直接加到子列表内部
            Vector subValueList = (Vector) existValueList.get(elementIndex);
            subValueList.add(attributeValue);
        }
    }

    /**
     * 将解析出的attibuteValue添加至已有的向量类型的参数值列表，如目前已解析出feature为数组[[a,b,c]],
     * 现又有参数feature_array_1_array_4=d，解析后，feature的值应该为[[a,b,c,d]]
     *
     * @param existValueList 已有参数值列表
     * @param elementIndex 当前元素索引
     * @param attributeValue 具体的参数值
     */
    public static void addToExistArrayParams(ArrayList existValueList, int elementIndex, Object attributeValue) {
        if (existValueList.size() == elementIndex) {
            //当前列表已有n个元素（其index最大为n-1）,解析中又现index值为n的，则表示此解析元素为列表的新的元素
            //需要追加至列表尾
            ArrayList subValueList = new ArrayList();
            subValueList.add(attributeValue);
            existValueList.add(subValueList);
        } else {
            //解析出的为已有列表的新的值，则直接加到子列表内部
            ArrayList subValueList = (ArrayList) existValueList.get(elementIndex);
            subValueList.add(attributeValue);
        }
    }

    /**
     * 将解析出的attibuteValue生成新的列表并返回
     *
     * @param subType 元素值类型,Vector或Array
     * @param attributeValue 具体的参数值
     * @return 新生成的参数值列表
     */
    public static Collection formatNewParams(String subType, Object attributeValue) {
        Collection valueList;
        if (PARAM_TYPE_VECTOR.equalsIgnoreCase(subType)) {
            valueList = new Vector();
        } else {
            valueList = new ArrayList();
        }
        List subValueList;
        if (PARAM_TYPE_VECTOR.equalsIgnoreCase(subType)) {
            subValueList = new Vector();
        } else {
            subValueList = new ArrayList();
        }
        subValueList.add(attributeValue);
        valueList.add(subValueList);
        return valueList;
    }

    /**
     * 解析单层的数组或向量参数，如feature=[a]这种格式，经过扁平化处理为
     * feature_array_1=a
     *
     * @param args 存储解析结果的Map
     * @param attributeName 参数名称，如上述例子中的feature
     * @param attributeValue 参数值，如上述例子中的a,b,c
     * @param type 当前元素的类型,Array或Vector
     */
    public static void formatSingleLayerParams(Map<String, Object> args, String attributeName,
            Object attributeValue,
            String type) {
        if (args.containsKey(attributeName)) {
            Collection valueList = (Collection) args.get(attributeName);
            valueList.add(attributeValue);
        } else {
            Collection valueList;
            if (PARAM_TYPE_VECTOR.equalsIgnoreCase(type)) {
                valueList = new Vector();
            } else {
                valueList = new ArrayList();
            }
            valueList.add(attributeValue);
            args.put(attributeName, valueList);
        }
    }

    /**
     * 针对用户输入的数组或是向量，结合最终算法的要求，生成对应的转换器（interpreter）,转换器会应用于执行任务的json文件中
     * 对于一个函数的参数input_cols，在如下情况下，需要进行interpreter,即在算法配置中，此参数需要加入函数的interpreterList
     * 1. 算法中有函数setInputCols
     * 2. setInputCols的参数为String
     * 如果setInputCols函数的参数本身为vector或是array，那这个参数是不需要interpret的，在输入时按数组输入即可
     * 例如：
     * 用户输入示例为:
     * input_cols=[a,b,[c,d],[e,f],[g,h],[i,j]]
     * 算法要求的对应的转换规则(即function.needInterpreterCols)为：
     * ['input_cols','input_cols.*', 'input_cols.2', 'input_cols.3']
     * 其意义为：
     * 1. input_cols本身作为一个数组需要进行转换
     * 2. input_cols.* 表示其所有子元素（数组或向量）均需要转换(此规则与下面规则一般不会重复)
     * 3. input_cols.2(3) 表示input_cols的第2(3)个元素需要进行转换
     * 最终的转换即为：为每个需要转换的元素生成一个有固定名称的转换器(interpreter),供UC计算时取用
     * 上述例子最终生成结果为：
     * input_cols=input_cols
     * 对应转换器为 input_cols=(value:[a,b,input_cols_2,input_cols_3,[g,h],[i,j]], implement:Arrays)
     * input_cols_2=(value:[c,d], implement:Vectors)
     * input_cols_3=(value:[e,f], implement:Arrays)
     *
     * @param function 当前解析出的函数，通过其可以得到哪些变量需要进行转换
     * @param args 已经解析出的数据列表
     * @return 所有转换器及其对应的名称
     */
    public static Map<String, MLSqlInterpreterValue> parseInterpreter(MLSqlModelFunction function,
            Map<String, Object> args) {
        Map<String, MLSqlInterpreterValue> map = new LinkedHashMap<>();
        List<String> needInterpreterList = function.getNeedInterpreterCols();
        for (String columnInfo : needInterpreterList) {
            List<Integer> splitIndexes = new ArrayList<>();
            String column = getInterpreterColumn(columnInfo, splitIndexes);
            Object value = args.get(column);
            if (value == null) {
                //用户未输入此参数 do nothing
            } else if (!(value instanceof Collection)) {
                //用户输入的参数值不是集合
                String[] valueArray = ((String) value).split("\\.");
                if (valueArray.length < 2) {
                    //do nothing
                } else {
                    Map<String, String> labelMap = new HashMap<>();
                    labelMap.put(valueArray[0], valueArray[1]);
                    args.put(column, labelMap);
                }
            } else {
                Collection oldValue = (Collection) value;
                //处理子元素
                List newValueList = interpreteSubElements(oldValue, splitIndexes, column, map);
                args.put(column, newValueList);
                //处理顶层整个属性
                if (splitIndexes.contains(INTERPRETE_ONLY_PARENT)) {
                    interpreteWholeCollection(oldValue, args, column, map);
                }
            }
        }
        return map;
    }

    /**
     * 翻译列表中的各个子元素
     *
     * @param attributeValue 参数值
     * @param interpreterIndexes 需要翻译的元素索引
     * @param column 参数名称（多数情况下为列名）
     * @param map 用于存储最终解析结果的Map
     * @return 翻译之后的新的元素列表
     */
    public static List interpreteSubElements(Collection attributeValue, List<Integer> interpreterIndexes,
            String column, Map<String, MLSqlInterpreterValue> map) {
        List newValueList = new ArrayList();
        Object[] valueArray = attributeValue.toArray();
        for (int index = 0; index < valueArray.length; index++) {
            if (interpreterIndexes.contains(INTERPRETE_ALL_MEMBER) || interpreterIndexes.contains(index)) {
                Object itemValue = valueArray[index];
                String interpreterName = column + "_" + index;
                MLSqlInterpreterValue interpreterValue = new MLSqlInterpreterValue();
                interpreterValue.setValue(itemValue);
                if (itemValue instanceof Vector) {
                    interpreterValue.setImplement("Vectors");
                    newValueList.add(interpreterName);
                    map.put(interpreterName, interpreterValue);
                } else if (itemValue instanceof ArrayList) {
                    interpreterValue.setImplement("Arrays");
                    newValueList.add(interpreterName);
                    map.put(interpreterName, interpreterValue);
                } else {
                    newValueList.add(valueArray[index]);
                }
            } else {
                newValueList.add(valueArray[index]);
            }
        }
        return newValueList;
    }

    /**
     * 翻译整个列表，如用户输入为[a,b,[c,d]]，此函数将用户输入转换为Array或Vector，并不处理其中子元素[c,d]
     *
     * @param attributeValue 用户输入的参数值
     * @param args 用于存储用户所有参数的Map
     * @param column 参数名称
     * @param map 用户记录所有翻译结果的Map
     */
    public static void interpreteWholeCollection(Collection attributeValue, Map<String, Object> args,
            String column,
            Map<String, MLSqlInterpreterValue> map) {
        Object newValue = args.get(column);
        args.put(column, column);
        MLSqlInterpreterValue interpreterValue = new MLSqlInterpreterValue();
        interpreterValue.setValue(newValue);
        if (attributeValue instanceof Vector) {
            interpreterValue.setImplement("Vectors");
        } else {
            interpreterValue.setImplement("Arrays");
        }
        map.put(column, interpreterValue);
    }

    /**
     * 根据算法指定的翻译规则，获取需要进行翻译处理的参数名称，以及需要翻译的子元素索
     * 如用户指定规则为:input_cols.1,表示input_cols的第1个元素需要翻译，那此函数返回input_cols,indexList=[1]
     * 如用户指定规则为 input_cols.2,input_cols.3,表示input_cols的第2，3个元素需要翻译，则返回input_cols,indexList=[2,3]
     * 如果用户指定为input_cols，表示仅需要翻译input_cols本身，则返回input_cols,indexList=[-2](-2表示此特定规则)
     * 如果用户指定为input_cols.*，则表示需要翻译input_cols的每一个元素，则返回input_cols, indexList=[-1]（-1表示此特定规则）
     *
     * @param interpreterRule 用户输入的翻译规则
     * @param interpreterIndexList 需要记录的元素索引
     * @return 需要翻译的属性名称
     */
    public static String getInterpreterColumn(String interpreterRule, List<Integer> interpreterIndexList) {
        String[] colInterpreterSplits = interpreterRule.split(",");
        String column = null;
        for (String split : colInterpreterSplits) {
            String[] splitInfo = split.split("\\.");
            column = splitInfo[0];
            if (splitInfo.length > 1) {
                if ("*".equalsIgnoreCase(splitInfo[1])) {
                    //表示所有成员均需要转义
                    interpreterIndexList.add(INTERPRETE_ALL_MEMBER);
                } else {
                    //表示仅指定成员需要转义
                    interpreterIndexList.add(Integer.valueOf(splitInfo[1]));
                }
            } else {
                //仅需要转换其本身，成员不用转换
                interpreterIndexList.add(INTERPRETE_ONLY_PARENT);
            }
        }
        return column;
    }

    /**
     * 处理train语法解析结果里，concat操作保持的参数，即options里用户输入参数信息
     *
     * @param attributeName concat中的参数名称
     * @param inFieldNames 用于存储表列名，索引与元数据中列的索引一致
     * @param args 用于存储所有解析结果的参数map
     * @param value concat中的参数值，可能是常数，列名或是其它函数操作
     */
    public static void parseTrainOptions(String attributeName,
            List<String> inFieldNames,
            Map<String, Object> args,
            RexNode value) {
        Object attributeValue = null;
        if (value instanceof RexLiteral) {
            attributeValue = ((RexLiteral) value).getValueAs(String.class);
        } else if (value instanceof RexCall) {
            RexCall callValue = (RexCall) value;
            RexNode callValueOperand = callValue.getOperands().get(0);
            if (callValueOperand instanceof RexLiteral) {
                attributeValue = ((RexLiteral) callValueOperand).getValue();
            } else if (callValueOperand instanceof RexInputRef) {
                attributeValue = inFieldNames.get(((RexInputRef) callValueOperand).getIndex());
            }
        } else if (value instanceof RexInputRef) {
            RexInputRef refValue = (RexInputRef) value;
            attributeValue = inFieldNames.get(refValue.getIndex());
        }
        dealWithRegex(args, attributeName, attributeValue);
    }

    /**
     * 解析train操作中的各个参数，包括模型名字、Options信息等
     *
     * @param attributeName concat中的参数名称
     * @param value concat中的参数值，可能是常数，列名或是其它函数操作
     * @param processor 存储最终transform中processor信息
     * @param inFieldNames 用于存储表列名，索引与元数据中列的索引一致
     * @param args 用于存储所有解析结果的参数map
     * @param builder 用于生成最终task的builder，主要用于设置modelName及type
     * @return 返回解析的函数及模型
     */
    public static Map<String, Object> parseTrainParameters(String attributeName,
            RexNode value,
            MLSqlProcessor processor,
            List<String> inFieldNames,
            Map<String, Object> args,
            Builder builder) {
        Map<String, Object> result = new HashMap<>();
        String modelName = null;
        MLSqlModelFunction function = null;
        if (attributeName.equalsIgnoreCase(RelParser.MLSQL_MODEL_NAME)) {
            //处理保留字之一：model_name,在train的时候用于
            modelName = ((RexLiteral) value).getValueAs(String.class);
            builder.modelName(modelName);
        } else if (attributeName.equalsIgnoreCase(RelParser.MLSQL_ALGORITHM_NAME)) {
            //处理保留字之二：algorithm_name,在train的时候，需要在options里指明
            String algorithmName = ((RexLiteral) value).getValueAs(String.class);
            function = (MLSqlModelFunction) MLSqlOperatorTable.getFunctionByName(algorithmName);
            if (function == null) {
                throw new MessageLocalizedExceptionV1(
                        "model.not.found.error",
                        new Object[]{algorithmName},
                        MLSqlDeParser.class,
                        ErrorCode.MODEL_OR_ALG_NOT_FOUNT_ERR
                );
            }
            processor.setName(algorithmName);
        } else {
            //其它非保留参数
            //这里主要针对的是train过程中的options参数
            //其格式与预测时类似，但使用方式不是函数，因此是逐个返回
            RelParser.parseTrainOptions(attributeName, inFieldNames, args, value);
            processor.setType(MLSqlProcessorType.train.getValue());
            processor.setArgs(args);
            builder.type(MLSqlTransformType.model);
        }
        if (!StringUtils.isBlank(modelName)) {
            result.put(MODEL_NAME_KEY, modelName);
        }
        if (function != null) {
            result.put(FUNCTION_KEY, function);
        }
        return result;
    }

    /**
     * 生成builder里的基本信息，包括名称，id，描述信息等
     *
     * @param targetTableName 目标表名称（可能有）
     * @param modelName 使用到的模型名称（生成的或使用的）
     * @param context sql执行的上下文件信息
     * @param model 使用或生成的模型对象
     * @param evaluateMap 评估信息（可能有）
     * @param rel 语法信息，与context相结合得到输入表的信息
     * @param builder builder对象
     */
    public static void buildBasicParams(String targetTableName, String modelName, MLSqlTaskContext context,
            MLSqlModel model, Map<String, String> evaluateMap, MLSqlRel rel, Builder builder) {
        StringBuilder idBuilder = new StringBuilder();
        if (!StringUtils.isBlank(targetTableName)) {
            //create and insert
            builder.tableName(targetTableName);
            idBuilder.append(modelName).append("_run");
        } else {
            //train
            builder.tableName(MLSqlRels.generateTableName(context, rel));
            idBuilder.append(modelName).append("_train");
        }
        builder.name(idBuilder.toString());
        if (model != null) {
            builder.model(model);
        }
        if (evaluateMap != null) {
            builder.evaluateMap(evaluateMap);
        }

        builder.id(idBuilder.toString());
        builder.description(idBuilder.toString());
    }

    /**
     * 向Builder中添加transform相关的信息，包括processor, function等
     *
     * @param inTask 从sql中解析的任务对象
     * @param isLateral 是否为lateral操作的标记
     * @param processor 参数中的Processor对象
     * @param function 算法信息
     * @param builder builder对象
     * @return 返回从任务中解析得到的模型名称（如果有），以便后续操作使用
     */
    public static String buildTransformParams(MLSqlTableNameAccess inTask, boolean isLateral,
            MLSqlProcessor processor, MLSqlModelFunction function, String modelName, Builder builder) {
        if (isLateral) {
            //lateral-create,由于lateral语句解析的特殊性，这里直接使用inTask的结果
            MLSqlTask mlSqlTask = (MLSqlTask) inTask;
            builder.processor(mlSqlTask.getProcessor());
            builder.type(mlSqlTask.getType());
            //生成interpreter
            mlSqlTask.getInterpreter().forEach((key, value) -> builder.addInterpreter(key, value));
            modelName = mlSqlTask.getName();
            builder.model(mlSqlTask.getModel());
        } else {
            //insert or normal-create
            builder.processor(processor);
            Map<String, MLSqlInterpreterValue> map = parseInterpreter(function, processor.getArgs());
            map.forEach((key, value) -> builder.addInterpreter(key, value));
        }
        if (!StringUtils.isBlank(inTask.getTableName())) {
            builder.addParent(inTask.getTableName());
        }
        return modelName;
    }

    /**
     * 解析SqlNode中文本(literal)参数信息
     *
     * @param attributeName 属性名称
     * @param attributeValue 属性值
     * @param functionParserParams 用于存储结果的函数参数对象
     */
    public static void parseRexLiteral(String attributeName, Object attributeValue,
            FunctionParserParams functionParserParams) {
        if (attributeName.equalsIgnoreCase(MLSQL_MODEL_NAME)) {
            //attribute value为模型的名字
            String modelName = String.valueOf(attributeValue);
            functionParserParams.setModelName(modelName);
            MLSqlModel model = MLSqlModelUtils.getMLSqlModel(modelName);
            String algorithmName = null;
            if (model == null) {
                algorithmName = modelName;
                functionParserParams.setType(MLSqlProcessorType.untrained_run);
                //这种情况下返回一个虚拟的模型，用于存储特征提取用的算法信息
                model = new MLSqlModel();
                model.setAlgorithmName(algorithmName);
                model.setModelName(modelName);
            } else {
                algorithmName = model.getAlgorithmName();
            }
            functionParserParams.setModel(model);
            functionParserParams.setAlgorithmName(algorithmName);
            MLSqlModelFunction function = (MLSqlModelFunction) MLSqlOperatorTable
                    .getFunctionByName(algorithmName);
            if (function == null) {
                throw new MessageLocalizedExceptionV1(
                        "model.not.found.error",
                        new Object[]{modelName},
                        MLSqlDeParser.class,
                        ErrorCode.MODEL_OR_ALG_NOT_FOUNT_ERR
                );
            }
            functionParserParams.setFunction(function);
        } else if (attributeName.equalsIgnoreCase(MLSQL_EVALUATE_FUNCTION)) {
            String evaluateFunction = String.valueOf(attributeValue);
            functionParserParams.setEvaluateFunction(evaluateFunction);
        } else if (attributeName.equalsIgnoreCase(MLSQL_EVALUATE_LABEL)) {
            String evaluateLabel = String.valueOf(attributeValue);
            functionParserParams.setEvaluateLabel(evaluateLabel);
        }
    }

    /**
     * 解析RexCall对象
     *
     * @param attributeName 属性名称
     * @param callValue 输入的RexCall对象
     * @param inFieldNames 表字段列表，主要是用于根据索引找到具体的字段名称
     * @param functionParserParams 存储解析结果的函数参数
     * @return 根据RexCall操作参数的不同，返回不同类型的属性值
     */
    public static Object parseRexCall(String attributeName, RexCall callValue, List<String> inFieldNames,
            FunctionParserParams functionParserParams) {
        Object attributeValue = null;
        RexNode callValueOperand = callValue.getOperands().get(0);
        if (callValueOperand instanceof RexLiteral) {
            attributeValue = ((RexLiteral) callValueOperand).getValue();
        } else if (callValueOperand instanceof RexInputRef) {
            attributeValue = inFieldNames.get(((RexInputRef) callValueOperand).getIndex());
            if (attributeName.equalsIgnoreCase(MLSQL_EVALUATE_LABEL)) {
                String evaluateLabel = String.valueOf(attributeValue);
                functionParserParams.setEvaluateLabel(evaluateLabel);
            }
        } else if (callValueOperand instanceof RexFieldAccess) {
            attributeValue = ((RexFieldAccess) callValueOperand).getField().getName();
        }
        return attributeValue;
    }

    /**
     * 从函数参数对象中解析需要返回的内容，并写入map中返回
     *
     * @param functionParserParams 经解析得到的函数参数对象
     * @return 需要向上层返回的内容
     */
    public static Map<String, Object> getFunctionParamsResult(FunctionParserParams functionParserParams) {
        Map<String, Object> result = new HashMap<>();
        result.put(MODEL_NAME_KEY, functionParserParams.getModelName());
        result.put(FUNCTION_KEY, functionParserParams.getFunction());
        if (functionParserParams.getModel() != null) {
            result.put(MODEL_OBJECT_KEY, functionParserParams.getModel());
        }
        Map<String, String> evaluateMap = new HashMap<>();
        if (functionParserParams.getFunction() != null) {
            evaluateMap.put(MLSQL_EVALUATE_TYPE, functionParserParams.getFunction().getAlgorithmType());
        }
        if (StringUtils.isNotBlank(functionParserParams.getEvaluateFunction())) {
            evaluateMap.put(EVALUATE_FUNCTION_KEY, functionParserParams.getEvaluateFunction());
        }
        if (StringUtils.isNotBlank(functionParserParams.getEvaluateLabel())) {
            evaluateMap.put(EVALUATE_LABEL_KEY, functionParserParams.getEvaluateLabel());
        }
        result.put(EVALUATE_MAP_KEY, evaluateMap);
        return result;
    }

}
