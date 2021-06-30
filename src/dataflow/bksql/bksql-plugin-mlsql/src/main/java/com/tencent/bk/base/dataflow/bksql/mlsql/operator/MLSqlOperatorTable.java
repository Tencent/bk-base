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

package com.tencent.bk.base.dataflow.bksql.mlsql.operator;

import com.google.common.base.CaseFormat;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlAddOsTableFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlAftSurvivalRegressionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlAlsFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlBinarizerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlBisectingKMeansFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlBucketizerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlDctFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlDecisionTreeClassifierFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlDecisionTreeRegressorFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlElementwiseProductFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlGaussianMixtureFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlGbtClassifierFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlGbtRegressorFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlGeneralizedLinearRegressionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlImputerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlIndexToStringFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlInteractionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlIsotonicRegressionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlKMeansFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlLdaFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlLinearRegressionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlLinearSvcFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlLogisticRegressionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlMaxAbsScalerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlMinMaxScalerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlModelLateralFunciton;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlMultilayerPerceptronClassifierFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlMultinomialLogisticRegressionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlNGramFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlNaiveBayesFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlNormalizerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlOneHotEncoderEstimatorFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlOneHotEncoderFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlPcaFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlPolynomialExpansionFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlQuantileDiscretizerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlRandomForestClassifierFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlRandomForestRegressorFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlRegexTokenizerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlStandardScalerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlStopWordsRemoverFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlStringIndexerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlTokenizerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlVectorAssemblerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlVectorIndexerFunction;
import com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.MLSqlVectorSizeHintFunction;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.commons.lang.StringUtils;

public class MLSqlOperatorTable extends ReflectiveSqlOperatorTable {

    public static final SqlFunction ADD_OS = new MLSqlAddOsTableFunction();
    public static final SqlFunction MODEL_FUNC = new MLSqlModelFunction();
    public static final SqlFunction LATERAL_MODEL_FUNC = new MLSqlModelLateralFunciton();
    public static final MLSqlPcaFunction PCA = MLSqlPcaFunction.createFunction();
    public static final MLSqlTokenizerFunction TOKENIZER = MLSqlTokenizerFunction.createFunction();
    public static final MLSqlStopWordsRemoverFunction STOP_WORDS_REMOVER = MLSqlStopWordsRemoverFunction
            .createFunction();
    public static final MLSqlNGramFunction N_GRAM = MLSqlNGramFunction.createFunction();
    public static final MLSqlBinarizerFunction BINARIZER = MLSqlBinarizerFunction.createFunction();
    public static final MLSqlPolynomialExpansionFunction POLYNOMIAL_EXPANSION = MLSqlPolynomialExpansionFunction
            .createFunction();
    public static final MLSqlDctFunction DCT = MLSqlDctFunction.createFunction();
    public static final MLSqlStringIndexerFunction STRING_INDEXER = MLSqlStringIndexerFunction.createFunction();
    public static final MLSqlAftSurvivalRegressionFunction SURVIVAL_REGRESSION = MLSqlAftSurvivalRegressionFunction
            .createFunction();
    public static final MLSqlBisectingKMeansFunction BISECTING_K_MEANS = MLSqlBisectingKMeansFunction.createFunction();
    public static final MLSqlBucketizerFunction BUCKETIZER = MLSqlBucketizerFunction.createFunction();
    public static final MLSqlDecisionTreeClassifierFunction DECISION_TREE_CLASSIFIER =
            MLSqlDecisionTreeClassifierFunction.createFunction();
    public static final MLSqlDecisionTreeRegressorFunction DECISION_TREE_REGRESSOR = MLSqlDecisionTreeRegressorFunction
            .createFunction();
    public static final MLSqlElementwiseProductFunction ELEMENTWISE_PRODUCT = MLSqlElementwiseProductFunction
            .createFunction();
    public static final MLSqlGaussianMixtureFunction GAUSSIAN_MIXTURE = MLSqlGaussianMixtureFunction.createFunction();
    public static final MLSqlGbtClassifierFunction GBT_CLASSIFIER = MLSqlGbtClassifierFunction.createFunction();
    public static final MLSqlGbtRegressorFunction GBT_REGRESSOR = MLSqlGbtRegressorFunction.createFunction();
    public static final MLSqlGeneralizedLinearRegressionFunction GENERALIZED_LINEAR_REGRESSION =
            MLSqlGeneralizedLinearRegressionFunction.createFunction();
    public static final MLSqlImputerFunction IMPUTER = MLSqlImputerFunction.createFunction();
    public static final MLSqlIndexToStringFunction INDEX_TO_STRING = MLSqlIndexToStringFunction.createFunction();
    public static final MLSqlInteractionFunction INTERACTION = MLSqlInteractionFunction.createFunction();
    public static final MLSqlIsotonicRegressionFunction ISOTONIC_REGRESSION_FUNCTION = MLSqlIsotonicRegressionFunction
            .createFunction();
    public static final MLSqlKMeansFunction K_MEANS = MLSqlKMeansFunction.createFunction();
    public static final MLSqlLdaFunction LDA = MLSqlLdaFunction.createFunction();
    public static final MLSqlLinearRegressionFunction LINEAR_REGRESSION = MLSqlLinearRegressionFunction
            .createFunction();
    public static final MLSqlLinearSvcFunction LINEAR_SVC = MLSqlLinearSvcFunction.createFunction();
    public static final MLSqlLogisticRegressionFunction LOGISTIC_REGRESSION = MLSqlLogisticRegressionFunction
            .createFunction();
    public static final MLSqlMaxAbsScalerFunction MAX_ABS_SCALER = MLSqlMaxAbsScalerFunction.createFunction();
    public static final MLSqlMinMaxScalerFunction MIN_MAX_SCALER = MLSqlMinMaxScalerFunction.createFunction();
    public static final MLSqlMultilayerPerceptronClassifierFunction MULTILAYER_PERCEPTRON_CLASSIFIER =
            MLSqlMultilayerPerceptronClassifierFunction.createFunction();
    public static final MLSqlMultinomialLogisticRegressionFunction MULTINOMIAL_LOGISTIC_REGRESSION =
            MLSqlMultinomialLogisticRegressionFunction.createFunction();
    public static final MLSqlNaiveBayesFunction NAIVE_BAYES = MLSqlNaiveBayesFunction.createFunction();
    public static final MLSqlNormalizerFunction NORMALIZER = MLSqlNormalizerFunction.createFunction();
    public static final MLSqlOneHotEncoderEstimatorFunction ONE_HOT_ENCODER_ESTIMATOR =
            MLSqlOneHotEncoderEstimatorFunction.createFunction();
    public static final MLSqlOneHotEncoderFunction ONE_HOT_ENCODER = MLSqlOneHotEncoderFunction.createFunction();
    public static final MLSqlQuantileDiscretizerFunction QUANTILE_DISCRETIZER = MLSqlQuantileDiscretizerFunction
            .createFunction();
    public static final MLSqlRandomForestClassifierFunction RANDOM_FOREST_CLASSIFIER =
            MLSqlRandomForestClassifierFunction.createFunction();
    public static final MLSqlRandomForestRegressorFunction RANDOM_FOREST_REGRESSOR = MLSqlRandomForestRegressorFunction
            .createFunction();
    public static final MLSqlStandardScalerFunction STANDARD_SCALER = MLSqlStandardScalerFunction.createFunction();
    public static final MLSqlVectorAssemblerFunction VECTOR_ASSEMBLER = MLSqlVectorAssemblerFunction.createFunction();
    public static final MLSqlVectorIndexerFunction VECTOR_INDEXER = MLSqlVectorIndexerFunction.createFunction();
    public static final MLSqlVectorSizeHintFunction VECTOR_SIZE_HINT = MLSqlVectorSizeHintFunction.createFunction();
    public static final MLSqlRegexTokenizerFunction REGEX_TOKENIZER_FUNCTION = MLSqlRegexTokenizerFunction
            .createFunction();
    public static final MLSqlAlsFunction ALS_FUNCTION = MLSqlAlsFunction.createFunction();
    private static final List<SqlOperator> OPERATOR_LIST = Arrays.asList(
            //functions
            MODEL_FUNC,
            PCA,
            TOKENIZER,
            STOP_WORDS_REMOVER,
            N_GRAM,
            BINARIZER,
            POLYNOMIAL_EXPANSION,
            DCT,
            STRING_INDEXER,
            SURVIVAL_REGRESSION,
            BISECTING_K_MEANS,
            BUCKETIZER,
            DECISION_TREE_CLASSIFIER,
            DECISION_TREE_REGRESSOR,
            ELEMENTWISE_PRODUCT,
            GAUSSIAN_MIXTURE,
            GBT_CLASSIFIER,
            GBT_REGRESSOR,
            GENERALIZED_LINEAR_REGRESSION,
            IMPUTER,
            INDEX_TO_STRING,
            INTERACTION,
            ISOTONIC_REGRESSION_FUNCTION,
            K_MEANS,
            LDA,
            LINEAR_REGRESSION,
            LINEAR_SVC,
            LOGISTIC_REGRESSION,
            MAX_ABS_SCALER,
            MIN_MAX_SCALER,
            MULTILAYER_PERCEPTRON_CLASSIFIER,
            MULTINOMIAL_LOGISTIC_REGRESSION,
            NAIVE_BAYES,
            NORMALIZER,
            ONE_HOT_ENCODER_ESTIMATOR,
            ONE_HOT_ENCODER,
            QUANTILE_DISCRETIZER,
            RANDOM_FOREST_CLASSIFIER,
            RANDOM_FOREST_REGRESSOR,
            STANDARD_SCALER,
            VECTOR_ASSEMBLER,
            VECTOR_INDEXER,
            VECTOR_SIZE_HINT,
            REGEX_TOKENIZER_FUNCTION,
            ALS_FUNCTION,
            // SET OPERATORS
            SqlStdOperatorTable.UNION,
            SqlStdOperatorTable.UNION_ALL,
            SqlStdOperatorTable.EXCEPT,
            SqlStdOperatorTable.EXCEPT_ALL,
            SqlStdOperatorTable.INTERSECT,
            SqlStdOperatorTable.INTERSECT_ALL,
            // BINARY OPERATORS
            SqlStdOperatorTable.AND,
            SqlStdOperatorTable.AS,
            SqlStdOperatorTable.DIVIDE_INTEGER,
            SqlStdOperatorTable.DOT,
            SqlStdOperatorTable.EQUALS,
            SqlStdOperatorTable.GREATER_THAN,
            SqlStdOperatorTable.IS_DISTINCT_FROM,
            SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            SqlStdOperatorTable.LESS_THAN,
            SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            SqlStdOperatorTable.MINUS,
            SqlStdOperatorTable.MULTIPLY,
            SqlStdOperatorTable.NOT_EQUALS,
            SqlStdOperatorTable.OR,
            SqlStdOperatorTable.PLUS,
            SqlStdOperatorTable.DATETIME_PLUS,
            // POSTFIX OPERATORS
            SqlStdOperatorTable.DESC,
            SqlStdOperatorTable.NULLS_FIRST,
            SqlStdOperatorTable.IS_NOT_NULL,
            SqlStdOperatorTable.IS_NULL,
            SqlStdOperatorTable.IS_NOT_TRUE,
            SqlStdOperatorTable.IS_TRUE,
            SqlStdOperatorTable.IS_NOT_FALSE,
            SqlStdOperatorTable.IS_FALSE,
            SqlStdOperatorTable.IS_NOT_UNKNOWN,
            SqlStdOperatorTable.IS_UNKNOWN,
            // PREFIX OPERATORS
            SqlStdOperatorTable.NOT,
            SqlStdOperatorTable.UNARY_MINUS,
            SqlStdOperatorTable.UNARY_PLUS,
            // GROUPING FUNCTIONS
            SqlStdOperatorTable.GROUP_ID,
            SqlStdOperatorTable.GROUPING,
            SqlStdOperatorTable.GROUPING_ID,
            // AGGREGATE OPERATORS
//            SqlStdOperatorTable.SUM,
//            SqlStdOperatorTable.SUM0,
            SqlStdOperatorTable.COUNT,
            SqlStdOperatorTable.COLLECT,
            SqlStdOperatorTable.MIN,
            SqlStdOperatorTable.MAX,
//            SqlStdOperatorTable.AVG,
            SqlStdOperatorTable.STDDEV_POP,
            SqlStdOperatorTable.STDDEV_SAMP,
            SqlStdOperatorTable.VAR_POP,
            SqlStdOperatorTable.VAR_SAMP,
            // ARRAY OPERATORS
            SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
            SqlStdOperatorTable.ELEMENT,
            // MAP OPERATORS
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            // ARRAY MAP SHARED OPERATORS
            SqlStdOperatorTable.ITEM,
            SqlStdOperatorTable.CARDINALITY,
            // SPECIAL OPERATORS
            SqlStdOperatorTable.ROW,
            SqlStdOperatorTable.OVERLAPS,
            SqlStdOperatorTable.LITERAL_CHAIN,
            SqlStdOperatorTable.BETWEEN,
            SqlStdOperatorTable.SYMMETRIC_BETWEEN,
            SqlStdOperatorTable.NOT_BETWEEN,
            SqlStdOperatorTable.SYMMETRIC_NOT_BETWEEN,
            SqlStdOperatorTable.NOT_LIKE,
            SqlStdOperatorTable.LIKE,
            SqlStdOperatorTable.NOT_SIMILAR_TO,
            SqlStdOperatorTable.SIMILAR_TO,
            SqlStdOperatorTable.CASE,
            SqlStdOperatorTable.REINTERPRET,
            SqlStdOperatorTable.EXTRACT,
            SqlStdOperatorTable.IN,
            // FUNCTIONS
            SqlStdOperatorTable.SUBSTRING,
            SqlStdOperatorTable.OVERLAY,
            SqlStdOperatorTable.TRIM,
            SqlStdOperatorTable.POSITION,
            SqlStdOperatorTable.CHAR_LENGTH,
            SqlStdOperatorTable.CHARACTER_LENGTH,
            SqlStdOperatorTable.UPPER,
            SqlStdOperatorTable.LOWER,
            SqlStdOperatorTable.INITCAP,
            SqlStdOperatorTable.POWER,
            SqlStdOperatorTable.SQRT,
            SqlStdOperatorTable.MOD,
            SqlStdOperatorTable.LN,
            SqlStdOperatorTable.LOG10,
            SqlStdOperatorTable.ABS,
            SqlStdOperatorTable.EXP,
            SqlStdOperatorTable.NULLIF,
            SqlStdOperatorTable.COALESCE,
            SqlStdOperatorTable.FLOOR,
            SqlStdOperatorTable.CEIL,
            SqlStdOperatorTable.LOCALTIME,
            SqlStdOperatorTable.LOCALTIMESTAMP,
            SqlStdOperatorTable.CURRENT_TIME,
            SqlStdOperatorTable.CURRENT_TIMESTAMP,
            SqlStdOperatorTable.CURRENT_DATE,
            SqlStdOperatorTable.CAST,
            SqlStdOperatorTable.EXTRACT,
            SqlStdOperatorTable.QUARTER,
            SqlStdOperatorTable.SCALAR_QUERY,
            SqlStdOperatorTable.EXISTS,
            SqlStdOperatorTable.SIN,
            SqlStdOperatorTable.COS,
            SqlStdOperatorTable.TAN,
            SqlStdOperatorTable.COT,
            SqlStdOperatorTable.ASIN,
            SqlStdOperatorTable.ACOS,
            SqlStdOperatorTable.ATAN,
            SqlStdOperatorTable.DEGREES,
            SqlStdOperatorTable.RADIANS,
            SqlStdOperatorTable.SIGN,
            SqlStdOperatorTable.PI,
            SqlStdOperatorTable.RAND,
            SqlStdOperatorTable.RAND_INTEGER,
            SqlStdOperatorTable.TIMESTAMP_ADD,
            SqlStdOperatorTable.TRUNCATE,
            SqlStdOperatorTable.REPLACE,
            SqlStdOperatorTable.CONCAT,
            ADD_OS);
    private static final MLSqlOperatorTable INSTANCE = new MLSqlOperatorTable();

    private MLSqlOperatorTable() {
        OPERATOR_LIST.forEach(this::register);
    }

    private MLSqlOperatorTable(SqlFunction function) {
        OPERATOR_LIST.forEach(this::register);
        this.register(function);
    }

    public static SqlOperatorTable instance() {
        return INSTANCE;
    }

    public static SqlOperatorTable instance(MLSqlModelLateralFunciton function) {
        if (function != null) {
            INSTANCE.register(function);
        }
        return INSTANCE;
    }

    public static void registerModelFunction(List<SqlOperator> modelFunctions) {
        INSTANCE.getOperatorList().addAll(modelFunctions);
    }

    public static void registerModelFunction(SqlOperator modelFunction) {
        INSTANCE.getOperatorList().add(modelFunction);
    }

    /**
     * 根据函数名称获取具体的函数语法对象
     *
     * @param name 函数名称
     * @return 具体的函数对象
     */
    public static SqlOperator getFunctionByName(String name) {
        SqlOperator function = BlueKingAlgorithmFactory.getAlgorithmFunction(name);
        if (function == null) {
            try {
                String newname = StringUtils
                        .capitalize(CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name.toLowerCase()));
                StringBuffer sb = new StringBuffer();
                sb.append("com.tencent.bk.base.dataflow.bksql.mlsql.operator.tabfunc.");
                sb.append("MLSql");
                sb.append(newname);
                sb.append("Function");
                for (SqlOperator operator : OPERATOR_LIST) {
                    if (Class.forName(sb.toString()).isInstance(operator)) {
                        return operator;
                    }
                }
                return null;
            } catch (Exception ee) {
                return null;
            }
        } else {
            return function;
        }
    }

    public void registerFunction(SqlFunction function) {
        this.register(function);
    }
}
