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

package com.tencent.bk.base.dataflow.bksql.mlsql.rule;

import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlCorrelate;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlRel;
import com.tencent.bk.base.dataflow.bksql.mlsql.rel.MLSqlStaticRel2;
import com.tencent.bk.base.dataflow.bksql.rule.AbstractMLSqlConverterRule;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCorrelate;

public class MLSqlCorrelateRule extends AbstractMLSqlConverterRule {

    public static final MLSqlCorrelateRule INSTANCE = new MLSqlCorrelateRule();

    private MLSqlCorrelateRule() {
        super(LogicalCorrelate.class, "MLSqlCorrelateRule");
    }

    @Override
    public RelNode convert(RelNode relNode) {
        LogicalCorrelate correlate = (LogicalCorrelate) relNode;
        List<RelNode> inputs = correlate.getInputs();
        RelNode left = inputs.get(0);
        RelNode right = inputs.get(1);

        if (isStatic2(left) == isStatic2(right)) {
            // join between 2 static2 tables is not allowed
            return null;
        }
        if (isStatic2(left)) {
            return new MLSqlCorrelate(
                    correlate.getCluster(),
                    correlate.getTraitSet().replace(MLSqlRel.CONVENTION),
                    convert(left, MLSqlStaticRel2.CONVENTION),
                    convert(right, MLSqlRel.CONVENTION),
                    correlate.getCorrelationId(),
                    correlate.getRequiredColumns(),
                    correlate.getJoinType());
        }
        if (isStatic2(right)) {
            return new MLSqlCorrelate(
                    correlate.getCluster(),
                    correlate.getTraitSet().replace(MLSqlRel.CONVENTION),
                    convert(left, MLSqlRel.CONVENTION),
                    convert(right, MLSqlStaticRel2.CONVENTION),
                    correlate.getCorrelationId(),
                    correlate.getRequiredColumns(),
                    correlate.getJoinType());
        }
        throw new AssertionError();
    }
}
