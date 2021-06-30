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

import com.tencent.bk.base.dataflow.bksql.task.MLSqlTableNameAccess;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskContext;
import com.tencent.bk.base.dataflow.bksql.task.MLSqlTaskGroup;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

public class MLSqlTableFunctionScan extends TableFunctionScan implements MLSqlStaticRel2 {

    public MLSqlTableFunctionScan(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, RexNode rexCall,
            Type elementType, RelDataType rowType, Set<RelColumnMapping> columnMappings) {
        super(cluster, traits, inputs, rexCall, elementType, rowType, columnMappings);
    }

    @Override
    public TableFunctionScan copy(RelTraitSet traits, List<RelNode> inputs, RexNode rexCall, Type elementType,
            RelDataType rowType, Set<RelColumnMapping> columnMappings) {
        return new MLSqlTableFunctionScan(getCluster(), traits, inputs, rexCall, elementType, rowType, columnMappings);
    }

    @Override
    public MLSqlTableNameAccess translateToTask(MLSqlTaskContext context, MLSqlTaskGroup taskGroup, MLSqlRel mlSqlRel) {
        return null;
    }
}
