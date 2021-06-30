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

import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.JoinToCorrelateRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;

public class MLSqlRuleSets {

    private static final RuleSet INSTANCE = RuleSets.ofList(
            // general rules
            AggregateStarTableRule.INSTANCE,
            AggregateStarTableRule.INSTANCE2,
            TableScanRule.INSTANCE,
            ProjectMergeRule.INSTANCE,
            FilterTableScanRule.INSTANCE,
            ProjectFilterTransposeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            JoinPushExpressionsRule.INSTANCE,
            AggregateExpandDistinctAggregatesRule.INSTANCE,
//            AggregateReduceFunctionsRule.INSTANCE,
            FilterAggregateTransposeRule.INSTANCE,
            ProjectWindowTransposeRule.INSTANCE,
            JoinCommuteRule.INSTANCE,
            JoinPushThroughJoinRule.RIGHT,
            JoinPushThroughJoinRule.LEFT,
            SortProjectTransposeRule.INSTANCE,
            SortJoinTransposeRule.INSTANCE,
            SortUnionTransposeRule.INSTANCE,

            // removal rules
            AggregateProjectPullUpConstantsRule.INSTANCE2,
            UnionPullUpConstantsRule.INSTANCE,
            PruneEmptyRules.UNION_INSTANCE,
            PruneEmptyRules.INTERSECT_INSTANCE,
            PruneEmptyRules.MINUS_INSTANCE,
            PruneEmptyRules.PROJECT_INSTANCE,
            PruneEmptyRules.FILTER_INSTANCE,
            PruneEmptyRules.SORT_INSTANCE,
            PruneEmptyRules.AGGREGATE_INSTANCE,
            PruneEmptyRules.JOIN_LEFT_INSTANCE,
            PruneEmptyRules.JOIN_RIGHT_INSTANCE,
            PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
            UnionMergeRule.INSTANCE,
            UnionMergeRule.INTERSECT_INSTANCE,
            UnionMergeRule.MINUS_INSTANCE,
            ProjectToWindowRule.PROJECT,
            FilterMergeRule.INSTANCE,
            DateRangeRules.FILTER_INSTANCE,
            IntersectToDistinctRule.INSTANCE,
            JoinToCorrelateRule.INSTANCE,
            MLSqlProjectRule.INSTANCE,
            MLSqlCorrelateRule.INSTANCE);

    private MLSqlRuleSets() {

    }

    public static RuleSet instance() {
        return INSTANCE;
    }
}
