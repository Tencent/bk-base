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

package com.tencent.bk.base.dataflow.flink.streaming.function;

import com.tencent.bk.base.dataflow.flink.streaming.function.base.IFlinkFunction;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.table.functions.AggregateFunction;

public class FlinkGroupConcat extends AggregateFunction<String, FlinkGroupConcat.GroupConcatAccum> implements
        IFlinkFunction {

    private boolean orderBy = false;
    private String order;
    private boolean distinct = false;

    @Override
    public GroupConcatAccum createAccumulator() {
        return new GroupConcatAccum();
    }

    @Override
    public String getValue(GroupConcatAccum accumulator) {
        if (orderBy) {
            switch (order) {
                case "asc":
                    accumulator.sortedValue.sort((CustomPair::compareTo));
                    break;
                case "desc":
                    accumulator.sortedValue.sort(Collections.reverseOrder());
                    break;
                default:
                    throw new RuntimeException("order params not support: " + order);
            }
            if (distinct) {
                return accumulator.sortedValue
                        .stream()
                        .distinct()
                        .map(o -> String.valueOf(o.getKey()))
                        .collect(Collectors.joining(accumulator.separator));
            } else {
                return accumulator.sortedValue
                        .stream()
                        .map(o -> String.valueOf(o.getKey()))
                        .collect(Collectors.joining(accumulator.separator));
            }
        } else {
            if (distinct) {
                return accumulator.value.stream()
                        .distinct()
                        .collect(Collectors.joining(accumulator.separator));
            } else {
                return accumulator.value.stream()
                        .collect(Collectors.joining(accumulator.separator));
            }
        }
    }

    /**
     * group_concat(value, [separator], [order_value], [ASC/DESC], [DISTINCT])
     *
     * @param acc group_concat agg accum
     * @param args input args
     */
    public void accumulate(GroupConcatAccum acc, Object... args) {
        // sort is or not
        setOrder(args);
        // distinct or not
        setDistinct(args);
        // set separator
        setSeparator(acc, args);

        if (args[0] != null) {
            // order by and no distinct
            onlySort(acc, args);
            // order by and distinct
            sortAndDistinct(acc, args);
            // no order by and no distinct
            if (!orderBy && !distinct) {
                acc.value.add(String.valueOf(args[0]));
            }
            // no order by and distinct
            onlyDistinct(acc, args);
        }
    }

    private void onlyDistinct(GroupConcatAccum acc, Object... args) {
        if (!orderBy && distinct) {
            String tmpValue = String.valueOf(args[0]);
            if (!acc.value.contains(tmpValue)) {
                acc.value.add(tmpValue);
            }
        }
    }

    private void sortAndDistinct(GroupConcatAccum acc, Object... args) {
        if (orderBy && distinct) {
            CustomPair<Object, Object> tmpValue = new CustomPair<>(args[0], args[2]);
            if (!acc.sortedValue.contains(tmpValue)) {
                acc.sortedValue.add(tmpValue);
            }
        }
    }

    private void onlySort(GroupConcatAccum acc, Object... args) {
        if (orderBy && !distinct) {
            acc.sortedValue.add(new CustomPair(args[0], args[2]));
        }
    }

    private void setOrder(Object... args) {
        if (args.length >= 4 && args[2] != null && args[3] != null
                && Arrays.asList("asc", "desc").contains(String.valueOf(args[3]).toLowerCase())) {
            orderBy = true;
            order = String.valueOf(args[3]).toLowerCase();
        }
    }

    private void setDistinct(Object... args) {
        if (args.length == 5 && args[4] != null
                && String.valueOf(args[4]).equalsIgnoreCase("distinct")) {
            distinct = true;
        }
    }

    private void setSeparator(GroupConcatAccum acc, Object... args) {
        if (args.length >= 2 && args[1] != null) {
            acc.separator = String.valueOf(args[1]);
        }
    }

    /**
     * Merges a group of accumulator instances into one accumulator instance. This function must be implemented for
     * datastream session window grouping aggregate and dataset grouping aggregate.
     *
     * @param acc the accumulator which will keep the merged aggregate results. It should be noted that the
     *         accumulator may contain the previous aggregated results. Therefore user should not replace or clean this
     *         instance in the custom merge method.
     * @param it an [[java.lang.Iterable]] pointed to a group of accumulators that will be merged.
     */
    public void merge(GroupConcatAccum acc, Iterable<GroupConcatAccum> it) {
        Iterator<GroupConcatAccum> iter = it.iterator();
        while (iter.hasNext()) {
            GroupConcatAccum a = iter.next();
            if (orderBy) {
                acc.sortedValue.addAll(a.sortedValue);
            } else {
                acc.value.addAll(a.value);
            }
        }
    }

    public void resetAccumulator(GroupConcatAccum acc) {
        acc.value.clear();
        acc.sortedValue.clear();
    }

    @Override
    public String getFunctionName() {
        return "group_concat";
    }

    @Override
    public Object getInnerFunction() {
        return null;
    }

    public static class GroupConcatAccum {

        // no have sort value
        public List<String> value = new ArrayList<>();
        // have sort value, the value is sort reference field
        public List<CustomPair> sortedValue = new ArrayList<>();
        public String separator = ",";
    }

    public static class CustomPair<K, V> implements Serializable, Comparable<CustomPair> {

        private K key;
        private V value;

        /**
         * Creates a new pair
         *
         * @param key The key for this pair
         * @param value The value to use for this pair
         */
        public CustomPair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        /**
         * Gets the key for this pair.
         *
         * @return key for this pair
         */
        public K getKey() {
            return key;
        }

        /**
         * Gets the value for this pair.
         *
         * @return value for this pair
         */
        public V getValue() {
            return value;
        }

        /**
         * <p><code>String</code> representation of this
         * <code>Pair</code>.</p>
         *
         * <p>The default name/value delimiter '=' is always used.</p>
         *
         * @return <code>String</code> representation of this <code>Pair</code>
         */
        @Override
        public String toString() {
            return key + "=" + value;
        }

        /**
         * <p>Generate a hash code for this <code>Pair</code>.</p>
         *
         * <p>The hash code is calculated using both the name and
         * the value of the <code>Pair</code>.</p>
         *
         * @return hash code for this <code>Pair</code>
         */
        @Override
        public int hashCode() {
            return key.hashCode() * 13 + (value == null ? 0 : value.hashCode());
        }

        /**
         * Compare only the keys to determine if the pair is equal.
         *
         * @param o to test for equality with this
         * @return is equal to this pair
         */
        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof CustomPair) {
                CustomPair pair = (CustomPair) o;
                if (getKey() != null ? !getKey().equals(pair.getKey()) : pair.getKey() != null) {
                    return false;
                }
                return true;
            }
            return false;
        }

        @Override
        public int compareTo(CustomPair otherPair) {
            return String.valueOf(value).compareTo(String.valueOf(otherPair.value));
        }
    }
}


