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

package com.tencent.bk.base.datahub.iceberg;

import static com.tencent.bk.base.datahub.iceberg.C.BUCKET;
import static com.tencent.bk.base.datahub.iceberg.C.DAY;
import static com.tencent.bk.base.datahub.iceberg.C.HOUR;
import static com.tencent.bk.base.datahub.iceberg.C.IDENTITY;
import static com.tencent.bk.base.datahub.iceberg.C.MONTH;
import static com.tencent.bk.base.datahub.iceberg.C.TRUNCATE;
import static com.tencent.bk.base.datahub.iceberg.C.YEAR;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class PartitionMethod {

    private static final Set<String> NO_ARGS_METHODS = Stream.of(YEAR, MONTH, DAY, HOUR, IDENTITY)
            .collect(Collectors.toSet());
    private static final Set<String> ONE_ARGS_METHODS = Stream.of(BUCKET, TRUNCATE)
            .collect(Collectors.toSet());

    private final String field;
    private final String method;
    private int args;

    /**
     * 构造函数。字段名称和分区方法均会转换为小写字符。
     *
     * @param field 字段名称
     * @param method 分区方法
     */
    public PartitionMethod(String field, String method) {
        Preconditions.checkArgument(NO_ARGS_METHODS.contains(method.toLowerCase()),
                "not a valid partition transformer " + method);
        this.field = field.toLowerCase();
        this.method = method.toLowerCase();
    }

    /**
     * 构造函数。字段名称和分区方法均会转换为小写字符。
     *
     * @param field 字段名称
     * @param method 分区方法
     * @param args 分区方法对应的参数值
     */
    public PartitionMethod(String field, String method, int args) {
        Preconditions.checkArgument(ONE_ARGS_METHODS.contains(method.toLowerCase()),
                String.format("not a valid partition transformer %s(%d)", method, args));
        this.field = field.toLowerCase();
        this.method = method.toLowerCase();
        this.args = args;
    }

    /**
     * 添加分区方法
     *
     * @param builder 分区builder对象
     * @return 分区builder对象
     */
    public PartitionSpec.Builder addPartitionMethod(PartitionSpec.Builder builder) {
        switch (method) {
            case YEAR:
                return builder.year(field);
            case MONTH:
                return builder.month(field);
            case DAY:
                return builder.day(field);
            case HOUR:
                return builder.hour(field);
            case IDENTITY:
                return builder.identity(field);
            case BUCKET:
                return builder.bucket(field, args);
            case TRUNCATE:
                return builder.truncate(field, args);
            default:
                // 逻辑不会走到这里，因为构造函数里校验了method的值。
                throw new RuntimeException("partition transformer not supported: " + method);
        }
    }

    /**
     * 获取分区字段名称，小写字符
     *
     * @return 字段名称
     */
    public String getField() {
        return field;
    }

    /**
     * 获取分区的方法，小写字符
     *
     * @return 分区方法名
     */
    public String getMethod() {
        return method;
    }
}