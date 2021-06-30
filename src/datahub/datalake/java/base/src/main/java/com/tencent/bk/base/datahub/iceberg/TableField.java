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

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class TableField {

    private final String name;
    private final Type type;
    private final boolean allowNull;

    /**
     * 构造函数，默认此字段值非null
     *
     * @param name 字段名称
     * @param type 字段类型
     */
    public TableField(String name, String type) {
        this(name, type, false);
    }

    /**
     * 构造函数
     *
     * @param name 字段名称
     * @param type 字段类型
     * @param allowNull 字段值是否能为null值
     */
    public TableField(String name, String type, boolean allowNull) {
        this.name = name.toLowerCase();
        this.type = Utils.convertType(type.toLowerCase());
        this.allowNull = allowNull;
    }

    /**
     * 生成表schema中字段的定义
     *
     * @param id 字段在schema中的编号
     * @return 字段的定义
     */
    public Types.NestedField buildField(int id) {
        if (allowNull) {
            return Types.NestedField.optional(id, name, type);
        } else {
            return Types.NestedField.required(id, name, type);
        }
    }

    public String name() {
        return name;
    }

    public Type type() {
        return type;
    }

    public boolean isAllowNull() {
        return allowNull;
    }

    /**
     * 用于描述对象的字符串
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return String.format("{name=%s, type=%s, allowNull=%s}", name, type.toString(), allowNull);
    }
}