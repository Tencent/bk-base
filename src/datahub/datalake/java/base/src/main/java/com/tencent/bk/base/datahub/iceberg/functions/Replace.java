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

package com.tencent.bk.base.datahub.iceberg.functions;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.data.Record;

public class Replace implements ValFunction {

    private final String fieldToUse;
    private final String toReplace;
    private final String replacement;

    /**
     * 构造函数
     *
     * @param fieldToUse 指定使用此字段的值计算更新值
     * @param toReplace 待替换的字符串
     * @param replacement 替换为的字符串
     */
    public Replace(String fieldToUse, String toReplace, String replacement) {
        this.fieldToUse = fieldToUse;
        this.toReplace = toReplace;
        this.replacement = replacement;
    }

    /**
     * 将fieldToUse的值取出，替换其中的字符串，并将得到的值更新到fieldName字段上。
     *
     * @param record 一条记录
     * @param fieldName 待处理的字段
     */
    @Override
    public void apply(Record record, String fieldName) {
        // 注意字段值可能为null
        Object v = record.getField(fieldToUse);
        String s = v == null ? null : StringUtils.replace(v.toString(), toReplace, replacement);
        record.setField(fieldName, s);
    }
}