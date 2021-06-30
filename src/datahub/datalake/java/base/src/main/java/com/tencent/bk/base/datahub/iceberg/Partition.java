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

import java.util.Arrays;
import org.apache.iceberg.StructLike;

class Partition implements StructLike {

    private final Object[] partitionTuple;

    /**
     * 构造函数
     *
     * @param data 对象数组
     */
    Partition(Object[] data) {
        partitionTuple = data;
    }

    /**
     * 获取分区信息长度
     *
     * @return 分区信息长度
     */
    @Override
    public int size() {
        return partitionTuple.length;
    }

    /**
     * 获取指定位置的分区值
     *
     * @param pos 位置
     * @param javaClass Java类
     * @param <T> 数据类型
     * @return 指定位置的分区值
     */
    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return javaClass.cast(partitionTuple[pos]);
    }

    /**
     * 设置指定位置的分区值
     *
     * @param pos 位置
     * @param value 分区值
     * @param <T> 分区值的数据类型
     */
    @Override
    public <T> void set(int pos, T value) {
        partitionTuple[pos] = value;
    }

    /**
     * 转换为字符串
     *
     * @return 字符串表示的本对象
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < partitionTuple.length; i += 1) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(partitionTuple[i]);
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * 对比对象是否相同
     *
     * @param o 对比的对象
     * @return True/False
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Partition that = (Partition) o;
        return Arrays.equals(partitionTuple, that.partitionTuple);
    }

    /**
     * 哈希编码
     *
     * @return 哈希编码
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(partitionTuple);
    }
}