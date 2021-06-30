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

package com.tencent.bk.base.datalab.queryengine.common.collection;

public class ArrayUtil {

    public static boolean isEndWith(byte[] bytes, byte[] end, int offset) {
        if (end.length == 0) {
            return true;
        }
        if (bytes.length - offset < end.length) {
            return false;
        }
        for (int i = (end.length - 1); i >= 0; i--) {
            if (bytes[offset + i] != end[i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean isEmptyArray(Object[] array) {
        return array == null || array.length == 0;
    }

    public static boolean isEmpty2dArray(Object[] array) {
        if (array == null || array.length == 0) {
            return true;
        }
        for (Object o : array) {
            Object[] oo = (Object[]) o;
            if (oo != null && oo.length > 0) {
                return false;
            }
        }
        return true;
    }

}
