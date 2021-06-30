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

package com.tencent.bk.base.datalab.queryengine.common.numeric;


public class NumberUtil {

    private static final int BYTE_SIZE_8 = 8;

    /**
     * int 整数转换为4字节的 byte 数组
     *
     * @param i 整数
     * @return byte 数组
     */
    public static byte[] intToByte4(int i) {
        byte[] targets = new byte[4];
        targets[3] = (byte) (i & 0xFF);
        targets[2] = (byte) (i >> 8 & 0xFF);
        targets[1] = (byte) (i >> 16 & 0xFF);
        targets[0] = (byte) (i >> 24 & 0xFF);
        return targets;

    }

    /**
     * long 整数转换为8字节的 byte 数组
     *
     * @param lo long 整数
     * @return byte 数组
     */
    public static byte[] longToByte8(long lo) {
        byte[] result = new byte[8];
        for (int i = BYTE_SIZE_8 - 1; i >= 0; i--) {
            result[i] = (byte) (lo & 0xFF);
            lo >>= 8;
        }
        return result;
    }

    /**
     * short 整数转换为2字节的 byte 数组
     *
     * @param s short 整数
     * @return byte 数组
     */
    public static byte[] unsignedShortToByte2(int s) {
        byte[] targets = new byte[2];
        targets[0] = (byte) (s >> 8 & 0xFF);
        targets[1] = (byte) (s & 0xFF);
        return targets;
    }

    /**
     * byte 数组转换为无符号 short 整数
     *
     * @param bytes byte 数组
     * @return short 整数
     */
    public static int byte2ToUnsignedShort(byte[] bytes) {
        return byte2ToUnsignedShort(bytes, 0);
    }

    /**
     * byte 数组转换为无符号 short 整数
     *
     * @param bytes byte 数组
     * @param off 开始位置
     * @return short 整数
     */
    public static int byte2ToUnsignedShort(byte[] bytes, int off) {
        int high = bytes[off];
        int low = bytes[off + 1];
        return (high << 8 & 0xFF00) | (low & 0xFF);
    }

    /**
     * byte 数组转换为 int 整数
     *
     * @param bytes byte 数组
     * @param off 开始位置
     * @return int 整数
     */
    public static int byte4ToInt(byte[] bytes, int off) {
        int b0 = bytes[off] & 0xFF;
        int b1 = bytes[off + 1] & 0xFF;
        int b2 = bytes[off + 2] & 0xFF;
        int b3 = bytes[off + 3] & 0xFF;
        return (b0 << 24) | (b1 << 16) | (b2 << 8) | b3;
    }


    /**
     * 四位字节转换成 long 型整数
     *
     * @param b
     * @return long 型整数
     */
    public static long byte4ToLong(byte[] b) {
        long s0 = b[0] & 0xff;
        s0 <<= 24;
        long s1 = b[1] & 0xff;
        s1 <<= 16;
        long s2 = b[2] & 0xff;
        s2 <<= 8;
        final long s3 = b[3] & 0xff;
        long s4 = 0;
        s4 <<= 8 * 7;
        long s5 = 0;
        s5 <<= 8 * 6;
        long s6 = 0;
        s6 <<= 8 * 5;
        long s7 = 0;
        s7 <<= 8 * 4;
        long s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
        return s;
    }
}
