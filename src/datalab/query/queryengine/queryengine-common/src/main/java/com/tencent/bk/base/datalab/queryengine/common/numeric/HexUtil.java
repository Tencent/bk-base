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

import java.nio.charset.StandardCharsets;

public class HexUtil {

    public static byte[] intToBytes(int n) {
        byte[] b = new byte[4];
        b[3] = (byte) (n & 0xFF);
        b[2] = (byte) (n >> 8 & 0xFF);
        b[1] = (byte) (n >> 16 & 0xFF);
        b[0] = (byte) (n >> 24 & 0xFF);
        return b;
    }

    public static String bytesToHexString(byte[] bs) {
        StringBuilder resultBuilder = new StringBuilder();
        for (int i = 0; i < bs.length; ++i) {
            resultBuilder.append(byteToHexString(bs[i]));
            if (i != bs.length - 1) {
                resultBuilder.append(" ");
            }
        }
        return resultBuilder.toString();
    }

    public static String byteToHexString(byte b) {
        String hexStr = "0123456789ABCDEF";
        int i = (b >> 4) & 0x0F;
        char highChar = hexStr.charAt(i);

        i = b & 0x0F;
        char lowChar = hexStr.charAt(i);

        StringBuilder sb = new StringBuilder(2);
        return sb.append(highChar)
                .append(lowChar)
                .toString();
    }

    public static String stringToHexString(String str) {
        byte[] bs;
        bs = str.getBytes(StandardCharsets.ISO_8859_1);
        return bytesToHexString(bs);
    }

    public static String intToHexString(int i) {
        byte[] bs = intToBytes(i);
        return bytesToHexString(bs);
    }
}
