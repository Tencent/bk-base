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

package com.tencent.bk.base.datalab.queryengine.common.character;

/**
 * 字符串操作工具类
 */
public class BaseStringUtil {

    /**
     * 判断输入字符串是否为空
     *
     * @param str 输入字符串
     * @return 是否是空串
     */
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * 判断输入字符串是否为空
     *
     * @param str 输入字符串
     * @return 是否是空串
     */
    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    /**
     * 判断是否存在空字符串
     *
     * @param css 输入字符串
     * @return 是否存在空字符串
     */
    public static boolean isAnyBlank(CharSequence... css) {
        if (css == null || css.length == 0) {
            return true;
        }
        for (CharSequence cs : css) {
            if (isBlank(cs)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 判断字符串是否为空
     *
     * @param cs 输入字符串
     * @return 是否为空
     */
    private static boolean isBlank(final CharSequence cs) {
        if (cs == null || cs.length() == 0) {
            return true;
        }
        for (int i = 0; i < cs.length(); i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 计算子字符串出现的次数
     *
     * @param str 源字符串
     * @param subStr 子字符串
     * @return 出现次数
     */
    public static int getSubStrCount(String str, String subStr) {
        int count = 0;
        int index = 0;
        while ((index = str.indexOf(subStr, index)) != -1) {
            index = index + subStr.length();
            count++;
        }
        return count;
    }

    /**
     * 字符串替换
     *
     * @param inString 源字符串
     * @param oldPattern 需要替换的字符串/正则表达式
     * @param newPattern 替换的字符串/正则表达式
     * @return 替换后的字符串
     */
    public static String replace(String inString, String oldPattern, String newPattern) {
        if (isNotEmpty(inString) && isNotEmpty(oldPattern) && newPattern != null) {
            int index = inString.indexOf(oldPattern);
            if (index == -1) {
                return inString;
            } else {
                int capacity = inString.length();
                if (newPattern.length() > oldPattern.length()) {
                    capacity += 16;
                }

                StringBuilder sb = new StringBuilder(capacity);
                int pos = 0;

                for (int patLen = oldPattern.length(); index >= 0;
                        index = inString.indexOf(oldPattern, pos)) {
                    sb.append(inString, pos, index);
                    sb.append(newPattern);
                    pos = index + patLen;
                }
                sb.append(inString.substring(pos));
                return sb.toString();
            }
        } else {
            return inString;
        }
    }

    /**
     * 格式化字符串 (替换符为%s）
     *
     * @param format 字符串格式
     * @param args 参数
     * @return 格式化后的字符串
     */
    public static String formatIfArgs(String format, Object... args) {
        if (isEmpty(format)) {
            return format;
        }
        return (args == null || args.length == 0) ? String
                .format(format.replaceAll("%([^n])", "%%$1")) : String.format(format, args);
    }

    /**
     * 格式化字符串 (替换符自己指定）
     *
     * @param format 字符串格式
     * @param args 参数
     * @return 格式化后的字符串
     */
    public static String formatIfArgs(String format, String replaceOperator, Object... args) {
        if (isEmpty(format) || isEmpty(replaceOperator)) {
            return format;
        }
        format = replace(format, replaceOperator, "%s");
        return formatIfArgs(format, args);
    }
}
