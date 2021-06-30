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

package com.tencent.bk.base.datalab.bksql.util;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public final class QuoteUtil {

    private static final Pattern SPLITTABLE_PATTERN = Pattern
            .compile("(`[^`]+`|\"[^\"]+\"|\\[[^\\]]+\\]|[^\\.]+)");

    private QuoteUtil() {

    }

    public static String trimQuotes(String text) {
        if (StringUtils.isEmpty(text)) {
            return text;
        }
        Matcher matcher = SPLITTABLE_PATTERN.matcher(text);
        List<String> builder = new LinkedList<>();
        while (matcher.find()) {
            builder.add(trim0(matcher.group(1)));
        }
        return StringUtils.join(builder.toArray(new String[builder.size()]), '.');
    }

    private static String trim0(String name) {
        if (StringUtils.isNotEmpty(name) && (name.startsWith("`") && name.endsWith("`")
                || name.startsWith("[") && name.endsWith("]")
                || name.startsWith("\"") && name.endsWith("\""))) {
            return name.substring(1, name.length() - 1);
        }
        return name;
    }

    public static boolean isQuoted(String text) {
        return text.startsWith("`") && text.endsWith("`")
                || text.startsWith("[") && text.endsWith("]")
                || text.startsWith("\"") && text.endsWith("\"");
    }

    public static String addDoubleQuotes(String text) {
        if (StringUtils.isEmpty(text)) {
            return text;
        }
        if (text.contains("\"")) {
            return text;
        }
        return "\"" + text.replace("\"", "\\\"") + "\"";
    }

    public static String replaceValueInsideQuotes(String quoted, String newVal) {
        String trimmed = trimQuotes(quoted);
        if (StringUtils.equals(trimmed, quoted)) {
            return newVal;
        }
        char head = quoted.charAt(0);
        char tail = quoted.charAt(quoted.length() - 1);
        return String.format("%c%s%c", head, newVal, tail);
    }
}
