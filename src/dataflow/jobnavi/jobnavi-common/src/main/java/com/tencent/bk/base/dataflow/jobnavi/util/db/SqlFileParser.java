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

package com.tencent.bk.base.dataflow.jobnavi.util.db;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class SqlFileParser {

    /**
     * Parse sql file
     *
     * @param is sql file InputStream
     * @return sql list
     * @throws IOException file read IOException
     */
    public static String[] parse(InputStream is) throws IOException {
        List<String> sqlList = new ArrayList<>();
        InputStreamReader reader = new InputStreamReader(is, StandardCharsets.UTF_8);
        int i;
        StringBuilder sqlBuilder = new StringBuilder();
        while ((i = reader.read()) != -1) {
            char c = (char) i;
            if (c == ';') {
                if (sqlBuilder.length() > 0) {
                    sqlList.add(sqlBuilder.toString());
                    sqlBuilder = new StringBuilder();
                }
            } else if (c == '\r' || c == '\n') {
                continue;
            } else {
                sqlBuilder.append(c);
            }
        }
        return sqlList.toArray(new String[sqlList.size()]);
    }
}
