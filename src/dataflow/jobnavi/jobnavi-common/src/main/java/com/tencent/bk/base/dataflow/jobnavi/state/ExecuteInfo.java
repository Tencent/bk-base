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

package com.tencent.bk.base.dataflow.jobnavi.state;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ExecuteInfo implements Cloneable {

    private long id;
    private String host;
    private double rank;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public double getRank() {
        return rank;
    }

    public void setRank(double rank) {
        this.rank = rank;
    }

    /**
     * parse execute info json string
     *
     * @param maps
     */
    public boolean parseJson(Map<?, ?> maps) {
        boolean parseOK = true;
        if (maps.get("id") != null) {
            id = Long.parseLong(maps.get("id").toString());
        } else {
            parseOK = false;
        }
        if (maps.get("host") != null) {
            host = (String) maps.get("host");
        }
        if (maps.get("rank") != null) {
            rank = (Double) maps.get("rank");
        }
        return parseOK;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecuteInfo that = (ExecuteInfo) o;
        return id == that.id && Double.compare(that.rank, rank) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, rank);
    }

    @Override
    public ExecuteInfo clone() {
        try {
            return (ExecuteInfo) super.clone();
        } catch (CloneNotSupportedException e) {
            // this shouldn't happen, since we are Cloneable
            throw new InternalError(e.getMessage());
        }
    }

    public Map<String, Object> toHttpJson() {
        Map<String, Object> json = new HashMap<>();
        json.put("id", id);
        json.put("host", host);
        json.put("rank", rank);
        return json;
    }
}
