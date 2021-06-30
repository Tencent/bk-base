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

package com.tencent.bk.base.datahub.databus.connect.druid.wrapper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.datahub.databus.connect.druid.DruidSinkTask;


public final class DimensionSpecWrapper {

    public static final DimensionSpecWrapper THEDATE = new DimensionSpecWrapper("thedate", "long");
    public static final DimensionSpecWrapper DTEVENTTIME = new DimensionSpecWrapper("dtEventTime", "string");
    public static final DimensionSpecWrapper DTEVENTTIMESTAMP = new DimensionSpecWrapper("dtEventTimeStamp", "long");
    public static final DimensionSpecWrapper LOCALTIME = new DimensionSpecWrapper("__localtime", "string");
    public static final DimensionSpecWrapper TIMESTAMP_DAY = new DimensionSpecWrapper(
            DruidSinkTask.ADDING_DATA_KEY_TIMESTAMP_DAY, "long");
    public static final DimensionSpecWrapper TIMESTAMP_HOUR = new DimensionSpecWrapper(
            DruidSinkTask.ADDING_DATA_KEY_TIMESTAMP_HOUR, "long");
    public static final DimensionSpecWrapper TIMESTAMP_MINUTE = new DimensionSpecWrapper(
            DruidSinkTask.ADDING_DATA_KEY_TIMESTAMP_MINUTE, "long");
    public static final DimensionSpecWrapper TIMESTAMP_SECOND = new DimensionSpecWrapper(
            DruidSinkTask.ADDING_DATA_KEY_TIMESTAMP_SECOND, "long");
    public static final DimensionSpecWrapper TIMESTAMP_MILLISECOND = new DimensionSpecWrapper(
            DruidSinkTask.ADDING_DATA_KEY_TIMESTAMP_MILLISECOND, "long");
    public static final DimensionSpecWrapper DAY = new DimensionSpecWrapper(DruidSinkTask.ADDING_DATA_KEY_DAY,
            "string");
    public static final DimensionSpecWrapper HOUR = new DimensionSpecWrapper(DruidSinkTask.ADDING_DATA_KEY_HOUR,
            "string");
    public static final DimensionSpecWrapper MINUTE = new DimensionSpecWrapper(DruidSinkTask.ADDING_DATA_KEY_MINUTE,
            "string");
    public static final DimensionSpecWrapper SECOND = new DimensionSpecWrapper(DruidSinkTask.ADDING_DATA_KEY_SECOND,
            "string");
    public static final DimensionSpecWrapper MILLISECOND = new DimensionSpecWrapper(
            DruidSinkTask.ADDING_DATA_KEY_MILLISECOND, "string");
    public static final DimensionSpecWrapper INGESTION_TIME = new DimensionSpecWrapper(
            DruidSinkTask.ADDING_DATA_KEY_INGESTION_TIME, "long");

    public final String name;
    public final String type;

    @JsonCreator
    public DimensionSpecWrapper(
            @JsonProperty("name") String name,
            @JsonProperty("type") String type) {
        this.name = name;
        this.type = type;
    }
}
