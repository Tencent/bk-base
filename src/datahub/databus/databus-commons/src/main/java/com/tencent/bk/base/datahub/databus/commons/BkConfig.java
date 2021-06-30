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

package com.tencent.bk.base.datahub.databus.commons;

public interface BkConfig {

    // 这两个配置项用于打点数据上报，用于标示cluster名称和connector的名称
    String GROUP_ID = "group.id";
    String GROUP_ID_DOC = "cluster name";
    String GROUP_ID_DISPLAY = "the cluster name of the kafka connect cluster in which this connector is running";

    String CONNECTOR_NAME = "name";
    String CONNECTOR_NAME_DOC = "the name of the connector";
    String CONNECTOR_NAME_DISPLAY = "the name of the connector";

    // 配置result table id
    String RT_ID = "rt.id";
    String RT_ID_DOC = "the result table id";
    String RT_ID_DISPLAY = "Result Table ID";

    // 配置data id
    String DATA_ID = "data.id";
    String DATA_ID_DOC = "the data id";
    String DATA_ID_DISPLAY = "DATA ID";

}
