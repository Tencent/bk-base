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

package com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.util;

import com.tencent.bk.base.datahub.databus.connect.source.datanode.transform.SplitBy;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消息元数据工具类
 */
public class EventMetaTools implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(SplitBy.class);
    private static final long serialVersionUID = 8877647412369941250L;
    private Map<String, Integer> columnMap = new HashMap<>();

    public EventMetaTools(String columnParam) {
        init(columnParam);
    }

    public EventMetaTools(String[] columnArray) {
        init(columnArray);
    }

    /**
     * 初始化字段映射
     *
     * @param columnArray 字段列表
     */
    private void init(String[] columnArray) {
        if (columnArray != null) {
            for (int i = 0; i < columnArray.length; i++) {
                columnMap.put(columnArray[i], i);
            }
        }
    }

    /**
     * 初始化字段映射
     *
     * @param columnParam 字段名
     */
    private void init(String columnParam) {
        if (StringUtils.isNotBlank(columnParam)) {
            String[] columnArray = columnParam.split("\\|");
            for (int i = 0; i < columnArray.length; i++) {
                columnMap.put(columnArray[i], i);
            }
        }
    }

    /**
     * 按照列名来获取相应的值
     *
     * @param colName 列名
     */
    public int getColumnIdx(String colName) {
        if (StringUtils.isNotBlank(colName) && columnMap.containsKey(colName)) {
            return columnMap.get(colName);
        } else {
            LogUtils.warn(log, "column {} is not in {}", colName, columnMap);
            return -1;
        }
    }

    /**
     * 按照列名来获取相应的值
     *
     * @param vals 字段值数组
     * @param colName 列名
     */
    public String getColumnValue(String[] vals, String colName) {
        if (vals == null || StringUtils.isBlank(colName)) {
            return null;
        }
        try {
            return vals[getColumnIdx(colName)];
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, "",
                    String.format("failed to get column %s value in %s", colName, StringUtils.join(vals, ",")), e);
        }
        return null;
    }

    /**
     * 按照列名来获取相应的值
     *
     * @param vals 字段值列表
     * @param colName 列名
     */
    public Object getColumnValue(List<Object> vals, String colName) {
        if (vals == null || StringUtils.isBlank(colName)) {
            return null;
        }
        try {
            return vals.get(getColumnIdx(colName));
        } catch (Exception e) {
            LogUtils.reportExceptionLog(log, "",
                    String.format("failed to get column %s value in %s", colName, StringUtils.join(vals, ",")), e);
        }
        return null;
    }
}
