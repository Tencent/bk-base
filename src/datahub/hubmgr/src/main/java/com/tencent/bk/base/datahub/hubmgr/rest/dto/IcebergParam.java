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

package com.tencent.bk.base.datahub.hubmgr.rest.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;


@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergParam {

    private static final int RECORD_NUM = 10;
    private static final int EXPIRE_DAYS = -1;


    @NotBlank
    private String databaseName;

    @NotBlank
    private String tableName;

    @NotEmpty
    private Map<String, String> config;

    private List<Map<String, String>> schema = new ArrayList<>();

    private List<Map<String, String>> partition = new ArrayList<>();

    private List<Map<String, String>> addFields = new ArrayList<>();

    private List<String> removeFields = new ArrayList<>();

    private Map<String, String> renameFields = new HashMap<>();

    private Map<String, String> deletePartition = new HashMap<>();

    private Map<Object, Object> conditionExpression = new HashMap<>();

    private Map<String, Map<String, Object>> assignExpression = new HashMap<>();

    private Integer recordNum = RECORD_NUM;

    private Integer expireDays = EXPIRE_DAYS;

    private String timeField = "";

    private String compactStart = "";

    private String compactEnd = "";

    private boolean asyncCompact = false;

    private String location = "";

    private String msg = "";

    private List<String> files = new ArrayList<>();

    // 设置commitMsg所需
    private Map<String, Object> offsetMsgs = new HashMap<>();

    private String newTableName = "";

    private Map<String, String> updateProperties = new HashMap<>();

    /**
     * 获取库名
     *
     * @return 库名
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * 获取表名
     *
     * @return 表名
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取表的字段信息，字段有序排列
     *
     * @return 字段的映射
     */
    public List<Map<String, String>> getSchema() {
        return schema;
    }

    /**
     * 获取表的hive和hdfs连接信息
     *
     * @return 连接信息的映射
     */
    public Map<String, String> getConfig() {
        return config;
    }

    /**
     * 获取表的分区方式
     *
     * @return 分区信息的映射
     */
    public List<Map<String, String>> getPartition() {
        return partition;
    }

    /**
     * 获取待添加字段信息
     *
     * @return 字段映射
     */
    public List<Map<String, String>> getAddFields() {
        return addFields;
    }

    /**
     * 获取待改名字段信息
     *
     * @return 字段映射
     */
    public Map<String, String> getRenameFields() {
        return renameFields;
    }

    /**
     * 获取待删除字段信息
     *
     * @return 字段列表
     */
    public List<String> getRemoveFields() {
        return removeFields;
    }

    /**
     * 获取待删除分区的集合
     *
     * @return 分区信息的映射
     */
    public Map<String, String> getDeletePartition() {
        return deletePartition;
    }

    /**
     * 获取待删除字段信息
     *
     * @return 条件表达式映射
     */
    public Map<Object, Object> getConditionExpression() {
        return conditionExpression;
    }

    /**
     * 获取待删除字段信息
     *
     * @return 赋值表达式映射
     */
    public Map<String, Map<String, Object>> getAssignExpression() {
        return assignExpression;
    }

    /**
     * 获取快照,commitMsg或者数据记录数
     *
     * @return 整型recordNum值
     */
    public Integer getRecordNum() {
        // 传入参数不合法时使用默认值
        return recordNum >= 1 ? recordNum : RECORD_NUM;
    }

    /**
     * 获取数据过期天数
     *
     * @return 过期周期，整型
     */
    public Integer getExpireDays() {
        return expireDays;
    }

    /**
     * 获取分区时间字段
     *
     * @return 时间字段名
     */
    public String getTimeField() {
        return timeField;
    }

    /**
     * 获取合并操作的起始时间
     *
     * @return 时间字段名
     */
    public String getCompactStart() {
        return compactStart;
    }

    /**
     * 获取合并操作的结束时间
     *
     * @return 时间字段名
     */
    public String getCompactEnd() {
        return compactEnd;
    }

    /**
     * 获取合并小文件时是否生成异步任务
     *
     * @return true/false，是否异步执行小文件合并
     */
    public boolean getAsyncCompact() {
        return asyncCompact;
    }

    /**
     * 获取更新表的location地址。
     *
     * @return 表location地址
     */
    public String getLocation() {
        return location;
    }

    /**
     * 获取msg的值
     *
     * @return msg的值
     */
    public String getMsg() {
        return msg;
    }

    /**
     * 获取数据文件列表
     *
     * @return 数据文件列表
     */
    public List<String> getFiles() {
        return files;
    }

    /**
     * 获取iceberg在hdfs存储中的offset信息
     *
     * @return 时间字段名
     */
    public Map<String, String> getOffsetMsgs() {
        Map<String, String> offsets = new HashMap<>();
        offsetMsgs.forEach((k, v) -> offsets.put(k, v.toString()));
        return offsets;
    }

    /**
     * 获取新表名
     *
     * @return 时间字段名
     */
    public String getNewTableName() {
        return newTableName;
    }

    /**
     * 获取待变更的表属性
     *
     * @return 待更新表属性
     */
    public Map<String, String> getUpdateProperties() {
        return updateProperties;
    }
}


