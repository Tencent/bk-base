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

package com.tencent.bk.base.datalab.queryengine.server.third;

import java.util.List;
import java.util.Map;

/**
 * storeKitApi 第三方接口
 */
public interface StoreKitApiService {

    /**
     * 获取当前的存储集群状态
     *
     * @param storageConfigId 存储集群 Id
     * @return StorageState 存储集群状态
     */
    StorageState getStorageState(int storageConfigId);

    /**
     * 获取指定存储对应的物理表名
     *
     * @param resultTableId 结果表 Id
     * @param clusterType 存储类型
     * @return 物理表名
     */
    String fetchPhysicalTableName(String resultTableId, String clusterType);

    /**
     * 获取 es 集群物理 clusterName
     *
     * @param clusterName 逻辑 clusterName
     * @return 物理 clusterName
     */
    String getEsClusterName(String clusterName);

    /**
     * 获取结果表数据量
     *
     * @param resultTableId 结果表 Id
     * @return 结果表数据量
     */
    long fetchResultTableRows(String resultTableId);

    /**
     * 获取正在迁移到 iceberg 的结果表
     *
     * @return 迁移中的结果表列表
     */
    List<String> fetchOnMigrateResultTables();

    /**
     * 获取结果表 hdfs 存储的连接信息
     *
     * @param resultTableId 结果表 Id
     * @return hdfs存储的连接信息
     */
    Map<String, Object> fetchHdfsConf(String resultTableId);
}
