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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.tencent.bk.base.datalab.meta.StorageCluster;
import com.tencent.bk.base.datalab.meta.Storages;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.queryengine.common.codec.JacksonUtil;
import com.tencent.bk.base.datalab.queryengine.server.constant.StorageConstants;
import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTable;
import com.tencent.bk.base.datalab.queryengine.server.dto.ResultTableProperty;
import com.tencent.bk.base.datalab.queryengine.server.meta.FieldMeta;
import java.util.List;
import java.util.Map;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBootTest
@EnableAutoConfiguration
public class DataLabApiServiceImplTest {

    @Autowired
    private DataLabApiService dataLabApiService;

    @Test
    @Ignore
    public void registerResultTable() {
        //构建StorageCluster
        StorageCluster storageCluster = new StorageCluster();
        storageCluster.setClusterName("hdfs_default");
        storageCluster.setClusterType(StorageConstants.DEVICE_TYPE_HDFS);
        Map<String, String> connectionInfo = Maps.newHashMap();
        connectionInfo.put("path", "/dataquery/BK000000000000001");
        connectionInfo.put("format", "parquet");
        connectionInfo.put("compress", "none");
        storageCluster.setConnectionInfo(JacksonUtil.object2Json(connectionInfo));

        //构建StoragesProperty
        StoragesProperty hdfsProp = new StoragesProperty();
        hdfsProp.setPhysicalTableName("/dataquery/BK000000000000001");
        hdfsProp.setStorageCluster(storageCluster);

        //构建Storages
        Storages storages = new Storages();
        storages.setAdditionalProperty(StorageConstants.DEVICE_TYPE_HDFS, hdfsProp);

        //构建ResultTable
        ResultTable resultTable = new ResultTable();
        resultTable.setResultTableId("tab" + System.currentTimeMillis());
        List<FieldMeta> fieldMetaList = Lists.newArrayList();
        fieldMetaList.add(
                FieldMeta.builder()
                        .fieldIndex(1)
                        .fieldName("id")
                        .fieldAlias("id")
                        .fieldType("int")
                        .build());
        resultTable.setFields(fieldMetaList);

        //构建ResultTableProperty
        ResultTableProperty rtProperty = new ResultTableProperty();
        rtProperty.setUserName("bkdata");
        rtProperty.setAuthMethod("username");
        rtProperty.setResultTableList(ImmutableList.of(resultTable));
        boolean result = dataLabApiService.registerResultTable("749", "1", rtProperty);
        Assert.assertTrue(result);
    }
}