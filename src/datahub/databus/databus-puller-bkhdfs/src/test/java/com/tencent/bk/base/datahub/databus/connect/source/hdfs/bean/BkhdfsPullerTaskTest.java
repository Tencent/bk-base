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

package com.tencent.bk.base.datahub.databus.connect.source.hdfs.bean;

import com.tencent.bk.base.datahub.databus.connect.common.Consts;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class BkhdfsPullerTaskTest {

    private Map<String, String> getRtProps() {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.COLUMNS, "id=int");
        rtProps.put(BkConfig.RT_ID, "123_test_rt");
        rtProps.put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);
        return rtProps;
    }


    @Test
    @PrepareForTest({HttpUtils.class})
    public void testConstruction() throws Exception {
        Map<String, String> rtProps = getRtProps();
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo("123_test_rt")).thenReturn(rtProps);

        Map<String, Object> taskMap = new HashMap<>();
        taskMap.put("id", "123456");
        taskMap.put("hdfs_custom_property", "{\"dfs.nameservices\": \"testHdfs\"}");
        taskMap.put("hdfs_conf_dir", "");
        taskMap.put("kafka_bs", "kafka.bootstrap.url:9092");
        taskMap.put("rt_id", "123_test_rt");
        taskMap.put("data_dir", "hdfs://testHdfs//123456/test_rt_100708/2021/05/13/08");
        taskMap.put("status", "part-00099-6ceed36b-741d-4ee6-a937-bbdc43dfceee-c000.snappy.parquet|-1");

        BkhdfsPullerTask task = new BkhdfsPullerTask(taskMap, 1);
        Assert.assertEquals("kafka.bootstrap.url:9092", task.getChannelServer());
        Assert.assertEquals("dtEventTime", task.getColsInOrder()[0]);
        Assert.assertEquals("dtEventTimeStamp", task.getColsInOrder()[1]);
        Assert.assertEquals("localTime", task.getColsInOrder()[2]);
        Assert.assertEquals("id", task.getColsInOrder()[3]);
        Assert.assertEquals("thedate", task.getColsInOrder()[4]);

        Assert.assertEquals("int", task.getColumns().get("id"));
        Assert.assertEquals("hdfs://testHdfs//123456/test_rt_100708/2021/05/13/08",
                task.getDataDir());
        Assert.assertEquals("", task.getHdfsConfDir());
        Assert.assertEquals("{\"dfs.nameservices\": \"testHdfs\"}", task.getHdfsCustomProperty());
        Assert.assertEquals("part-00099-6ceed36b-741d-4ee6-a937-bbdc43dfceee-c000.snappy.parquet",
                task.getLastReadFileName());
        Assert.assertEquals(-1, task.getLastReadLineNumber());
        Assert.assertEquals("123_test_rt", task.getResultTableId());

        Assert.assertEquals("part-00099-6ceed36b-741d-4ee6-a937-bbdc43dfceee-c000.snappy.parquet|-1", task.getStatus());
        Assert.assertEquals(123456L, task.getTaskId());
        Assert.assertEquals("table_123_test_rt", task.getTopic());
    }
}
