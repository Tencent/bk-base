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


package com.tencent.bk.base.datahub.databus.connect.sink.clickhouse;

import static org.apache.kafka.connect.data.Schema.STRING_SCHEMA;

import com.tencent.bk.base.datahub.databus.commons.BasicProps;
import com.tencent.bk.base.datahub.databus.commons.BkConfig;
import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.utils.AvroUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.HttpUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;


@Slf4j
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class ClickHouseSinkTaskTest {

    MockSinkTaskContext context = new MockSinkTaskContext();
    private static final String rtId = "100_test";

    /**
     * 创建db和table
     *
     * @throws SQLException
     */
    @Before
    public void setup() {
        BasicProps.getInstance()
                .addProps(Collections.singletonMap(Consts.API_DNS, "localhost:8080"));
    }

    /**
     * 覆盖put方法中 startTask 的情况
     *
     * @throws Exception
     */
    @Test
    @PrepareForTest({ClickHouseBean.class, HttpUtils.class})
    public void testStartTask() {
        Map<String, String> props = getProps();
        ConcurrentMap<String, Integer> weights = new ConcurrentHashMap<>();
        weights.put("jdbc:clickhouse://127.0.0.1:8123", 100);
        PowerMockito.mockStatic(ClickHouseBean.class);
        PowerMockito.when(ClickHouseBean
                .getWeightMap("http://localhost:8080/v3/storekit/clickhouse/100_test/weights/"))
                .thenReturn(weights);
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(getRtProps());

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.initialize(context);
        task.start(props);
        task.stop();
    }

    private Map<String, String> getProps() {
        Map<String, String> props = new HashMap<>();
        props.put(BkConfig.CONNECTOR_NAME, "clickhouse-table_100_test");
        props.put(BkConfig.RT_ID, "100_test");
        props.put(BkConfig.GROUP_ID, "xxxxx");
        props.put("connector", "shipper_test_connector");
        props.put("rt.id", "100_test");
        props.put("db.name", "clickhouse_591");
        props.put("replicated.table", "100_test");
        props.put("clickhouse.properties",
                "{\"http_port\": 8123, \"tcp_port\": 9000, \"http_tgw\": \"127.0.0.1:8123\", \"user\": \"xxx\", "
                        + "\"tcp_tgw\": \"127.0.0.1:9000\", \"inner_cluster\": \"default_cluster\", \"password\": "
                        + "\"xxx\", \"shipper_flush_size\":1}");
        props.put("clickhouse.column.order",
                "__time:DateTime,dteventtime:String,thedate:Int32,dteventtimestamp:Int64,localtime:String,"
                        + "gseindex:Int32,ip:String,path:String,report_time:String");
        props.put("api.dns", "localhost:8080");
        return props;

    }

    private Map<String, String> getRtProps() {
        Map<String, String> rtProps = new HashMap<>();
        rtProps.put(Consts.COLUMNS,
                "dtEventTime=string,thedate=int,dtEventTimeStamp=long,localTime=string,gseindex=int,ip=string,"
                        + "path=string,report_time=string");
        rtProps.put(BkConfig.RT_ID, rtId);
        rtProps.put(Consts.MSG_SOURCE_TYPE, Consts.AVRO);

        return rtProps;
    }


    // 测试前请确保测试环境已经部署clickhouse-server, http端口号8123, 推荐docker部署
    @Test
    @PrepareForTest({ClickHouseBean.class, HttpUtils.class})
    public void testPutSuccess() throws Exception {
        Map<String, String> props = getProps();
        ConcurrentMap<String, Integer> weights = new ConcurrentHashMap<>();
        weights.put("jdbc:clickhouse://127.0.0.1:8123", 100);
        PowerMockito.mockStatic(ClickHouseBean.class);
        PowerMockito.when(ClickHouseBean
                .getWeightMap("http://localhost:8080/v3/storekit/clickhouse/100_test/weights/"))
                .thenReturn(weights);
        PowerMockito.mockStatic(HttpUtils.class);
        PowerMockito.when(HttpUtils.getRtInfo(rtId)).thenReturn(getRtProps());

        ClickHouseSinkTask task = new ClickHouseSinkTask();
        task.initialize(context);
        task.start(props);

        String value = getAvroBinaryString();
        SinkRecord sinkRecord1 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142826", STRING_SCHEMA, value, 0);
        SinkRecord sinkRecord2 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142827", STRING_SCHEMA, value, 2);
        SinkRecord sinkRecord3 = new SinkRecord("xxx", 0, STRING_SCHEMA, "20190111142828", STRING_SCHEMA, value, 3);
        Collection<SinkRecord> records = new ArrayList<>();
        records.add(sinkRecord1);
        records.add(sinkRecord2);
        records.add(sinkRecord3);

        String sql = "CREATE TABLE IF NOT EXISTS clickhouse_591.100_test(`__time` DateTime, `dteventtime` String, "
                + "`thedate` Int32, `dteventtimestamp` Int64, `localtime` String, `gseindex` Int32, `ip` String, "
                + "`path` String, `report_time` String) ENGINE = MergeTree() PARTITION BY toDate(dteventtime) ORDER "
                + "BY dteventtime SAMPLE BY dteventtime SETTINGS index_granularity = 8192";
        ClickHouseConnection conn = new ClickHouseDataSource("jdbc:clickhouse://127.0.0.1:8123").getConnection();
        conn.createStatement().execute(sql);
        sql = "truncate clickhouse_591.100_test";
        conn.createStatement().execute(sql);
        task.put(records);
        log.info("executed write");
        sql = "select count(*) as count from clickhouse_591.100_test";
        ResultSet rs = conn.createStatement().executeQuery(sql);
        rs.next();
        int count = rs.getInt("count");
        //Assert.assertEquals(count, 3);
        log.info("---------------------------testPutSuccess finished------------------------------{}", count);

        task.stop();
    }


    private String getAvroBinaryString() throws IOException {
        Map<String, String> columns = new HashMap<>();
        columns.put("dtEventTime", "string");
        columns.put("thedate", "int");
        columns.put("dtEventTimeStamp", "long");
        columns.put("localTime", "string");
        columns.put("gseindex", "int");
        columns.put("ip", "string");
        columns.put("path", "string");
        columns.put("report_time", "string");
        Schema recordSchema = AvroUtils.getRecordSchema(columns);
        Schema recordArrSchema = Schema.createArray(recordSchema);
        GenericRecord avroRecord = new GenericData.Record(recordSchema);
        avroRecord.put("dtEventTime", "2021-05-01 04:01:02");
        avroRecord.put("thedate", 20210501);
        avroRecord.put("dtEventTimeStamp", 1619812862000L);
        avroRecord.put("localTime", "2021-05-01 04:01:02");
        avroRecord.put("gseindex", 20210501);
        avroRecord.put("ip", "127.0.0.1");
        avroRecord.put("path", "/path/index");
        avroRecord.put("report_time", "2021-05-01 04:01:02");
        GenericArray<GenericRecord> avroArray = new GenericData.Array<>(1, recordArrSchema);
        avroArray.add(avroRecord);
        Schema msgSchema = SchemaBuilder.record("kafkamsg_0")
                .fields()
                .name(Consts._VALUE_).type().array().items(recordSchema).noDefault()
                .endRecord();
        GenericRecord msgRecord = new GenericData.Record(msgSchema);
        msgRecord.put(Consts._VALUE_, avroArray);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
                new GenericDatumWriter<GenericRecord>(msgSchema));
        ByteArrayOutputStream output = new ByteArrayOutputStream(5120);
        dataFileWriter.create(msgSchema, output);
        dataFileWriter.append(msgRecord);
        dataFileWriter.flush();
        String result = output.toString(Consts.ISO_8859_1);
        return result;
    }

    protected static class MockSinkTaskContext implements SinkTaskContext {

        private Map<TopicPartition, Long> offsets;
        private long timeoutMs;

        public MockSinkTaskContext() {
            this.offsets = new HashMap<>();
            this.timeoutMs = -1L;
        }

        @Override
        public void offset(Map<TopicPartition, Long> offsets) {
            this.offsets.putAll(offsets);
        }

        @Override
        public void offset(TopicPartition tp, long offset) {
            offsets.put(tp, offset);
        }

        /**
         * Get offsets that the SinkTask has submitted to be reset. Used by the Copycat framework.
         *
         * @return the map of offsets
         */
        public Map<TopicPartition, Long> offsets() {
            return offsets;
        }

        @Override
        public void timeout(long timeoutMs) {
            this.timeoutMs = timeoutMs;
        }

        /**
         * Get the timeout in milliseconds set by SinkTasks. Used by the Copycat framework.
         *
         * @return the backoff timeout in milliseconds.
         */
        public long timeout() {
            return timeoutMs;
        }

        /**
         * Get the timeout in milliseconds set by SinkTasks. Used by the Copycat framework.
         *
         * @return the backoff timeout in milliseconds.
         */

        @Override
        public Set<TopicPartition> assignment() {
            return new HashSet<>();
        }

        @Override
        public void pause(TopicPartition... partitions) {
            return;
        }

        @Override
        public void resume(TopicPartition... partitions) {
            return;
        }

        @Override
        public void requestCommit() {

        }
    }

}
