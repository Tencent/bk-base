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

package com.tencent.bk.base.datahub.iceberg;

import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.TableProperties.METADATA_PREVIOUS_VERSIONS_MAX;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tencent.bk.base.datahub.iceberg.errors.CommitFileFailed;
import com.tencent.bk.base.datahub.iceberg.errors.TableExists;
import com.tencent.bk.base.datahub.iceberg.errors.TableNotExists;
import com.tencent.bk.base.datahub.iceberg.functions.Assign;
import com.tencent.bk.base.datahub.iceberg.functions.Sum;
import com.tencent.bk.base.datahub.iceberg.functions.ValFunction;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveLockUtils;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestBkTable extends HiveMetastoreTest {

    private static final String TB1 = "t1";
    private static final String TB2 = "t2";
    private static final String TB3 = "t3";
    private static final String TB4 = "t4";
    private static final String TB5 = "t5";
    private static final String TB6 = "t6";
    private static final String BAD_DB = "111";
    private static final String BAD_TB = "e+";
    private static final String TEST_KEY = "only.for.test";
    private static final String TEST_VALUE = "testvalue";
    private final Map<String, String> props = new HashMap<>();

    private static String getTableBasePath(String tableName) {
        String databasePath = metastore.getDatabasePath(DB_NAME);
        return Paths.get(databasePath, tableName).toAbsolutePath().toString();
    }

    protected static Path getTableLocationPath(String tableName) {
        return new Path("file", null, Paths.get(getTableBasePath(tableName)).toString());
    }

    protected static String getTableLocation(String tableName) {
        return getTableLocationPath(tableName).toString();
    }

    private static String metadataLocation(String tableName) {
        return Paths.get(getTableBasePath(tableName), "metadata").toString();
    }

    private static List<String> metadataFiles(String tableName) {
        return Arrays.stream(new File(metadataLocation(tableName)).listFiles())
                .map(File::getAbsolutePath)
                .collect(Collectors.toList());
    }

    protected static List<String> metadataVersionFiles(String tableName) {
        return filterByExtension(tableName, getFileExtension(TableMetadataParser.Codec.NONE));
    }

    protected static List<String> manifestFiles(String tableName) {
        return filterByExtension(tableName, ".avro");
    }

    private static List<String> filterByExtension(String tableName, String extension) {
        return metadataFiles(tableName)
                .stream()
                .filter(f -> f.endsWith(extension))
                .collect(Collectors.toList());
    }

    @Before
    public void createTestTable() {
        props.put(C.DEFAULT_FS, DEFAULT_FS);
        props.put("hive.metastore.uris", TestHiveMetastore.META_URI);
        props.put("hive.metastore.warehouse.dir", WAREHOUSE);
        props.put("table." + TEST_KEY, TEST_VALUE);

        BkTable table = new BkTable(DB_NAME, TB1, props);

        List<TableField> fields = Stream
                .of("TS,timestamp", "BALance,lONg", "salary,INT", "info,string", "aa,double",
                        "bb,float", "cc,text")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1]))
                .collect(Collectors.toList());
        List<PartitionMethod> pMethods = Collections
                .singletonList(new PartitionMethod("tS", "DAY"));

        table.createTable(fields, pMethods);
        SnapshotEventCollector.getInstance().setReportUrl("127.0.0.1", "/report");
    }

    @After
    public void dropTestTable() throws Exception {
        // drop the table data
        TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TB1);
        catalog.dropTable(TABLE_IDENTIFIER, true /* metadata only, location was already deleted */);
    }

    @Test
    public void testCheckArguments() {
        AssertHelpers.assertThrows("bad args should fail",
                NullPointerException.class,
                "configurations cannot be null",
                () -> new BkTable(DB_NAME, TB2, null));

        Map<String, String> props = new HashMap<>();
        AssertHelpers.assertThrows("bad config should fail",
                IllegalArgumentException.class,
                "fs.defaultFS missing",
                () -> new BkTable(DB_NAME, TB2, props));

        props.put(C.DEFAULT_FS, DEFAULT_FS);
        props.put("hive.metastore.uris", TestHiveMetastore.META_URI);
        props.put("hive.metastore.warehouse.dir", WAREHOUSE);

        AssertHelpers.assertThrows("bad db name should fail",
                IllegalArgumentException.class,
                "db name not valid",
                () -> new BkTable(BAD_DB, TB2, props));
        AssertHelpers.assertThrows("bad table name should fail",
                IllegalArgumentException.class,
                "bad name",
                () -> new BkTable(DB_NAME, BAD_TB, props));
    }

    @Test
    public void testRenameTable() {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();
        String renameTo = TB1 + "_todelete_20200819100001";
        table.renameTable(renameTo);
        Namespace ns = TableIdentifier.of(DB_NAME, TB1).namespace();
        List<TableIdentifier> tables = catalog.listTables(ns);
        Assert.assertEquals(1, tables.size());
        Assert.assertEquals(renameTo, tables.get(0).name());
    }

    @Test
    public void testCreateTable() throws IOException {
        BkTable table = new BkTable(DB_NAME, TB2, props);
        AssertHelpers.assertThrows("empty args should fail",
                IllegalArgumentException.class,
                "empty args",
                () -> table.createTable(new ArrayList<>(), new ArrayList<>()));

        List<TableField> fields = Stream
                .of("TS,timestamp", "BALance,lONg", "salary,INT", "info,string", "aa,double",
                        "bb,float", "cc,text")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1]))
                .collect(Collectors.toList());
        List<PartitionMethod> badMethods = Collections
                .singletonList(new PartitionMethod("abc", "DAY"));
        AssertHelpers.assertThrows("partition by non-existing field should fail",
                IllegalArgumentException.class,
                "all partition fields should exist in table fields",
                () -> table.createTable(fields, badMethods));

        // 正常创建成功一张表
        List<PartitionMethod> pMethods = Collections
                .singletonList(new PartitionMethod("tS", "DAY"));
        table.createTable(fields, pMethods);
        Schema schema = table.schema();
        PartitionSpec spec = table.table().spec();
        String info = table.info();
        Assert.assertEquals("table should have 7 columns", 7, schema.columns().size());
        Assert.assertEquals("ts should be timestamp type", Types.TimestampType.withZone(),
                schema.findType("ts"));
        Assert.assertEquals("partition method should be *day*", "day",
                spec.fields().get(0).transform().toString());
        Assert.assertEquals("table identifier should equal", String.format("%s.%s", DB_NAME, TB2),
                table.toString());
        Assert.assertEquals("default fs should be file:///", DEFAULT_FS,
                table.hadoopConf().get(C.DEFAULT_FS));
        Assert.assertTrue("should contain table name in info",
                info.contains("\"table\":\"hivedb.t2\""));
        String tbInfo = "{\"table\":\"hivedb.t2\"," +
                "\"partition\":[{\"sourceId\":1,\"fieldId\":1000,\"name\":\"ts_day\",\"transform\":\"DAY\"}]," +
                "\"schema\":[{\"id\":1,\"name\":\"ts\",\"type\":\"TIMESTAMP\",\"optional\":false}," +
                "{\"id\":2,\"name\":\"balance\",\"type\":\"LONG\",\"optional\":false}," +
                "{\"id\":3,\"name\":\"salary\",\"type\":\"INTEGER\",\"optional\":false}," +
                "{\"id\":4,\"name\":\"info\",\"type\":\"STRING\",\"optional\":false}," +
                "{\"id\":5,\"name\":\"aa\",\"type\":\"DOUBLE\",\"optional\":false}," +
                "{\"id\":6,\"name\":\"bb\",\"type\":\"FLOAT\",\"optional\":false}," +
                "{\"id\":7,\"name\":\"cc\",\"type\":\"STRING\",\"optional\":false}]}";
        Map<String, Object> tbInfoMap = new ObjectMapper()
                .readValue(tbInfo, new TypeReference<Map<String, Object>>() {
                });
        Map<String, Object> infoMap = new ObjectMapper()
                .readValue(info, new TypeReference<Map<String, Object>>() {
                });
        Assert.assertEquals("table info should equals", tbInfoMap.get("table"),
                infoMap.get("table"));
        Assert.assertEquals("table info should equals", tbInfoMap.get("partition"),
                infoMap.get("partition"));
        Assert.assertEquals("table info should equals", tbInfoMap.get("schema"),
                infoMap.get("schema"));
        Assert.assertEquals(1, metadataVersionFiles(TB2).size());

        // 修改表分区方式
        List<PartitionMethod> pMethods2 = Collections
                .singletonList(new PartitionMethod("tS", "month"));
        table.changePartitionSpec(pMethods2);
        Assert.assertEquals(2, metadataVersionFiles(TB2).size());
        Assert.assertEquals(2, table.table().specs().size());
        spec = table.table().spec();
        Assert.assertEquals("month", spec.fields().get(0).transform().toString());
        Assert.assertEquals(1, spec.fields().size());

        // 校验属性和location地址
        Map<String, String> tableProps = new HashMap<>(table.table().properties());
        Assert.assertTrue(tableProps.containsKey(METADATA_DELETE_AFTER_COMMIT_ENABLED));
        Assert.assertEquals("true", tableProps.get(METADATA_DELETE_AFTER_COMMIT_ENABLED));
        String location = "file://///tmp/hive/hivedb.db/t2";
        Assert.assertEquals(location, table.table().location());

        // 更新表属性和表位置
        location = "file:///tmp/hive/hivedb.db/t2"; // 修改location
        table.updateLocation(location);
        Assert.assertEquals(3, metadataVersionFiles(TB2).size());

        tableProps.put(METADATA_PREVIOUS_VERSIONS_MAX, "1");
        table.updateProperties(tableProps);
        // 保留一个，当前一个，一共两个版本文件
        Assert.assertEquals(2, metadataVersionFiles(TB2).size());
        Assert.assertEquals("1", table.table().properties().get(METADATA_PREVIOUS_VERSIONS_MAX));

        // 再次创建同名表会报错
        BkTable table2 = new BkTable(DB_NAME, TB2, props);
        AssertHelpers.assertThrows("create already exist table should fail",
                TableExists.class,
                "table is already exists, please don't create it again!",
                () -> table2.createTable(fields, pMethods));

        // 使用一个不存在的db建表，此时应该db和表都创建成功。
        String DB_NOT_EXISTS = "not_exist";
        BkTable table3 = new BkTable(DB_NOT_EXISTS, TB1, props);
        table3.createTable(fields, pMethods);
        Assert.assertNotNull("db and table should be created: " + table3, table3.table());

        Map<Namespace, List<TableIdentifier>> tables = table.listCatalog();
        Assert.assertEquals("should be two databases", 2, tables.keySet().size());
        Assert.assertEquals(1, tables.get(TableIdentifier.of(DB_NOT_EXISTS, TB1).namespace()).size());
        Assert.assertEquals(2, tables.get(TableIdentifier.of(DB_NAME, TB1).namespace()).size());

        table.dropTable();
        table2.dropTable();
        table3.dropTable();
    }

    @Test
    public void testLoadTable() {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();
        Assert.assertEquals("file format should be parquet", C.FILE_FORMAT,
                table.table().properties().get(DEFAULT_FILE_FORMAT));
        Assert.assertEquals(TEST_VALUE, table.table().properties().get(TEST_KEY));

        // 验证addCommitMsg方法
        Map<String, String> map = ImmutableMap.of("topic-0", "100", "topic-1", "25");
        CommitMsg msg = new CommitMsg(C.DT_RESET, map);
        table.addCommitMsg(msg);
        Map<String, String> summary = table.table().currentSnapshot().summary();
        Assert.assertTrue(summary.containsKey(C.DT_SDK_MSG));
        Optional<CommitMsg> msg2 = Utils.parseCommitMsg(summary.get(C.DT_SDK_MSG));
        Assert.assertTrue(msg2.isPresent());
        Assert.assertEquals(msg2.get(), msg);

        boolean success = table.dropTable();
        Assert.assertTrue("drop table should success", success);

        BkTable table2 = new BkTable(DB_NAME, "table_not_exists", props);
        AssertHelpers.assertThrows("load table should fail",
                TableNotExists.class,
                "please create the table before load it!",
                table2::loadTable);

    }

    @Test
    public void testChangeSchema() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB5, props);
        List<TableField> fields = Stream
                .of("TS,timestamp", "BALance,lONg", "salary,INT", "info,string")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1]))
                .collect(Collectors.toList());
        // 正常创建成功一张表
        List<PartitionMethod> pMethods = Collections.singletonList(new PartitionMethod("tS", "hour"));
        table.createTable(fields, pMethods);

        TableField t1 = new TableField("tS", "Long");
        TableField t2 = new TableField("aaaa", "string", false);
        List<TableField> f1 = Stream.of(t1, t2).collect(Collectors.toList());
        AssertHelpers.assertThrows("could not add already exist fields",
                IllegalArgumentException.class,
                "field should not already exist in table during adding",
                () -> table.addFields(f1));

        f1.remove(t1);
        table.addFields(f1);
        Assert.assertEquals("table should have 5 fields now", 5, table.schema().columns().size());
        Assert.assertEquals("table field aaaa's type should be string", Types.StringType.get(),
                table.schema().findField("aaaa").type());
        Assert.assertNotNull("table field balance should exists", table.schema().findField("balance"));

        Set<String> s1 = Stream.of("aaaA", "ts", "not_exists").collect(Collectors.toSet());
        AssertHelpers.assertThrows("could not remove not exist fields",
                IllegalArgumentException.class,
                "field should exists and not a partition field during removing",
                () -> table.removeFields(s1));

        s1.remove("not_exists");
        AssertHelpers.assertThrows("could not remove partition fields",
                IllegalArgumentException.class,
                "field should exists and not a partition field during removing",
                () -> table.removeFields(s1));

        s1.remove("ts");
        table.removeFields(s1);
        Assert.assertEquals("table should have 4 fields now", 4, table.schema().columns().size());

        TableField t3 = new TableField("bb", "string", true);
        TableField t4 = new TableField("cc", "double", true);
        List<TableField> f2 = Stream.of(t3, t4).collect(Collectors.toList());
        Set<String> s2 = Stream.of("balance", "salary").collect(Collectors.toSet());
        table.modifyFields(f2, s2);
        Assert.assertEquals("table should have 4 fields now", 4, table.schema().columns().size());
        Assert.assertEquals("table field bb's type should be string", Types.StringType.get(),
                table.schema().findField("bb").type());
        Assert.assertNull("table field balance should not exists anymore", table.schema().findField("balance"));
        Assert.assertNull("table field salary should not exists anymore", table.schema().findField("salary"));

        BkTable table2 = new BkTable("mapleleaf_591", "iceberg_xxx_591", props);
        List<TableField> fields2 = Stream
                .of("dtEventTimeStamp,long", "report_time,string", "log,string",
                        "dtEventTime,timestamp", "ip,string", "path_n,string", "gseindex,long",
                        "localTime,string")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1], true))
                .collect(Collectors.toList());
        // 建表
        List<PartitionMethod> pMethods2 = Collections.singletonList(new PartitionMethod("dtEventTime", "hour"));
        table2.createTable(fields2, pMethods2);
        Assert.assertEquals("table should have 8 fields now", 8, table2.schema().columns().size());
        // 添加字段
        TableField t5 = new TableField("path_new", "string", true);
        table2.addFields(Collections.singletonList(t5));
        Assert.assertEquals("table should have 9 fields now", 9, table2.schema().columns().size());
        // 删除字段
        Set<String> remove = new HashSet<>();
        remove.add("path_n");
        table2.removeFields(remove);
        String info = "{\"table\":\"mapleleaf_591.iceberg_xxx_591\",\"partition\":" +
                "[{\"sourceId\":4,\"fieldId\":1000,\"name\":\"dteventtime_hour\",\"transform\":\"HOUR\"}]," +
                "\"schema\":[{\"id\":1,\"name\":\"dteventtimestamp\",\"type\":\"LONG\",\"optional\":true}," +
                "{\"id\":2,\"name\":\"report_time\",\"type\":\"STRING\",\"optional\":true}," +
                "{\"id\":3,\"name\":\"log\",\"type\":\"STRING\",\"optional\":true}," +
                "{\"id\":4,\"name\":\"dteventtime\",\"type\":\"TIMESTAMP\",\"optional\":true}," +
                "{\"id\":5,\"name\":\"ip\",\"type\":\"STRING\",\"optional\":true}," +
                "{\"id\":7,\"name\":\"gseindex\",\"type\":\"LONG\",\"optional\":true}," +
                "{\"id\":8,\"name\":\"localtime\",\"type\":\"STRING\",\"optional\":true}," +
                "{\"id\":9,\"name\":\"path_new\",\"type\":\"STRING\",\"optional\":true}]}";
        Map<String, Object> infoMap = new ObjectMapper().readValue(table2.info(),
                new TypeReference<Map<String, Object>>() {
                });
        Map<String, Object> map = new ObjectMapper().readValue(info, new TypeReference<Map<String, Object>>() {
        });
        Assert.assertEquals("table info should equals", infoMap.get("table"), map.get("table"));
        Assert.assertEquals("table info should equals", infoMap.get("partition"), map.get("partition"));
        Assert.assertEquals("table info should equals", infoMap.get("schema"), map.get("schema"));
        Assert.assertEquals("table should have 8 fields now", 8, table2.schema().columns().size());

        // 修改字段名称, report_time -> report, log -> LOG_value, ip -> ip_addr
        String str1 = "report_time";
        String str11 = "report";
        String str2 = "log";
        String str22 = "log_value";
        Types.NestedField field1 = table2.schema().findField(str1);
        Types.NestedField field2 = table2.schema().findField(str2);
        Map<String, String> rename = ImmutableMap.of(str1, str11, str2.toUpperCase(), str22.toUpperCase());
        table2.renameFields(rename);
        Types.NestedField field1_new = table2.schema().findField(str11);
        Types.NestedField field2_new = table2.schema().findField(str22);
        Assert.assertEquals(field1.fieldId(), field1_new.fieldId());
        Assert.assertEquals(field2.fieldId(), field2_new.fieldId());

        table.dropTable();
        table2.dropTable();
    }

    @Test
    public void testUnPartitionedTable() {
        BkTable table = new BkTable(DB_NAME, TB5, props);
        List<TableField> fields = Stream
                .of("TS,timestamp", "BALance,lONg", "salary,INT", "info,string")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1]))
                .collect(Collectors.toList());
        // 正常创建成功一张表，非分区表
        table.createTable(fields);

        // 测试写入数据
        int count = 100_000;
        String countStr = String.valueOf(count);
        DataBuffer buffer = new DataBuffer(table, 300_000, 1_000_000);
        Map<String, String> addCommitMsgs = new HashMap<>();
        List<Record> records = RandomGenericData.generate(table.schema(), count, 0L);
        addCommitMsgs.put("topic-test-0", countStr);
        buffer.add(records, addCommitMsgs);
        buffer.close();

        // 校验数据量
        Map<String, String> summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(countStr, summary.get("total-records"));
        Assert.assertEquals("1", summary.get("total-data-files"));
        Assert.assertEquals(countStr, summary.get("added-records"));
        Assert.assertEquals("1", summary.get("added-data-files"));
        Assert.assertEquals("1", summary.get("changed-partition-count"));
        Assert.assertEquals(0, table.getPartitionPaths().size());

        List<DataFile> dataFiles = table.getAllDataFiles();
        Assert.assertEquals(1, dataFiles.size());

        // 删除数据
        String field = "salary";
        int val = 1_000_000;
        Expression expression = greaterThan(field, val);
        OpsMetric metric = table.deleteData(expression);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(countStr, summary.get("deleted-records"));
        Assert.assertEquals(summary.get("added-records"), summary.get("total-records"));
        Assert.assertEquals("1", summary.get("deleted-data-files"));
        Assert.assertEquals("1", summary.get("added-data-files"));
        Assert.assertEquals("1", summary.get("total-data-files"));
        Assert.assertEquals("1", summary.get("changed-partition-count"));
        int notMatchCount = Integer.parseInt(summary.get("total-records"));

        Assert.assertEquals(String.valueOf(count - notMatchCount), metric.summary().get("affected.records"));
        Assert.assertEquals(countStr, metric.summary().get("scan.records"));
        Assert.assertEquals("1", metric.summary().get("scan.files"));
        dataFiles = table.getAllDataFiles();
        Assert.assertEquals(1, dataFiles.size());

        // 更新数据
        expression = Expressions.not(expression);
        metric = table.updateData(expression, ImmutableMap.of(field, new Sum(999_999_999)));
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(notMatchCount), summary.get("deleted-records"));
        Assert.assertEquals(summary.get("added-records"), summary.get("total-records"));
        Assert.assertEquals("1", summary.get("deleted-data-files"));
        Assert.assertEquals("1", summary.get("added-data-files"));
        Assert.assertEquals("1", summary.get("total-data-files"));
        Assert.assertEquals("1", summary.get("changed-partition-count"));
        Assert.assertEquals(String.valueOf(notMatchCount), metric.summary().get("affected.records"));
        Assert.assertEquals(metric.summary().get("affected.records"), metric.summary().get("scan.records"));
        Assert.assertEquals("1", metric.summary().get("scan.files"));

        dataFiles = table.getAllDataFiles();
        Assert.assertEquals(1, dataFiles.size());

        table.dropTable();
    }

    @Test
    public void testDelete() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();
        Assert.assertEquals(0L, table.totalCount());

        // 写入数据
        DataBuffer buffer1 = new DataBuffer(table, 300_000, 1_000_000);
        Map<String, String> addCommitMsgs = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            List<Record> records = RandomGenericData.generate(table.schema(), 50_000, 0L);
            addCommitMsgs.put("topic-test-" + i, "50000");
            buffer1.add(records, addCommitMsgs);
        }
        buffer1.close();

        Assert.assertEquals(500_000L, table.totalCount());
        Map<String, String> summary = table.table().currentSnapshot().summary();
        Assert.assertEquals("3", summary.get("added-data-files"));
        Assert.assertEquals("3", summary.get("changed-partition-count"));
        Assert.assertEquals("500000", summary.get("total-records"));
        Assert.assertEquals("3", summary.get("total-data-files"));

        List<String> partitionPaths = table.getPartitionPaths();
        Assert.assertEquals(3, partitionPaths.size());
        Assert.assertTrue(partitionPaths.contains("ts_day=2019-12-31"));
        Assert.assertTrue(partitionPaths.contains("ts_day=2020-01-01"));
        Assert.assertTrue(partitionPaths.contains("ts_day=2020-01-02"));

        // 测试删除部分分区
        OpsMetric metric = table.deletePartitions("tS", Collections.singleton("2019-12-31"));
        Assert.assertTrue(metric.toString().contains("took"));
        partitionPaths = table.getPartitionPaths();
        Assert.assertEquals(2, partitionPaths.size());
        Assert.assertFalse(partitionPaths.contains("ts_day=2019-12-31"));
        Map<String, String> sum = metric.summary();
        Assert.assertEquals("true", sum.get(C.FINISHED));
        Assert.assertEquals("1", sum.get(C.SCAN_FILES));
        Assert.assertEquals(sum.get(C.SCAN_RECORDS), sum.get(C.AFFECTED_RECORDS));

        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals("1", summary.get("deleted-data-files"));
        Assert.assertEquals("1", summary.get("changed-partition-count"));
        Assert.assertEquals("2", summary.get("total-data-files"));
        String totalRecords = summary.get("total-records");

        // 测试通过表达式删除数据
        AssertHelpers.assertThrows("bad expression",
                IllegalArgumentException.class,
                "can't delete all data in table",
                () -> table.deleteData(alwaysTrue()));

        Expression expression = greaterThan("salary", 100_000);
        metric = table.deleteData(expression);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(totalRecords, summary.get("deleted-records")); // 所有数据文件均被重写，所以删除的记录数等于之前的总记录数
        Assert.assertEquals("2", summary.get("added-data-files"));
        Assert.assertEquals("2", summary.get("deleted-data-files"));
        Assert.assertEquals("2", summary.get("changed-partition-count"));
        Assert.assertEquals("2", summary.get("total-data-files"));
        sum = metric.summary();
        Assert.assertEquals("true", sum.get(C.FINISHED));
        Assert.assertEquals("2", sum.get(C.SCAN_FILES));
        Assert.assertTrue(Long.parseLong(sum.get(C.SCAN_RECORDS)) > Long.parseLong(sum.get(C.AFFECTED_RECORDS)));

        List<CommitMsg> msgs = table.getLatestCommitMsg(10);
        Assert.assertEquals(3, msgs.size());

        CommitMsg msg = msgs.get(0);
        Assert.assertEquals(C.DT_UPDATE, msg.getOperation());
        // 对于实际上是删除的操作，其fields字段内容为空
        Assert.assertEquals("", msg.getData().get("fields"));
        Assert.assertEquals(expression.toString(), msg.getData().get("expr"));

        msg = msgs.get(1);
        Assert.assertEquals(C.DT_DELETE, msg.getOperation());
        // 根据ts字段的分区值为"2019-12-31"进行删除
        Assert.assertTrue(msg.getData().containsKey("ts_day"));
        Assert.assertEquals("2019-12-31", msg.getData().get("ts_day"));

        msg = msgs.get(msgs.size() - 1); // get the first commit msg in this table
        Assert.assertEquals(C.DT_ADD, msg.getOperation());
        Assert.assertEquals(addCommitMsgs, msg.getData());

        // 按照时间条件过滤数据进行删除
        String timestamp1 = "2020-01-01T03:50:55Z";
        String timestamp2 = "2020-01-01T18:00:05Z";
        OffsetDateTime dt1 = OffsetDateTime.parse(timestamp1);
        OffsetDateTime dt2 = OffsetDateTime.parse(timestamp2);
        expression = and(greaterThan("ts", timestamp1),
                lessThan("ts", timestamp2));
        metric = table.deleteData(expression);
        sum = metric.summary();
        Assert.assertEquals("true", sum.get(C.FINISHED));
        Assert.assertEquals("1", sum.get(C.SCAN_FILES));
        Assert.assertTrue(Long.parseLong(sum.get(C.SCAN_RECORDS)) > Long.parseLong(sum.get(C.AFFECTED_RECORDS)));
        Assert.assertEquals(2, table.getPartitionPaths().size());

        // 符合条件的数据已全部删除
        AtomicInteger count = new AtomicInteger(0);
        try (CloseableIterable<Record> iter = table.readRecords(expression)) {
            iter.forEach(r -> count.getAndIncrement());
        } catch (IOException ignore) {
            // ignore
        }
        Assert.assertEquals(0, count.get());

        // 剩下的数据都不符合条件
        try (CloseableIterable<Record> iter = table.readRecords(alwaysTrue())) {
            iter.forEach(r -> {
                Assert.assertTrue(((OffsetDateTime) r.getField("ts")).compareTo(dt1) <= 0
                        || ((OffsetDateTime) r.getField("ts")).compareTo(dt2) >= 0);
            });
        } catch (IOException ignore) {
            // ignore
        }

        // 检索表的数据量
        String start = "2020-01-01T00:00:00Z";
        String end = "2020-01-02T00:00:00Z";
        expression = and(greaterThanOrEqual("ts", start), lessThan("ts", end));
        OffsetDateTime startDt = OffsetDateTime.parse(start);
        OffsetDateTime endDt = OffsetDateTime.parse(end);
        long countBetween = table.getRecordCountBetween("ts", startDt, endDt);
        long toDeleteCount = table.getRecordCount(expression);
        Assert.assertEquals(countBetween, toDeleteCount);
        // 异常场景，例如开始时间晚于结束时间，或者时间字符串不满足ISO_LOCAL_DATE_TIME，无法解析
        Assert.assertEquals(0L, table.getRecordCountBetween("ts", endDt, startDt));

        // 尝试删除整个分区的数据
        metric = table.deleteData(expression);
        sum = metric.summary();
        Assert.assertEquals("true", sum.get(C.FINISHED));
        Assert.assertEquals("1", sum.get(C.SCAN_FILES));
        Assert.assertEquals(sum.get(C.SCAN_RECORDS), sum.get(C.AFFECTED_RECORDS));
        // 删除2020-01-01分区后，只剩下2020-01-02一个分区
        Assert.assertEquals(1, table.getPartitionPaths().size());
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(sum.get(C.AFFECTED_RECORDS), String.valueOf(toDeleteCount));
        Assert.assertEquals(sum.get(C.AFFECTED_RECORDS), summary.get("deleted-records"));
        Assert.assertEquals("1", summary.get("deleted-data-files"));
        Assert.assertEquals("1", summary.get("changed-partition-count"));
        Assert.assertEquals("1", summary.get("total-data-files"));

        // 获取表的数据文件和元文件
        Set<String> mFiles = TableUtils.allManifestPaths(table.table());
        Set<String> dFiles = TableUtils.allDataFilePaths(table.table());
        Set<String> mlFiles = TableUtils.allManifestListPaths(table.table());
        Set<String> otherFiles = TableUtils.otherMetadataPaths(table.table());

        Assert.assertEquals(7, mFiles.size());
        Assert.assertEquals(6, dFiles.size());
        Assert.assertEquals(5, mlFiles.size());
        Assert.assertEquals(7, otherFiles.size());
    }

    @Test
    public void testUpdate() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();

        // 写入数据
        DataBuffer buffer1 = new DataBuffer(table, 300_000, 1_000_000);
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            records = RandomGenericData.generate(table.schema(), 50_000, 0L);
            buffer1.add(records, ImmutableMap.of("topic-test-" + i, "50000"));
        }
        buffer1.close();

        Map<String, String> summary = table.table().currentSnapshot().summary();
        String totalRecords = summary.get("total-records");

        // 更新salary字段的值，将大于100w的都更新为200w
        Map<String, ValFunction> transformers = new HashMap<>();
        transformers.put("salary", (r, f) -> r.setField(f, 2_000_000));
        Expression expr1 = greaterThan("salary", 1_000_000);
        long matchRecords1 = TestUtils.getMatchRecordCount(table.table(), expr1);

        // 更新表数据
        OpsMetric metric = table.updateData(expr1, transformers);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(totalRecords, summary.get("total-records"));
        Assert.assertEquals(totalRecords, summary.get("deleted-records"));
        Assert.assertEquals(totalRecords, summary.get("added-records"));
        Assert.assertEquals("3", summary.get("added-data-files"));
        Assert.assertEquals("3", summary.get("deleted-data-files"));
        Assert.assertEquals("3", summary.get("changed-partition-count"));
        Map<String, String> sum = metric.summary();
        Assert.assertEquals("3", sum.get(C.SCAN_FILES));
        Assert.assertEquals("500000", sum.get(C.SCAN_RECORDS));
        Assert.assertTrue(Long.parseLong(sum.get(C.SCAN_RECORDS)) > Long.parseLong(sum.get(C.AFFECTED_RECORDS)));

        Expression expr2 = equal("salary", 2_000_000);
        long matchRecords2 = TestUtils.getMatchRecordCount(table.table(), expr2);
        Assert.assertEquals(matchRecords1, matchRecords2);

        // 数据类型不匹配，会抛出异常。
        transformers.put("salary", (r, f) -> r.setField(f, 10_000_000L));
        AssertHelpers.assertThrows("column type mismatching, unable to update",
                CommitFileFailed.class,
                "hivedb.t1 failed to append data files",
                () -> table.updateData(expr2, transformers));

        // 使用sdk中提供的Assign数值转换类做处理
        transformers.put("salary", new Assign<>(5_000_000));
        metric = table.updateData(expr2, transformers);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(totalRecords, summary.get("total-records"));
        Assert.assertEquals(totalRecords, summary.get("deleted-records"));
        Assert.assertEquals(totalRecords, summary.get("added-records"));
        Assert.assertEquals("3", summary.get("added-data-files"));
        Assert.assertEquals("3", summary.get("deleted-data-files"));
        Assert.assertEquals("3", summary.get("changed-partition-count"));
        sum = metric.summary();
        Assert.assertEquals("3", sum.get(C.SCAN_FILES));
        Assert.assertEquals("500000", sum.get(C.SCAN_RECORDS));
        Assert.assertTrue(Long.parseLong(sum.get(C.SCAN_RECORDS)) > Long.parseLong(sum.get(C.AFFECTED_RECORDS)));

        List<CommitMsg> msgs1 = table.getLatestCommitMsg(10);
        List<CommitMsg> msgs2 = table.getLatestCommitMsg(1);
        Assert.assertEquals(3, msgs1.size());
        Assert.assertEquals(1, msgs2.size());
        // 接口获取的CommitMsg列表为倒序排列，最新的CommitMsg在最前面，所以这两个列表中第一个元素相同。
        Assert.assertEquals(msgs1.get(0), msgs2.get(0));
        CommitMsg msg = msgs1.get(0);
        Assert.assertEquals(C.DT_UPDATE, msg.getOperation());
        Assert.assertEquals("salary", msg.getData().get("fields"));
        Assert.assertEquals(expr2.toString(), msg.getData().get("expr"));

        Expression expr3 = equal("salary", 5_000_000);
        long matchRecords3 = TestUtils.getMatchRecordCount(table.table(), expr3);
        Assert.assertEquals(matchRecords2, matchRecords3);

        // 使用另一个字段更新本字段，将cc字段的值更新到info字段里。
        Map<String, ValFunction> trans1 = new HashMap<>();
        trans1.put("info", (r, f) -> r.setField(f, r.getField("cc")));
        table.updateData(expr3, trans1);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(totalRecords, summary.get("total-records"));
        Assert.assertEquals(totalRecords, summary.get("deleted-records"));
        Assert.assertEquals(totalRecords, summary.get("added-records"));
        Assert.assertEquals("3", summary.get("added-data-files"));
        Assert.assertEquals("3", summary.get("deleted-data-files"));
        Assert.assertEquals("3", summary.get("changed-partition-count"));
        try (CloseableIterable<Record> iter = table.readRecords(expr3)) {
            iter.forEach(r -> Assert.assertEquals(r.getField("info"), r.getField("cc")));
        } catch (IOException ignore) {
            // ignore
        }

        // 使用Sum对int/long/float/double类型的数据进行处理
        for (Record r : records) {
            if (((Integer) r.getField("salary")) < 1_000_000) {
                Expression expr4 = and(equal("balance", r.getField("balance")),
                        equal("salary", r.getField("salary")),
                        equal("info", r.getField("info")),
                        equal("aa", r.getField("aa")),
                        equal("bb", r.getField("bb")),
                        equal("cc", r.getField("cc")));
                long count1 = TestUtils.getMatchRecordCount(table.table(), expr4);

                long add1 = 1_000_000L;
                int add2 = 1_000;
                double add3 = 3.1415d;
                float add4 = 1.6888888f;
                Map<String, ValFunction> trans = new HashMap<>();
                trans.put("balance", new Sum(add1));
                trans.put("salary", new Sum(add2));
                trans.put("aa", new Sum(add3));
                trans.put("bb", new Sum(add4));
                table.updateData(expr4, trans);
                Expression expr5 = and(equal("balance", (Long) r.getField("balance") + add1),
                        equal("salary", (Integer) r.getField("salary") + add2),
                        equal("info", r.getField("info")),
                        equal("aa", (Double) r.getField("aa") + add3),
                        equal("bb", (Float) r.getField("bb") + add4),
                        equal("cc", r.getField("cc")));
                long count2 = TestUtils.getMatchRecordCount(table.table(), expr5);
                Assert.assertEquals(count1, count2);

                break;
            }
        }

        // 测试truncate表功能
        table.truncateTable();
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals("0", summary.get("total-records"));
    }

    @Test
    public void testSnapshotExpire() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();

        // 写入数据
        int batch = 100_000;
        int loop = 20;
        DataBuffer buffer = new DataBuffer(table, 300_000, batch);
        for (int i = 0; i < loop; i++) {
            List<Record> records = RandomGenericData.generate(table.schema(), batch, 100L * i);
            buffer.add(records, ImmutableMap.of("topic-test-0", String.valueOf(batch * i)));
        }
        buffer.close();

        // 应该commit次数和loop次数相同，和snapshot的个数也相同。
        List<Long> snapshotIds = new ArrayList<>();
        table.table().snapshots().forEach(s -> snapshotIds.add(s.snapshotId()));
        Assert.assertEquals(loop, snapshotIds.size());

        String HALF_MILLION = "500000";
        List<Snapshot> snapshots = table.filterSnapshots(s -> HALF_MILLION.equals(s.summary().get("total-records")));
        Assert.assertEquals(1, snapshots.size());
        Assert.assertEquals(HALF_MILLION, snapshots.get(0).summary().get("total-records"));

        // 删除一半的快照
        ExpireSnapshots expire = table.table().expireSnapshots();
        IntStream.range(0, loop / 2).forEach(i -> expire.expireSnapshotId(snapshotIds.get(i)));
        expire.commit();

        snapshots = table.filterSnapshots(s -> HALF_MILLION.equals(s.summary().get("total-records")));
        Assert.assertEquals(0, snapshots.size());

        List<Long> snapshotIdsNew = new ArrayList<>();
        table.table().snapshots().forEach(s -> snapshotIdsNew.add(s.snapshotId()));
        Assert.assertEquals(loop / 2, snapshotIdsNew.size());

        List<String> partitionPaths = table.getPartitionPaths();
        Assert.assertEquals(3, partitionPaths.size());
        Assert.assertTrue(partitionPaths.contains("ts_day=2019-12-31"));
        Assert.assertTrue(partitionPaths.contains("ts_day=2020-01-01"));
        Assert.assertTrue(partitionPaths.contains("ts_day=2020-01-02"));

        // 删除一个分区，然后过期snapshot，只保留最新的snapshot。
        // 过期的snapshot里只包含add-data-file，所以此时不清理数据文件。
        table.deletePartitions("tS", Collections.singleton("2019-12-31"));
        long currentSnapshotTs = table.table().currentSnapshot().timestampMillis();
        table.table().expireSnapshots().expireOlderThan(currentSnapshotTs).commit();
        Assert.assertEquals(1, table.getLatestSnapshots(10).size());
        Assert.assertEquals("40",
                table.table().currentSnapshot().summary().get("total-data-files"));

        table.deletePartitions("tS", Collections.singleton("2020-01-01"));
        currentSnapshotTs = table.table().currentSnapshot().timestampMillis();
        // 此时清理 ts_day=2019-12-31目录中数据文件
        table.table().expireSnapshots().expireOlderThan(currentSnapshotTs).commit();
        Assert.assertEquals(1, table.getLatestSnapshots(10).size());
        Assert.assertEquals("20",
                table.table().currentSnapshot().summary().get("total-data-files"));

        table.deletePartitions("tS", Collections.singleton("2020-01-02"));
        currentSnapshotTs = table.table().currentSnapshot().timestampMillis();
        // 此时清理 ts_day=2020-01-01目录中数据文件
        table.table().expireSnapshots().expireOlderThan(currentSnapshotTs).commit();
        Assert.assertEquals(1, table.getLatestSnapshots(10).size());
        Assert.assertEquals("0", table.table().currentSnapshot().summary().get("total-data-files"));

        // 再次写一批数据
        buffer = new DataBuffer(table, 300_000, batch);
        List<Record> records = RandomGenericData.generate(table.schema(), batch, 0L);
        buffer.add(records, ImmutableMap.of("topic-test-0", String.valueOf(batch * loop)));
        buffer.close();
        currentSnapshotTs = table.table().currentSnapshot().timestampMillis();
        // 此时清理 ts_day=2020-01-02目录中数据文件
        table.table().expireSnapshots().expireOlderThan(currentSnapshotTs).commit();
        Assert.assertEquals(1, table.getLatestSnapshots(10).size());
        Assert.assertEquals("3", table.table().currentSnapshot().summary().get("total-data-files"));

        String info = table.info();
        Map<String, Object> tbInfoMap = new ObjectMapper()
                .readValue(info, new TypeReference<Map<String, Object>>() {
                });
        Assert.assertEquals("name should equals", tbInfoMap.get("table"), table.toString());
        Assert.assertEquals("partition column should be 1", 1,
                ((List) tbInfoMap.get("partition")).size());
        Assert.assertEquals("fields should be 7", 7, ((List) tbInfoMap.get("schema")).size());
        Assert.assertEquals("snapshots should be 1", 1,
                ((List) tbInfoMap.get("snapshots")).size());
        Assert.assertEquals("partition paths should be 3", 3,
                ((List) tbInfoMap.get("partition_paths")).size());
    }

    @Test
    public void testExpireData() {
        // 新建一个以小时为分区的表，测试数据过期方法
        BkTable table2 = new BkTable(DB_NAME, TB3, props);
        List<TableField> fields = Stream.of("dtEventTime,timestamp", "BALance,lONg", "info,string")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1]))
                .collect(Collectors.toList());
        List<PartitionMethod> pMethods = Collections.singletonList(new PartitionMethod("dtEventTime", "hour"));
        table2.createTable(fields, pMethods);

        List<Record> recordList = table2.sampleRecords(10);
        Assert.assertEquals(0, recordList.size());

        DataBuffer buffer = new DataBuffer(table2, 30_000, 5_000_000);
        List<Record> records = RandomGenericData.generate(table2.schema(), 1_000_000, 0L);
        buffer.add(records, ImmutableMap.of("topic-test-0", "1000000"));
        buffer.close();

        recordList = table2.sampleRecords(10);
        Assert.assertEquals(10, recordList.size());

        // 此时有 2019-12-31/2020-01-01/2020-01-02三个日期，实际是2020-01-01T12:00Z前后48小时，对应48个小时分区
        List<String> paths = table2.getPartitionPaths();
        Assert.assertEquals(48, paths.size());
        Assert.assertEquals("48", table2.table().currentSnapshot().summary().get("total-data-files"));

        // 2020-01-01 00:00:00 UTC
        OffsetDateTime dateTime = OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 1, ZoneOffset.UTC);
        OffsetDateTime now = Instant.now().atOffset(ZoneOffset.UTC);
        int expireDays = (int) ChronoUnit.DAYS.between(dateTime, now);
        table2.expireEarlyData("dtEventTime", expireDays);

        recordList = table2.sampleRecords(10);
        Assert.assertEquals(10, recordList.size());

        paths = table2.getPartitionPaths();
        int hour = now.getHour();
        int remainPartitions = 48 - 12 - hour;
        Assert.assertEquals(remainPartitions, paths.size());
        Assert.assertEquals(String.valueOf(remainPartitions),
                table2.table().currentSnapshot().summary().get("total-data-files"));

        // 修改表分区方式，使用日期作为分区函数
        List<PartitionMethod> pMethods2 = Collections.singletonList(new PartitionMethod("dtEventTime", "day"));
        table2.changePartitionSpec(pMethods2);

        buffer = new DataBuffer(table2, 30_000, 5_000_000);
        records = RandomGenericData.generate(table2.schema(), 100_000, 0L);
        buffer.add(records, ImmutableMap.of("topic-test-1", "100000"));
        buffer.close();

        paths = table2.getPartitionPaths();
        Assert.assertEquals(remainPartitions + 3, paths.size());
        Assert.assertTrue(paths.contains("dteventtime_day=2019-12-31"));
        Assert.assertTrue(paths.contains("dteventtime_day=2020-01-01"));
        Assert.assertTrue(paths.contains("dteventtime_day=2020-01-02"));

        // 通过指定分区方式删除，可以删除新旧两种分区方式和对应的数据
        table2.deletePartitions("dtEventTime", ImmutableSet.of("2020-01-02-01", "2020-01-01"));
        recordList = table2.sampleRecords(10);
        Assert.assertEquals(10, recordList.size());
        // 新增三个分区，然后删除两个分区
        Assert.assertEquals(remainPartitions + 1, table2.getPartitionPaths().size());

        // 当前四个快照，设置过期数据时至少保留两个快照
        List<Snapshot> snapshots = new ArrayList<>();
        table2.table().snapshots().forEach(snapshots::add);
        Assert.assertEquals(4, snapshots.size());
        table2.updateProperties(ImmutableMap.of(C.PRESERVE_SNAPSHOT_NUMS, "20", C.PRESERVE_SNAPSHOT_DAYS, "0"));

        // 所有符合条件的数据均会被清理，无论是按照天分区还是小时分区的
        table2.expireEarlyData("dtEventTime", expireDays - 10);
        recordList = table2.sampleRecords(10);
        Assert.assertEquals(0, recordList.size());
        Assert.assertEquals(0, table2.getPartitionPaths().size());

        // 指定最多保留两个快照，这里过期快照后只剩下两个快照
        table2.updateProperties(ImmutableMap.of(C.PRESERVE_SNAPSHOT_NUMS, "2", C.PRESERVE_SNAPSHOT_DAYS, "0"));
        table2.expireSnapshots();
        snapshots.clear();
        table2.table().snapshots().forEach(snapshots::add);
        Assert.assertEquals(2, snapshots.size());

        table2.dropTable();
    }

    @Test
    public void testCompactData() {
        // 批量写入一批数据
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();
        // 当一批写入DataBuffer中的数据生成的数据文件达到2个时，触发刷盘数据
        table.updateProperties(ImmutableMap.of(C.MAX_COMMIT_DATAFILES, "2"));

        // 写入数据
        int batch = 50_000;
        int loop = 5;
        DataBuffer buffer = new DataBuffer(table, 300_000, batch * 10);
        for (int i = 0; i < loop; i++) {
            List<Record> records = RandomGenericData.generate(table.schema(), batch, 100L * i);
            buffer.add(records, ImmutableMap.of("topic-test-0", String.valueOf(batch * i)));
        }
        buffer.close();

        // 此时每个分区内10个数据文件，一共三个分区 2019-12-31， 2020-01-01， 2020-01-02
        Map<String, String> summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(batch * loop), summary.get("total-records"));
        Assert.assertEquals(String.valueOf(3 * loop), summary.get("total-data-files"));

        // 将分区2020-01-01中的所有数据文件重写为一个
        OffsetDateTime start = OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime end = OffsetDateTime.of(2020, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC);
        table.compactDataFile("ts", start, end);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(batch * loop), summary.get("total-records"));
        Assert.assertEquals(String.valueOf(2 * loop + 1), summary.get("total-data-files"));

        // 将所有分区的数据文件均重写，此时2020-01-01中由于只有一个数据文件，无需重写
        start = start.minusDays(1);
        end = end.plusDays(1);
        table.compactDataFile("ts", start, end);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(batch * loop), summary.get("total-records"));
        Assert.assertEquals("3", summary.get("total-data-files"));

        // 再按照天分区写一批数据，使每个分区目录下数据文件大于1个
        buffer = new DataBuffer(table, 300_000, batch * loop);
        List<Record> recordList = RandomGenericData.generate(table.schema(), batch * loop, 111);
        buffer.add(recordList, ImmutableMap.of("abc", "11111"));
        buffer.close();
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(batch * loop * 2), summary.get("total-records"));
        Assert.assertEquals("6", summary.get("total-data-files"));

        // 修改为按照小时分区
        List<PartitionMethod> pMethods2 = Collections.singletonList(new PartitionMethod("tS", "hour"));
        table.changePartitionSpec(pMethods2);
        // 合并小文件，此时分区定义发生过变化
        table.compactDataFile("ts", start, end);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(batch * loop * 2), summary.get("total-records"));
        Assert.assertEquals("48", summary.get("total-data-files")); // 48小时分区，每个分区里一个文件

        // 再次写入一批数据
        buffer = new DataBuffer(table, 300_000, batch);
        for (int i = 0; i < loop; i++) {
            List<Record> records = RandomGenericData.generate(table.schema(), batch, 100L * i);
            buffer.add(records, ImmutableMap.of("topic-test-0", String.valueOf(batch * i)));
        }
        buffer.close();
        // 合并小文件，此时分区定义未发生过变化
        table.compactDataFile("ts", start, end);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(batch * loop * 3), summary.get("total-records"));
        Assert.assertEquals("48", summary.get("total-data-files")); // 48小时分区，每个分区里一个文件

        int batch2 = 150_000;
        int loop2 = 5;
        buffer = new DataBuffer(table, 300_000, batch2);
        for (int i = 0; i < loop2; i++) {
            List<Record> records = RandomGenericData.generate(table.schema(), batch2, i);
            buffer.add(records, ImmutableMap.of("topic-test-0", "abc" + i));
        }
        buffer.close();
        // 合并小文件，此时分区定义未发生变化
        table.compactDataFile("ts", start, end);
        summary = table.table().currentSnapshot().summary();
        Assert.assertEquals(String.valueOf(batch * loop * 3 + batch2 * loop2), summary.get("total-records"));
        Assert.assertEquals("48", summary.get("total-data-files")); // 48小时分区，每个分区里一个文件
    }

    @Test
    public void testReadRecords() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();

        int count = 100;
        List<Record> recordList = table.sampleRecords(count);
        Assert.assertEquals(0, recordList.size());

        DataBuffer buffer = new DataBuffer(table, 30_000, 1_000_000);
        List<Record> add1 = RandomGenericData.generate(table.schema(), count, 100L);
        buffer.add(add1, ImmutableMap.of("topic-test-0", "100"));
        buffer.close();

        Expression all = alwaysTrue();
        Snapshot s1 = table.table().currentSnapshot();
        recordList = table.sampleRecords(count);
        Assert.assertEquals(count, recordList.size());
        recordList.forEach(r -> Assert.assertTrue(add1.contains(r)));
        int fieldNum = table.schema().columns().size();
        try (CloseableIterable<Record> iter = table.readRecords(all)) {
            iter.forEach(r -> Assert.assertTrue(add1.contains(r) && r.struct().fields().size() == fieldNum));
        } catch (IOException ignore) {
            // ignore
        }

        try (CloseableIterable<Record> iter = table.readRecords(all, "tS", "salary", "BALance")) {
            iter.forEach(r -> Assert.assertEquals(3, r.struct().fields().size()));
        } catch (IOException ignore) {
            // ignore
        }

        try (CloseableIterable<Record> iter = table.readSnapshotRecords(all,
                s1.snapshotId())) {
            iter.forEach(r -> Assert.assertTrue(add1.contains(r)));
        } catch (IOException ignore) {
            // ignore
        }

        List<Object[]> objArr = table.readRecordsInArray(all, 10, "tS", "salary", "BALance");
        Assert.assertEquals(10, objArr.size());
        objArr.forEach(arr -> {
            Assert.assertEquals(3, arr.length);
            Assert.assertTrue(arr[0] instanceof OffsetDateTime);
            Assert.assertTrue(arr[1] instanceof Integer);
            Assert.assertTrue(arr[2] instanceof Long);
        });

        buffer = new DataBuffer(table, 30_000, 1_000_000);
        List<Record> add2 = RandomGenericData.generate(table.schema(), count, 333L);
        buffer.add(add2, ImmutableMap.of("topic-test-0", "20"));
        buffer.close();

        Snapshot s2 = table.table().currentSnapshot();
        recordList = table.sampleRecords(2 * count);
        Assert.assertEquals(2 * count, recordList.size());
        // 这里只会读取最新一个快照中的数据，只能读取到10条数据
        List<Record> latestRecords = table.getLatestRecords(10);
        Assert.assertEquals(10, latestRecords.size());
        List<Record> list = new ArrayList<>();
        try (CloseableIterable<Record> iter = table.readAddedRecordsInSnapshot(alwaysTrue(), s2)) {
            // 这里最近一个snapshot中只有10条数据，所以即使整个表有20条数据，此方法也只能读到10条。
            iter.forEach(list::add);
            Assert.assertEquals(count, list.size());
        } catch (IOException ignore) {
            // ignore
        }

        for (Record r : latestRecords) {
            Assert.assertTrue(list.contains(r));
        }

        list.forEach(r -> Assert.assertTrue(add2.contains(r)));
        // 表中所有数据要么是第一次添加的，要么是第二次添加的
        try (CloseableIterable<Record> iter = table.readRecords(all)) {
            iter.forEach(r -> Assert.assertTrue(add2.contains(r) || add1.contains(r)));
        } catch (IOException ignore) {
            // ignore
        }
        try (CloseableIterable<Record> iter = table.readSnapshotRecords(all, s1.snapshotId())) {
            iter.forEach(r -> Assert.assertTrue(add1.contains(r)));
        } catch (IOException ignore) {
            // ignore
        }
        try (CloseableIterable<Record> iter = table.readSnapshotRecords(all, s2.snapshotId())) {
            iter.forEach(r -> Assert.assertTrue(add2.contains(r) || add1.contains(r)));
        } catch (IOException ignore) {
            // ignore
        }
        try (CloseableIterable<Record> iter = table.readSnapshotRecordsBetween(all,
                s1.snapshotId(), s2.snapshotId())) {
            iter.forEach(r -> Assert.assertTrue(add2.contains(r)));
        } catch (IOException ignore) {
            // ignore
        }

        BkTableScan ts = BkTableScan.read(table).where(equal("salary", 0)).select("*").build();
        ts.forEach(r -> {
            Assert.assertTrue(add2.contains(r) || add1.contains(r));
        });
        ts.close();

        // 测试按照时间过滤，进行查询。第一个时间查询条件满足分区数据文件以及数据文件里所有的数据，在0.8.0版本中不会报错。
        String timestamp1 = "2020-01-01T23:59:59.999999-00:00";
        OffsetDateTime dt1 = OffsetDateTime.parse(timestamp1);

        ts = BkTableScan.read(table).where(greaterThan("ts", timestamp1)).select("*").build();
        ts.forEach(r -> {
            Assert.assertTrue(((OffsetDateTime) r.getField("ts")).compareTo(dt1) > 0);
        });
        ts.close();

        // 这个条件过滤出符合条件的数据文件后，还需要逐条过滤数据。在0.8.0版本中，会抛出异常。
        // 需要包含 https://github.com/apache/iceberg/pull/983 这个的patch，才会正常执行。
        String timestamp2 = "2020-01-01T12:00:05Z";
        OffsetDateTime dt2 = OffsetDateTime.parse(timestamp2);
        ts = BkTableScan.read(table)
                .where(greaterThan("ts", timestamp2))
                .where(lessThan("ts", timestamp1))
                .select("*")
                .build();
        ts.forEach(r -> {
            Assert.assertTrue(((OffsetDateTime) r.getField("ts")).compareTo(dt2) > 0);
        });
        ts.close();
    }

    @Test
    public void testDataBuffer() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB3, props);
        List<TableField> fields = Stream.of("TS,timestamp", "BALance,lONg", "info,string")
                .map(i -> i.split(","))
                .map(arr -> new TableField(arr[0], arr[1]))
                .collect(Collectors.toList());
        List<PartitionMethod> pMethods = Collections
                .singletonList(new PartitionMethod("tS", "DAY"));
        table.createTable(fields, pMethods);

        // 使用默认构造函数，5分钟或者100w刷盘一次
        DataBuffer dataBuffer = new DataBuffer(table);

        int loop = 10;
        int batch = 10_000;
        for (int i = 0; i < loop; i++) {
            List<Record> records = RandomGenericData.generate(table.schema(), batch, 0L);
            dataBuffer.add(records, ImmutableMap.of("topic-test-" + i, "50000"));
        }

        // 此时没有达到任何刷盘条件
        Assert.assertEquals("data buffer not committed", 0, dataBuffer.getSuccessCommit());
        // 调用刷盘逻辑
        dataBuffer.close();
        Assert.assertEquals("data buffer is committed", 1, dataBuffer.getSuccessCommit());

        // 缩短构造函数中刷盘时间间隔，0.1s或者100w刷盘一次，首次时间条件触发刷盘延迟30s
        BkTable table1 = new BkTable(DB_NAME, TB4, props);
        table1.createTable(fields, pMethods);
        DataBuffer dataBuffer1 = new DataBuffer(table1, 100, 1_000_000, 30_000);

        for (int i = 0; i < loop; i++) {
            List<Record> records = RandomGenericData.generate(table1.schema(), batch, 0L);
            dataBuffer1.add(records, ImmutableMap.of("topic-test-" + i, "50000"));
            Thread.sleep(100);
        }

        // 时间刷盘和数据量刷盘条件均未达到
        Assert.assertEquals("data buffer committed 0 times", 0, dataBuffer1.getSuccessCommit());
        // 强制刷盘
        dataBuffer1.close();
        Assert.assertEquals("data buffer committed 1 times", 1, dataBuffer1.getSuccessCommit());

        DataBuffer dataBuffer2 = new DataBuffer(table1, 30_000, 50_000);
        batch = 50_000;
        for (int i = 0; i < loop; i++) {
            List<Record> records = RandomGenericData.generate(table1.schema(), batch, 0L);
            dataBuffer2.add(records, ImmutableMap.of("topic-test-" + i, "50000"));
        }

        Assert.assertEquals("data buffer committed 10 times", loop, dataBuffer2.getSuccessCommit());
        dataBuffer2.close(); // 无数据触发刷盘
        Assert.assertEquals("data buffer committed 10 times", loop, dataBuffer2.getSuccessCommit());

        table.dropTable();
        table1.dropTable();
    }

    @Test
    public void testConvertToRecords() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB6, props);
        TableField t1 = new TableField("dtEventTime", "timestamp");
        TableField t2 = new TableField("dtEventTimeStamp", "long");
        TableField t3 = new TableField("a", "string", true);
        TableField t4 = new TableField("b", "text", true);
        TableField t5 = new TableField("c", "int", true);
        TableField t6 = new TableField("d", "double", false);
        TableField t7 = new TableField("e", "float", false);
        List<TableField> f1 = Stream.of(t1, t2, t3, t4, t5, t6, t7).collect(Collectors.toList());
        // 正常创建成功一张表
        List<PartitionMethod> pMethods = Collections
                .singletonList(new PartitionMethod("dtEventTime", "hour"));
        table.createTable(f1, pMethods);

        // 测试写数据
        File file = new File(
                System.getProperty("user.dir") + File.separator + "target" + File.separator + UUID
                        .randomUUID().toString() + ".parquet");
        FileAppender<Record> fileAppender = Parquet.write(Files.localOutput(file))
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();

        List<Object> l1 = Stream.of(1589975212000000L, 1589975212000L, "a1", "b1", 1111, 1.1d, 1.1f)
                .collect(Collectors.toList());
        List<Object> l2 = Stream
                .of(Instant.ofEpochMilli(1589975230000L).atOffset(ZoneOffset.UTC), null, "a2", "b2",
                        2222, 2.2d, 2.2f).collect(Collectors.toList());
        List<Object> l3 = Stream
                .of(Instant.ofEpochMilli(1589975230000L).atOffset(ZoneOffset.UTC), 1589975212000L,
                        null, null, 3333, 3.3d, 3.3f).collect(Collectors.toList());
        List<List<Object>> records = Stream.of(l1, l2, l3).collect(Collectors.toList());
        String[] fieldArr = new String[]{"dtEventTime", "dtEventTimeStamp", "a", "b", "c", "d",
                "e"};
        fieldArr = Utils.lowerCaseArray(fieldArr);
        List<Record> myRecords = Utils.convertRecords(table.schema(), records, fieldArr);

        Record record1 = myRecords.get(0);
        AssertHelpers.assertThrows("timestamp field should be OffsetDateTime type",
                ClassCastException.class,
                "java.lang.Long",
                () -> fileAppender.add(record1));

        Record record2 = myRecords.get(1);
        AssertHelpers.assertThrows("required field can't be null value",
                NullPointerException.class,
                "",
                () -> fileAppender.add(record2));

        // add valid record should success
        fileAppender.add(myRecords.get(2));
        fileAppender.close();
        FileUtils.deleteQuietly(file);

        File file1 = new File(
                System.getProperty("user.dir") + File.separator + "target" + File.separator + UUID
                        .randomUUID().toString() + ".parquet");
        FileAppender<Record> fileAppender1 = Parquet.write(Files.localOutput(file1))
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();
        l1 = Stream.of("2020-05-20T11:47:00Z", 1589975212000L, "a1", "b1", 1111, 1.1d, 1.1f)
                .collect(Collectors.toList());
        l2 = Stream.of("2020-05-19T05:13:00Z", 1589865213000L, null, "b2", 2222, 2.2d, 2.2f)
                .collect(Collectors.toList());
        l3 = Stream.of("2020-05-18T01:33:00Z", 1589765614000L, "a3", "b3", null, 3.3d, 3.3f)
                .collect(Collectors.toList());
        records = Stream.of(l1, l2, l3).collect(Collectors.toList());
        myRecords = Utils.convertRecordsWithEventTime(table.schema(), records, fieldArr, 1, 0);
        myRecords.forEach(fileAppender1::add);
        fileAppender1.close();

        List<Record> readRecords = new ArrayList<>(3);
        CloseableIterable<Record> closeableIterable = Parquet.read(Files.localInput(file1))
                .project(table.schema())
                .createReaderFunc(
                        fileSchema -> GenericParquetReaders.buildReader(table.schema(), fileSchema))
                .build();
        for (Record record : closeableIterable) {
            readRecords.add(record);
        }
        closeableIterable.close();

        int size = myRecords.size();
        Assert.assertEquals("record size should equals", size, readRecords.size());
        for (int i = 0; i < size; i++) {
            Assert.assertEquals("record in list should be the same", myRecords.get(i),
                    readRecords.get(i));
        }

        FileUtils.deleteQuietly(file1);
        table.dropTable();
    }

    @Test
    public void testCatalogCache() {
        Configuration conf = new Configuration();
        props.forEach(conf::set);
        HiveCatalog cat = BkCatalogs.loadCatalog(conf);
        BkCatalogs.removeCatalogCache(DEFAULT_FS);
        Assert.assertNotNull("hive catalog should not be null", cat);
    }

    @Test
    public void testTableLocks() throws Exception {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();
        table.table().io();

        HiveMetaStoreClient client = new HiveMetaStoreClient(metastore.hiveConf());
        TableIdentifier ti = TableIdentifier.of(DB_NAME, TB1);
        ShowLocksResponse response = HiveLockUtils.listTableLocks(ti, client);
        Assert.assertEquals(0, response.getLocksSize());

        LockResponse lock = HiveLockUtils.createLock(ti, client);
        Assert.assertTrue(lock.getLockid() > 0);

        response = HiveLockUtils.listTableLocks(ti, client);
        Assert.assertEquals(1, response.getLocksSize());

        boolean result = table.forceReleaseLock();
        Assert.assertTrue(result);

        response = HiveLockUtils.listTableLocks(ti, client);
        Assert.assertEquals(0, response.getLocksSize());
    }

    @Test
    public void testConcurrentUpdate() {
        BkTable table = new BkTable(DB_NAME, TB1, props);
        table.loadTable();

        try (DataBuffer buffer = new DataBuffer(table, 30_000, 1_000_000)) {
            List<Record> add1 = RandomGenericData.generate(table.schema(), 600_000, 100L);
            buffer.add(add1, ImmutableMap.of("topic-test-0", "600000"));
        }

        Expression expr = and(greaterThanOrEqual("ts", "2020-01-01T00:00:00Z"),
                lessThan("ts", "2020-01-02T00:00:00Z"));
        String field = "info";
        String expected = Stream.of("1", "2", "3").parallel()
                .map(loop -> {
                    Map<String, ValFunction> update = new HashMap<>();
                    update.put(field, new Assign<>(loop));
                    try {
                        OpsMetric m = table.updateData(expr, update);
                        System.out.println(">>> " + loop + " updated takes: " + m.summary());
                        return loop;
                    } catch (Exception e) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .findFirst()
                .orElse("");
        try (CloseableIterable<Record> records = table.readRecords(expr)) {
            for (Record record : records) {
                Assert.assertEquals(expected, record.getField(field));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
