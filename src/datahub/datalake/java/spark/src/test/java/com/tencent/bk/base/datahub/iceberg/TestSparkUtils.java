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

import static com.tencent.bk.base.datahub.iceberg.SparkUtils.readTable;
import static com.tencent.bk.base.datahub.iceberg.SparkUtils.writeTable;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.lit;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSparkUtils extends HiveMetastoreTest {

    private static final Logger log = LoggerFactory.getLogger(TestSparkUtils.class);

    private static final String DATA_FILE = "file:///tmp/hive/12345.parquet";
    private static final TableIdentifier TI1 = TableIdentifier.of(DB_NAME, "table_year");
    private static final String TB1 = TI1.toString();
    private static final TableIdentifier TI2 = TableIdentifier.of(DB_NAME, "table_month");
    private static final String TB2 = TI2.toString();
    private static final TableIdentifier TI3 = TableIdentifier.of(DB_NAME, "table_day");
    private static final String TB3 = TI3.toString();
    private static final TableIdentifier TI4 = TableIdentifier.of(DB_NAME, "table_hour");
    private static final String TB4 = TI4.toString();
    private static final TableIdentifier NO_PARTITION_TI = TableIdentifier.of(DB_NAME,
            "table_no_partition");
    private static final String NO_PARTITION_TB = NO_PARTITION_TI.toString();
    private static final TableIdentifier BUCKET_PARTITION_TI = TableIdentifier.of(DB_NAME,
            "table_bucket_partition");
    private static final String BUCKET_PARTITION_TB = BUCKET_PARTITION_TI.toString();
    private static final Configuration CONF = new Configuration();
    private static final Map<String, Object> STORAGE_CONF = new HashMap<>();
    private static final int RECORD_COUNT = 1_000_000;
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "long", Types.LongType.get()),
            Types.NestedField.optional(2, "int", Types.IntegerType.get()),
            Types.NestedField.optional(3, "string", Types.StringType.get()),
            Types.NestedField.optional(4, "double", Types.DoubleType.get()),
            Types.NestedField.optional(5, "float", Types.FloatType.get()),
            Types.NestedField.optional(6, "dteventtime", Types.StringType.get()),
            Types.NestedField.optional(7, SparkUtils.ET, Types.TimestampType.withZone())
    );
    private static final PartitionSpec TS_YEAR = PartitionSpec.builderFor(SCHEMA)
            .year(SparkUtils.ET)
            .build();
    private static final PartitionSpec TS_MONTH = PartitionSpec.builderFor(SCHEMA)
            .month(SparkUtils.ET)
            .build();
    private static final PartitionSpec TS_DAY = PartitionSpec.builderFor(SCHEMA)
            .day(SparkUtils.ET)
            .build();
    private static final PartitionSpec TS_HOUR = PartitionSpec.builderFor(SCHEMA)
            .hour(SparkUtils.ET)
            .build();
    private static final PartitionSpec BUCKET_TEN = PartitionSpec.builderFor(SCHEMA)
            .bucket("string", 10)
            .build();
    private static SparkSession SPARK;

    @BeforeClass
    public static void createTestTable() throws Exception {
        // 设置hive metastore相关配置项、hdfs相关配置项
        CONF.set("fs.defaultFS", DEFAULT_FS);
        CONF.set("hive.metastore.uris", TestHiveMetastore.META_URI);
        CONF.set("hive.metastore.warehouse.dir", WAREHOUSE);

        Map<String, String> props = ImmutableMap.of(DEFAULT_FILE_FORMAT, "parquet",
                COMMIT_NUM_RETRIES, "100",
                METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");
        // 建表
        Map<TableIdentifier, PartitionSpec> m = ImmutableMap.of(TI1, TS_YEAR, TI2, TS_MONTH,
                TI3, TS_DAY, TI4, TS_HOUR);
        m.forEach((k, v) -> {
            String location = String.format(
                    "%s/%s/%s.db/%s",
                    DEFAULT_FS,
                    WAREHOUSE,
                    k.namespace().level(0),
                    k.name());
            catalog.createTable(k, SCHEMA, v, location, props);
        });

        // 创建无分区的表
        catalog.createTable(NO_PARTITION_TI, SCHEMA, PartitionSpec.unpartitioned(), props);
        // 创建bucket分区的表
        catalog.createTable(BUCKET_PARTITION_TI, SCHEMA, BUCKET_TEN, props);

        STORAGE_CONF.put(SparkUtils.HIVE_METASTORE_URIS, TestHiveMetastore.META_URI);
        STORAGE_CONF.put("flush.size", 100);

        // 生成测试用的数据文件
        File dataFile = new File(new URI(DATA_FILE));
        FileUtils.deleteQuietly(dataFile);
        List<Record> records = RandomGenericData.generate(SCHEMA, RECORD_COUNT, 0L);
        FileAppender<Record> fa = Parquet.write(HadoopOutputFile.fromLocation(DATA_FILE, CONF))
                .schema(SCHEMA)
                .setAll(props)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .build();
        fa.addAll(records);
        fa.close();
        // 生成spark的session，需要包含hive的metastore配置
        Map<String, String> conf = SparkUtils.buildSparkConfForIceberg(TestHiveMetastore.META_URI);
        SPARK = SparkUtils.buildSparkSessionForIceberg("local[2]", conf);
    }

    @AfterClass
    public static void dropTestTable() {
        // 清理表和数据文件
        Stream.of(TI1, TI2, TI3, TI4, NO_PARTITION_TI).forEach(k -> catalog.dropTable(k, true));
        // 停止spark的session
        if (SPARK != null) {
            SPARK.stop();
        }
    }

    @Test
    public void testWriteAndReadTable() throws Exception {
        Dataset<Row> dsDisk = SPARK.read().load(DATA_FILE);
        dsDisk.show(10);
        // 默认数据读取出来为一个分区，拆分分区后，写入时会按照分区数量生成对应的分区datafile文件
        Dataset<Row> ds = dsDisk.repartition(3, column("long"));
        // 按照年、月、日、小时各种分区写入数据。对于空表，append和overwrite行为一致
        SparkUtils.writeTable(TB1, STORAGE_CONF, ds, SaveMode.Append);
        SparkUtils.writeTable(TB2, TestHiveMetastore.META_URI, ds, SaveMode.Append);
        SparkUtils.writeTable(TB3, STORAGE_CONF, ds, SaveMode.Overwrite);
        SparkUtils.writeTable(TB4, TestHiveMetastore.META_URI, ds, SaveMode.Overwrite);
        SparkUtils.writeTable(NO_PARTITION_TB, STORAGE_CONF, ds, SaveMode.Overwrite);

        AssertHelpers.assertThrows("should partition by ET",
                IllegalArgumentException.class,
                "partition field should be ____et",
                () -> SparkUtils.writeTable(BUCKET_PARTITION_TB, STORAGE_CONF, ds, SaveMode.Append));

        // 读取数据
        long t1 = System.currentTimeMillis();
        Dataset<Row> tb1 = SparkUtils.readTable("local[2]", new HashMap<>(), TB1, STORAGE_CONF);
        Assert.assertEquals(RECORD_COUNT, tb1.count());
        long t2 = System.currentTimeMillis();
        Dataset<Row> tb2 = SparkUtils.readTable(SPARK, TB2);
        Assert.assertEquals(RECORD_COUNT, tb2.count());
        long t3 = System.currentTimeMillis();
        Dataset<Row> tb3 = SparkUtils.readTable(SPARK, TB3, STORAGE_CONF);
        Assert.assertEquals(RECORD_COUNT, tb3.count());
        long t4 = System.currentTimeMillis();
        Dataset<Row> tb4 = SparkUtils.readTable(SPARK, TB4, STORAGE_CONF);
        Assert.assertEquals(RECORD_COUNT, tb4.count());
        long t5 = System.currentTimeMillis();
        Dataset<Row> tb5 = SparkUtils.readTable(SPARK, NO_PARTITION_TB, STORAGE_CONF);
        Assert.assertEquals(RECORD_COUNT, tb5.count());
        long t6 = System.currentTimeMillis();
        log.info(">>> scan record count load time: {} {} {} {} {}", t2 - t1, t3 - t2, t4 - t3, t5 - t4, t6 - t5);

        long cnt = SparkUtils.calcTableRecordCount(NO_PARTITION_TB, STORAGE_CONF);
        Assert.assertEquals(RECORD_COUNT, cnt);
        long t7 = System.currentTimeMillis();
        cnt = SparkUtils.calcTableRecordCount(TB1, STORAGE_CONF);
        Assert.assertEquals(RECORD_COUNT, cnt);
        long t8 = System.currentTimeMillis();
        log.info(">>> get total table records takes: {}, {}", t7 - t6, t8 - t7);

        OffsetDateTime start = OffsetDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
        OffsetDateTime end = OffsetDateTime.of(2020, 1, 2, 0, 0, 0, 0, ZoneOffset.UTC);
        t1 = System.currentTimeMillis();
        long count1 = SparkUtils.calcRecordCount(TB1, STORAGE_CONF, start, end);
        t2 = System.currentTimeMillis();
        long count2 = SparkUtils.calcRecordCount(TB2, STORAGE_CONF, start, end);
        t3 = System.currentTimeMillis();
        long count3 = SparkUtils.calcRecordCount(TB3, STORAGE_CONF, start, end);
        t4 = System.currentTimeMillis();
        long count4 = SparkUtils.calcRecordCount(TB4, STORAGE_CONF, start, end);
        t5 = System.currentTimeMillis();
        log.info(">>> calc record count load time: {} {} {} {}", t2 - t1, t3 - t2, t4 - t3, t5 - t4);
        Assert.assertEquals(count1, count2);
        Assert.assertEquals(count1, count3);
        Assert.assertEquals(count1, count4);

        // 过滤数据
        Dataset<Row> ds2 = tb1.filter(SparkUtils.ET + " >= cast('2020-01-01 00:00:00' as TIMESTAMP)")
                .filter(SparkUtils.ET + " < cast('2020-02-01 00:00:00' as TIMESTAMP)")
                .filter("int > 50000000")
                .filter("long = 0");
        Dataset<Row> ds3 = tb1.filter(SparkUtils.ET + " >= cast('2020-01-01 00:00:00' as TIMESTAMP)");
        long totalCount = tb1.count();
        long oneMonthCount = ds2.count();
        long yearCount = ds3.count();

        // 按年分区的数据，所有2020年的数据都会被覆盖为ds2的数据集
        SparkUtils.writeTable(TB1, STORAGE_CONF, ds2, SaveMode.Overwrite);
        Dataset<Row> ds4 = tb1.filter(SparkUtils.ET + " >= cast('2020-01-01 00:00:00' as TIMESTAMP)");
        long yearCountNew = ds4.count();
        log.info(">>> {} = {} < {}", oneMonthCount, yearCountNew, yearCount);
        Assert.assertEquals(oneMonthCount, yearCountNew);
        long totalCountNew = tb1.count();
        log.info(">>> {} < {}", totalCountNew, totalCount);
        Assert.assertTrue(totalCount > totalCountNew);
        Assert.assertEquals(RECORD_COUNT, totalCountNew - yearCountNew + yearCount);

        // 测试dataset字段顺序和表字段顺序不一致时写入是否正常
        Dataset<Row> ds2New = ds2.drop("string").withColumn("string", lit("2020-07-31"));
        SparkUtils.writeTable(TB1, STORAGE_CONF, ds2New, SaveMode.Overwrite);
        long updatedCount = tb1.filter("string = '2020-07-31'").count();
        Assert.assertEquals(updatedCount, oneMonthCount);
    }

    @Test
    public void testBadArgs() {
        AssertHelpers.assertThrows("bad table name",
                IllegalArgumentException.class,
                "db name not found",
                () -> SparkUtils.validateAndLoad("abc", STORAGE_CONF));

        AssertHelpers.assertThrows("bad storage conf",
                IllegalArgumentException.class,
                "metastore uri not found",
                () -> SparkUtils.validateAndLoad("abc.aaa", new HashMap<>()));

        AssertHelpers.assertThrows("table not found in hive",
                IllegalArgumentException.class,
                "not found in hive",
                () -> SparkUtils.validateAndLoad("abc.aaa", STORAGE_CONF));
    }

}