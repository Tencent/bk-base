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

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaTimestamp;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.udf;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.RecordWrapper;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveCatalogs;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkUtils {

    public static final String ICEBERG = "iceberg";
    public static final String ET = "____et";  // 默认时间分区字段
    public static final String PARTITION = "__part";
    public static final String SPARK_METASTORE_URIS = "spark.hadoop.hive.metastore.uris";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String LOCALITY = "locality";
    public static final String VECTORIZATION = "vectorization-enabled";
    public static final String HIVE_DYNAMIC_PARTITION = "hive.exec.dynamic.partition";
    public static final String HIVE_DYNAMIC_PARTITION_MODE = "hive.exec.dynamic.partition.mode";
    public static final String SPARK_SQL_TIMEZONE = "spark.sql.session.timeZone";
    public static final String CHECK_ORDERING = "check-ordering";

    private static final Logger log = LoggerFactory.getLogger(SparkUtils.class);

    /**
     * 读取iceberg表的内容
     *
     * @param master spark的master
     * @param sparkConf spark的配置
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param storageConf 表存储的配置
     * @return 表的数据集
     */
    public static Dataset<Row> readTable(String master, Map<String, String> sparkConf,
            String fullTableName, Map<String, Object> storageConf) {
        // spark 2.4 不支持catalogs，需要指明数据格式，以及完整的表名称（db.table）
        SparkSession spark = validateAndBuild(master, sparkConf, fullTableName, storageConf);
        return spark.read()
                .option(LOCALITY, false)
                .format(ICEBERG)
                .load(fullTableName);
    }

    /**
     * 读取iceberg表的内容
     *
     * @param spark spark session
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @return 表的数据集
     */
    public static Dataset<Row> readTable(SparkSession spark, String fullTableName) {
        checkArgument(spark.conf().contains(SPARK_METASTORE_URIS), "metastore uri not found");
        Map<String, Object> storageConf = ImmutableMap.of(HIVE_METASTORE_URIS,
                spark.conf().get(SPARK_METASTORE_URIS));

        return readTable(spark, fullTableName, storageConf);
    }

    /**
     * 读取iceberg表的内容
     *
     * @param spark spark session
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param storageConf 表的存储配置
     * @return 表的数据集
     */
    public static Dataset<Row> readTable(SparkSession spark, String fullTableName,
            Map<String, Object> storageConf) {
        validateAndLoad(fullTableName, storageConf);
        storageConf.forEach((k, v) -> spark.conf().set(k, v.toString()));

        return spark.read()
                .option(LOCALITY, false)
                .format(ICEBERG)
                .load(fullTableName);
    }

    /**
     * 写数据到iceberg表中
     *
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param metastoreUri hive metastore uri地址
     * @param ds 待写入的数据集
     * @param mode 写入模式，支持append和overwrite。append即为插入，overwrite会覆盖数据所在的分区，
     */
    public static void writeTable(String fullTableName, String metastoreUri, Dataset<Row> ds,
            SaveMode mode) {
        Map<String, Object> m = ImmutableMap.of(HIVE_METASTORE_URIS, metastoreUri);
        writeTable(fullTableName, m, ds, mode);
    }

    /**
     * 写数据到iceberg表中
     *
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param storageConf 表存储的配置
     * @param ds 待写入的数据集
     * @param mode 写入模式，支持append和overwrite。append即为插入，overwrite会覆盖数据所在的分区，
     */
    public static void writeTable(String fullTableName, Map<String, Object> storageConf,
            Dataset<Row> ds, SaveMode mode) {
        SparkSession spark = ds.sparkSession();
        storageConf.forEach((k, v) -> spark.conf().set(k, v.toString()));

        // 2.4 仅仅支持append和overwrite，并且overwrite的行为是覆盖传入的数据所在的分区，并非覆盖整张表。
        checkArgument(mode == SaveMode.Append || mode == SaveMode.Overwrite,
                "Save mode %s is not supported", mode);
        // 目前仅仅支持按照____et来分区写数据，未来支持更多分区方式和字段
        Table table = validateAndLoad(fullTableName, storageConf);
        if (PartitionSpec.unpartitioned().equals(table.spec())) {
            // 支持非分区表
            ds.write().format(ICEBERG)
                    .option(CHECK_ORDERING, false)
                    .mode(mode)
                    .save(fullTableName);
        } else {
            validatePartitionSpec(table);
            PartitionField pf = table.spec().fields().get(0);

            // 获取表的分区定义，构造用于分区数据的udf函数
            Transform<Long, Integer> transform = (Transform<Long, Integer>) pf.transform();
            UserDefinedFunction partition = udf(
                    (UDF1<Timestamp, Integer>) ts -> transform.apply((Long) fromJavaTimestamp(ts)),
                    DataTypes.IntegerType);

            // 需要对数据在分区内按照iceberg partition排序，写数据文件时顺序写入，否则会抛异常
            ds.withColumn(PARTITION, partition.apply(column(ET)))
                    .sortWithinPartitions(PARTITION)
                    .drop(PARTITION)
                    .write()
                    .format(ICEBERG)
                    .option(CHECK_ORDERING, false)
                    .mode(mode)
                    .save(fullTableName);
        }
    }

    /**
     * 指定开始时间（包含）和结束时间（不包含），计算表中符合此条件的记录数量。
     *
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param storageConf 表存储的配置
     * @param start 开始时间，包含
     * @param end 结束时间，不包含
     * @return 符合条件的记录数量
     * @throws IOException 异常
     */
    public static long calcRecordCount(String fullTableName, Map<String, Object> storageConf,
            OffsetDateTime start, OffsetDateTime end) throws IOException {
        Table table = validateAndLoad(fullTableName, storageConf);
        validatePartitionSpec(table);

        Expression expr = and(greaterThanOrEqual(ET, start.toString()), lessThan(ET, end.toString()));
        TableScan ts = table.newScan().caseSensitive(false).filter(expr).select("*");
        AtomicLong count = new AtomicLong(0);

        try (CloseableIterable<FileScanTask> tasks = ts.planFiles()) {
            // 时间相关的字段/FIXED字段/复杂结构字段，需要使用wrapper在过滤器中访问
            RecordWrapper wrapper = new RecordWrapper(table.schema().asStruct());
            for (FileScanTask task : tasks) {
                if (task.residual().equals(Expressions.alwaysTrue())) {
                    // 整个数据文件中数据都符合条件，全部加上
                    count.getAndAdd(task.file().recordCount());
                    log.debug("all {} records match in {}", task.file().recordCount(), task.file().path());
                } else {
                    // 遍历文件中的数据，累加符合条件的记录
                    try (CloseableIterable<Record> records = openParquetDataFile(table, task)) {
                        Evaluator filter = new Evaluator(table.schema().asStruct(), task.residual(), false);
                        log.debug("scan file {} by {}", task.file().path(), task.residual());
                        for (Record r : records) {
                            if (filter.eval(wrapper.wrap(r))) {
                                count.getAndIncrement();
                            }
                        }
                    }
                }
            }
        } catch (IOException ioe) {
            log.error(String.format("%s: get record count between %s %s failed.", fullTableName, start, end), ioe);
            throw ioe;
        }

        return count.get();
    }

    /**
     * 获取整表的数据量
     *
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param storageConf 表的存储配置
     * @return 表中所有的记录数
     * @throws IOException 异常
     */
    public static long calcTableRecordCount(String fullTableName, Map<String, Object> storageConf) throws IOException {
        Table table = validateAndLoad(fullTableName, storageConf);
        TableScan ts = table.newScan().caseSensitive(false).filter(Expressions.alwaysTrue()).select("*");
        AtomicLong count = new AtomicLong(0);

        try (CloseableIterable<FileScanTask> tasks = ts.planFiles()) {
            for (FileScanTask task : tasks) {
                count.getAndAdd(task.file().recordCount());
            }
        } catch (IOException ioe) {
            log.error(fullTableName + ": get record count failed!", ioe);
            throw ioe;
        }

        return count.get();
    }

    /**
     * 校验并构建spark的session
     *
     * @param master spark的master
     * @param sparkConf spark的配置
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param storageConf 表存储的配置
     * @return spark的session
     */
    public static SparkSession validateAndBuild(String master, Map<String, String> sparkConf,
            String fullTableName, Map<String, Object> storageConf) {
        Table table = validateAndLoad(fullTableName, storageConf);

        String metastoreUri = storageConf.get(HIVE_METASTORE_URIS).toString();
        Map<String, String> map = buildSparkConfForIceberg(metastoreUri);
        map.putAll(sparkConf);
        map.putAll(table.properties());

        return buildSparkSessionForIceberg(master, map);
    }

    /**
     * 校验并加载iceberg的表
     *
     * @param fullTableName 完整表名称，包含db名称和表名，用.分割
     * @param storageConf 表存储的配置
     * @return iceberg表对象
     */
    public static Table validateAndLoad(String fullTableName, Map<String, Object> storageConf) {
        checkArgument(fullTableName.contains("."), "db name not found: " + fullTableName);
        checkArgument(storageConf.containsKey(HIVE_METASTORE_URIS), "metastore uri not found");

        // 检查表的配置，需要是hive catalog中注册的表
        TableIdentifier ti = TableIdentifier.parse(fullTableName);
        Configuration conf = new Configuration();
        storageConf.forEach((k, v) -> conf.set(k, v.toString()));
        HiveCatalog hiveCatalog = HiveCatalogs.loadCatalog(conf);
        List<TableIdentifier> tables = hiveCatalog.listTables(ti.namespace());
        checkArgument(tables.contains(ti), fullTableName + " not found in hive");

        return hiveCatalog.loadTable(ti);
    }

    /**
     * 校验表的分区定义，确保以时间字段____et作为分区
     *
     * @param table iceberg表对象
     */
    public static void validatePartitionSpec(Table table) {
        PartitionSpec spec = table.spec();
        // 目前仅支持一个分区字段，且字段必须为____et
        checkArgument(spec.fields().size() == 1, "only support one partition field");
        PartitionField pf = spec.fields().get(0);
        Types.NestedField field = table.schema().findField(pf.sourceId());
        checkArgument(field.name().equals(ET), "partition field should be ____et");
    }

    /**
     * 构建spark session，启用hive支持
     *
     * @param master spark的master
     * @param conf spark的配置
     * @return spark的session
     */
    public static SparkSession buildSparkSessionForIceberg(String master,
            Map<String, String> conf) {
        SparkSession.Builder builder = SparkSession.builder().master(master);
        conf.forEach(builder::config);

        return builder.getOrCreate();
    }

    /**
     * 构建spark读取iceberg表的配置参数
     *
     * @param metastoreUri iceberg hive的metastore地址
     * @return 配置参数
     */
    public static Map<String, String> buildSparkConfForIceberg(String metastoreUri) {
        Map<String, String> map = new HashMap<>();
        map.put(SPARK_METASTORE_URIS, metastoreUri);
        map.put(VECTORIZATION, "true");
        map.put(HIVE_DYNAMIC_PARTITION, "true");
        map.put(HIVE_DYNAMIC_PARTITION_MODE, "nonstrict");
        // 必须使用UTC时区，因为____et在iceberg中设置的是timestamptz类型
        map.put(SPARK_SQL_TIMEZONE, "UTC");

        return map;
    }

    /**
     * 打开parquet格式的数据文件，返回可遍历的记录集。
     *
     * @param table iceberg表对象
     * @param task 文件扫描任务
     * @return 可遍历的记录集
     */
    public static CloseableIterable<Record> openParquetDataFile(Table table, FileScanTask task) {
        InputFile input = table.io().newInputFile(task.file().path().toString());
        // 所有数据文件都是parquet格式
        Parquet.ReadBuilder parquet = Parquet.read(input)
                .project(table.schema())
                .createReaderFunc(schema -> GenericParquetReaders.buildReader(table.schema(), schema));

        return parquet.build();
    }

}