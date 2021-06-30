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

import static com.tencent.bk.base.datahub.iceberg.C.FILE_FORMAT;
import static com.tencent.bk.base.datahub.iceberg.C.MAX_RETRY_WAIT_MS;
import static com.tencent.bk.base.datahub.iceberg.C.MIN_RETRY_WAIT_MS;
import static com.tencent.bk.base.datahub.iceberg.C.RETRY_TIMES;
import static com.tencent.bk.base.datahub.iceberg.C.TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;

import com.tencent.bk.base.datahub.iceberg.errors.CommitFileFailed;
import com.tencent.bk.base.datahub.iceberg.errors.ModifyTableFailed;
import com.tencent.bk.base.datahub.iceberg.errors.TableExists;
import com.tencent.bk.base.datahub.iceberg.errors.TableNotExists;
import com.tencent.bk.base.datahub.iceberg.functions.ValFunction;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.RecordWrapper;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hive.HiveLockUtils;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types.NestedField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkTable {

    public static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z]\\w+$");

    private static final Logger log = LoggerFactory.getLogger(BkTable.class);

    private final TableIdentifier ti;
    private final Configuration conf;
    private final BaseMetastoreCatalog catalog;
    private final Boolean useHiveCatalog;
    private final Map<String, String> properties;
    private Table table;

    /**
     * 构造函数，指定hive.metastore.uris配置时使用hive metastore作为catalog， 其他情况使用hadoop catalog。
     *
     * @param db db名称
     * @param name 表名称
     * @param props 表属性集，包含HDFS集群属性、hive metastore地址等
     */
    public BkTable(String db, String name, Map<String, String> props) {
        // 校验props中包含HDFS集群配置信息、hive meta store uri配置信息
        Preconditions.checkNotNull(props, "configurations cannot be null");
        Preconditions.checkArgument(props.containsKey(C.DEFAULT_FS), "fs.defaultFS missing");
        Preconditions.checkArgument(NAME_PATTERN.matcher(db).matches(), "db name not valid: " + db);
        Preconditions.checkArgument(NAME_PATTERN.matcher(name).matches(), "bad name: " + name);

        properties = ImmutableMap.copyOf(props);
        conf = new Configuration();
        properties.entrySet().stream()
                .filter(e -> !e.getKey().startsWith(C.TABLE_PREFIX))
                .forEach(e -> conf.set(e.getKey(), e.getValue()));
        useHiveCatalog = props.containsKey(HiveConf.ConfVars.METASTOREURIS.varname);
        if (useHiveCatalog) {
            Preconditions.checkArgument(props.containsKey(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                    "hive.metastore.warehouse.dir config not exists");
            catalog = BkCatalogs.loadCatalog(conf);
        } else {
            String warehouse = props.getOrDefault(C.HADOOP_WAREHOUSE_DIR, C.HADOOP_WAREHOUSE_DIR_DEFAULT);
            catalog = new HadoopCatalog(conf, warehouse);
        }
        ti = TableIdentifier.of(db, name);
    }

    /**
     * 从HiveCatalog中加载表
     */
    public void loadTable() {
        try {
            table = catalog.loadTable(ti);
        } catch (NoSuchTableException e) {
            log.warn("table {} not exists, failed to load it.", ti);
            throw new TableNotExists(e, "please create the table before load it!");
        }
    }

    /**
     * 创建非分区表，字段名称和类型会被转换为小写字符
     *
     * @param fieldList 字段列表
     */
    public void createTable(List<TableField> fieldList) {
        Schema schema = Utils.buildSchema(fieldList);
        PartitionSpec spec = PartitionSpec.unpartitioned();
        createTableWithProps(schema, spec);
    }

    /**
     * 创建分区表，字段名称和类型会被转换为小写字符
     *
     * @param fieldList 表字段列表
     * @param methodList 表分区信息
     * @throws IllegalArgumentException 参数校验异常。
     */
    public void createTable(List<TableField> fieldList, List<PartitionMethod> methodList) {
        boolean valid = !(fieldList == null || fieldList.isEmpty() || methodList == null || methodList.isEmpty());
        Preconditions.checkArgument(valid, "empty args");
        Set<String> fs = fieldList.stream().map(TableField::name).collect(Collectors.toSet());
        // 校验partitionFields中字段都存在
        valid = methodList.stream().allMatch(k -> fs.contains(k.getField()));
        Preconditions.checkArgument(valid, "all partition fields should exist in table fields");

        Schema schema = Utils.buildSchema(fieldList);
        PartitionSpec spec = Utils.buildPartitionSpec(schema, methodList);
        createTableWithProps(schema, spec);
    }

    /**
     * 创建iceberg表，支持分区表和非分区表，会从BkTable的属性中解析出表的属性，并设置在表级别。
     *
     * @param schema 表的schema信息
     * @param spec 表的分区信息
     */
    private void createTableWithProps(Schema schema, PartitionSpec spec) {
        int start = C.TABLE_PREFIX.length();
        Map<String, String> props = properties.entrySet().stream()
                .filter(e -> e.getKey().startsWith(C.TABLE_PREFIX))
                .collect(Collectors.toMap(e -> e.getKey().substring(start), Entry::getValue));
        props.putIfAbsent(DEFAULT_FILE_FORMAT, FILE_FORMAT);
        props.putIfAbsent(COMMIT_NUM_RETRIES, RETRY_TIMES);
        props.putIfAbsent(COMMIT_MIN_RETRY_WAIT_MS, MIN_RETRY_WAIT_MS);
        props.putIfAbsent(COMMIT_MAX_RETRY_WAIT_MS, MAX_RETRY_WAIT_MS);
        props.putIfAbsent(COMMIT_TOTAL_RETRY_TIME_MS, TOTAL_RETRY_TIME_MS);
        // 设置表在commit时自动清理metadata.json版本文件
        props.putIfAbsent(METADATA_DELETE_AFTER_COMMIT_ENABLED, "true");

        try {
            if (useHiveCatalog) {
                // 如果db不存在，则创建db
                if (!((SupportsNamespaces) catalog).listNamespaces().contains(ti.namespace())) {
                    log.info("{}, going to create database {} in catalog", ti, ti.namespace());
                    ((SupportsNamespaces) catalog).createNamespace(ti.namespace());
                }
                String location = String.format(
                        "%s/%s/%s.db/%s",
                        conf.get(C.DEFAULT_FS),
                        conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname),
                        ti.namespace().level(0),
                        ti.name());
                table = catalog.createTable(ti, schema, spec, location, props);
            } else {
                table = catalog.createTable(ti, schema, spec, null, props);
            }
        } catch (AlreadyExistsException e) {
            log.warn("table {} is already exists, unable to create it again!", ti);
            throw new TableExists(e, "table is already exists, please don't create it again!");
        }
    }

    /**
     * 列出catalog下所有的db以及所有的表。
     *
     * @return catalog下所有的db和所有的表。
     */
    public Map<Namespace, List<TableIdentifier>> listCatalog() {
        Map<Namespace, List<TableIdentifier>> result = ((SupportsNamespaces) catalog).listNamespaces()
                .stream()
                .map(catalog::listTables)
                .flatMap(List::stream)
                .collect(Collectors.groupingBy(TableIdentifier::namespace));
        log.debug("list catalog result: {}", result);

        return result;
    }

    /**
     * 在iceberg表中增加字段
     *
     * @param fields 待增加的字段
     * @return 增加字段是否成功，True/False
     * @throws ModifyTableFailed 提交表字段变更失败，发生异常
     */
    public boolean addFields(List<TableField> fields) throws ModifyTableFailed {
        // 字段不能已经存在
        boolean valid = fields.stream().allMatch(k -> schema().findField(k.name()) == null);
        Preconditions.checkArgument(valid, "field should not already exist in table during adding");

        try {
            UpdateSchema us = table.updateSchema();
            fields.forEach(k -> us.addColumn(k.name(), k.type()));
            us.commit();
            return true;
        } catch (Exception e) {
            log.warn("modify table schema failed.", e);
            throw new ModifyTableFailed(e, "add table fields failed: " + fields.toString());
        }
    }

    /**
     * 在iceberg表中删除字段
     *
     * @param fields 待删除的字段集合
     * @return 删除字段是否成功，True/False
     * @throws ModifyTableFailed 提交表字段变更失败，发生异常
     */
    public boolean removeFields(Set<String> fields) throws ModifyTableFailed {
        // 校验字段存在，且字段没用于分区
        fields = Utils.lowerCaseSet(fields);
        Set<String> partitionFields = table.spec()
                .fields()
                .stream()
                .map(f -> schema().findField(f.sourceId()).name())
                .collect(Collectors.toSet());
        boolean fieldsExist = fields.stream().allMatch(k -> schema().findField(k) != null);
        boolean notPartitionField = fields.stream().noneMatch(partitionFields::contains);
        Preconditions.checkArgument(fieldsExist && notPartitionField,
                "field should exists and not a partition field during removing");

        try {
            UpdateSchema us = table.updateSchema();
            fields.forEach(us::deleteColumn);
            us.commit();
            return true;
        } catch (Exception e) {
            log.warn("modify table schema failed.", e);
            throw new ModifyTableFailed(e, "remove table fields failed: " + fields.toString());
        }
    }

    /**
     * 修改iceberg表的字段，增加并删除部分字段。
     *
     * @param toAdd 待增加的字段列表
     * @param toDelete 待删除的字段集合
     * @return 删除字段是否成功，True/False
     * @throws ModifyTableFailed 提交表字段变更失败，发生异常
     */
    public boolean modifyFields(List<TableField> toAdd, Set<String> toDelete) throws ModifyTableFailed {
        toDelete = Utils.lowerCaseSet(toDelete);
        Set<String> partFields = table.spec()
                .fields()
                .stream()
                .map(f -> schema().findField(f.sourceId()).name())
                .collect(Collectors.toSet());

        boolean addNotExist = toAdd.stream().allMatch(k -> schema().findField(k.name()) == null);
        boolean delNotExist = toDelete.stream().allMatch(k -> schema().findField(k) != null);
        boolean notPartField = toDelete.stream().noneMatch(partFields::contains);
        Preconditions.checkArgument(addNotExist && delNotExist && notPartField,
                "add and delete fields not valid.");

        try {
            UpdateSchema us = table.updateSchema();
            toAdd.forEach(k -> us.addColumn(k.name(), k.type()));
            toDelete.forEach(us::deleteColumn);
            us.commit();
            return true;
        } catch (Exception e) {
            log.warn("modify table schema failed.", e);
            throw new ModifyTableFailed(e, "modify table fields failed. toAdd: %s, toDelete: %s",
                    toAdd, toDelete);
        }
    }

    /**
     * 修改表字段名称
     *
     * @param renameTo 修改前的字段名称和修改后的字段名称的映射关系
     * @return 修改字段名称是否成功，True/False
     * @throws ModifyTableFailed 提交表字段变更失败，发生异常
     */
    public boolean renameFields(Map<String, String> renameTo) throws ModifyTableFailed {
        Map<String, String> map = new HashMap<>(renameTo.size());
        renameTo.forEach((k, v) -> map.put(k.toLowerCase(), v.toLowerCase()));
        // 表中所有字段均为小写
        boolean validField = map.keySet().stream().allMatch(k -> schema().findField(k) != null);
        boolean validRename = map.values().stream().allMatch(k -> schema().findField(k) == null);
        Preconditions.checkArgument(validField && validRename, "field name not valid or rename not valid" + renameTo);

        try {
            UpdateSchema us = table.updateSchema();
            map.forEach(us::renameColumn);
            us.commit();
            return true;
        } catch (Exception e) {
            log.warn("modify table schema failed.", e);
            throw new ModifyTableFailed(e, "rename table fields failed: " + renameTo);
        }
    }

    /**
     * 修改表的分区方式
     *
     * @param methods 表分区信息
     */
    public void changePartitionSpec(List<PartitionMethod> methods) {
        // 不支持将非分区表转换为分区表
        boolean isPartitionTable = !PartitionSpec.unpartitioned().equals(table.spec());
        Preconditions.checkArgument(isPartitionTable, "unable to change partition spec for unpartitioned table");

        Set<String> fields = schema().columns().stream().map(NestedField::name)
                .collect(Collectors.toSet());
        boolean valid = methods.stream().allMatch(k -> fields.contains(k.getField()));
        Preconditions.checkArgument(valid, "all partition fields should exist in table fields");

        PartitionSpec newSpec = Utils.buildPartitionSpec(schema(), methods);
        BaseTable t = (BaseTable) table;
        TableMetadata metadata = t.operations().current();
        t.operations().commit(metadata, metadata.updatePartitionSpec(newSpec));
    }

    /**
     * 更新表的属性
     *
     * @param props 表属性键值对
     */
    public void updateProperties(Map<String, String> props) {
        BaseTable t = (BaseTable) table;
        TableMetadata metadata = t.operations().current();
        Map<String, String> map = new HashMap<>(metadata.properties());
        map.putAll(props);
        t.operations().commit(metadata, metadata.replaceProperties(map));
    }

    /**
     * 更新表的位置
     *
     * @param newLocation 表位置
     */
    public void updateLocation(String newLocation) {
        BaseTable t = (BaseTable) table;
        TableMetadata metadata = t.operations().current();
        t.operations().commit(metadata, metadata.updateLocation(newLocation));
    }

    /**
     * 重命名表
     *
     * @param newTableName 新的表名称
     */
    public void renameTable(String newTableName) {
        TableIdentifier renameTo = TableIdentifier.of(ti.namespace(), newTableName);
        catalog.renameTable(ti, renameTo);
        log.info("table {} has been renamed to {}", ti, renameTo);
    }

    /**
     * 删除表
     *
     * @return 是否删除成功
     */
    public boolean dropTable() {
        boolean success = catalog.dropTable(ti, true);
        log.info("table {} was dropped {}", ti, success);
        return success;
    }

    /**
     * truncate表中数据，保留表schema
     */
    public void truncateTable() {
        Set<DataFile> files = new HashSet<>(getAllDataFiles());
        String fileCnt = String.valueOf(files.size());
        String filesStr = files.stream().map(f -> f.path().toString()).collect(Collectors.joining(","));
        TableUtils.deleteTableDataFiles(table, files, ImmutableMap.of(C.TRUNCATE_FILES, filesStr, C.COUNT, fileCnt));
        log.info("{}: truncate table success, deleted data files are {}", ti, filesStr);
    }

    /**
     * 增加一个快照，使用指定的commit msg作为dt-sdk-msg的属性值。
     *
     * @param msg commit msg对象
     */
    public void addCommitMsg(CommitMsg msg) {
        AppendFiles append = table.newAppend();
        append.set(C.DT_SDK_MSG, msg.toString());
        append.commit();
    }

    /**
     * 对于分区的时间字段dateField，以当前时间为基点，将expireDays天前的数据从当前表 中清理掉。 并只保留最近n天表的快照数据，之前的快照清理掉。
     *
     * @param dateField 时间字段，且是用于分区的字段之一。
     * @param expireDays 数据保存天数。
     * @throws IllegalArgumentException 参数校验异常。
     */
    public void expireEarlyData(String dateField, int expireDays) {
        TableUtils.expireEarlyData(table, dateField, expireDays);
    }

    /**
     * 清理iceberg表的快照，只保留最近n天的或至少m个快照，其他的快照清理掉。
     */
    public void expireSnapshots() {
        TableUtils.expireSnapshots(table);
    }

    /**
     * 压缩指定时间范围内的小数据文件（默认设置为小于16M的数据文件），将其重写为更大的数据文件。
     *
     * @param dateField 时间字段，且为分区字段
     * @param start 开始日期
     * @param end 结束日期
     * @throws IllegalArgumentException 参数校验异常。
     */
    public void compactDataFile(String dateField, OffsetDateTime start, OffsetDateTime end) {
        TableUtils.compactDataFile(table, dateField, start, end);
    }

    /**
     * 删除表中指定的分区数据
     *
     * @param fieldName 分区的字段名称
     * @param partitions 分区的值的集合
     * @throws IllegalArgumentException 参数校验异常。
     */
    public OpsMetric deletePartitions(String fieldName, Set<String> partitions) {
        return TableUtils.deletePartitions(table, fieldName, partitions);
    }

    /**
     * 根据表达式删除iceberg表中符合条件的数据
     *
     * @param expression 表达式
     * @throws CommitFileFailed 异常
     * @throws IllegalArgumentException 参数校验异常。
     */
    public OpsMetric deleteData(Expression expression) {
        // 校验expression中必须包含第一个分区字段
        Preconditions.checkArgument(alwaysTrue() != expression,
                "can't delete all data in table");

        OpsMetric metric = TableUtils.processingTableData(table, expression, new HashMap<>(),
                (r, b, t) -> b ? Optional.empty() : Optional.of(r));
        log.info("{} finished deleting records match {}", table, expression);

        return metric;
    }

    /**
     * 根据表达式更新iceberg表中符合条件的数据
     *
     * @param expression 表达式
     * @param transforms 数据转换器，可用于更新一个或多个字段的值
     * @throws CommitFileFailed 异常
     * @throws IllegalArgumentException 参数校验异常。
     */
    public OpsMetric updateData(Expression expression, Map<String, ValFunction> transforms) {
        OpsMetric metric = TableUtils.processingTableData(table, expression, transforms, (r, match, trans) -> {
            if (match) {
                // 对符合条件的数据使用转换函数设定新的列值
                trans.forEach((k, v) -> v.apply(r, k));
            }
            return Optional.of(r);
        });
        log.info("{} finished updating records match {}", table, expression);

        return metric;
    }

    /**
     * 获取表的基本信息，包含表名称、schema、分区信息等
     *
     * @return 表基本信息
     */
    public String info() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put(C.TABLE, ti.toString());

        if (table != null) {
            info.put(C.PROPERTIES, tableProperties());
            info.put(C.PARTITION, currentPartitionInfo());
            info.put(C.SCHEMA, fieldsInfo());
            List<Map<String, Object>> snapshotInfo = getLatestSnapshots(10)
                    .stream()
                    .map(snapshot -> {
                        Map<String, Object> m = new HashMap<>();
                        m.put(C.ID, String.valueOf(snapshot.snapshotId()));
                        m.put(C.TIMESTAMP_MS, String.valueOf(snapshot.timestampMillis()));
                        m.put(C.OPERATION, snapshot.operation());
                        m.put(C.SUMMARY, snapshot.summary());
                        m.put(C.MANIFEST_LIST_LOCATION, snapshot.manifestListLocation());
                        return m;
                    })
                    .collect(Collectors.toList());
            info.put(C.SNAPSHOTS, snapshotInfo);
            info.put(C.PARTITION_PATHS, getPartitionPaths());
            List<String> sample = getLatestRecords(10)
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
            info.put(C.SAMPLE, sample);
            info.put(C.LOCATION, currentLocation());
        }

        return Utils.toJsonString(info);
    }

    /**
     * 获取表字段列表简要信息
     *
     * @return 字段信息
     */
    public List<Map<String, Object>> fieldsInfo() {
        return table.schema()
                .columns()
                .stream()
                .map(field -> {
                    Map<String, Object> f = new LinkedHashMap<>();
                    f.put(C.ID, field.fieldId());
                    f.put(C.NAME, field.name());
                    f.put(C.TYPE, field.type().typeId());
                    f.put(C.OPTIONAL, field.isOptional());
                    return f;
                })
                .collect(Collectors.toList());
    }

    /**
     * 获取表当前分区字段的列表
     *
     * @return 当前分区字段列表
     */
    public List<PartitionField> currentPartitionInfo() {
        return table.spec().fields();
    }

    /**
     * 获取表当前location
     *
     * @return 当前location
     */
    public String currentLocation() {
        return table.location();
    }

    /**
     * 获取表的属性信息
     *
     * @return 表属性信息
     */
    public Map<String, String> tableProperties() {
        return ImmutableMap.copyOf(table.properties());
    }

    /**
     * 获取表总记录数量
     *
     * @return 总记录数量
     */
    public long totalCount() {
        if (table.currentSnapshot() == null) {
            return 0L;
        } else {
            return Long.parseLong(table.currentSnapshot().summary().get(TOTAL_RECORDS_PROP));
        }
    }

    /**
     * 获取符合查询条件的表记录数量
     *
     * @param expression 查询条件
     * @return 记录数量。当读取表记录发生异常时，返回-1。
     */
    public long getRecordCount(Expression expression) {
        TableScan ts = table.newScan().caseSensitive(false).filter(expression).select("*");
        AtomicLong count = new AtomicLong(0);

        try (CloseableIterable<FileScanTask> tasks = ts.planFiles()) {
            // 时间相关的字段/FIXED字段/复杂结构字段，需要使用wrapper在过滤器中访问
            RecordWrapper wrapper = new RecordWrapper(table.schema().asStruct());
            for (FileScanTask task : tasks) {
                if (task.residual().equals(Expressions.alwaysTrue())) {
                    // 整个数据文件中数据都符合条件，全部加上
                    count.getAndAdd(task.file().recordCount());
                } else {
                    // 遍历文件中的数据，累加符合条件的记录
                    try (CloseableIterable<Record> records = TableUtils.openParquetDataFile(table, task)) {
                        Evaluator filter = new Evaluator(table.schema().asStruct(), task.residual(), false);
                        for (Record r : records) {
                            if (filter.eval(wrapper.wrap(r))) {
                                count.getAndIncrement();
                            }
                        }
                    }
                }
            }

            return count.get();
        } catch (IOException ioe) {
            log.error(String.format("%s: get record count by expression %s failed.", table, expression), ioe);
            return -1L;
        }
    }

    /**
     * 获取指定时间范围内表记录数量。开始时间和结束时间为OffsetDateTime对象，即包含时区信息的不变时间对象。
     *
     * @param field 时间字段的名称。
     * @param start 开始时间，包含。
     * @param end 结束时间，不包含。
     * @return 记录数量。当开始时间不早于结束时间时，返回0。当读取表记录发生异常时，返回-1。
     */
    public long getRecordCountBetween(String field, OffsetDateTime start, OffsetDateTime end) {
        // 校验时间字符串合法，且结束时间大于开始时间
        if (start.isBefore(end)) {
            Expression expr = and(greaterThanOrEqual(field, start.toString()), lessThan(field, end.toString()));
            return getRecordCount(expr);
        } else {
            log.warn("{}: start time {} should before end time {}!", table, start, end);
            return 0L;
        }
    }

    /**
     * 获取最近的n个快照
     *
     * @param fetchCount 获取快照数量
     * @return 最近的n个快照
     */
    public List<Snapshot> getLatestSnapshots(int fetchCount) {
        List<Snapshot> result = new ArrayList<>(fetchCount);
        Snapshot snapshot = table.currentSnapshot();
        for (int i = 0; i < fetchCount && snapshot != null; i++) {
            result.add(snapshot);
            Long parentId = snapshot.parentId();
            snapshot = parentId != null ? table.snapshot(parentId) : null;
        }

        return result;
    }

    /**
     * 获取符合条件的快照集合
     *
     * @param predicate 条件
     * @return 快照结果集
     */
    public List<Snapshot> filterSnapshots(Predicate<Snapshot> predicate) {
        List<Snapshot> result = new ArrayList<>();
        // 从最新的snapshot开始向前找，保证新的snapshot在结果集的前面
        Snapshot ss = table.currentSnapshot();
        while (ss != null) {
            if (predicate.test(ss)) {
                result.add(ss);
            }
            ss = ss.parentId() == null ? null : table.snapshot(ss.parentId());
        }

        return result;
    }

    /**
     * 获取iceberg表指定数量的最新的commit msg记录。
     *
     * @param fetchCount 需获取的commit msg记录数量
     * @return 表的commit msg记录列表，新记录在前。
     */
    public List<CommitMsg> getLatestCommitMsg(int fetchCount) {
        List<CommitMsg> result = new ArrayList<>(fetchCount);
        // 在返回的结果集中，最新的commit msg放在最前面
        getLatestSnapshots(fetchCount).forEach(snapshot -> {
            Map<String, String> summary = snapshot.summary();
            if (summary.containsKey(C.DT_SDK_MSG)) {
                Optional<CommitMsg> msg = Utils.parseCommitMsg(summary.get(C.DT_SDK_MSG));
                msg.ifPresent(result::add);
            }
        });

        return result;
    }

    /**
     * 获取表当前所有分区对应的路径集合，去重，并按照字母序排列。
     *
     * @return 表分区对应的路径
     */
    public List<String> getPartitionPaths() {
        if (PartitionSpec.unpartitioned().equals(table.spec())) {
            return new ArrayList<>();  // 非分区表，分区路径为空集合
        } else {
            // 然后遍历表所有的数据文件，获取数据文件的分区信息，对比分区是否在待删除分区里
            return TableUtils.getTableScanTasks(table, alwaysTrue())
                    .stream()
                    .map(task -> task.spec().partitionToPath(task.file().partition()))
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /**
     * 采样表中的数据
     *
     * @param count 采样数量
     * @return 采样的数据
     */
    public List<Record> sampleRecords(int count) {
        return iterRecords(readRecords(alwaysTrue()), count);
    }

    /**
     * 获取表最新的一定数据量的数据
     *
     * @param count 数据量
     * @return 数据集
     */
    public List<Record> getLatestRecords(int count) {
        // 向前找到第一个有数据添加的快照，读取此快照内的数据
        Snapshot s = table.currentSnapshot();
        Set<String> deleteFiles = new HashSet<>();
        // 新增文件且最近快照未被删除
        List<DataFile> addedFiles = new ArrayList<>();

        // 当未发现新增文件或且存在下一个快照
        while (s != null) {
            // 记录当前快照被删除的文件
            s.deletedFiles().forEach(file -> deleteFiles.add(file.path().toString()));
            // 记录新增文件
            s.addedFiles().forEach(dataFile -> {
                if (!deleteFiles.contains(dataFile.path().toString())) {
                    addedFiles.add(dataFile);
                }
            });

            if (addedFiles.isEmpty()) {
                s = s.parentId() == null ? null : table.snapshot(s.parentId());
            } else {
                break;
            }
        }

        if (addedFiles.isEmpty()) {
            log.info("{}: added files is emtpy in all valid snapshots, going to scan table", table);
            return sampleRecords(count);
        } else {
            addedFiles.sort(Comparator.comparing(file -> ((DataFile) file).path().toString()).reversed());
            DataFile file = addedFiles.get(0);
            log.info("{}: reading records from file {}", table, file.path());
            return iterRecords(TableUtils.openParquetDataFile(table, file), count);
        }
    }

    /**
     * 获取当前表全量数据文件
     *
     * @return 文件集
     */
    public List<DataFile> getAllDataFiles() {
        try (BkTableScan ts = BkTableScan.read(this).where(alwaysTrue()).build()) {
            CloseableIterable<FileScanTask> tasks = ts.tasks();
            List<DataFile> files = new ArrayList<>();
            tasks.forEach(f -> files.add(f.file()));

            return files;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * 读取表中符合条件的数据，放入数组返回。可以指定字段列表和最多返回的数据条数。
     *
     * @param expr 表达式
     * @param count 最大数据条数
     * @param columns 字段列表
     * @return 数据集
     */
    public List<Object[]> readRecordsInArray(Expression expr, int count, String... columns) {
        String[] lowerCols = Utils.lowerCaseArray(columns);
        CloseableIterable<Record> iterable = readRecords(expr, lowerCols);
        List<Object[]> result = new ArrayList<>(count);

        try {
            Iterator<Record> iter = iterable.iterator();
            while (iter.hasNext() && count-- > 0) {
                Record record = iter.next();
                Object[] item = new Object[lowerCols.length];
                IntStream.range(0, item.length).forEach(i -> item[i] = record.getField(lowerCols[i]));
                result.add(item);
            }

            iterable.close();
        } catch (IOException ioe) {
            throw new UncheckedIOException("failed to read records", ioe);
        }

        return result;
    }

    /**
     * 遍历数据集，获取最多指定数量的数据并返回。
     *
     * @param closeableIterable 可关闭可迭代的记录集
     * @param count 数据数量
     * @return 数据集
     */
    public List<Record> iterRecords(CloseableIterable<Record> closeableIterable, int count) {
        List<Record> result = new ArrayList<>();
        try {
            Iterator<Record> iter = closeableIterable.iterator();
            while (iter.hasNext() && count-- > 0) {
                result.add(iter.next());
            }

            closeableIterable.close();
        } catch (IOException ioe) {
            throw new UncheckedIOException("failed to read records", ioe);
        }

        return result;
    }

    /**
     * 读取表中的数据，按照表达式进行过滤。必须遍历完结果集，以便底层的资源能正常关闭。
     *
     * @param expression 表达式
     * @return 表中符合条件的数据
     */
    public CloseableIterable<Record> readRecords(Expression expression) {
        return BkTableScan.read(this).where(expression).build();
    }

    /**
     * 指定条件，指定读取的字段列表，读取表中的数据。必须遍历完结果集，以便底层的资源能正常关闭。
     *
     * @param expression 表达式
     * @param columns 读取的字段列表
     * @return 表中符合条件的数据
     */
    public CloseableIterable<Record> readRecords(Expression expression, String... columns) {
        String[] lowerCaseColumns = Utils.lowerCaseArray(columns);
        return BkTableScan.read(this).select(lowerCaseColumns).where(expression).build();
    }

    /**
     * 读取指定快照中新增的数据，按照表达式进行过滤。必须遍历完结果集，以便底层的资源能正常关闭。
     *
     * @param expression 表达式
     * @return 表中符合条件的数据
     */
    public CloseableIterable<Record> readAddedRecordsInSnapshot(Expression expression, Snapshot snapshot) {
        if (snapshot != null) {
            Long parentId = snapshot.parentId();
            // parent id 存在，且parent snapshot未被清理
            if (parentId != null && table.snapshot(parentId) != null) {
                return BkTableScan.read(this)
                        .appendsBetween(parentId, snapshot.snapshotId())
                        .where(expression)
                        .build();
            } else {
                return BkTableScan.read(this)
                        .useSnapshot(snapshot.snapshotId())
                        .where(expression)
                        .build();
            }
        }

        // 快照找不到时，默认返回匹配不到数据的空结果
        return BkTableScan.read(this).where(Expressions.alwaysFalse()).build();
    }

    /**
     * 读取指定快照中的数据，按照表达式进行过滤。必须遍历完结果集，以便底层的资源能正常关闭。
     *
     * @param expression 表达式
     * @param snapshotId 快照id
     * @return 表中符合条件的数据
     */
    public CloseableIterable<Record> readSnapshotRecords(Expression expression, long snapshotId) {
        return BkTableScan.read(this).useSnapshot(snapshotId).where(expression).build();
    }

    /**
     * 读取指定快照范围内(fromSnapshotId, toSnapshotId]的数据，按照表达式进行过滤。必须遍历完结果集，以便底层的资源能正常关闭。
     *
     * @param expression 表达式
     * @param from 起始快照id，不包含
     * @param to 结束快照id，包含
     * @return 表中符合条件的数据
     */
    public CloseableIterable<Record> readSnapshotRecordsBetween(Expression expression, long from, long to) {
        return BkTableScan.read(this)
                .appendsBetween(from, to)
                .where(expression)
                .build();
    }

    /**
     * 强制释放hive metastore中此表相关的锁
     *
     * @return 是否释放锁成功
     */
    public boolean forceReleaseLock() {
        return HiveLockUtils.forceReleaseTableLock(ti, hadoopConf());
    }

    /**
     * 获取表关联的HDFS集群配置
     *
     * @return HDFS集群配置
     */
    public Configuration hadoopConf() {
        return new Configuration(conf);
    }

    /**
     * 获取iceberg表对象
     *
     * @return iceberg表对象
     */
    Table table() {
        return table;
    }

    /**
     * 获取iceberg表的schema
     *
     * @return iceberg表的schema
     */
    public Schema schema() {
        return table.schema();
    }

    /**
     * 获取此对象字符串表示
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return ti.toString();
    }
}