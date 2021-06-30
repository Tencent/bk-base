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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThan;

import com.tencent.bk.base.datahub.iceberg.errors.CommitFileFailed;
import com.tencent.bk.base.datahub.iceberg.functions.RecordHandler;
import com.tencent.bk.base.datahub.iceberg.functions.ValFunction;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.iceberg.AllManifestsTable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.RecordWrapper;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableUtils {

    private static final Logger log = LoggerFactory.getLogger(TableUtils.class);

    private static final int maxRewriteFiles = 1000;
    private static final int addBatchSize = 1000;
    private static final int snapshotPreserveDays = 1;
    private static final int snapshotPreserveNums = 500;
    private static final long maxCompactSize = 16L * 1024 * 1024; // 16M
    private static final int workerThreads = 10;


    /**
     * 覆盖表中部分数据文件
     *
     * @param bkTable BkTable表对象
     * @param toAdd 添加的数据文件
     * @param toDelete 删除的数据文件
     * @param msgData commit的消息内容
     */
    public static void rewriteTableDataFiles(BkTable bkTable, Set<DataFile> toAdd, Set<DataFile> toDelete,
            Map<String, String> msgData) {
        rewriteTableDataFiles(bkTable.table(), toAdd, toDelete, msgData);
    }

    /**
     * 覆盖表中部分数据文件
     *
     * @param table iceberg表对象
     * @param toAdd 添加的数据文件
     * @param toDelete 删除的数据文件
     * @param msgData commit的消息内容
     */
    public static void rewriteTableDataFiles(Table table, Set<DataFile> toAdd, Set<DataFile> toDelete,
            Map<String, String> msgData) {
        // commit数据文件
        RewriteFiles rewriteFiles = table.newRewrite();
        rewriteFiles.rewriteFiles(toDelete, toAdd);
        CommitMsg msg = new CommitMsg(C.DT_UPDATE, msgData);
        rewriteFiles.set(C.DT_SDK_MSG, msg.toString());
        rewriteFiles.commit();
        log.info("{} rewrote datafiles {} with {}", table,
                toDelete.stream().map(DataFile::path).collect(toSet()),
                toAdd.stream().map(DataFile::path).collect(toSet()));
    }

    /**
     * 删除表中部分数据文件
     *
     * @param bkTable B'kTable表对象
     * @param toDelete 待删除的数据文件
     * @param msgData commit的消息内容
     */
    public static void deleteTableDataFiles(BkTable bkTable, Set<DataFile> toDelete, Map<String, String> msgData) {
        deleteTableDataFiles(bkTable.table(), toDelete, msgData);
    }

    /**
     * 删除表中部分数据文件
     *
     * @param table iceberg表对象
     * @param toDelete 待删除的数据文件
     * @param msgData commit的消息内容
     */
    public static void deleteTableDataFiles(Table table, Set<DataFile> toDelete, Map<String, String> msgData) {
        DeleteFiles delete = table.newDelete();
        toDelete.forEach(delete::deleteFile);
        CommitMsg msg = new CommitMsg(C.DT_DELETE, msgData);
        delete.set(C.DT_SDK_MSG, msg.toString());
        delete.commit();
        log.info("{} deleted datafiles: {}", table, toDelete.stream().map(DataFile::path).collect(toSet()));
    }

    /**
     * 添加数据文件到表中
     *
     * @param bkTable BkTable表对象
     * @param toAdd 待添加的数据文件
     * @param msgData commit的消息内容
     */
    public static void appendTableDataFiles(BkTable bkTable, Set<DataFile> toAdd, Map<String, String> msgData) {
        appendTableDataFiles(bkTable.table(), toAdd, msgData);
    }

    /**
     * 添加数据文件到表中
     *
     * @param table iceberg表对象
     * @param toAdd 待添加的数据文件
     * @param msgData commit的消息内容
     */
    public static void appendTableDataFiles(Table table, Set<DataFile> toAdd, Map<String, String> msgData) {
        AppendFiles appends = table.newAppend();
        toAdd.forEach(appends::appendFile);
        CommitMsg msg = new CommitMsg(C.DT_ADD, msgData);
        appends.set(C.DT_SDK_MSG, msg.toString());
        appends.commit();
        log.info("{} added datafiles: {}", table, toAdd.stream().map(DataFile::path).collect(toSet()));
    }

    /**
     * 对于分区的时间字段dateField，以当前时间为基点，将expireDays天前的数据从当前表 中清理掉。并只保留最近n天表的快照数据，之前的快照清理掉。
     *
     * @param table iceberg表对象
     * @param dateField 时间字段，且是用于分区的字段之一。
     * @param expireDays 数据保存天数。
     * @throws IllegalArgumentException 参数校验异常。
     */
    public static void expireEarlyData(Table table, String dateField, int expireDays) {
        PartitionField field = getFirstPartitionField(dateField, table.schema(), table.spec());
        // 校验过期时间 > 1，且dateField是分区字段，且为timestamp字段
        boolean isTsField = table.schema().findType(field.sourceId()) == Types.TimestampType.withZone();
        Preconditions.checkArgument(isTsField && expireDays > 1,
                "bad arguments: " + dateField + " " + expireDays);

        // 此处需要用 毫秒 * 1000 获取微秒值，iceberg中时间戳类型存储的就是微秒（Microsecond）
        long expireTs = Instant.now().minus(expireDays, ChronoUnit.DAYS).toEpochMilli() * 1000;

        // 获取当前分区集合，计算出已过期的分区。分区路径格式： name1=partition1/name2=partition2/name3=partition3
        // timestamp类型对应的分区字符串格式为 yyyy、yyyy-MM、yyyy-MM-dd、yyyy-MM-dd-HH
        List<String> allPaths = getPartitionPaths(table);
        Set<String> toDelete = new HashSet<>();

        for (PartitionSpec spec : table.specs().values()) {
            // 分区函数可能发生变化，比如从hour变为day，导致分区字段名称变化，需要处理这种情况
            field = getFirstPartitionField(dateField, table.schema(), spec);
            // 对于timestamp类型的转换器，都是从Long类型（时间戳，毫秒）转换为Integer类型（年、月、日、时）
            Transform<Long, Integer> tsTransform = (Transform<Long, Integer>) field.transform();
            String expirePartition = tsTransform.toHumanString(tsTransform.apply(expireTs));

            String nameEquals = field.name() + C.EQUALS;
            Pattern pattern = Pattern.compile(nameEquals + "(\\d+[\\-\\d]*)");
            allPaths.stream()
                    .filter(path -> path.contains(nameEquals))
                    .map(path -> {
                        Matcher m = pattern.matcher(path);
                        return m.find() ? m.group(1) : expirePartition;
                    })
                    .filter(str -> str.compareTo(expirePartition) < 0)
                    .forEach(toDelete::add);
        }

        if (toDelete.size() > 0) {
            log.info("{}: expire partition before {}. To delete: {}, all partitions: {}", table,
                    Instant.ofEpochMilli(expireTs / 1000), toDelete, allPaths);
            deletePartitions(table, dateField, toDelete);
        }

        expireSnapshots(table);
    }

    /**
     * 将iceberg表中的部分快照过期
     *
     * @param table iceberg表对象
     */
    public static void expireSnapshots(Table table) {
        // 保留最近几天的snapshot，之前的snapshot全部过期掉，至少保留指定数量的snapshots
        int days = Utils.getOrDefaultInt(table.properties().get(C.PRESERVE_SNAPSHOT_DAYS), snapshotPreserveDays);
        int nums = Utils.getOrDefaultInt(table.properties().get(C.PRESERVE_SNAPSHOT_NUMS), snapshotPreserveNums);
        log.info("{}: going to expire snapshots with preserve days {} and preserve count {}", table, days, nums);

        table.expireSnapshots()
                .expireOlderThan(System.currentTimeMillis() - days * 24 * 3_600_000)
                .retainLast(nums)
                .commit();
    }

    /**
     * 压缩指定时间范围内的小数据文件（默认设置为小于128M的数据文件），将其重写为更大的数据文件。
     *
     * @param table iceberg表对象
     * @param dateField 时间字段，且为分区字段
     * @param start 开始日期
     * @param end 结束日期
     * @throws IllegalArgumentException 参数校验异常。
     */
    public static void compactDataFile(Table table, String dateField, OffsetDateTime start, OffsetDateTime end) {
        // 过滤出来满足条件的文件列表，将小于一定大小的数据文件全部列出，读取数据并写回成新文件
        PartitionField field = getFirstPartitionField(dateField, table.schema(), table.spec());
        // 校验过期时间 > 1，且dateField是分区字段，且为timestamp字段
        boolean isTsField = table.schema().findType(field.sourceId()) == Types.TimestampType.withZone();
        Preconditions.checkArgument(isTsField && start.until(end, ChronoUnit.DAYS) >= 1,
                String.format("bad arguments: %s %s %s", dateField, start, end));

        Expression expr = and(greaterThanOrEqual(dateField, start.toString()), lessThan(dateField, end.toString()));
        long maxSize = Utils.getOrDefaultLong(table.properties().get(C.MAX_COMPACT_FILE_SIZE), maxCompactSize);
        List<FileScanTask> tasks = getTableScanTasks(table, expr);

        // 获取分区定义未变化且需要合并的文件，此时如果分区目录下只有一个数据文件，无需重写。
        Map<StructLike, Set<DataFile>> partNotChangedFiles = findRewriteDataFiles(tasks,
                t -> t.file().fileSizeInBytes() < maxSize && t.spec().equals(table.spec()),
                e -> e.getValue().size() > 1);
        // 获取分区定义变化且需要合并的文件，此时如果分区下只有一个数据文件，也需要重写，因为新分区目录变了。
        Map<StructLike, Set<DataFile>> partChangedFiles = findRewriteDataFiles(tasks,
                t -> t.file().fileSizeInBytes() < maxSize && !t.spec().equals(table.spec()),
                e -> true);

        Map<String, String> msg = ImmutableMap.of("field", dateField, "start", start.toString(), "end", end.toString());
        rewriteData(table, false, partNotChangedFiles, msg);
        rewriteData(table, true, partChangedFiles, msg);
    }

    /**
     * 删除表中指定的分区数据
     *
     * @param table iceberg表对象
     * @param fieldName 分区的字段名称
     * @param partitions 分区的值的集合
     * @throws IllegalArgumentException 参数校验异常。
     */
    public static OpsMetric deletePartitions(Table table, String fieldName, Set<String> partitions) {
        OpsMetric metric = new OpsMetric();
        if (partitions.size() > 0) {
            // 获取分区字段的索引编号，表分区定义可能发生变化，导致分区数据以及分区字段位置发生变化
            PartitionField field = getFirstPartitionField(fieldName, table.schema(), table.spec());
            // 然后遍历表所有的数据文件，获取数据文件的分区信息，对比分区是否在待删除分区里
            List<FileScanTask> tasks = getTableScanTasks(table, alwaysTrue());

            Set<DataFile> toDelete = new HashSet<>();
            for (FileScanTask task : tasks) {
                String s = getPartitionString(fieldName, table.schema(), task.spec(), task.file().partition());
                // 如果文件的分区字段转换为的字符串在要删除的分区集合中，则标记需要删除此数据文件
                if (partitions.contains(s)) {
                    toDelete.add(task.file());
                    metric.addDataFile(task.file()).addAffectedRecords(task.file().recordCount());
                }
            }

            // 从iceberg表中删除数据文件，此时并未物理上删除数据文件
            if (toDelete.size() > 0) {
                Map<String, String> msgData = ImmutableMap.of(field.name(), StringUtils.join(partitions, ","));
                deleteTableDataFiles(table, toDelete, msgData);
            }
        }

        metric.finish();
        return metric;
    }

    /**
     * 处理iceberg表中数据。根据expression过滤表中的数据，将符合条件的数据使用handler函数处理。
     *
     * @param table iceberg表对象
     * @param expression 表达式
     * @param transforms 数据转换器，可用于更新一个或多个字段的值
     * @param handler 处理函数
     * @throws CommitFileFailed 异常
     * @throws IllegalArgumentException 参数校验异常。
     */
    public static OpsMetric processingTableData(Table table, Expression expression, Map<String, ValFunction> transforms,
            RecordHandler handler) {
        OpsMetric metric = new OpsMetric();
        List<FileScanTask> tasks = getTableScanTasks(table, expression);
        Set<DataFile> toDelete = tasks.stream().map(FileScanTask::file).collect(toSet());
        // 当扫描文件数量达到一个上限时，不允许更新数据
        int cnt = Utils.getOrDefaultInt(table.properties().get(C.MAX_REWRITE_FILES), maxRewriteFiles);
        Preconditions.checkArgument(toDelete.size() <= cnt, toDelete.size() + " data files affected, exceeds limit");

        DataBuffer dataBuffer = new DataBuffer(table, toDelete);
        List<Record> buffer = new ArrayList<>(addBatchSize);
        String expr = StringUtils.replace(expression.toString(), "\"", "\\\"");
        String trans = StringUtils.join(transforms.keySet(), ",");
        Map<String, String> msg = ImmutableMap.of(C.EXPR, expr, C.FIELDS, trans);

        RecordWrapper w = new RecordWrapper(table.schema().asStruct());
        for (FileScanTask task : tasks) {
            try (CloseableIterable<Record> records = openParquetDataFile(table, task)) {
                metric.addDataFile(task.file());
                // 需要对文件里记录进行过滤，部分符合条件的数据进行处理，剩余数据不变，都写入新文件。
                Evaluator filter = new Evaluator(table.schema().asStruct(), task.residual(), true);
                log.info("{} processing data in {}, residual expression {}", table, task.file().path(),
                        task.residual());

                for (Record r : records) {
                    boolean match = filter.eval(w.wrap(r));
                    if (match) {
                        metric.addAffectedRecords(1);  // 符合expression中条件，要么被删除，要么被更新。
                    }

                    Optional<Record> result = handler.handle(r, match, transforms);
                    if (result.isPresent()) {
                        buffer.add(result.get());

                        if (buffer.size() == addBatchSize) {
                            dataBuffer.add(buffer, msg);
                            buffer.clear();
                        }
                    }
                }
            } catch (IOException e) {
                log.error("{} failed to processing data. {} {}", table, expression, transforms);
                throw new CommitFileFailed(e, "{} processing data failed", table);
            }
        }

        // 将剩余数据加入dataBuffer中，然后关闭写入，刷盘文件
        dataBuffer.add(buffer, msg);
        dataBuffer.close();
        metric.finish();

        return metric;
    }

    /**
     * 找到使用此字段作为分区定义的第一个分区字段
     *
     * @param name 表字段名称
     * @param schema 表的schema定义
     * @param spec 表分区定义
     * @return 分区字段
     * @throws IllegalArgumentException 表中未找到此字段或者在分区定义中未使用此字段时抛出异常。
     */
    public static PartitionField getFirstPartitionField(String name, Schema schema, PartitionSpec spec) {
        // 字段必须存在，且是分区定义的一部分
        Types.NestedField field = schema.findField(name.toLowerCase());
        Preconditions.checkArgument(field != null, "table has no field " + name);

        List<PartitionField> fields = spec.getFieldsBySourceId(field.fieldId());
        Preconditions.checkArgument(fields.size() > 0, "not partition field: " + name);

        // 取第一个符合的分区字段
        return fields.get(0);
    }

    /**
     * 获取表当前所有分区对应的路径集合，去重，并按照字母序排列。
     *
     * @param table iceberg表对象
     * @return 表分区对应的路径
     */
    public static List<String> getPartitionPaths(Table table) {
        if (PartitionSpec.unpartitioned().equals(table.spec())) {
            return new ArrayList<>();  // 非分区表，分区路径为空集合
        } else {
            // 然后遍历表所有的数据文件，获取数据文件的分区信息，对比分区是否在待删除分区里
            return getTableScanTasks(table, alwaysTrue())
                    .stream()
                    .map(task -> task.spec().partitionToPath(task.file().partition()))
                    .distinct()
                    .sorted()
                    .collect(Collectors.toList());
        }
    }

    /**
     * 获取字符串的分区值
     *
     * @param name 分区字段
     * @param schema 表schema定义
     * @param spec 分区定义
     * @param partition 分区数据
     * @return 字符串的分区值
     */
    public static String getPartitionString(String name, Schema schema, PartitionSpec spec, StructLike partition) {
        Types.NestedField field = schema.findField(name.toLowerCase());
        PartitionField partField = spec.getFieldsBySourceId(field.fieldId()).get(0);
        int index = spec.fields().indexOf(partField);
        Class clz = spec.javaClasses()[index];

        return partField.transform().toHumanString(Utils.get(partition, index, clz));
    }

    /**
     * 读取parquet格式的数据文件
     *
     * @param table iceberg表对象
     * @param task 读取数据文件的任务
     * @return 数据文件中包含的可遍历的数据集
     */
    public static CloseableIterable<Record> openParquetDataFile(Table table, FileScanTask task) {
        return openParquetDataFile(table, task.file(), task.start(), task.length());
    }

    /**
     * 读取parquet格式的数据文件中全部内容
     *
     * @param table iceberg表对象
     * @param file 数据文件
     * @return 数据文件中包含的可遍历的数据集
     */
    public static CloseableIterable<Record> openParquetDataFile(Table table, DataFile file) {
        return openParquetDataFile(table, file, 0, file.fileSizeInBytes());
    }

    /**
     * 读取parquet格式的数据文件，指定开始位置和读取长度。
     *
     * @param table iceberg表对象
     * @param file 读取数据文件的任务
     * @param start 读取文件的开始位置
     * @param length 读取文件的长度
     * @return 数据文件中包含的可遍历的数据集
     */
    public static CloseableIterable<Record> openParquetDataFile(Table table, DataFile file, long start, long length) {
        InputFile input = table.io().newInputFile(file.path().toString());
        // 所有数据文件都是parquet格式
        Parquet.ReadBuilder parquet = Parquet.read(input)
                .project(table.schema())
                .createReaderFunc(schema -> GenericParquetReaders.buildReader(table.schema(), schema))
                .split(start, length);

        return parquet.build();
    }

    /**
     * 根据表达式对表的数据文件进行扫描，返回符合条件的数据文件列表。
     *
     * @param table iceberg表对象
     * @param expression 表达式
     * @return 数据文件列表
     */
    public static List<FileScanTask> getTableScanTasks(Table table, Expression expression) {
        TableScan ts = table.newScan().caseSensitive(false).filter(expression).select("*");
        return Lists.newArrayList(Iterables.concat(ts.planFiles()));
    }

    /**
     * 寻找符合条件的数据文件，例如需要合并的小文件
     *
     * @param tasks 表的数据文件列表
     * @param fileFilter 数据文件过滤条件
     * @param entryFilter 结果集entry的过滤条件
     * @return 符合条件的数据文件
     */
    private static Map<StructLike, Set<DataFile>> findRewriteDataFiles(List<FileScanTask> tasks,
            Predicate<FileScanTask> fileFilter, Predicate<Map.Entry<StructLike, Set<DataFile>>> entryFilter) {
        return tasks.stream()
                .filter(fileFilter)
                .collect(groupingBy(t -> t.file().partition(), mapping(FileScanTask::file, toSet())))
                .entrySet()
                .stream()
                .filter(entryFilter)
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    /**
     * 重写数据文件，无论数据所在分区是否发生变化
     *
     * @param table 表对象
     * @param partChanged 是否分区的定义发生变化，导致数据分区发生变化
     * @param partToFiles 当前分区对象到数据文件集合的映射
     * @param props commit的消息内容
     */
    private static void rewriteData(Table table, boolean partChanged, Map<StructLike, Set<DataFile>> partToFiles,
            Map<String, String> props) {
        if (partToFiles.size() == 0) {
            return;
        }

        Set<DataFile> rewrite = partToFiles.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
        String files = rewrite.stream().map(DataFile::path).collect(toSet()).toString();
        Map<String, String> msg = new HashMap<>(props);
        msg.put("rewriteFiles", files);
        log.info("{}: compacting data files {} ", table, msg);

        // 当扫描文件数量达到一个上限时，不允许更新数据
        int cnt = Utils.getOrDefaultInt(table.properties().get(C.MAX_REWRITE_FILES), maxRewriteFiles);
        Preconditions.checkArgument(rewrite.size() <= cnt, rewrite.size() + " data files affected, exceeds limit");

        if (!rewrite.isEmpty()) {
            long startTs = System.currentTimeMillis();
            if (partChanged) {
                // 如果分区定义发生过变化，则逐个文件打开，通过DataBuffer写入数据
                rewritePartChangedDataFiles(table, rewrite, msg);
            } else {
                // 如果分区定义未变化，则并行打开一个分区目录下的文件，合并写入此目录
                rewritePartNotChangedDataFiles(table, partToFiles, msg);
            }

            log.info("{}: finished compact data file {}, partition spec changed: {}, takes {}ms",
                    table, msg, partChanged, System.currentTimeMillis() - startTs);
        }
    }

    /**
     * 重写表的数据文件，数据文件所在分区发生变化
     *
     * @param table iceberg表对象
     * @param rewrite 待重写的数据文件集合
     * @param msgData commit消息内容
     */
    public static void rewritePartChangedDataFiles(Table table, Set<DataFile> rewrite, Map<String, String> msgData) {
        DataBuffer dataBuffer = new DataBuffer(table, rewrite);
        List<Record> buffer = new ArrayList<>(addBatchSize);
        rewrite.forEach(file -> {
            try (CloseableIterable<Record> records = openParquetDataFile(table, file)) {
                for (Record r : records) {
                    buffer.add(r);
                    if (buffer.size() == addBatchSize) {
                        dataBuffer.add(buffer, msgData);
                        buffer.clear();
                    }
                }
            } catch (IOException e) {
                log.error("{} failed to compact files. {} ", table,
                        rewrite.stream().map(DataFile::path).collect(toSet()));
                throw new CommitFileFailed(e, "{} compacting data failed", table);
            }
        });

        // 将剩余数据加入dataBuffer中，然后关闭写入，刷盘文件
        dataBuffer.add(buffer, msgData);
        dataBuffer.close();
    }

    /**
     * 重写表的数据文件，数据文件所在分区不变
     *
     * @param table iceberg表对象
     * @param partToFiles 分区数据到文件集合的映射
     * @param msgData commit消息内容
     */
    public static void rewritePartNotChangedDataFiles(Table table, Map<StructLike, Set<DataFile>> partToFiles,
            Map<String, String> msgData) {
        String filename = String.format("%s.%s", UUID.randomUUID().toString(), C.FILE_FORMAT);
        Set<DataFile> toAdd = ConcurrentHashMap.newKeySet();
        Set<DataFile> toDelete = ConcurrentHashMap.newKeySet();
        List<Callable<Boolean>> tasks = new ArrayList<>(partToFiles.size());
        partToFiles.forEach((partition, files) -> tasks.add(() -> {
            if (files.size() <= 1) { // 只有当数据文件大于1个时，才能合并文件
                return false;
            }
            String location = table.locationProvider().newDataLocation(table.spec(), partition, filename);
            try {
                FileAppender<Record> appender = Parquet
                        .write(table.io().newOutputFile(location))
                        .forTable(table)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .build();
                for (DataFile file : files) {
                    toDelete.add(file);
                    try (CloseableIterable<Record> records = openParquetDataFile(table, file)) {
                        for (Record r : records) {
                            appender.add(r);
                        }
                    }
                }

                appender.close();
                DataFile datafile = DataFiles.builder(table.spec())
                        .withInputFile(table.io().newInputFile(location))
                        .withPartition(partition)
                        .withMetrics(appender.metrics())
                        .build();
                toAdd.add(datafile);
            } catch (IOException ioe) {
                log.error("{} failed to compact files. {}", table,
                        files.stream().map(DataFile::path).collect(toSet()));
                throw new CommitFileFailed(ioe, "{} compacting data failed", table);
            }
            return true;
        }));

        int threads = Utils.getOrDefaultInt(table.properties().get(C.WORKER_THREADS), workerThreads);
        ExecutorService pool = Executors.newFixedThreadPool(threads,
                new ThreadFactoryBuilder().setNameFormat("iceberg-compact-%d").build());
        try {
            List<Future<Boolean>> futures = pool.invokeAll(tasks);
            for (Future<Boolean> f : futures) {
                f.get();
            }
        } catch (Exception e) {
            log.error("{} running compact files in pool failed.", table);
            throw new CommitFileFailed(e, "{} compacting data failed", table);
        } finally {
            pool.shutdown();
        }

        // commit数据文件
        rewriteTableDataFiles(table, toAdd, toDelete, msgData);
    }

    /**
     * 获取iceberg表的所有manifest文件集合
     *
     * @param table iceberg表对象
     * @return manifest文件路径集合
     * @throws IOException IO异常
     */
    public static Set<String> allManifestPaths(Table table) throws IOException {
        Set<String> allFiles = new HashSet<>();
        AllManifestsTable mt = new AllManifestsTable(((BaseTable) table).operations(), table);
        TableScan ts = mt.newScan();
        try (CloseableIterable<FileScanTask> files = ts.planFiles()) {
            files.forEach(f -> {
                try (CloseableIterable<StructLike> rows = f.asDataTask().rows()) {
                    // ref ManifestsTable#manifestFileToRow()，rows返回的结果集中索引为0的是path字段
                    rows.forEach(r -> allFiles.add(r.get(0, String.class)));
                } catch (IOException ioe) {
                    log.error("{} get all manifest file paths failed. {}", table, ioe.getMessage());
                    throw new UncheckedIOException(ioe);
                }
            });
        }

        return allFiles;
    }

    /**
     * 获取iceberg表的所有数据文件集合
     *
     * @param table iceberg表对象
     * @return 数据文件路径集合
     */
    public static Set<String> allDataFilePaths(Table table) {
        return new HashSet<>(transformAllDataFiles(table, f -> f.path().toString()));
    }

    /**
     * 获取iceberg表的所有数据文件集合
     *
     * @param table iceberg表对象
     * @return 数据文件集合
     */
    public static <R> List<R> transformAllDataFiles(Table table, Function<DataFile, R> transform) {
        long start = System.currentTimeMillis();
        List<R> dataFiles = new ArrayList<>();
        // 参考AllDataFilesTable中的逻辑实现，遍历所有的manifest文件中的datafile记录。
        Set<ManifestFile> manifestFiles = new HashSet<>();
        table.snapshots().forEach(s -> manifestFiles.addAll(s.dataManifests()));

        manifestFiles.forEach(m -> {
            try (ManifestReader<DataFile> dfs = ManifestFiles.read(m, table.io())) {
                dfs.forEach(df -> dataFiles.add(transform.apply(df)));
            } catch (IOException ioe) {
                log.error("{} transform all data files failed. {}", table, ioe.getMessage());
                throw new UncheckedIOException(ioe);
            }
        });

        log.info("{}: {} manifest file read, got {} data files, takes {}ms",
                table, manifestFiles.size(), dataFiles.size(), System.currentTimeMillis() - start);

        return dataFiles;
    }

    /**
     * 获取iceberg表的所有manifest list文件集合
     *
     * @param table iceberg表对象
     * @return manifest list文件路径集合
     */
    public static Set<String> allManifestListPaths(Table table) {
        Set<String> mlFiles = new HashSet<>();
        for (Snapshot snapshot : table.snapshots()) {
            String location = snapshot.manifestListLocation();
            if (location != null) {
                mlFiles.add(location);
            }
        }

        return mlFiles;
    }

    /**
     * 获取iceberg表的其他元文件集合
     *
     * @param table iceberg表对象
     * @return 其他元文件路径集合
     */
    public static Set<String> otherMetadataPaths(Table table) {
        Set<String> files = new HashSet<>();
        TableOperations ops = ((BaseTable) table).operations();
        TableMetadata metadata = ops.current();
        String metaLoc = metadata.metadataFileLocation();
        files.add(metaLoc);
        files.add(metaLoc.substring(0, metaLoc.lastIndexOf("/")) + "/version-hint.text");
        metadata.previousFiles().forEach(e -> files.add(e.file()));

        return files;
    }
}
