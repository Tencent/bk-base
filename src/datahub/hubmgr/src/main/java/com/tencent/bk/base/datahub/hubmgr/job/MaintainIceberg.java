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

package com.tencent.bk.base.datahub.hubmgr.job;

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.EXPIRES;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.HDFS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_CLEAN_ORPHAN_DAYS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_CLEAN_ORPHAN_METHOD;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_COMPACT_DAYS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_MAINTAIN_PARALLEL;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_REMOVE_TODELETE_DAYS;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_TO_DELETE_TABLE_EXPIRE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.PROCESSING_TYPE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.QUERYSET;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.SNAPSHOT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.STORAGES;
import static com.tencent.bk.base.datahub.iceberg.C.ET;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.DistributeWorkers;
import com.tencent.bk.base.datahub.hubmgr.utils.HdfsUtils;
import com.tencent.bk.base.datahub.iceberg.TableUtils;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.util.Tasks;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MaintainIceberg implements Job {

    private static final Logger log = LoggerFactory.getLogger(MaintainIceberg.class);

    private static final String MAINTAIN_ICEBERG = "/databusmgr/iceberg_maintain/workers";
    private static final String ICEBERG_TO_DELETE = "iceberg/to_delete";
    private static final String FOREVER = "-1";
    private static final String DELETE = "delete";
    private static final String MOVE = "move";
    private static final String PRINT = "print";
    private static final String DFS_NAMESERVICES = "dfs.nameservices";

    // 上次获取的配置项
    private static Map<String, String> lastProps = new HashMap<>();

    private final Configuration hdfsConf = new Configuration();
    private final LocalDate today = LocalDate.now();

    private DistributeWorkers disWorkers = new DistributeWorkers(MAINTAIN_ICEBERG, 10_000);

    private int expireDay;
    private int minusDays;
    private boolean cleanOrphan;


    /**
     * iceberg维护任务执行逻辑
     *
     * @param context 任务执行的上下文
     */
    @Override
    public void execute(JobExecutionContext context) {
        LogUtils.info(log, "maintain iceberg triggered at {}", context.getFireTime().getTime());
        // 可以多个节点并行的进行数据维护
        expireDay = DatabusProps.getInstance().getOrDefault(ICEBERG_TO_DELETE_TABLE_EXPIRE, 15);
        minusDays = DatabusProps.getInstance().getOrDefault(ICEBERG_COMPACT_DAYS, 1);
        // 默认每月的1号、11号、21号运行。清理孤儿数据文件无需太频繁，只有匹配到配置中指定的日期才运行。
        String cleanOrphanDays = DatabusProps.getInstance().getOrDefault(ICEBERG_CLEAN_ORPHAN_DAYS, "1,11,21");
        String dayOfMonth = String.valueOf(today.getDayOfMonth());
        cleanOrphan = Arrays.asList(StringUtils.split(cleanOrphanDays, ",")).contains(dayOfMonth);

        Map<String, String> props = HdfsUtils.getIcebergAllHdfsConf();
        if (props.size() == 0 && lastProps.size() == 0) {
            LogUtils.warn(log, "got empty hdfs conf props for iceberg, skip maintain iceberg tables");
            return;
        } else if (props.size() > 0) {
            lastProps = props;
        }

        lastProps.forEach(hdfsConf::set);
        ConcurrentHashMap<String, Set<String>> result = new ConcurrentHashMap<>();
        try (HiveCatalog catalog = new HiveCatalog(hdfsConf)) {
            List<TableIdentifier> tables = catalog.listNamespaces()
                    .stream()
                    .filter(ti -> ti.toString().startsWith("iceberg_"))
                    .map(catalog::listTables)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());

            // 将所有的表按照索引编号分配给不同的worker
            Set<TableIdentifier> todo = new HashSet<>();
            IntStream.range(0, tables.size()).filter(i -> i % disWorkers.getWorkerCnt() == disWorkers.getWorkerIdx())
                    .forEach(i -> todo.add(tables.get(i)));
            LogUtils.info(log, "current worker-{}/{} has {}/{} tables to maintain, todo: {}", disWorkers.getWorkerIdx(),
                    disWorkers.getWorkerCnt(), todo.size(), tables.size(), todo);

            int parallel = DatabusProps.getInstance().getOrDefault(ICEBERG_MAINTAIN_PARALLEL, 5);
            ForkJoinPool customThreadPool = new ForkJoinPool(parallel);  // 未来并行度可以配置
            try {
                customThreadPool.submit(() -> todo.parallelStream().forEach(ti -> processTable(catalog, ti, result)))
                        .get();
                LogUtils.info(log, "finish maintain iceberg tables, result: {}", result);
            } catch (InterruptedException | ExecutionException e) {
                LogUtils.warn(log, "failed to maintain iceberg tables concurrently!", e);
            } finally {
                customThreadPool.shutdown();
            }
        }

        int days = DatabusProps.getInstance().getOrDefault(ICEBERG_REMOVE_TODELETE_DAYS, 45);
        removeOutdatedToDeleteFolders(days); // 清理iceberg/to_delete目录下过期的文件
    }

    /**
     * 维护iceberg表
     *
     * @param catalog hive的catalog对象
     * @param ti 表的id
     * @param result 处理结果对象
     */
    private void processTable(HiveCatalog catalog, TableIdentifier ti, ConcurrentHashMap<String, Set<String>> result) {
        try {
            // 部分表名称格式为xxx_591_todelete_20201215170921，为待删除表
            String fullTn = ti.toString();
            String tn = fullTn.contains(".") ? fullTn.substring(fullTn.lastIndexOf(".") + 1) : fullTn;
            Thread.currentThread().setName("iceberg-maintain-" + fullTn);
            Matcher matcher = CommUtils.TODELETE_TABLE.matcher(tn);
            if (matcher.find()) {
                int diffDays = CommUtils.daysBeforeToday(matcher.group(3).substring(9));
                if (diffDays > expireDay) {
                    // 删除日期和当前日期相隔默认天数或以上的，清理掉表数据和hive元信息，默认配置15天
                    LogUtils.info(log, "{}: drop table as it was deleted more than {} days", ti, diffDays);
                    catalog.dropTable(ti);
                    result.computeIfAbsent("deleted", k -> ConcurrentHashMap.newKeySet()).add(fullTn);
                } else {
                    result.computeIfAbsent("skip_deleted", k -> ConcurrentHashMap.newKeySet()).add(fullTn);
                }

                return;
            }

            // 跳过无法获取rt元信息的表
            String rtId = CommUtils.parseResultTableId(fullTn);
            Map<String, String> meta = CommUtils.getRtMeta(rtId);
            if (meta.size() == 0) {
                LogUtils.info(log, "{}: query meta failed, skipping maintain table", ti);
                result.computeIfAbsent("bad_meta", k -> ConcurrentHashMap.newKeySet()).add(fullTn);
                return;
            }

            // 跳过snapshot/queryset类型的rt
            String processingType = meta.get(PROCESSING_TYPE);
            if (SNAPSHOT.equals(processingType) || QUERYSET.equals(processingType)) {
                LogUtils.info(log, "{}: skipping maintain table by processing type {}", ti, processingType);
                result.computeIfAbsent("skip_processing", k -> ConcurrentHashMap.newKeySet()).add(fullTn);
                return;
            }

            Table table = catalog.loadTable(ti);
            // 通过接口获取表过期时间，过期数据和过期快照
            String expireDays = meta.getOrDefault(String.format("%s.%s.%s", STORAGES, HDFS, EXPIRES), FOREVER);
            int expires = CommUtils.parseExpiresDays(expireDays);
            if (expires > 0) {
                // 过期数据的方法中包含快照过期处理
                TableUtils.expireEarlyData(table, ET, expires + 1);
            } else {
                // 对于数据永不过期的表，或者获取过期时间失败的表，也进行快照过期处理。
                TableUtils.expireSnapshots(table);
            }

            // 合并小文件，设定需要合并的数据的起止时间，默认合并昨天的数据文件
            LocalDateTime compactDay = LocalDate.now().minusDays(minusDays).atStartOfDay();
            OffsetDateTime start = compactDay.atZone(ZoneId.systemDefault()).toOffsetDateTime();
            OffsetDateTime end = compactDay.plusDays(1).atZone(ZoneId.systemDefault()).toOffsetDateTime();
            TableUtils.compactDataFile(table, ET, start, end);

            // 清理孤儿数据文件
            if (cleanOrphan) {
                LogUtils.info(log, "{}: going to clean orphan files, date {}", table, today);
                cleanOrphanFiles(table);
                result.computeIfAbsent("clean_orphan", k -> ConcurrentHashMap.newKeySet()).add(fullTn);
            }

            // 重写manifest文件，如有必要 to do
            result.computeIfAbsent("maintain_success", k -> ConcurrentHashMap.newKeySet()).add(fullTn);
        } catch (Exception e) {
            LogUtils.warn(log, "{}: failed to maintain iceberg table!", ti, e);
        }
    }

    /**
     * 清理iceberg表中孤儿文件
     *
     * @param table iceberg表对象
     * @throws IOException IO异常
     */
    private void cleanOrphanFiles(Table table) throws IOException {
        // 获取表的数据文件和元文件
        Set<String> validFiles = new HashSet<>();
        validFiles.addAll(TableUtils.allDataFilePaths(table));
        validFiles.addAll(TableUtils.allManifestPaths(table));
        validFiles.addAll(TableUtils.allManifestListPaths(table));
        validFiles.addAll(TableUtils.otherMetadataPaths(table));

        // 从元文件和数据文件路径中提取表当前和历史的location地址。location中尾部由db名称和table名称组成。
        String location = table.location();
        location = location.replaceAll("/*$", "");
        String[] arr = StringUtils.split(location, "/");
        String tablePath = String.format("%s/%s", arr[arr.length - 2], arr[arr.length - 1]);
        Set<String> locations = validFiles.stream()
                .filter(s -> s.contains(tablePath))
                .map(s -> s.substring(0, s.indexOf(tablePath)) + tablePath)
                .collect(Collectors.toSet());

        for (String loc : locations) {
            cleanOrphanFilesForLocation(table, loc, validFiles);
        }
    }

    /**
     * 清理表位于location目录中的孤儿文件
     *
     * @param table iceberg表对象
     * @param location 扫描孤儿文件的地址
     * @param validFiles iceberg表中合法的文件
     * @throws IOException IO异常
     */
    private void cleanOrphanFilesForLocation(Table table, String location, Set<String> validFiles) throws IOException {
        // 获取表对应HDFS目录下所有15天未修改的文件
        long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(15);
        Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;
        Path path = new Path(location);
        FileSystem fs = path.getFileSystem(hdfsConf);
        List<String> actualFiles = new ArrayList<>();
        HdfsUtils.listDirRecursively(fs, location, predicate, actualFiles);

        List<String> orphanFiles = actualFiles.stream().filter(f -> !validFiles.contains(f))
                .collect(Collectors.toList());
        if (orphanFiles.size() == 0) {
            LogUtils.info(log, "{} no orphan file found which is older than {}", table, olderThanTimestamp);
            return;
        }

        int thirdSlashIdx = location.indexOf("/", 7);  // 以 hdfs://xxx/ 开头
        String toMovePrefix = String.format("%s/%s/%s",
                location.substring(0, thirdSlashIdx), ICEBERG_TO_DELETE, today.toString());
        String action = DatabusProps.getInstance().getOrDefault(ICEBERG_CLEAN_ORPHAN_METHOD, PRINT);
        switch (action) {
            case DELETE:
                Tasks.foreach(orphanFiles)
                        .noRetry()
                        .suppressFailureWhenFinished()
                        .onFailure((file, exc) -> log.warn("{}: failed to delete file {}. {}", table, file, exc))
                        .run(file -> table.io().deleteFile(file));
                LogUtils.info(log, "{}: orphan files are deleted! {}", table, orphanFiles);
                break;

            case MOVE:
                Tasks.foreach(orphanFiles)
                        .noRetry()
                        .suppressFailureWhenFinished()
                        .onFailure((file, exc) -> log.warn("{}: failed to rename file {}. {}", table, file, exc))
                        .run(file -> {
                            Path src = new Path(file);
                            Path dst = new Path(toMovePrefix + file.substring(thirdSlashIdx));
                            try {
                                fs.mkdirs(dst.getParent());
                                if (!fs.rename(src, dst)) {
                                    LogUtils.warn(log, "{}: failed to move file {} to {}", table, src, dst);
                                }
                            } catch (IOException e) {
                                LogUtils.warn(log, "{}: move file {} to {} failed. exception {}", table, src, dst,
                                        e.getMessage());
                            }
                        });
                LogUtils.info(log, "{}: orphan files are moved to folder {}: {}", table, toMovePrefix, orphanFiles);
                break;

            case PRINT:
                // 和default一样，默认行为，这里无需break
            default:
                LogUtils.info(log, "{} has {} orphan files: {}", table, orphanFiles.size(), orphanFiles);

        }
    }

    /**
     * 删除iceberg待删除目录下超出指定时间的子目录。
     *
     * @param days 子目录修改时间和当前时间对比超出的天数，超出时则删除此子目录。
     */
    private void removeOutdatedToDeleteFolders(int days) {
        String nameServices = lastProps.getOrDefault(DFS_NAMESERVICES, "");
        for (String ns : StringUtils.split(nameServices, ",")) {
            if (StringUtils.isBlank(ns)) {
                continue;
            }

            Path toDeletePath = new Path(String.format("hdfs://%s/%s", ns.trim(), ICEBERG_TO_DELETE));
            try {
                FileSystem fs = toDeletePath.getFileSystem(hdfsConf);
                fs.mkdirs(toDeletePath); // 尝试创建目录以防目录不存在
                long cleanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days);
                FileStatus[] files = fs.listStatus(toDeletePath);
                if (files == null) {
                    continue;
                }

                Arrays.stream(files)
                        .filter(f -> f.getModificationTime() < cleanTimestamp)
                        .forEach(f -> {
                            try {
                                LogUtils.info(log, "going to delete folder {}", f.getPath());
                                fs.delete(f.getPath(), true);
                            } catch (IOException e) {
                                LogUtils.warn(log, "failed to delete folder {} as {}", f.getPath(), e.getMessage());
                            }
                        });

            } catch (IOException ioe) {
                LogUtils.warn(log, "delete outdated folders under {} failed {}", toDeletePath, ioe.getMessage());
            }
        }
    }


}
