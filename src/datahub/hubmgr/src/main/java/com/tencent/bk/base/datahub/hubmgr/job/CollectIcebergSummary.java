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

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_SUMMARY_PARALLEL;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ICEBERG_TABLE_STAT;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ZERO;
import static com.tencent.bk.base.datahub.iceberg.C.SUMMARY;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;
import com.tencent.bk.base.datahub.databus.commons.utils.Utils;
import com.tencent.bk.base.datahub.hubmgr.utils.CommUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.DistributeWorkers;
import com.tencent.bk.base.datahub.hubmgr.utils.HdfsUtils;
import com.tencent.bk.base.datahub.hubmgr.utils.TsdbWriter;
import com.tencent.bk.base.datahub.iceberg.BkTableScan;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CollectIcebergSummary implements Job {

    private static final Logger log = LoggerFactory.getLogger(CollectIcebergSummary.class);
    // 上次获取的配置项
    private static Map<String, String> lastProps = new HashMap<>();
    private static final String ICEBERG_SUMMARY = "/databusmgr/iceberg_summary/workers";

    // 分布式任务分配
    private DistributeWorkers disWorkers = new DistributeWorkers(ICEBERG_SUMMARY, 10_000);

    /**
     * 查询iceberg table的汇总信息total-data-file和total-records，将此结果数据写入指定的统计表中。
     *
     * @param context 作业执行上下文
     */
    public void execute(JobExecutionContext context) {
        reportSummary(context);
    }

    /**
     * 获取hdfs配置
     */
    private Configuration getHdfsConf() {
        Map<String, String> props = HdfsUtils.getIcebergAllHdfsConf();
        if (props.size() == 0 && lastProps.size() == 0) {
            LogUtils.warn(log, "got empty hdfs conf props for iceberg, skip collect summary info");
            throw new IllegalArgumentException("empty hdfs conf props for iceberg");
        } else if (props.size() > 0) {
            lastProps = props;
        }
        final Configuration conf = new Configuration();
        lastProps.forEach(conf::set);

        return conf;
    }


    /**
     * 上报采集的统计信息
     */
    private void reportSummary(JobExecutionContext context) {
        long triggerTime = context.getFireTime().getTime();
        LogUtils.info(log, "collect iceberg table summary triggered at {}", triggerTime);

        final Configuration conf = getHdfsConf();
        final AtomicLong sumFiles = new AtomicLong(0);
        final AtomicLong sumRecords = new AtomicLong(0);
        final AtomicLong sumSize = new AtomicLong(0);

        int parallel = DatabusProps.getInstance().getOrDefault(ICEBERG_SUMMARY_PARALLEL, 5);
        ForkJoinPool customThreadPool = new ForkJoinPool(parallel);  // 未来并行度可以配置

        try (HiveCatalog catalog = new HiveCatalog(conf)) {
            List<TableIdentifier> tiList = catalog.listNamespaces()
                    .parallelStream()
                    .filter(namespace -> namespace.toString().startsWith("iceberg_"))
                    .map(catalog::listTables)
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            LogUtils.info(log, "{} iceberg tables in catalog", tiList.size());

            Set<TableIdentifier> todo = new HashSet<>();
            tiList.stream()
                    .filter(ti -> Math.abs(ti.hashCode() % disWorkers.getWorkerCnt()) == disWorkers.getWorkerIdx())
                    .forEach(todo::add);
            LogUtils.info(log, "current worker-{}/{} has {}/{} tables to summary, todo: {}", disWorkers.getWorkerIdx(),
                    disWorkers.getWorkerCnt(), todo.size(), tiList.size(), todo);

            customThreadPool.submit(() -> todo.parallelStream().forEach(ti -> {
                try {
                    Table table = catalog.loadTable(ti);
                    Snapshot snapshot = table.currentSnapshot();
                    if (snapshot == null) {
                        return;
                    }

                    TableOperations ops = ((BaseTable) table).operations();
                    String metaLoc = ops.current().metadataFileLocation();
                    long metaSize = table.io().newInputFile(metaLoc).getLength();
                    long totalSize = getTableDataFileSize(table);
                    Map<String, String> summary = snapshot.summary();
                    long totalFiles = Long.parseLong(summary.getOrDefault(TOTAL_DATA_FILES_PROP, ZERO));
                    long totalRecords = Long.parseLong(summary.getOrDefault(TOTAL_RECORDS_PROP, ZERO));
                    reportToTsdb("hive." + ti.toString(), totalFiles, totalRecords, totalSize, metaSize,
                            System.currentTimeMillis() / 1000);
                    sumFiles.addAndGet(totalFiles);
                    sumRecords.addAndGet(totalRecords);
                    sumSize.addAndGet(totalSize);

                } catch (Exception e) {
                    LogUtils.warn(log, "{}: failed to collect iceberg table summary!", ti.toString(), e);
                }
            })).get();
            LogUtils.info(log, "finish collect iceberg table summary");

        } catch (InterruptedException | ExecutionException e) {
            LogUtils.warn(log, "failed to collect iceberg table summary!", e);
        } finally {
            customThreadPool.shutdown();
        }

        reportToTsdb(SUMMARY, sumFiles.get(), sumRecords.get(), sumSize.get(), 0, triggerTime / 1000);
    }

    /**
     * 获取iceberg表当前数据文件的总大小
     *
     * @param table iceberg表对象
     * @return 当前数据文件的总大小
     */
    private long getTableDataFileSize(Table table) {
        AtomicLong totalSize = new AtomicLong(0);
        try (BkTableScan ts = BkTableScan.read(table).where(alwaysTrue()).build()) {
            CloseableIterable<FileScanTask> tasks = ts.tasks();
            tasks.forEach(task -> totalSize.getAndAdd(task.file().fileSizeInBytes()));
        } catch (IOException ioe) {
            LogUtils.warn(log, "{}: get table data file size failed. {}", table, ioe.getMessage());
        }

        return totalSize.get();
    }

    /**
     * 将sample数据落地到influxdb中
     *
     * @param table 表名
     * @param totalFiles 总文件数
     * @param totalRecords 总记录数
     * @param totalSize 当前数据文件累积大小
     * @param metaSize 当前表metadata文件大小
     * @param reportTime 数据上报时间
     */
    private void reportToTsdb(String table, long totalFiles, long totalRecords, long totalSize, long metaSize,
            long reportTime) {
        String rtId = CommUtils.parseResultTableId(table);
        String tagStr = String.format("table=%s,operation=sample,rt_id=%s,ip=%s", table, rtId, Utils.getInnerIp());
        String fieldsStr = String.format("total_files=%si,total_records=%si,total_size=%si,meta_size=%si",
                totalFiles, totalRecords, totalSize, metaSize);
        TsdbWriter.getInstance().reportData(ICEBERG_TABLE_STAT, tagStr, fieldsStr, reportTime);
    }
}

