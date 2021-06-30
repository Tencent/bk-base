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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.RecordWrapper;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public class BkTableScan extends CloseableGroup implements CloseableIterable<Record> {

    private final TableOperations ops;
    private final Schema schema;
    private final CloseableIterable<FileScanTask> tasks;

    /**
     * 构造函数
     *
     * @param scan tableScan对象
     */
    private BkTableScan(TableScan scan) {
        this.ops = ((HasTableOperations) scan.table()).operations();
        this.schema = scan.schema();
        this.tasks = scan.planFiles();
    }

    /**
     * 构造扫描表数据的扫描器
     *
     * @param table BkTable对象
     * @return 扫描器
     */
    public static ScanBuilder read(BkTable table) {
        return read(table.table());
    }

    /**
     * 构造扫描表数据的扫描器
     *
     * @param table iceberg表
     * @return 扫描器
     */
    public static ScanBuilder read(Table table) {
        return new ScanBuilder(table);
    }

    /**
     * 迭代器
     *
     * @return 遍历数据的迭代器
     */
    @Override
    public CloseableIterator<Record> iterator() {
        BkTableScan.ScanIterator iter = new ScanIterator(tasks);
        addCloseable(iter);
        return iter;
    }

    /**
     * 获取符合条件的文件扫描任务迭代器
     *
     * @return 文件扫描任务迭代器
     */
    public CloseableIterable<FileScanTask> tasks() {
        return tasks;
    }

    /**
     * 释放打开的资源
     *
     * @throws IOException IO异常
     */
    @Override
    public void close() throws IOException {
        tasks.close(); // close manifests from scan planning
        super.close(); // close data files
    }

    /**
     * 打开FileScanTask中的数据文件，返回可遍历的数据对象
     *
     * @param task 文件扫描任务
     * @return 可遍历的数据对象
     */
    private CloseableIterable<Record> open(FileScanTask task) {
        InputFile input = ops.io().newInputFile(task.file().path().toString());
        // 所有数据文件都是parquet格式
        Parquet.ReadBuilder parquet = Parquet.read(input)
                .project(schema)
                .createReaderFunc(s -> GenericParquetReaders.buildReader(schema, s))
                .split(task.start(), task.length());

        return parquet.build();
    }

    public static class ScanBuilder {

        private TableScan tableScan;

        /**
         * 构造函数
         *
         * @param table iceberg表
         */
        public ScanBuilder(Table table) {
            this.tableScan = table.newScan();
        }

        /**
         * 过滤数据的表达式
         *
         * @param rowFilter 表达式
         * @return 构造器
         */
        public ScanBuilder where(Expression rowFilter) {
            this.tableScan = tableScan.filter(rowFilter);
            return this;
        }

        /**
         * 选择结果字段
         *
         * @param selectedColumns 选中的字段数组
         * @return 构造器
         */
        public ScanBuilder select(String... selectedColumns) {
            this.tableScan = tableScan.select(ImmutableList.copyOf(selectedColumns));
            return this;
        }

        /**
         * 扫描指定快照的数据
         *
         * @param scanSnapshotId 快照id
         * @return 构造器
         */
        public ScanBuilder useSnapshot(long scanSnapshotId) {
            this.tableScan = tableScan.useSnapshot(scanSnapshotId);
            return this;
        }

        /**
         * 扫描指定起止快照之间的数据
         *
         * @param fromSnapshotId 扫描开始的快照id，不包含
         * @param toSnapshotId 扫描结束的快照id，包含
         * @return 构造器
         */
        public ScanBuilder appendsBetween(long fromSnapshotId, long toSnapshotId) {
            this.tableScan = tableScan.appendsBetween(fromSnapshotId, toSnapshotId);
            return this;
        }

        /**
         * 构造器函数
         *
         * @return 扫描结果对象
         */
        public BkTableScan build() {
            return new BkTableScan(tableScan);
        }
    }

    private class ScanIterator implements CloseableIterator<Record>, Closeable {

        private final Iterator<FileScanTask> tasks;
        private final RecordWrapper wrap;
        private Closeable closeable = null;
        private Iterator<Record> iter = Collections.emptyIterator();

        /**
         * 构造函数
         *
         * @param tasks 扫描任务列表
         */
        private ScanIterator(CloseableIterable<FileScanTask> tasks) {
            this.tasks = tasks.iterator();
            this.wrap = new RecordWrapper(schema.asStruct());
        }

        /**
         * 迭代器中是否还有数据
         *
         * @return True/False
         */
        @Override
        public boolean hasNext() {
            while (true) {
                if (iter.hasNext()) {
                    return true;

                } else if (tasks.hasNext()) {
                    if (closeable != null) {
                        try {
                            closeable.close();
                        } catch (IOException e) {
                            throw new RuntimeIOException(e, "Failed to close task");
                        }
                    }

                    FileScanTask task = tasks.next();
                    CloseableIterable<Record> reader = open(task);
                    closeable = reader;

                    if (task.residual() != null && task.residual() != Expressions.alwaysTrue()) {
                        Evaluator eval = new Evaluator(schema.asStruct(), task.residual(), true);
                        iter = Iterables.filter(reader, r -> eval.eval(wrap.wrap(r))).iterator();
                    } else {
                        iter = reader.iterator();
                    }

                } else {
                    return false;
                }
            }
        }

        /**
         * 迭代器中下一条记录
         *
         * @return 数据记录
         */
        @Override
        public Record next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            return iter.next();
        }

        /**
         * 释放资源
         *
         * @throws IOException IO异常
         */
        @Override
        public void close() throws IOException {
            if (closeable != null) {
                closeable.close();
            }
        }
    }

}