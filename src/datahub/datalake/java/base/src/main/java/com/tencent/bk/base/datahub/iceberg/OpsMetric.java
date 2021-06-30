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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.DataFile;

public class OpsMetric {

    private final AtomicLong scanRecords = new AtomicLong();
    private final AtomicLong scanBytes = new AtomicLong();
    private final AtomicLong affectedRecords = new AtomicLong();
    private final Set<String> files = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicLong duration = new AtomicLong();
    private final long start;

    /**
     * 构造函数
     */
    public OpsMetric() {
        start = System.currentTimeMillis();
    }

    /**
     * 添加扫描的记录数
     *
     * @param cnt 记录数量
     * @return 此对象
     */
    public OpsMetric addScanRecords(long cnt) {
        scanRecords.addAndGet(cnt);
        return this;
    }

    /**
     * 添加扫描的字节数
     *
     * @param cnt 字节数量
     * @return 此对象
     */
    public OpsMetric addScanBytes(long cnt) {
        scanBytes.addAndGet(cnt);
        return this;
    }

    /**
     * 添加受影响的记录数量
     *
     * @param cnt 记录数量
     * @return 此对象
     */
    public OpsMetric addAffectedRecords(long cnt) {
        affectedRecords.addAndGet(cnt);
        return this;
    }

    /**
     * 添加扫描的文件
     *
     * @param file 数据文件
     * @return 此对象
     */
    public OpsMetric addDataFile(DataFile file) {
        if (files.add(file.path().toString())) {
            addScanRecords(file.recordCount());
            addScanBytes(file.fileSizeInBytes());
        }
        return this;
    }

    /**
     * 标记完成数据打点
     */
    public void finish() {
        if (!finished.get()) {
            duration.getAndAdd(System.currentTimeMillis() - start);
            finished.set(true);
        }
    }

    /**
     * 获取当前打点数据
     *
     * @return 包含扫描文件数/扫描字节数/扫描记录数/受影响记录数等信息的打点数据
     */
    public Map<String, String> summary() {
        Map<String, String> summary = new HashMap<>();
        summary.put(C.SCAN_FILES, String.valueOf(files.size()));
        summary.put(C.SCAN_BYTES, String.valueOf(scanBytes.get()));
        summary.put(C.SCAN_RECORDS, String.valueOf(scanRecords.get()));
        summary.put(C.AFFECTED_RECORDS, String.valueOf(affectedRecords.get()));
        summary.put(C.FINISHED, String.valueOf(finished.get()));
        summary.put(C.DURATION, String.valueOf(duration.get()));

        return summary;
    }

    @Override
    public String toString() {
        if (finished.get()) {
            return String.format("took %s(ms) to scan %s files %s bytes %s records, and %s records affected.",
                    duration.get(), files.size(), scanBytes.get(), scanRecords.get(), affectedRecords.get());
        } else {
            return String.format("still on scanning, %s files %s bytes %s records scanned, and %s records affected.",
                    files.size(), scanBytes.get(), scanRecords.get(), affectedRecords.get());
        }
    }

}