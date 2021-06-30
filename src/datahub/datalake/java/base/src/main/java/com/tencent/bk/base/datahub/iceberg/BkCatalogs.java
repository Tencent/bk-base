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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BkCatalogs {

    private static final Logger log = LoggerFactory.getLogger(BkCatalogs.class);
    private static final Cache<String, HiveCatalog> CATALOG_CACHE = Caffeine.newBuilder()
            .removalListener((RemovalListener<String, HiveCatalog>) (fsName, catalog, cause) -> {
                log.info("removing key {} as {}", fsName, cause);
                if (catalog != null) {
                    catalog.close();
                }
            })
            .build();

    private BkCatalogs() {
    }

    /**
     * 使用HDFS配置，从缓存中加载或者新创建对应的HiveCatalog
     *
     * @param conf HDFS的配置
     * @return HiveCatalog对象
     */
    public static HiveCatalog loadCatalog(Configuration conf) {
        // HDFS default file system
        String fs = conf.get(C.DEFAULT_FS, "");
        return CATALOG_CACHE.get(fs, fsName -> new HiveCatalog(conf));
    }

    /**
     * 仅仅用于测试使用
     *
     * @param fileSystem 待删除的key值
     */
    @Deprecated
    protected static void removeCatalogCache(String fileSystem) {
        CATALOG_CACHE.invalidate(fileSystem);
    }
}