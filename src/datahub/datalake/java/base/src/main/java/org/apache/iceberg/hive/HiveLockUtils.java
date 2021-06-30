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

package org.apache.iceberg.hive;

import java.net.InetAddress;
import java.net.UnknownHostException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveLockUtils {

    private static final Logger log = LoggerFactory.getLogger(HiveLockUtils.class);

    /**
     * 强制释放hive metastore中表的锁
     *
     * @param ti 表的id
     * @param conf 表的/hive/hdfs配置
     * @return True/False
     */
    public static boolean forceReleaseTableLock(TableIdentifier ti, Configuration conf) {
        try (HiveClientPool pool = new HiveClientPool(1, conf)) {
            HiveMetaStoreClient client = pool.newClient();
            ShowLocksResponse slr = listTableLocks(ti, client);

            log.info("{}: forcing release table locks, lock size {}", ti, slr.getLocksSize());
            for (ShowLocksResponseElement element : slr.getLocks()) {
                long lockId = element.getLockid();
                log.info("{}: release lock id {}, {}", ti, lockId, element);
                client.unlock(lockId);
            }

            return true;
        } catch (TException | RuntimeMetaException e) {
            log.warn(ti + ": forcing release table locks failed.", e);
            return false;
        }
    }

    /**
     * 列出表当前在hive metastore中的锁
     *
     * @param ti 表的id
     * @param client hive metastore 客户端
     * @return 表的锁列表
     * @throws TException thrift异常
     */
    public static ShowLocksResponse listTableLocks(TableIdentifier ti, HiveMetaStoreClient client) throws TException {
        ShowLocksRequest slr = new ShowLocksRequest();
        slr.setDbname(ti.namespace().toString());
        slr.setTablename(ti.name());
        ShowLocksResponse response = client.showLocks(slr);
        log.info("{}: table locks are {}", ti, response);

        return response;
    }

    /**
     * 在hive metastore中为表创建锁
     *
     * @param ti 表的id
     * @param client hive metastore 客户端
     * @return 锁表的response
     * @throws TException thrift异常
     * @throws UnknownHostException 本地主机未知异常
     */
    public static LockResponse createLock(TableIdentifier ti, HiveMetaStoreClient client)
            throws TException, UnknownHostException {
        final LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE,
                ti.namespace().toString());
        lockComponent.setTablename(ti.name());
        final LockRequest lockRequest = new LockRequest(Lists.newArrayList(lockComponent),
                System.getProperty("user.name"),
                InetAddress.getLocalHost().getHostName());

        return client.lock(lockRequest);
    }
}