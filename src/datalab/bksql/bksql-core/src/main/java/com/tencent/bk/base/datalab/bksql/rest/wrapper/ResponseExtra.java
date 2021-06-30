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

package com.tencent.bk.base.datalab.bksql.rest.wrapper;

import com.google.common.base.Objects;
import java.io.Serializable;

public class ResponseExtra implements Serializable {

    private int bizId;
    private String storage;
    private String cluster;
    private String resultCode;
    private int projectId;
    private String resultTableId;
    private long elapsedTime;

    public ResponseExtra() {
    }

    public ResponseExtra(int bizId, String storage, String cluster, String resultCode,
            int projectId, String resultTableId, long elapsedTime) {
        this.bizId = bizId;
        this.storage = storage;
        this.cluster = cluster;
        this.resultCode = resultCode;
        this.projectId = projectId;
        this.resultTableId = resultTableId;
        this.elapsedTime = elapsedTime;
    }

    public int getBizId() {
        return bizId;
    }

    public void setBizId(int bizId) {
        this.bizId = bizId;
    }

    public String getStorage() {
        return storage;
    }

    public void setStorage(String storage) {
        this.storage = storage;
    }

    public String getCluster() {
        return cluster;
    }

    public void setCluster(String cluster) {
        this.cluster = cluster;
    }

    public String getResultCode() {
        return resultCode;
    }

    public void setResultCode(String resultCode) {
        this.resultCode = resultCode;
    }

    public int getProjectId() {
        return projectId;
    }

    public void setProjectId(int projectId) {
        this.projectId = projectId;
    }

    public String getResultTableId() {
        return resultTableId;
    }

    public void setResultTableId(String resultTableId) {
        this.resultTableId = resultTableId;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public void setElapsedTime(long elapsedTime) {
        this.elapsedTime = elapsedTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResponseExtra that = (ResponseExtra) o;
        return bizId == that.bizId
                && projectId == that.projectId
                && elapsedTime == that.elapsedTime
                && Objects.equal(storage, that.storage)
                && Objects.equal(cluster, that.cluster)
                && Objects.equal(resultCode, that.resultCode)
                && Objects.equal(resultTableId, that.resultTableId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(bizId, storage, cluster, resultCode, projectId, resultTableId,
                elapsedTime);
    }

    @Override
    public String toString() {
        return "ResponseExtra{"
                + "bizId=" + bizId
                + ", storage='" + storage + '\''
                + ", cluster='" + cluster + '\''
                + ", resultCode='" + resultCode + '\''
                + ", projectId=" + projectId
                + ", resultTableId='" + resultTableId + '\''
                + ", elapsedTime=" + elapsedTime
                + '}';
    }
}
