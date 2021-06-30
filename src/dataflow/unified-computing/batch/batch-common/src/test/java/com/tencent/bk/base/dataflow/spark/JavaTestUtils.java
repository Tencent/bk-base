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

package com.tencent.bk.base.dataflow.spark;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.tencent.bk.base.dataflow.core.topo.NodeField;
import com.tencent.bk.base.dataflow.spark.topology.BkFieldConstructor;
import com.tencent.bk.base.dataflow.spark.topology.HDFSPathConstructor;
import com.tencent.bk.base.dataflow.spark.topology.IgniteStorageConstructor;
import java.util.List;

public class JavaTestUtils {

    public static HDFSPathConstructor getMockHdfsPathConstructor(String resultTableId, String fakeRoot) {
        HDFSPathConstructor hdfsPathConstructor = spy(new HDFSPathConstructor());
        doReturn(fakeRoot).when(hdfsPathConstructor).getHdfsRoot(resultTableId);
        doReturn(String.format("%s/copy", fakeRoot)).when(hdfsPathConstructor)
            .getHDFSCopyRoot(resultTableId);
        return hdfsPathConstructor;
    }

    public static BkFieldConstructor getMockFieldConstructor(String resultTableId) {
        BkFieldConstructor fieldConstructor = spy(new BkFieldConstructor());
        List<NodeField> fields = ScalaTestUtils$.MODULE$.createTableField1(resultTableId);
        doReturn(fields).when(fieldConstructor).getRtField(resultTableId);
        return fieldConstructor;
    }

    public static IgniteStorageConstructor getMockIgniteStorageConstructor(String igniteTableName) {
        IgniteStorageConstructor igniteConstructor = spy(new IgniteStorageConstructor(igniteTableName));
        doReturn(igniteTableName).when(igniteConstructor).getPhysicalName();
        igniteConstructor.setConnectionMap(ScalaTestUtils$.MODULE$.getIgniteFakeConfMap());
        return igniteConstructor;
    }

}
