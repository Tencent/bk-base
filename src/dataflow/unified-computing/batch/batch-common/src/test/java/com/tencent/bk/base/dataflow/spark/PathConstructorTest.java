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

import static org.junit.Assert.assertTrue;

import com.tencent.bk.base.dataflow.spark.topology.HDFSPathConstructor;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PathConstructorTest {

    @Test
    public void testPathConstructor1() {
        String fakeRoot = "/fake_root";
        HDFSPathConstructor pathConstructor = new HDFSPathConstructor();
        List<String> pathList = pathConstructor.getAllTimePerHourToEndAsJava(
            fakeRoot,
            "2020060100",
            "2020060103", 1);
        assertTrue(pathList.contains("/fake_root/2020/06/01/00"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/01"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/02"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/03"));
    }

    @Test
    public void testPathConstructor2() {
        String fakeRoot = "/fake_root";
        HDFSPathConstructor pathConstructor = new HDFSPathConstructor();
        List<String> pathList = pathConstructor.getAllTimePerHourToEndAsJava(
            fakeRoot,
            1590940800000L,
            1590951600000L, 1);
        assertTrue(pathList.contains("/fake_root/2020/06/01/00"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/01"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/02"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/03"));
    }

    @Test
    public void testPathConstructor3() {
        String fakeRoot = "/fake_root";
        HDFSPathConstructor pathConstructor = new HDFSPathConstructor();
        List<String> pathList = pathConstructor.getAllTimePerHourAsJava(
            fakeRoot,
            "2020060100",
            "2020060103", 1);
        assertTrue(pathList.contains("/fake_root/2020/06/01/00"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/01"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/02"));
    }

    @Test
    public void testPathConstructor4() {
        String fakeRoot = "/fake_root";
        HDFSPathConstructor pathConstructor = new HDFSPathConstructor();
        List<String> pathList = pathConstructor.getAllTimePerHourAsJava(
            fakeRoot,
            1590940800000L,
            1590951600000L, 1);
        assertTrue(pathList.contains("/fake_root/2020/06/01/00"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/01"));
        assertTrue(pathList.contains("/fake_root/2020/06/01/02"));
    }

}
