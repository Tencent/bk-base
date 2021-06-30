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

package com.tencent.bk.base.dataflow.codecheck;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.powermock.utils.Asserts;
import org.slf4j.Logger;

@RunWith(PowerMockRunner.class)
@PrepareForTest({MethodParserGroup.class, MethodParser.class})
public class MethodParserGroupTest {

    private AutoCloseable closeable;

    @Before
    public void openMocks() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @After
    public void releaseMocks() throws Exception {
        closeable.close();
    }

    @Test
    public void testGetSingleton() {
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.spy(MethodParserGroup.class);
        Assert.assertNotNull(MethodParserGroup.getSingleton());
        Asserts.internalAssertNotNull(MethodParserGroup.class, "parserMap");
    }

    @Test
    public void testAddParserGroup() throws Exception {
        MethodParser methodParser = PowerMockito.mock(MethodParser.class);
        PowerMockito.whenNew(MethodParser.class).withAnyArguments().thenReturn(methodParser);
        PowerMockito.doNothing().when(methodParser).init();

        String groupName = "group_a";
        String jarLib = "/data1/xxx;/data2/xxx";
        String sourceDir = "/data3/xxx";
        PowerMockito.mockStatic(MethodParserGroup.class);
        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        Map<String, MethodParser> parserMap = new HashMap<>();
        Whitebox.setInternalState(methodParserGroup, "parserMap", parserMap);
        PowerMockito.doCallRealMethod().when(methodParserGroup).addParserGroup(anyString(), anyString(), anyString());
        PowerMockito.doCallRealMethod().when(methodParserGroup, "addParserGroup",
                anyString(), anyList(), anyString());

        methodParserGroup.addParserGroup(groupName, jarLib, sourceDir);
        Assert.assertEquals(1, parserMap.size());
        Assert.assertTrue(parserMap.containsKey(groupName));
        Assert.assertEquals((Map) Whitebox.getInternalState(methodParserGroup, "parserMap"), parserMap);
    }

    @Test
    public void testGetParserGroup() {
        PowerMockito.mockStatic(MethodParserGroup.class);
        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        MethodParser methodParser = PowerMockito.mock(MethodParser.class);
        String groupName = "group_a";
        Map<String, MethodParser> parserMap = new HashMap<>();
        parserMap.put(groupName, methodParser);
        Whitebox.setInternalState(methodParserGroup, "parserMap", parserMap);
        PowerMockito.doCallRealMethod().when(methodParserGroup).getParserGroup(anyString());
        Assert.assertEquals(methodParser, methodParserGroup.getParserGroup(groupName));
    }

    @Test
    public void testListParserGroupGetOne() {
        PowerMockito.mockStatic(MethodParserGroup.class);
        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        MethodParser methodParser = PowerMockito.mock(MethodParser.class);
        String libDir = "/libdir";
        List<String> libDirList = new ArrayList<>();
        libDirList.add(libDir);
        String sourceDir = "/sourcedir";
        PowerMockito.when(methodParser.getAdditionalJars()).thenReturn(libDirList);
        PowerMockito.when(methodParser.getSourceDir()).thenReturn(sourceDir);

        String groupName = "group_a";
        Map<String, MethodParser> parserMap = new HashMap<>();
        parserMap.put(groupName, methodParser);
        Whitebox.setInternalState(methodParserGroup, "parserMap", parserMap);
        PowerMockito.doCallRealMethod().when(methodParserGroup).listParserGroup(anyString());
        Map expectedRetInner = new HashMap() {{
            put("lib_dir", new ArrayList<String>() {{
                add(libDir);
            }});
            put("source_dir", sourceDir);
        }};
        Map expectedRet = new HashMap() {{
            put(groupName, expectedRetInner);
        }};
        Assert.assertEquals(expectedRet, methodParserGroup.listParserGroup(groupName));
    }

    @Test
    public void testListParserGroupGetAll() {
        PowerMockito.mockStatic(MethodParserGroup.class);
        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);

        MethodParser methodParser1 = PowerMockito.mock(MethodParser.class);
        String libDir1 = "/libdira";
        List<String> libDirList1 = new ArrayList<>();
        libDirList1.add(libDir1);
        String sourceDir1 = "/sourcedira";
        PowerMockito.when(methodParser1.getAdditionalJars()).thenReturn(libDirList1);
        PowerMockito.when(methodParser1.getSourceDir()).thenReturn(sourceDir1);
        String groupName1 = "group_a";

        MethodParser methodParser2 = PowerMockito.mock(MethodParser.class);
        String libDir2 = "/libdirb";
        List<String> libDirList2 = new ArrayList<>();
        libDirList2.add(libDir2);
        String sourceDir2 = "/sourcedirb";
        PowerMockito.when(methodParser2.getAdditionalJars()).thenReturn(libDirList2);
        PowerMockito.when(methodParser2.getSourceDir()).thenReturn(sourceDir2);
        String groupName2 = "group_b";

        Map<String, MethodParser> parserMap = new HashMap<>();
        parserMap.put(groupName1, methodParser1);
        parserMap.put(groupName2, methodParser2);

        Whitebox.setInternalState(methodParserGroup, "parserMap", parserMap);
        PowerMockito.doCallRealMethod().when(methodParserGroup).listParserGroup(anyString());
        Map expectedRetInner1 = new HashMap() {{
            put("lib_dir", new ArrayList<String>() {{
                add(libDir1);
            }});
            put("source_dir", sourceDir1);
        }};
        Map expectedRetInner2 = new HashMap() {{
            put("lib_dir", new ArrayList<String>() {{
                add(libDir2);
            }});
            put("source_dir", sourceDir2);
        }};
        Map expectedRet = new HashMap() {{
            put(groupName1, expectedRetInner1);
            put(groupName2, expectedRetInner2);
        }};
        Assert.assertEquals(expectedRet, methodParserGroup.listParserGroup(""));
    }

    @Test
    public void testDeleteParserGroup() {
        Logger logger = PowerMockito.mock(Logger.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        Whitebox.setInternalState(MethodParserGroup.class, "logger", logger);

        MethodParser methodParser1 = PowerMockito.mock(MethodParser.class);
        String libDir1 = "/libdira";
        List<String> libDirList1 = new ArrayList<>();
        libDirList1.add(libDir1);
        String sourceDir1 = "/sourcedira";
        PowerMockito.when(methodParser1.getAdditionalJars()).thenReturn(libDirList1);
        PowerMockito.when(methodParser1.getSourceDir()).thenReturn(sourceDir1);
        String groupName1 = "group_a";

        MethodParser methodParser2 = PowerMockito.mock(MethodParser.class);
        String libDir2 = "/libdirb";
        List<String> libDirList2 = new ArrayList<>();
        libDirList2.add(libDir2);
        String sourceDir2 = "/sourcedirb";
        PowerMockito.when(methodParser2.getAdditionalJars()).thenReturn(libDirList2);
        PowerMockito.when(methodParser2.getSourceDir()).thenReturn(sourceDir2);
        String groupName2 = "group_b";

        Map<String, MethodParser> parserMap = new HashMap<>();
        parserMap.put(groupName1, methodParser1);
        parserMap.put(groupName2, methodParser2);

        Whitebox.setInternalState(methodParserGroup, "parserMap", parserMap);
        PowerMockito.doCallRealMethod().when(methodParserGroup).deleteParserGroup(anyString());
        methodParserGroup.deleteParserGroup(groupName1);
        Assert.assertTrue(
                ((Map<?, ?>) Whitebox.getInternalState(methodParserGroup, "parserMap"))
                        .containsKey(groupName2));
        Assert.assertFalse(
                ((Map<?, ?>) Whitebox.getInternalState(methodParserGroup, "parserMap"))
                        .containsKey(groupName1));
    }
}