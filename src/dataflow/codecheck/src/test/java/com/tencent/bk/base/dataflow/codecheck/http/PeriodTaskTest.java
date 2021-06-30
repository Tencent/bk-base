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

package com.tencent.bk.base.dataflow.codecheck.http;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.verifyPrivate;

import com.tencent.bk.base.dataflow.codecheck.MethodParserGroup;
import com.tencent.bk.base.dataflow.codecheck.util.BlackListGroup;
import com.tencent.bk.base.dataflow.codecheck.util.db.CodeCheckDBUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import org.slf4j.Logger;

@RunWith(PowerMockRunner.class)
@PrepareForTest({PeriodTask.class, Logger.class, CodeCheckDBUtil.class, MethodParserGroup.class,
        BlackListGroup.class})
public class PeriodTaskTest {

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
    public void testRun() throws Exception {
        Logger logger = PowerMockito.mock(Logger.class);
        PeriodTask periodTask = PowerMockito.mock(PeriodTask.class);
        Whitebox.setInternalState(PeriodTask.class, "logger", logger);
        PowerMockito.doCallRealMethod().when(periodTask).run();
        PowerMockito.doCallRealMethod().when(periodTask, "loadParserGroup");
        PowerMockito.doCallRealMethod().when(periodTask, "loadBlacklistGroup");

        List<Map> parserGroupFromDB = new ArrayList();
        parserGroupFromDB.add(new HashMap() {{
            put("parser_group_name", "a");
            put("src_dir", "/a");
            put("lib_dir", "/a");
        }});
        parserGroupFromDB.add(new HashMap() {{
            put("parser_group_name", "b");
            put("src_dir", "/b");
            put("lib_dir", "/b");
        }});
        PowerMockito.mockStatic(CodeCheckDBUtil.class);
        PowerMockito.when(CodeCheckDBUtil.listParserGroup()).thenReturn(parserGroupFromDB);

        Map<String, Map> parserGroupMap = new HashMap<>();
        parserGroupMap.put("a", new HashMap<String, String>() {{
            put("src_dir", "/a");
            put("lib_dir", "/a");
        }});
        parserGroupMap.put("c", new HashMap<String, String>() {{
            put("src_dir", "/c");
            put("lib_dir", "/c");
        }});
        MethodParserGroup methodParserGroup = PowerMockito.mock(MethodParserGroup.class);
        PowerMockito.mockStatic(MethodParserGroup.class);
        PowerMockito.when(MethodParserGroup.getSingleton()).thenReturn(methodParserGroup);
        PowerMockito.doReturn(parserGroupMap).when(methodParserGroup).listParserGroup(null);

        PowerMockito.doNothing().when(methodParserGroup).addParserGroup(anyString(), anyString(), anyString());
        PowerMockito.doNothing().when(methodParserGroup).deleteParserGroup(anyString());

        List<Map> blacklistGroupFromDB = new ArrayList();
        blacklistGroupFromDB.add(new HashMap() {{
            put("blacklist_group_name", "a");
            put("blacklist", "a.b;c.d.e");
        }});
        blacklistGroupFromDB.add(new HashMap() {{
            put("parser_group_name", "b");
            put("blacklist", "b.a;b.d.e");
        }});
        PowerMockito.when(CodeCheckDBUtil.listBlacklistGroup()).thenReturn(blacklistGroupFromDB);

        Map<String, Map> blacklistGroupMap = new HashMap<>();
        blacklistGroupMap.put("a", new HashMap<String, String>() {{
            put("blacklist", "a.b;y.d.e");
        }});
        blacklistGroupMap.put("c", new HashMap<String, String>() {{
            put("blacklist", "a.b;x.d.e");
        }});
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        PowerMockito.doReturn(blacklistGroupMap).when(blackListGroup).listBlackListGroup(null);

        PowerMockito.doNothing().when(blackListGroup).addBlackListGroup(anyString(), anyString());
        PowerMockito.doNothing().when(blackListGroup).deleteBlackListGroup(anyString());

        periodTask.run();
        verify(periodTask, times(3)).run();
        verifyPrivate(periodTask).invoke("loadParserGroup");
        verifyPrivate(periodTask).invoke("loadBlacklistGroup");
    }
}