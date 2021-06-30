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

package com.tencent.bk.base.dataflow.codecheck.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;

import com.gliwka.hyperscan.wrapper.CompileErrorException;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Scanner;
import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import com.tencent.bk.base.dataflow.codecheck.MethodParserResult;
import com.tencent.bk.base.dataflow.codecheck.util.BlackListGroup.DatabaseAndScanner;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
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
@PrepareForTest({Configuration.class, BlackListGroup.class, Logger.class, Database.class, Scanner.class,
        DatabaseAndScanner.class, List.class, AhoCorasickDoubleArrayTrie.class})
public class BlackListGroupTest {

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
    public void testGetSingleton() throws Exception {
        Configuration configuration = PowerMockito.mock(Configuration.class);
        PowerMockito.mockStatic(Configuration.class);
        PowerMockito.when(Configuration.getSingleton()).thenReturn(configuration);
        PowerMockito.when(configuration.getRegexMode()).thenReturn(Constants.DEAFAULT_REGEX_MODE);

        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.spy(BlackListGroup.class);
        PowerMockito.doNothing().when(BlackListGroup.class, "initRegexAC");

        Assert.assertNotNull(BlackListGroup.getSingleton());
    }

    @Test
    public void testInitBlackList_1() throws CompileErrorException {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        AhoCorasickDoubleArrayTrie<String> regexAcdat = PowerMockito.mock(AhoCorasickDoubleArrayTrie.class);
        PowerMockito.when(regexAcdat.matches(anyString())).thenReturn(true);
        Whitebox.setInternalState(blackListGroup, "regexMode", "java_pattern");
        Whitebox.setInternalState(blackListGroup, "regexJavaPatternBlackListMap",
                new HashMap<String, Object>());

        Logger logger = PowerMockito.mock(Logger.class);
        Whitebox.setInternalState(BlackListGroup.class, "logger", logger);
        Whitebox.setInternalState(BlackListGroup.class, "regexAcdat", regexAcdat);

        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        PowerMockito.doCallRealMethod().when(blackListGroup).initBlackList(anyString(), anyList());
        BlackListGroup.getSingleton().initBlackList("groupname", new ArrayList<String>() {{
            add("a.b");
        }});
        Asserts.internalAssertNotNull(BlackListGroup.class, "regexJavaPatternBlackListMap");
    }

    @Test
    public void testInitBlackList_2() throws CompileErrorException {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        AhoCorasickDoubleArrayTrie<String> regexAcdat = PowerMockito.mock(AhoCorasickDoubleArrayTrie.class);
        PowerMockito.when(regexAcdat.matches(anyString())).thenReturn(true);
        Whitebox.setInternalState(blackListGroup, "regexMode", "hyper_scan");
        Whitebox.setInternalState(blackListGroup, "regexHyperScanBlackListMap",
                new HashMap<String, Object>());

        Logger logger = PowerMockito.mock(Logger.class);
        Whitebox.setInternalState(BlackListGroup.class, "logger", logger);
        Whitebox.setInternalState(BlackListGroup.class, "regexAcdat", regexAcdat);

        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        PowerMockito.doCallRealMethod().when(blackListGroup).initBlackList(anyString(), anyList());
        BlackListGroup.getSingleton().initBlackList("groupname", new ArrayList<String>() {{
            add("a.b");
        }});
        Asserts.internalAssertNotNull(BlackListGroup.class, "regexHyperScanBlackListMap");
    }

    @Test
    public void testInitBlackList_3() throws CompileErrorException {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        AhoCorasickDoubleArrayTrie<String> regexAcdat = PowerMockito.mock(AhoCorasickDoubleArrayTrie.class);
        PowerMockito.when(regexAcdat.matches(anyString())).thenReturn(false);
        Whitebox.setInternalState(blackListGroup, "regexMode", "hyper_scan");
        Whitebox.setInternalState(blackListGroup, "literalBlackListMap",
                new HashMap<String, Object>());

        Logger logger = PowerMockito.mock(Logger.class);
        Whitebox.setInternalState(BlackListGroup.class, "logger", logger);
        Whitebox.setInternalState(BlackListGroup.class, "regexAcdat", regexAcdat);

        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        PowerMockito.doCallRealMethod().when(blackListGroup).initBlackList(anyString(), anyList());
        BlackListGroup.getSingleton().initBlackList("groupname", new ArrayList<String>() {{
            add("a.b");
        }});
        Asserts.internalAssertNotNull(BlackListGroup.class, "literalBlackListMap");
    }

    @Test
    public void testGetLiteralBlackList() {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);
        AhoCorasickDoubleArrayTrie<String> ac = new AhoCorasickDoubleArrayTrie<String>();

        Whitebox.setInternalState(blackListGroup, "literalBlackListMap",
                new HashMap<String, Object>() {{
                    put("a", ac);
                }});
        PowerMockito.doCallRealMethod().when(blackListGroup).getLiteralBlackList(anyString());
        AhoCorasickDoubleArrayTrie<String> ret = blackListGroup.getLiteralBlackList("a");
        Assert.assertEquals(ac, ret);
    }

    @Test
    public void testGetHyperScanBlackList() {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        Database database = PowerMockito.mock(Database.class);
        Scanner scanner = PowerMockito.mock(Scanner.class);
        DatabaseAndScanner da = new DatabaseAndScanner(database, scanner);

        Whitebox.setInternalState(blackListGroup, "regexHyperScanBlackListMap",
                new HashMap<String, Object>() {{
                    put("a", da);
                }});
        PowerMockito.doCallRealMethod().when(blackListGroup).getHyperScanBlackList(anyString());
        DatabaseAndScanner ret = blackListGroup.getHyperScanBlackList("a");
        Assert.assertEquals(da, ret);
    }

    @Test
    public void testGetJavaPatternBlackList() {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        List<Pattern> patterns = new ArrayList<>();

        Whitebox.setInternalState(blackListGroup, "regexJavaPatternBlackListMap",
                new HashMap<String, Object>() {{
                    put("a", patterns);
                }});
        PowerMockito.doCallRealMethod().when(blackListGroup).getJavaPatternBlackList(anyString());
        List<Pattern> ret = blackListGroup.getJavaPatternBlackList("a");
        Assert.assertEquals(patterns, ret);
    }

    @Test
    public void testFilterJavaMethodInBlackList_1() throws Exception {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        String[] literalPatterns = {"System.out.println", "java.util.list.add"};
        Map literalPatternMap = new HashMap<>();
        for (String onePattern : literalPatterns) {
            literalPatternMap.put(onePattern, onePattern);
        }
        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<>();
        acdat.build(literalPatternMap);
        String methodName = "System.out.println";
        PowerMockito.when(blackListGroup, "getLiteralBlackList", anyString()).thenReturn(acdat);

        List<Pattern> javaPatterns = new ArrayList<>();
        String[] javaPatternStrs = {"System.out.*", "java.util.list.*"};
        for (String s : javaPatternStrs) {
            Pattern pattern = Pattern.compile(s);
            javaPatterns.add(pattern);
        }
        PowerMockito.when(blackListGroup, "getJavaPatternBlackList", anyString())
                .thenReturn(javaPatterns);

        List<MethodParserResult> methodParserResult = new ArrayList<>();
        methodParserResult.add(new MethodParserResult("Test1.java",
                methodName, 12));

        PowerMockito.doCallRealMethod().when(blackListGroup, "isMethodInLiteralBlackList",
                any(), anyString());
        PowerMockito.doCallRealMethod().when(blackListGroup, "isMethodInJavaPatternBlackList",
                any(), anyString());
        Whitebox.setInternalState(blackListGroup, "regexMode", "java_pattern");
        PowerMockito.doCallRealMethod().when(blackListGroup, "filterJavaMethodInBlackList",
                anyString(), any());
        List<MethodParserResult> ret = BlackListGroup.getSingleton()
                .filterJavaMethodInBlackList("groupName", methodParserResult);
        Assert.assertEquals(methodParserResult, ret);
    }

    @Test
    public void testFilterJavaMethodInBlackList_2() throws Exception {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        String[] literalPatterns = {"System.out.println", "java.util.list.add"};
        Map literalPatternMap = new HashMap<>();
        for (String onePattern : literalPatterns) {
            literalPatternMap.put(onePattern, onePattern);
        }
        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<>();
        acdat.build(literalPatternMap);
        String methodName = "System.out.println";
        PowerMockito.when(blackListGroup, "getLiteralBlackList", anyString()).thenReturn(acdat);

        DatabaseAndScanner das = PowerMockito.mock(DatabaseAndScanner.class);
        PowerMockito.when(blackListGroup, "getHyperScanBlackList", anyString()).thenReturn(das);
        PowerMockito.when(blackListGroup, "isMethodInHyperScanBlackList", any(), anyString())
                .thenReturn(methodName);

        List<MethodParserResult> methodParserResult = new ArrayList<>();
        methodParserResult.add(new MethodParserResult("Test1.java",
                methodName, 12));

        PowerMockito.doCallRealMethod().when(blackListGroup, "isMethodInLiteralBlackList",
                any(), anyString());
        Whitebox.setInternalState(blackListGroup, "regexMode", "hyper_scan");
        PowerMockito.doCallRealMethod().when(blackListGroup, "filterJavaMethodInBlackList",
                anyString(), any());
        List<MethodParserResult> ret = BlackListGroup.getSingleton()
                .filterJavaMethodInBlackList("groupName", methodParserResult);
        Assert.assertEquals(methodParserResult, ret);
    }

    @Test
    public void testFilterPythonImportInBlackList_1() throws Exception {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        String[] literalPatterns = {"System.out.println", "java.util.list.add"};
        Map literalPatternMap = new HashMap<>();
        for (String onePattern : literalPatterns) {
            literalPatternMap.put(onePattern, onePattern);
        }
        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<>();
        acdat.build(literalPatternMap);
        String methodName = "System.out.println";
        PowerMockito.when(blackListGroup, "getLiteralBlackList", anyString()).thenReturn(acdat);

        List<Pattern> javaPatterns = new ArrayList<>();
        String[] javaPatternStrs = {"System.out.*", "java.util.list.*"};
        for (String s : javaPatternStrs) {
            Pattern pattern = Pattern.compile(s);
            javaPatterns.add(pattern);
        }
        PowerMockito.when(blackListGroup, "getJavaPatternBlackList", anyString())
                .thenReturn(javaPatterns);

        List<String> methodParserResult = new ArrayList<>();
        methodParserResult.add(methodName);

        PowerMockito.doCallRealMethod().when(blackListGroup, "isMethodInLiteralBlackList",
                any(), anyString());
        PowerMockito.doCallRealMethod().when(blackListGroup, "isMethodInJavaPatternBlackList",
                any(), anyString());
        Whitebox.setInternalState(blackListGroup, "regexMode", "java_pattern");
        PowerMockito.doCallRealMethod().when(blackListGroup, "filterPythonImportInBlackList",
                anyString(), any());
        List<String> ret = BlackListGroup.getSingleton()
                .filterPythonImportInBlackList("groupName", methodParserResult);
        Assert.assertEquals(methodParserResult, ret);
    }

    @Test
    public void testFilterPythonImportInBlackList_2() throws Exception {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        String[] literalPatterns = {"System.out.println", "java.util.list.add"};
        Map literalPatternMap = new HashMap<>();
        for (String onePattern : literalPatterns) {
            literalPatternMap.put(onePattern, onePattern);
        }
        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<>();
        acdat.build(literalPatternMap);
        String methodName = "System.out.println";
        PowerMockito.when(blackListGroup, "getLiteralBlackList", anyString()).thenReturn(acdat);

        DatabaseAndScanner das = PowerMockito.mock(DatabaseAndScanner.class);
        PowerMockito.when(blackListGroup, "getHyperScanBlackList", anyString()).thenReturn(das);
        PowerMockito.when(blackListGroup, "isMethodInHyperScanBlackList", any(), anyString())
                .thenReturn(methodName);

        List<String> pythonImportLists = new ArrayList<>();
        pythonImportLists.add(methodName);

        PowerMockito.doCallRealMethod().when(blackListGroup, "isMethodInLiteralBlackList",
                any(), anyString());
        Whitebox.setInternalState(blackListGroup, "regexMode", "hyper_scan");
        PowerMockito.doCallRealMethod().when(blackListGroup, "filterPythonImportInBlackList",
                anyString(), any());
        List<String> ret = BlackListGroup.getSingleton()
                .filterPythonImportInBlackList("groupName", pythonImportLists);
        Assert.assertEquals(pythonImportLists, ret);
    }

    @Test
    public void testAddBlackListGroup() throws Exception {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        String[] literalPatterns = {"System.out.println", "java.util.list.add"};
        Map literalPatternMap = new HashMap<>();
        for (String onePattern : literalPatterns) {
            literalPatternMap.put(onePattern, onePattern);
        }
        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<>();
        acdat.build(literalPatternMap);
        String methodName = "System.out.println";
        PowerMockito.when(blackListGroup, "getLiteralBlackList", anyString()).thenReturn(acdat);

        DatabaseAndScanner das = PowerMockito.mock(DatabaseAndScanner.class);
        PowerMockito.when(blackListGroup, "getHyperScanBlackList", anyString()).thenReturn(das);
        PowerMockito.when(blackListGroup, "isMethodInHyperScanBlackList", any(), anyString())
                .thenReturn(methodName);

        List<String> pythonImportLists = new ArrayList<>();
        pythonImportLists.add(methodName);

        PowerMockito.doCallRealMethod().when(blackListGroup, "isMethodInLiteralBlackList",
                any(), anyString());
        Whitebox.setInternalState(blackListGroup, "regexMode", "java_pattern");
        PowerMockito.doCallRealMethod().when(blackListGroup, "filterPythonImportInBlackList",
                anyString(), any());
        List<String> ret = BlackListGroup.getSingleton()
                .filterPythonImportInBlackList("groupName", pythonImportLists);
        Assert.assertEquals(pythonImportLists, ret);
    }

    @Test
    public void deleteBlackListGroup() throws Exception {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        String groupName = "a";
        Map<String, AhoCorasickDoubleArrayTrie<String>> literalBlackListMap = new HashMap<>();
        AhoCorasickDoubleArrayTrie<String> ac = PowerMockito.mock(AhoCorasickDoubleArrayTrie.class);
        literalBlackListMap.put(groupName, ac);
        Whitebox.setInternalState(blackListGroup, "literalBlackListMap", literalBlackListMap);

        Map<String, DatabaseAndScanner> regexHyperScanBlackListMap = new HashMap<>();
        DatabaseAndScanner das = PowerMockito.mock(DatabaseAndScanner.class);
        PowerMockito.doNothing().when(das).close();
        regexHyperScanBlackListMap.put(groupName, das);
        Whitebox.setInternalState(blackListGroup, "regexHyperScanBlackListMap",
                regexHyperScanBlackListMap);

        Map<String, List<Pattern>> regexJavaPatternBlackListMap = new HashMap<>();
        List<Pattern> regexJavaPatterns = PowerMockito.mock(List.class);
        PowerMockito.doNothing().when(regexJavaPatterns).clear();
        regexJavaPatternBlackListMap.put(groupName, regexJavaPatterns);
        Whitebox.setInternalState(blackListGroup, "regexJavaPatternBlackListMap",
                regexJavaPatternBlackListMap);

        Map<String, List<String>> originBlackListMap = new HashMap<>();
        List<String> originBlackList = PowerMockito.mock(List.class);
        PowerMockito.doNothing().when(originBlackList).clear();
        originBlackListMap.put(groupName, originBlackList);
        Whitebox.setInternalState(blackListGroup, "originBlackListMap", originBlackListMap);

        PowerMockito.doCallRealMethod().when(blackListGroup, "deleteBlackListGroup", anyString());
        BlackListGroup.getSingleton()
                .deleteBlackListGroup(groupName);
        Assert.assertFalse(
                ((Map) Whitebox.getInternalState(blackListGroup, "literalBlackListMap"))
                        .containsKey(groupName));
        Assert.assertFalse(
                ((Map) Whitebox.getInternalState(blackListGroup, "regexHyperScanBlackListMap"))
                        .containsKey(groupName));
        Assert.assertFalse(
                ((Map) Whitebox.getInternalState(blackListGroup, "regexJavaPatternBlackListMap"))
                        .containsKey(groupName));
        Assert.assertFalse(
                ((Map) Whitebox.getInternalState(blackListGroup, "originBlackListMap"))
                        .containsKey(groupName));

    }

    @Test
    public void testListBlackListGroup() throws Exception {
        BlackListGroup blackListGroup = PowerMockito.mock(BlackListGroup.class);
        PowerMockito.mockStatic(BlackListGroup.class);
        PowerMockito.when(BlackListGroup.getSingleton()).thenReturn(blackListGroup);

        Map<String, List<String>> originBlackListMap = new HashMap<>();
        String groupName = "a";
        List<String> originBlackList = PowerMockito.mock(List.class);
        originBlackListMap.put(groupName, originBlackList);

        String groupName2 = "b";
        originBlackListMap.put(groupName2, originBlackList);

        Whitebox.setInternalState(blackListGroup, "originBlackListMap", originBlackListMap);

        PowerMockito.doCallRealMethod().when(blackListGroup, "listBlackListGroup", any());
        Map ret = blackListGroup.listBlackListGroup(groupName);
        Assert.assertTrue(ret.containsKey(groupName));
        Assert.assertEquals(1, ret.keySet().size());

        Map ret2 = blackListGroup.listBlackListGroup(null);
        Assert.assertTrue(ret2.containsKey(groupName));
        Assert.assertTrue(ret2.containsKey(groupName2));
        Assert.assertEquals(2, ret2.keySet().size());
    }
}