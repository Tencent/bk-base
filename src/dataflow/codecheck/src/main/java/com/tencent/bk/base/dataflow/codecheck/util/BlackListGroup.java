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

import static com.tencent.bk.base.dataflow.codecheck.util.Constants.DEAFAULT_REGEX_MODE;
import static com.tencent.bk.base.dataflow.codecheck.util.Constants.HYPER_SCAN_MODE;
import static com.tencent.bk.base.dataflow.codecheck.util.Constants.JAVA_PATTERN_MODE;

import com.gliwka.hyperscan.wrapper.CompileErrorException;
import com.gliwka.hyperscan.wrapper.Database;
import com.gliwka.hyperscan.wrapper.Expression;
import com.gliwka.hyperscan.wrapper.ExpressionFlag;
import com.gliwka.hyperscan.wrapper.Match;
import com.gliwka.hyperscan.wrapper.Scanner;
import com.google.common.base.Splitter;
import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import com.tencent.bk.base.dataflow.codecheck.MethodParserResult;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlackListGroup {

    // https://docs.oracle.com/javase/tutorial/essential/regex/literals.html
    // just remove '.'
    public static final char[] ESCAPE_CHARS = {
            '<', '(', '[', '{', '\\', '^', '-', '=', '$', '!',
            '|', ']', '}', ')', '?', '*', '+', '>'};
    public static Logger logger = LoggerFactory.getLogger(BlackListGroup.class);
    private static volatile BlackListGroup singleton;
    // for common use
    private static AhoCorasickDoubleArrayTrie<String> regexAcdat;
    // using for literal blacklist (not contains any regex charater)
    // AhoCorasick algo to check if a method in blacklist, O(n),
    // where n is the length of the object to be checked
    // <flink_1_7_2, AhoCorasickDoubleArrayTrie>...
    // <spark_2_4_5, AhoCorasickDoubleArrayTrie>...
    private final Map<String, AhoCorasickDoubleArrayTrie<String>> literalBlackListMap;
    // regex using hyperscan
    // https://github.com/gliwka/hyperscan-java
    // https://intel.github.io/hyperscan/dev-reference/index.html
    private final Map<String, DatabaseAndScanner> regexHyperScanBlackListMap;
    // regex using java pattern
    private final Map<String, List<Pattern>> regexJavaPatternBlackListMap;
    private final Map<String, List<String>> originBlackListMap;
    private String regexMode = DEAFAULT_REGEX_MODE;

    private BlackListGroup(String regexMode) {
        this.literalBlackListMap = new HashMap<>();
        this.regexHyperScanBlackListMap = new HashMap<>();
        this.regexJavaPatternBlackListMap = new HashMap<>();
        this.originBlackListMap = new HashMap<>();
        this.regexMode = regexMode;
        initRegexAC();
    }

    /**
     * getSingleton
     *
     * @return com.tencent.bkdata.codecheck.util.BlackListGroup
     */
    public static BlackListGroup getSingleton() {
        if (singleton == null) {
            synchronized (BlackListGroup.class) {
                if (singleton == null) {
                    singleton = new BlackListGroup(Configuration.getSingleton().getRegexMode());
                }
            }
        }
        return singleton;
    }

    private static void initRegexAC() {
        TreeMap<String, String> regexCharMap = new TreeMap<>();

        for (Character key : ESCAPE_CHARS) {
            regexCharMap.put(key.toString(), key.toString());
        }
        // Build an AhoCorasickDoubleArrayTrie
        regexAcdat = new AhoCorasickDoubleArrayTrie<>();
        regexAcdat.build(regexCharMap);
    }

    /**
     * initBlackList
     *
     * @param groupName
     * @param blackList
     */
    public void initBlackList(String groupName, List<String> blackList)
            throws PatternSyntaxException, CompileErrorException {
        List<String> literalBlackList = new ArrayList<>();
        List<String> regexBlackList = new ArrayList<>();
        for (String oneItem : blackList) {
            if (regexAcdat.matches(oneItem)) {
                // this is a regex
                regexBlackList.add(oneItem);
            } else {
                literalBlackList.add(oneItem);
            }
        }

        List<Pattern> patterns = new ArrayList<>();
        DatabaseAndScanner databaseAndScanner = null;

        if (regexMode.equalsIgnoreCase(JAVA_PATTERN_MODE) && !regexBlackList.isEmpty()) {
            String oneRegex = null;
            try {
                for (String s : regexBlackList) {
                    oneRegex = s;
                    Pattern pattern = Pattern.compile(oneRegex);
                    patterns.add(pattern);
                }
                regexJavaPatternBlackListMap.put(groupName, patterns);
            } catch (PatternSyntaxException ex) {
                logger.warn("oneRegex {} parse throws PatternSyntaxException", oneRegex);
                throw ex;
            }
        } else if (regexMode.equalsIgnoreCase(HYPER_SCAN_MODE) && !regexBlackList.isEmpty()) {
            // process regexs
            Database db = null;
            Scanner scanner = new Scanner();
            LinkedList<Expression> expressions = new LinkedList<>();
            for (String oneExpr : regexBlackList) {
                expressions.add(new Expression(oneExpr, EnumSet.of(ExpressionFlag.SINGLEMATCH)));
            }
            try {
                db = Database.compile(expressions);
                scanner.allocScratch(db);
                databaseAndScanner = new DatabaseAndScanner(db, scanner);
                if (regexHyperScanBlackListMap.containsKey(groupName)) {
                    DatabaseAndScanner oldDatabaseAndScanner = regexHyperScanBlackListMap.get(groupName);
                    oldDatabaseAndScanner.close();
                }
                regexHyperScanBlackListMap.put(groupName, databaseAndScanner);
            } catch (CompileErrorException ce) {
                Expression failedExpression = ce.getFailedExpression();
                logger.warn("Expression {} parse throws CompileErrorException",
                        failedExpression.getExpression());
                throw ce;
            }
        }

        // Collect literal blacklist data
        if (literalBlackList.size() > 0) {
            TreeMap<String, String> map = new TreeMap<>();
            String[] keyArray = literalBlackList.toArray(new String[literalBlackList.size()]);
            for (String oneKey : keyArray) {
                map.put(oneKey, oneKey);
            }
            // Build an AhoCorasickDoubleArrayTrie
            AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<>();
            acdat.build(map);
            // when all is ready, put it into map
            // close old resource if necessary
            literalBlackListMap.put(groupName, acdat);
        }

        logger.info("init blacklist for group {}, total count {}, literal {}, java regex {}",
                groupName, blackList.size(), literalBlackList, regexBlackList);
    }

    /**
     * getLiteralBlackList
     *
     * @param groupName
     * @return com.hankcs.algorithm.AhoCorasickDoubleArrayTrie[java.lang.String]
     */
    public AhoCorasickDoubleArrayTrie<String> getLiteralBlackList(String groupName) {
        return literalBlackListMap.get(groupName);
    }

    /**
     * getHyperScanBlackList
     *
     * @param groupName
     * @return com.tencent.bkdata.codecheck.util.BlackListGroup.DatabaseAndScanner
     */
    public DatabaseAndScanner getHyperScanBlackList(String groupName) {
        return regexHyperScanBlackListMap.get(groupName);
    }

    /**
     * getJavaPatternBlackList
     *
     * @param groupName
     * @return java.util.List-java.util.regex.Pattern
     */
    public List<Pattern> getJavaPatternBlackList(String groupName) {
        return regexJavaPatternBlackListMap.get(groupName);
    }

    /**
     * isMethodInLiteralBlackList
     *
     * @param acdat
     * @param methodName
     * @return java.lang.String
     */
    private String isMethodInLiteralBlackList(AhoCorasickDoubleArrayTrie<String> acdat, String methodName) {
        return acdat.get(methodName);
    }

    /**
     * isMethodInHyperScanBlackList
     *
     * @param databaseAndScanner
     * @param methodName
     * @return java.lang.String
     */
    private String isMethodInHyperScanBlackList(DatabaseAndScanner databaseAndScanner, String methodName) {
        Database database = databaseAndScanner.getDatabase();
        Scanner scanner = databaseAndScanner.getScanner();
        List<Match> result = scanner.scan(database, methodName);
        for (Match oneMatch : result) {
            return oneMatch.getMatchedExpression().getExpression();
        }
        return null;
    }

    /**
     * isMethodInJavaPatternBlackList
     *
     * @param patterns
     * @param methodName
     * @return java.lang.String
     */
    private String isMethodInJavaPatternBlackList(List<Pattern> patterns, String methodName) {
        for (Pattern pattern : patterns) {
            if (pattern.matcher(methodName).matches()) {
                return pattern.pattern();
            }
        }
        return null;
    }

    /**
     * filterJavaMethodInBlackList
     *
     * @param groupName
     * @param methodParserResult
     * @return java.util.List-com.tencent.bkdata.codecheck.MethodParserResult
     */
    public List<MethodParserResult> filterJavaMethodInBlackList(String groupName,
            List<MethodParserResult> methodParserResult) {
        // search in literal blacklist
        List<MethodParserResult> ret = new ArrayList<>();
        AhoCorasickDoubleArrayTrie<String> acdat = getLiteralBlackList(groupName);
        if (acdat != null) {
            for (MethodParserResult result : methodParserResult) {
                String validStr = isMethodInLiteralBlackList(acdat, result.getMethodName());
                if (validStr != null) {
                    // 命中黑名单
                    result.setMessage(validStr);
                    ret.add(result);
                }
            }
        }

        // search in regex blacklist
        if (regexMode.equalsIgnoreCase(JAVA_PATTERN_MODE)) {
            List<Pattern> patterns = getJavaPatternBlackList(groupName);
            if (patterns != null) {
                for (MethodParserResult result : methodParserResult) {
                    String validStr = isMethodInJavaPatternBlackList(patterns, result.getMethodName());
                    if (validStr != null) {
                        // 命中黑名单
                        result.setMessage(validStr);
                        ret.add(result);
                    }
                }
            }
        } else if (regexMode.equalsIgnoreCase(HYPER_SCAN_MODE)) {
            DatabaseAndScanner databaseAndScanner = getHyperScanBlackList(groupName);
            if (databaseAndScanner != null) {
                for (MethodParserResult result : methodParserResult) {
                    String validStr = isMethodInHyperScanBlackList(databaseAndScanner, result.getMethodName());
                    if (validStr != null) {
                        // 命中黑名单
                        result.setMessage(validStr);
                        ret.add(result);
                    }
                }
            }
        }

        return new ArrayList<>(new LinkedHashSet<>(ret));
    }

    /**
     * filterPythonImportInBlackList
     *
     * @param groupName
     * @param pythonImports
     * @return java.util.List-java.lang.String
     */
    public List<String> filterPythonImportInBlackList(String groupName, List<String> pythonImports) {
        // search in literal blacklist
        List<String> ret = new ArrayList<>();
        AhoCorasickDoubleArrayTrie<String> acdat = getLiteralBlackList(groupName);
        if (acdat != null) {
            for (String oneImport : pythonImports) {
                String validStr = isMethodInLiteralBlackList(acdat, oneImport);
                if (validStr != null) {
                    // 命中黑名单
                    ret.add(oneImport);
                }
            }
        }

        // search in regex blacklist
        if (regexMode.equalsIgnoreCase(JAVA_PATTERN_MODE)) {
            List<Pattern> patterns = getJavaPatternBlackList(groupName);
            if (patterns != null) {
                for (String oneImport : pythonImports) {
                    String validStr = isMethodInJavaPatternBlackList(patterns, oneImport);
                    if (validStr != null) {
                        // 命中黑名单
                        ret.add(oneImport);
                    }
                }
            }
        } else if (regexMode.equalsIgnoreCase(HYPER_SCAN_MODE)) {
            DatabaseAndScanner databaseAndScanner = getHyperScanBlackList(groupName);
            if (databaseAndScanner != null) {
                for (String oneImport : pythonImports) {
                    String validStr = isMethodInHyperScanBlackList(databaseAndScanner, oneImport);
                    if (validStr != null) {
                        // 命中黑名单
                        ret.add(oneImport);
                    }
                }
            }
        }
        return new ArrayList<>(new LinkedHashSet<>(ret));
    }

    private void addBlackListGroup(String groupName, List<String> blackList)
            throws PatternSyntaxException, CompileErrorException {
        initBlackList(groupName, blackList);
    }

    /**
     * addBlackListGroup
     *
     * @param groupName
     * @param blackListStr
     */
    public void addBlackListGroup(String groupName, String blackListStr)
            throws PatternSyntaxException, CompileErrorException {
        List<String> blackList = Splitter.on(Constants.DELIMITER).trimResults().splitToList(blackListStr);
        originBlackListMap.put(groupName, blackList);
        addBlackListGroup(groupName, blackList);
    }

    /**
     * deleteBlackListGroup
     *
     * @param groupName
     */
    public void deleteBlackListGroup(String groupName) {
        literalBlackListMap.remove(groupName);
        if (regexHyperScanBlackListMap.containsKey(groupName)) {
            DatabaseAndScanner oldDatabaseAndScanner = regexHyperScanBlackListMap.remove(groupName);
            oldDatabaseAndScanner.close();
        }
        if (regexJavaPatternBlackListMap.containsKey(groupName)) {
            List<Pattern> regexJavaPatternBlackList = regexJavaPatternBlackListMap.remove(groupName);
            regexJavaPatternBlackList.clear();
        }
        if (originBlackListMap.containsKey(groupName)) {
            List<String> originBlackList = originBlackListMap.remove(groupName);
            originBlackList.clear();
        }
    }

    /**
     * listBlackListGroup
     *
     * @param groupName
     * @return java.util.Map
     */
    public Map<String, List<String>> listBlackListGroup(String groupName) {
        if (groupName == null || groupName.isEmpty()) {
            // list all blacklist
            return originBlackListMap;
        } else {
            Map<String, List<String>> ret = new HashMap<>();
            if (originBlackListMap.containsKey(groupName)) {
                ret.put(groupName, originBlackListMap.get(groupName));
            }
            return ret;
        }
    }

    /**
     * DatabaseAndScanner
     */
    public static class DatabaseAndScanner {

        private Database database;
        private Scanner scanner;

        public DatabaseAndScanner(Database database, Scanner scanner) {
            this.database = database;
            this.scanner = scanner;
        }

        public Database getDatabase() {
            return database;
        }

        public void setDatabase(Database database) {
            this.database = database;
        }

        public Scanner getScanner() {
            return scanner;
        }

        public void setScanner(Scanner scanner) {
            this.scanner = scanner;
        }

        public void close() {
            try {
                scanner.close();
                database.close();
            } catch (Exception ex) {
                // do nothing
            }
        }
    }

}
