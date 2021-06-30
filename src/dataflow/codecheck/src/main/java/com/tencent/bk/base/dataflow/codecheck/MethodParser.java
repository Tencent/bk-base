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

import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseProblemException;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.PackageDeclaration;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.resolution.declarations.ResolvedMethodDeclaration;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.javaparsermodel.JavaParserFacade;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.JavaParserTypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.ReflectionTypeSolver;
import com.tencent.bk.base.dataflow.codecheck.typesolver.MyJarTypeSolver;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MethodParser {

    public static Logger logger = LoggerFactory.getLogger(MethodParser.class);

    // 额外的jar包列表
    private List<String> additionalJars;

    // 代码的root目录，一般为相互引用的java源码文件列表的父目录
    // 比如"/xxx/codecheck/src/main/java/com/tencent/bkdata/codecheck/javaparser/"
    // 一般设置为null
    private String sourceDir;

    // for javaparser
    private ParserConfiguration parserConfiguration;
    private CombinedTypeSolver combinedTypeSolver;
    private JavaSymbolSolver symbolSolver;
    private MyJarTypeSolver myJarTypeSolver;

    private JavaParser innerJavaParser;

    /**
     * MethodParser
     *
     * @param additionalJars
     * @param sourceDir
     */
    public MethodParser(List<String> additionalJars, String sourceDir) {
        this.additionalJars = additionalJars;
        this.sourceDir = sourceDir;
    }

    /**
     * getParentInvoker
     *
     * @param methodCall
     * @return java.lang.String
     */
    private static String getParentInvoker(final MethodCallExpr methodCall) {
        StringBuilder path = new StringBuilder();

        methodCall.walk(Node.TreeTraversal.PARENTS, node -> {
            if (node instanceof ClassOrInterfaceDeclaration) {
                path.insert(0, ((ClassOrInterfaceDeclaration) node).getNameAsString());
                path.insert(0, '$');
            }
            if (node instanceof ObjectCreationExpr) {
                path.insert(0, ((ObjectCreationExpr) node).getType().getNameAsString());
                path.insert(0, '$');
            }
            if (node instanceof MethodDeclaration) {
                String methodIdentifier = ((MethodDeclaration) node).getDeclarationAsString(false, false, true);
                path.insert(0, methodIdentifier.substring(methodIdentifier.indexOf(" ") + 1));
                path.insert(0, '#');
            }
            if (node instanceof CompilationUnit) {
                final Optional<PackageDeclaration> pkg = ((CompilationUnit) node).getPackageDeclaration();
                if (pkg.isPresent()) {
                    path.replace(0, 1, ".");
                    path.insert(0, pkg.get().getNameAsString());
                }
            }
        });
        return path.toString();
    }

    private static <T extends Node> T handleResult(ParseResult<T> result) {
        if (result.isSuccessful() && result.getResult().isPresent()) {
            return result.getResult().get();
        }
        throw new ParseProblemException(result.getProblems());
    }

    /**
     * init
     */
    public void init() throws IOException {
        // init combinedTypeSolver
        combinedTypeSolver = new CombinedTypeSolver(
                CombinedTypeSolver.ExceptionHandlers.IGNORE_ALL,
                new ReflectionTypeSolver(false)
        );
        myJarTypeSolver = new MyJarTypeSolver();
        addAdditionalJars(additionalJars);
        addSourceRoot(sourceDir);

        combinedTypeSolver.setExceptionHandler(
                CombinedTypeSolver.ExceptionHandlers.IGNORE_ALL);
        // init symbolSolver
        symbolSolver = new JavaSymbolSolver(combinedTypeSolver);

        if (parserConfiguration == null) {
            parserConfiguration = new ParserConfiguration();
        }
//        parserConfiguration.setLanguageLevel(ParserConfiguration.LanguageLevel.JAVA_8);
        parserConfiguration.setAttributeComments(false);
        parserConfiguration.setSymbolResolver(symbolSolver);
        innerJavaParser = new JavaParser(parserConfiguration);
    }

    /**
     * clear
     */
    public synchronized void clear() {
        JavaParserFacade.clearInstances();
        symbolSolver = null;
        combinedTypeSolver = null;
        myJarTypeSolver = null;
        innerJavaParser = null;
    }

    /**
     * getAdditionalJars
     *
     * @return java.util.List-java.lang.String
     */
    public List<String> getAdditionalJars() {
        return additionalJars;
    }

    /**
     * getSourceDir
     *
     * @return java.lang.String
     */
    public String getSourceDir() {
        return sourceDir;
    }

    /**
     * addAdditionalJars
     *
     * @param additionalJars
     */
    private void addAdditionalJars(List<String> additionalJars) throws IOException {
        if (additionalJars != null && !additionalJars.isEmpty()) {
            // find all jars
            List<String> allJars = new ArrayList<>();
            for (String onePath : additionalJars) {
                if (Files.isRegularFile(Paths.get(onePath))) {
                    logger.info("onePath {} is regular file", onePath);
                    allJars.add(onePath);
                } else if (Files.isDirectory(Paths.get(onePath))) {
                    logger.info("onePath {} is dir", onePath);
                    String[] extensions = new String[]{"jar"};
                    Collection files = FileUtils.listFiles(
                            new File(onePath),
                            extensions,
                            true
                    );
                    for (Object oneFile : files) {
                        allJars.add(oneFile.toString());
                    }
                }
            }
            // add all jars into jartypesolver
            for (String oneJar : allJars) {
                myJarTypeSolver.addJar(oneJar);
            }
            combinedTypeSolver.add(myJarTypeSolver);
        }
    }

    /**
     * addSourceRoot
     *
     * @param oneSourceRoot
     */
    private void addSourceRoot(String oneSourceRoot) {
        if (sourceDir != null && !sourceDir.isEmpty()) {
            combinedTypeSolver.add(new JavaParserTypeSolver(Paths.get(oneSourceRoot), parserConfiguration, 64 * 1024));
        }
    }

    /**
     * getMethodCallInfo
     *
     * @param expr
     * @param inFile
     * @return com.tencent.bkdata.codecheck.MethodParserResult
     */
    private MethodParserResult getMethodCallInfo(MethodCallExpr expr, String inFile) {
        try {
            ResolvedMethodDeclaration me = expr.resolve();
            String parentNode = getParentInvoker(expr);
            if (me != null) {
                String qualifiedName = me.getQualifiedName();

                int lineNo = expr.getBegin().get().line;
                MethodParserResult result = new MethodParserResult(inFile, qualifiedName, lineNo, "", parentNode);
                return result;
            }
        } catch (Exception ex) {
            logger.warn("expr: {}, exception: {}", expr, ExceptionUtils.getStackTrace(ex));
        }
        return null;
    }

    /**
     * parseJavaCode
     *
     * @param code
     * @param isCode
     * @return java.util.List-com.tencent.bkdata.codecheck.MethodParserResult
     */
    public synchronized List<MethodParserResult> parseJavaCode(String code, boolean isCode)
            throws IOException, ParseProblemException {
        List<MethodParserResult> ret = new ArrayList<>();
        CompilationUnit cu = null;
        if (isCode) {
            // parse code
            cu = handleResult(innerJavaParser.parse(code));
        } else {
            // parse file
            cu = handleResult(innerJavaParser.parse(Paths.get(code)));
        }

        // get classname
        List<ClassOrInterfaceDeclaration> classExprs = cu.findAll(ClassOrInterfaceDeclaration.class);
        String className = "default";
        if (classExprs != null && classExprs.size() > 0) {
            className = classExprs.get(0).getFullyQualifiedName().get();
        }
        String sourceName;
        if (isCode) {
            sourceName = className;
        } else {
            sourceName = new File(code).getName();
        }
        // get methodcalls
        List<MethodCallExpr> methodCallExprs = cu.findAll(MethodCallExpr.class);
        for (MethodCallExpr mce : methodCallExprs) {
            // get all methods
            MethodParserResult methodParserResult = getMethodCallInfo(mce, sourceName);
            if (methodParserResult != null) {
                ret.add(methodParserResult);
            }
        }
        return ret;
    }
}
