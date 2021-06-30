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

package com.tencent.bk.base.dataflow.jobnavi.adaptor.flink.compiler;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import org.apache.log4j.Logger;

public class FileCompiler extends AbstractAbsCompiler {

    private static final Logger LOGGER = Logger.getLogger(FileCompiler.class);

    // 编译过程代码及文件所放位置
    private final String rootPath;
    // 编译过程加载的 classpath
    private final String classPath;

    public FileCompiler(String rootPath, String classPath) {
        this.rootPath = rootPath;
        // 清空 rootPath 下文件
        this.classPath = classPath;
    }

    public FileCompiler(String rootPath) {
        // 默认采用当前进程 classpath 进行编译
        this(rootPath, System.getProperty("java.class.path"));
    }

    @Override
    public void compile(String className, String sourceCode) throws Exception {
        // 生成本地文件
        if (className.contains(".")) {
            throw new RuntimeException("暂不支持包含小数点的类名");
        }
        File sourceFile = new File(this.rootPath + "/" + className + ".java");
        if (sourceFile.getParentFile() != null && (sourceFile.getParentFile().exists() || sourceFile.getParentFile()
                .mkdirs())) {
            try (FileOutputStream fileOutputStream = new FileOutputStream(sourceFile, false);
                 Writer writer = new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8)) {
                writer.write(sourceCode);
                writer.flush();
            }
        }
        // 构建编译诊断监听器
        DiagnosticCollector<JavaFileObject> collector = new DiagnosticCollector<>();
        // 获取待编译的 Java 类源码迭代器，包含所有内部类
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<JavaFileObject>();
        StandardJavaFileManager fileManager = this.javac.getStandardFileManager(diagnostics, null, null);

        Iterable<? extends JavaFileObject> compilationUnits
                = fileManager.getJavaFileObjectsFromFiles(Arrays.asList(sourceFile));
        // 生成编译任务
        LOGGER.info("Generate user code with -classpath: " + this.classPath);
        List<String> optionList = new ArrayList<String>();
        optionList.addAll(Arrays.asList("-classpath", this.classPath));

        JavaCompiler.CompilationTask task = this.javac.getTask(
                null, fileManager, collector, optionList, null, compilationUnits);
        // 执行编译任务
        boolean result = task.call();
        // 编辑结果分析
        if (!result || collector.getDiagnostics().size() > 0) {
            StringBuffer exceptionMsg = new StringBuffer();
            exceptionMsg.append("Unable to compile the source");
            boolean hasWarnings = false;
            boolean hasErrors = false;
            for (Diagnostic<? extends JavaFileObject> d : collector.getDiagnostics()) {
                switch (d.getKind()) {
                    case NOTE:
                    case MANDATORY_WARNING:
                    case WARNING:
                        hasWarnings = true;
                        break;
                    case OTHER:
                    case ERROR:
                    default:
                        hasErrors = true;
                        break;
                }
                // 编译结果诊断
                exceptionMsg.append("\n").append("[kind=").append(d.getKind());
                exceptionMsg.append(", ").append("line=").append(d.getLineNumber());
                exceptionMsg.append(", ").append("message=").append(d.getMessage(Locale.US)).append("]");
            }
            if (hasWarnings || hasErrors) {
                throw new RuntimeException(exceptionMsg.toString());
            }
        }
    }
}
