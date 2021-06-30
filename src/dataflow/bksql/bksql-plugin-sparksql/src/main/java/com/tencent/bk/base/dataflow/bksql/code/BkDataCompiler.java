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

package com.tencent.bk.base.dataflow.bksql.code;

import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.ErrorHandler;
import org.codehaus.commons.compiler.WarningHandler;
import org.codehaus.commons.nullanalysis.Nullable;
import org.codehaus.janino.ClassLoaderIClassLoader;
import org.codehaus.janino.IClassLoader;
import org.codehaus.janino.Java;
import org.codehaus.janino.Parser;
import org.codehaus.janino.Scanner;
import org.codehaus.janino.UnitCompiler;
import org.codehaus.janino.util.ClassFile;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

public class BkDataCompiler {

    private ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();

    @Nullable
    private ErrorHandler optionalCompileErrorHandler;
    @Nullable
    private WarningHandler optionalWarningHandler;

    private boolean debugSource = false;
    private boolean debugLines = false;
    private boolean debugVars = false;

    /**
     * udf代码编译,用于生成虚假的udf
     *
     * @param code udf代码
     * @return
     */
    public ClassFile[] compile(String code) throws IOException, CompileException {
        Reader r = (Reader) (new StringReader(code));
        Scanner scanner = new Scanner(null, r);
        Java.AbstractCompilationUnit compilationUnit = new Parser(scanner).parseAbstractCompilationUnit();
        IClassLoader icl = new ClassLoaderIClassLoader(this.parentClassLoader);

        ClassFile[] classFiles;

        try {

            // Compile compilation unit to class files.
            UnitCompiler unitCompiler = new UnitCompiler(compilationUnit, icl);
            unitCompiler.setCompileErrorHandler(this.optionalCompileErrorHandler);
            unitCompiler.setWarningHandler(this.optionalWarningHandler);

            classFiles = unitCompiler.compileUnit(this.debugSource, this.debugLines, this.debugVars);
        } finally {
            icl = null;
        }
        return classFiles;
    }
}
