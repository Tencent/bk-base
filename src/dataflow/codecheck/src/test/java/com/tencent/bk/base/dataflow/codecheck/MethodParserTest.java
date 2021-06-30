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

import static org.mockito.ArgumentMatchers.any;

import com.github.javaparser.JavaParser;
import com.github.javaparser.symbolsolver.JavaSymbolSolver;
import com.github.javaparser.symbolsolver.model.resolution.TypeSolver;
import com.github.javaparser.symbolsolver.resolution.typesolvers.CombinedTypeSolver;
import com.tencent.bk.base.dataflow.codecheck.typesolver.MyJarTypeSolver;
import java.util.ArrayList;
import java.util.List;
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

@RunWith(PowerMockRunner.class)
@PrepareForTest({MethodParser.class, CombinedTypeSolver.class, MyJarTypeSolver.class,
        JavaSymbolSolver.class, JavaParser.class})
public class MethodParserTest {

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
    public void testInit() throws Exception {
        MethodParser methodParser = PowerMockito.mock(MethodParser.class);

        CombinedTypeSolver combinedTypeSolver = PowerMockito.mock(CombinedTypeSolver.class);
        PowerMockito.whenNew(CombinedTypeSolver.class).withArguments(any(), any(TypeSolver.class))
                .thenReturn(combinedTypeSolver);
        PowerMockito.doNothing().when(combinedTypeSolver).setExceptionHandler(any());

        MyJarTypeSolver myJarTypeSolver = PowerMockito.mock(MyJarTypeSolver.class);
        PowerMockito.whenNew(MyJarTypeSolver.class).withAnyArguments().thenReturn(myJarTypeSolver);

        JavaSymbolSolver javaSymbolSolver = PowerMockito.mock(JavaSymbolSolver.class);
        PowerMockito.whenNew(JavaSymbolSolver.class).withAnyArguments().thenReturn(javaSymbolSolver);
        JavaParser javaParser = PowerMockito.mock(JavaParser.class);
        PowerMockito.whenNew(JavaParser.class).withAnyArguments().thenReturn(javaParser);

        Whitebox.setInternalState(methodParser, "combinedTypeSolver", combinedTypeSolver);
        Whitebox.setInternalState(methodParser, "myJarTypeSolver", myJarTypeSolver);
        Whitebox.setInternalState(methodParser, "symbolSolver", javaSymbolSolver);
        Whitebox.setInternalState(methodParser, "innerJavaParser", javaParser);

        PowerMockito.doCallRealMethod().when(methodParser).init();
        methodParser.init();
        Assert.assertNotNull(Whitebox.getInternalState(methodParser, "parserConfiguration"));
    }

    @Test
    public void testClear() {
        MethodParser methodParser = PowerMockito.mock(MethodParser.class);
        PowerMockito.doCallRealMethod().when(methodParser).clear();
        methodParser.clear();
        Assert.assertNull(Whitebox.getInternalState(methodParser, "combinedTypeSolver"));
        Assert.assertNull(Whitebox.getInternalState(methodParser, "myJarTypeSolver"));
        Assert.assertNull(Whitebox.getInternalState(methodParser, "symbolSolver"));
        Assert.assertNull(Whitebox.getInternalState(methodParser, "innerJavaParser"));
    }

    @Test
    public void testParseJavaCode() throws Exception {
        String code = "import java.util.HashMap;\n"
                + "\n"
                + "public class MapTest {\n"
                + "    public static void main(String[] args) {\n"
                + "        HashMap<String, String> result = new HashMap<>();\n"
                + "        result.put(\"1\", \"1\");\n"
                + "        System.out.println(\"result: \" + result);\n"
                + "    }\n"
                + "}";
        ;
        MethodParser mp = new MethodParser(null, null);
        mp.init();
        List<MethodParserResult> ret = mp.parseJavaCode(code, true);
        Assert.assertEquals(ret.size(), 2);
        List<String> methodNames = new ArrayList<>();
        for (MethodParserResult mpr : ret) {
            methodNames.add(mpr.getMethodName());
        }
        Assert.assertEquals(methodNames.get(0), "java.util.HashMap.put");
        Assert.assertEquals(methodNames.get(1), "java.io.PrintStream.println");
    }
}