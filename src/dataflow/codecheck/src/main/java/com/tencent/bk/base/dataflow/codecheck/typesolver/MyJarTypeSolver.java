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

package com.tencent.bk.base.dataflow.codecheck.typesolver;

import com.github.javaparser.resolution.UnsolvedSymbolException;
import com.github.javaparser.resolution.declarations.ResolvedReferenceTypeDeclaration;
import com.github.javaparser.symbolsolver.javassistmodel.JavassistFactory;
import com.github.javaparser.symbolsolver.model.resolution.SymbolReference;
import com.github.javaparser.symbolsolver.model.resolution.TypeSolver;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.NotFoundException;

public class MyJarTypeSolver implements TypeSolver {

    private final Map<String, ClasspathElement> classpathElements = new HashMap<>();
    private final ClassPool classPool = new ClassPool(false);
    private TypeSolver parent;

    /**
     * MyJarTypeSolver
     */
    public MyJarTypeSolver() {

    }

    /**
     * MyJarTypeSolver
     *
     * @param pathToJar
     */
    public MyJarTypeSolver(Path pathToJar) throws IOException {
        this(pathToJar.toFile());
    }

    /**
     * MyJarTypeSolver
     *
     * @param pathToJar
     */
    public MyJarTypeSolver(File pathToJar) throws IOException {
        this(pathToJar.getCanonicalPath());
    }

    /**
     * MyJarTypeSolver
     *
     * @param pathToJar
     */
    public MyJarTypeSolver(String pathToJar) throws IOException {
        addPathToJar(pathToJar);
    }

    /**
     * MyJarTypeSolver
     *
     * @param jarInputStream
     */
    public MyJarTypeSolver(InputStream jarInputStream) throws IOException {
        addPathToJar(jarInputStream);
    }

    /**
     * addJar
     *
     * @param pathToJar
     */
    public void addJar(String pathToJar) throws IOException {
        addPathToJar(pathToJar);
    }

//    public void addJar(Path pathToJar) throws IOException {
//        addPathToJar(pathToJar.toFile().getCanonicalPath());
//    }

//    public static MyJarTypeSolver getJarTypeSolver(String pathToJar) throws IOException {
//        if (instance == null) {
//            instance = new MyJarTypeSolver(pathToJar);
//        } else {
//            instance.addPathToJar(pathToJar);
//        }
//        return instance;
//    }
//
//    public static MyJarTypeSolver getJarTypeSolver() throws IOException {
//        return instance;
//    }

    private File dumpToTempFile(InputStream inputStream) throws IOException {
        File tempFile = File.createTempFile("jar_file_from_input_stream", ".jar");
        tempFile.deleteOnExit();

        byte[] buffer = new byte[8 * 1024];

        try (OutputStream output = new FileOutputStream(tempFile)) {
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                output.write(buffer, 0, bytesRead);
            }
        } finally {
            inputStream.close();
        }
        return tempFile;
    }

    private void addPathToJar(InputStream jarInputStream) throws IOException {
        addPathToJar(dumpToTempFile(jarInputStream).getAbsolutePath());
    }

    private void addPathToJar(String pathToJar) throws IOException {
        try {
            classPool.appendClassPath(pathToJar);
            classPool.appendSystemPath();
        } catch (NotFoundException e) {
            throw new RuntimeException(e);
        }
        JarFile jarFile = new JarFile(pathToJar);
        JarEntry entry;
        Enumeration<JarEntry> e = jarFile.entries();
        while (e.hasMoreElements()) {
            entry = e.nextElement();
            if (entry != null && !entry.isDirectory() && entry.getName().endsWith(".class")) {
                String name = entryPathToClassName(entry.getName());
                classpathElements.put(name, new MyJarTypeSolver.ClasspathElement(jarFile, entry));
            }
        }
        try {
            jarFile.close();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * getParent
     *
     * @return com.github.javaparser.symbolsolver.model.resolution.TypeSolver
     */
    @Override
    public TypeSolver getParent() {
        return parent;
    }

    /**
     * setParent
     *
     * @param parent
     */
    @Override
    public void setParent(TypeSolver parent) {
        Objects.requireNonNull(parent);
        if (this.parent != null) {
            throw new IllegalStateException("This TypeSolver already has a parent.");
        }
        if (parent == this) {
            throw new IllegalStateException("The parent of this TypeSolver cannot be itself.");
        }
        this.parent = parent;
    }

    private String entryPathToClassName(String entryPath) {
        if (!entryPath.endsWith(".class")) {
            throw new IllegalStateException();
        }
        String className = entryPath.substring(0, entryPath.length() - ".class".length());
        className = className.replace('/', '.');
        className = className.replace('$', '.');
        return className;
    }

    /**
     * tryToSolveType
     *
     * @param name
     * @return SymbolReference-com.github.javaparser.resolution.declarations.ResolvedReferenceTypeDeclaration
     */
    @Override
    public SymbolReference<ResolvedReferenceTypeDeclaration> tryToSolveType(String name) {
        try {
            if (classpathElements.containsKey(name)) {
                return SymbolReference.solved(
                        JavassistFactory.toTypeDeclaration(classpathElements.get(name).toCtClass(), getRoot()));
            } else {
                return SymbolReference.unsolved(ResolvedReferenceTypeDeclaration.class);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * solveType
     *
     * @param name
     * @return com.github.javaparser.resolution.declarations.ResolvedReferenceTypeDeclaration
     */
    @Override
    public ResolvedReferenceTypeDeclaration solveType(String name) throws UnsolvedSymbolException {
        SymbolReference<ResolvedReferenceTypeDeclaration> ref = tryToSolveType(name);
        if (ref.isSolved()) {
            return ref.getCorrespondingDeclaration();
        } else {
            throw new UnsolvedSymbolException(name);
        }
    }

    private class ClasspathElement {

        private final JarFile jarFile;
        private final JarEntry entry;

        ClasspathElement(JarFile jarFile, JarEntry entry) {
            this.jarFile = jarFile;
            this.entry = entry;
        }

        CtClass toCtClass() throws IOException {
            try (InputStream is = jarFile.getInputStream(entry)) {
                return classPool.makeClass(is);
            }
        }
    }
}

