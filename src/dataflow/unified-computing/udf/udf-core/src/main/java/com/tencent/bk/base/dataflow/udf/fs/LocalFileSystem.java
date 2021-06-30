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

package com.tencent.bk.base.dataflow.udf.fs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileSystem {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFileSystem.class);

    /**
     * The possible write modes. The write mode decides what happens if a file should be created,
     * but already exists.
     */
    public enum WriteMode {

        /**
         * Creates the target file only if no file exists at that path already.
         * Does not overwrite existing files and directories.
         */
        NO_OVERWRITE,

        /**
         * Creates a new target file regardless of any existing files or directories.
         * Existing files and directories will be deleted (recursively) automatically before
         * creating the new file.
         */
        OVERWRITE
    }

    /**
     * The shared instance of the local file system.
     */
    private static final LocalFileSystem INSTANCE = new LocalFileSystem();

    /**
     * Path pointing to the current working directory.
     * Because Paths are not immutable, we cannot cache the proper path here
     */
    private final URI workingDir;

    /**
     * Path pointing to the current working directory.
     * Because Paths are not immutable, we cannot cache the proper path here.
     */
    private final URI homeDir;

    /**
     * The host name of this machine.
     */
    private final String hostName;

    /**
     * Constructs a new <code>LocalFileSystem</code> object.
     */
    public LocalFileSystem() {
        this.workingDir = new File(System.getProperty("user.dir")).toURI();
        this.homeDir = new File(System.getProperty("user.home")).toURI();

        String tmp = "unknownHost";
        try {
            tmp = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOG.error("Could not resolve local host", e);
        }
        this.hostName = tmp;
    }

    public boolean exists(Path f) throws IOException {
        final File path = pathToFile(f);
        return path.exists();
    }

    public boolean mkdirs(final Path f) throws IOException {
        return mkdirsInternal(pathToFile(f));
    }

    private boolean mkdirsInternal(File file) throws IOException {
        if (file.isDirectory()) {
            return true;
        } else if (file.exists() && !file.isDirectory()) {
            // Important: The 'exists()' check above must come before the 'isDirectory()' check to
            //            be safe when multiple parallel instances try to create the directory

            // exists and is not a directory -> is a regular file
            throw new FileAlreadyExistsException(file.getAbsolutePath());
        } else {
            File parent = file.getParentFile();
            return (parent == null || mkdirsInternal(parent)) && (file.mkdir() || file.isDirectory());
        }
    }

    public AbstractFSDataOutputStream create(final Path filePath, final WriteMode overwrite) throws IOException {

        if (exists(filePath) && overwrite == WriteMode.NO_OVERWRITE) {
            throw new FileAlreadyExistsException("File already exists: " + filePath);
        }

        final Path parent = filePath.getParent();
        if (parent != null && !mkdirs(parent)) {
            throw new IOException("Mkdirs failed to create " + parent);
        }

        final File file = pathToFile(filePath);
        return new LocalDataOutputStream(file);
    }

    /**
     * Converts the given Path to a File for this file system.
     *
     * If the path is not absolute, it is interpreted relative to this FileSystem's working directory.
     */
    public File pathToFile(Path path) {
        if (!path.isAbsolute()) {
            path = new Path(getWorkingDirectory(), path);
        }
        return new File(path.toUri().getPath());
    }

    public Path getWorkingDirectory() {
        return new Path(workingDir);
    }

    /**
     * Gets the shared instance of this file system.
     *
     * @return The shared instance of this file system.
     */
    public static LocalFileSystem getSharedInstance() {
        return INSTANCE;
    }
}
