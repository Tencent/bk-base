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

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

public class Path implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A pre-compiled regex/state-machine to match the windows drive pattern.
     */
    private static final Pattern WINDOWS_ROOT_DIR_REGEX = Pattern.compile("/\\p{Alpha}+:/");

    /**
     * The directory separator, a slash.
     */
    private static final String SEPARATOR = "/";

    /**
     * Character denoting the current directory.
     */
    private static final String CUR_DIR = ".";

    /**
     * The internal representation of the path, a hierarchical URI.
     */
    private URI uri;

    /**
     * Constructs a path object from a given URI.
     *
     * @param uri the URI to construct the path object from
     */
    public Path(URI uri) {
        this.uri = uri;
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, String child) {
        this(parent, new Path(child));
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent the parent path
     * @param child the child path
     */
    public Path(Path parent, Path child) {
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        final String parentPath = parentUri.getPath();
        if (!("/".equals(parentPath) || "".equals(parentPath))) {
            try {
                parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri.getPath() + "/", null,
                        null);
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }

        if (child.uri.getPath().startsWith(Path.SEPARATOR)) {
            child = new Path(child.uri.getScheme(), child.uri.getAuthority(), child.uri.getPath().substring(1));
        }

        final URI resolved = parentUri.resolve(child.uri);
        initialize(resolved.getScheme(), resolved.getAuthority(), normalizePath(resolved.getPath()));
    }

    /**
     * Construct a Path from a scheme, an authority and a path string.
     *
     * @param scheme the scheme string
     * @param authority the authority string
     * @param path the path string
     */
    public Path(String scheme, String authority, String path) {
        path = checkAndTrimPathArg(path);
        initialize(scheme, authority, path);
    }

    /**
     * Construct a path from a String. Path strings are URIs, but with unescaped
     * elements and some additional normalization.
     *
     * @param pathString the string to construct a path from
     */
    public Path(String pathString) {
        pathString = checkAndTrimPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString, false)) {
            pathString = "/" + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
            // scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start) && (pathString.length() - start > 2)) { // has authority
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        final String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path);
    }

    /**
     * Checks if the provided path string is either null or has zero length and throws
     * a {@link IllegalArgumentException} if any of the two conditions apply.
     * In addition, leading and tailing whitespaces are removed.
     *
     * @param path the path string to be checked
     * @return The checked and trimmed path.
     */
    private String checkAndTrimPathArg(String path) {
        // disallow construction of a Path from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        path = path.trim();
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
        return path;
    }

    /**
     * Checks if the provided path string contains a windows drive letter.
     *
     * @param path the path to check
     * @param slashed true to indicate the first character of the string is a slash, false otherwise
     * @return <code>true</code> if the path string contains a windows drive letter, false otherwise
     */
    private boolean hasWindowsDrive(String path, boolean slashed) {
        final int start = slashed ? 1 : 0;
        return path.length() >= start + 2
                && (!slashed || path.charAt(0) == '/')
                && path.charAt(start + 1) == ':'
                && ((path.charAt(start) >= 'A' && path.charAt(start) <= 'Z') || (path.charAt(start) >= 'a' && path
                .charAt(start) <= 'z'));
    }

    /**
     * Initializes a path object given the scheme, authority and path string.
     *
     * @param scheme the scheme string.
     * @param authority the authority string.
     * @param path the path string.
     */
    private void initialize(String scheme, String authority, String path) {
        try {
            this.uri = new URI(scheme, authority, normalizePath(path), null, null).normalize();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


    /**
     * Normalizes a path string.
     *
     * @param path the path string to normalize
     * @return the normalized path string
     */
    private String normalizePath(String path) {

        // remove leading and tailing whitespaces
        path = path.trim();

        // remove consecutive slashes & backslashes
        path = path.replace("\\", "/");
        path = path.replaceAll("/+", "/");

        // remove tailing separator
        if (path.endsWith(SEPARATOR)
                && !path.equals(SEPARATOR)  // UNIX root path
                && !WINDOWS_ROOT_DIR_REGEX.matcher(path).matches()) {  // Windows root path)

            // remove tailing slash
            path = path.substring(0, path.length() - SEPARATOR.length());
        }

        return path;
    }

    /**
     * Returns the FileSystem that owns this Path.
     *
     * @return the FileSystem that owns this Path
     * @throws IOException thrown if the file system could not be retrieved
     */
    public LocalFileSystem getFileSystem() throws IOException {
        return LocalFileSystem.getSharedInstance();
    }

    /**
     * Checks if the directory of this path is absolute.
     *
     * @return <code>true</code> if the directory of this path is absolute, <code>false</code> otherwise
     */
    public boolean isAbsolute() {
        final int start = hasWindowsDrive(uri.getPath(), true) ? 3 : 0;
        return uri.getPath().startsWith(SEPARATOR, start);
    }

    /**
     * Converts the path object to a {@link URI}.
     *
     * @return the {@link URI} object converted from the path object
     */
    public URI toUri() {
        return uri;
    }

    /**
     * Returns the parent of a path, i.e., everything that precedes the last separator
     * or <code>null</code> if at root.
     *
     * @return the parent of a path or <code>null</code> if at root.
     */
    public Path getParent() {
        final String path = uri.getPath();
        final int lastSlash = path.lastIndexOf('/');
        final int start = hasWindowsDrive(path, true) ? 3 : 0;
        if ((path.length() == start) || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }
        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            final int end = hasWindowsDrive(path, true) ? 3 : 0;
            parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
        }
        return new Path(uri.getScheme(), uri.getAuthority(), parent);
    }
}
