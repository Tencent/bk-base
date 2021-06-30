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

package com.tencent.bk.base.datalab.bksql.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.spi.ResourceBundleControlProvider;

public class UTF8ResourceBundleControlProvider implements ResourceBundleControlProvider {

    private UTF8ResourceBundleControlProvider() {
    }

    public static void enable() {
        // following is a kind of workaround for ResourceBundle encoding issue known in JDK 8 and
        // earlier.
        try {
            Class.forName("java.util.ResourceBundle");
            Field field = ResourceBundle.class.getDeclaredField("providers");
            field.setAccessible(true);
            Field modifiersField = Field.class.getDeclaredField("modifiers");
            modifiersField.setAccessible(true);
            modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
            field.set(null, Collections.singletonList(new UTF8ResourceBundleControlProvider()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ResourceBundle.Control getControl(String baseName) {
        return new Utf8Control();
    }

    /**
     * Custom ResourceBundle.Control implementation which allows explicitly read the properties
     * files as UTF-8.
     *
     * @author <a href="mailto:nesterenko-aleksey@list.ru">Aleksey Nesterenko</a>
     */
    private static class Utf8Control extends ResourceBundle.Control {

        @Override
        public ResourceBundle newBundle(String aBaseName, Locale aLocale, String aFormat,
                ClassLoader aLoader, boolean aReload) throws IOException {
            // The below is a copy of the default implementation.
            final String bundleName = toBundleName(aBaseName, aLocale);
            final String resourceName = toResourceName(bundleName, "properties");
            InputStream stream = null;
            if (aReload) {
                final URL url = aLoader.getResource(resourceName);
                if (url != null) {
                    final URLConnection connection = url.openConnection();
                    if (connection != null) {
                        connection.setUseCaches(false);
                        stream = connection.getInputStream();
                    }
                }
            } else {
                stream = aLoader.getResourceAsStream(resourceName);
            }
            ResourceBundle resourceBundle = null;
            if (stream != null) {
                final Reader streamReader = new InputStreamReader(stream, StandardCharsets.UTF_8);
                try {
                    // Only this line is changed to make it to read properties files as UTF-8.
                    resourceBundle = new PropertyResourceBundle(streamReader);
                } finally {
                    stream.close();
                }
            }
            return resourceBundle;
        }
    }
}
