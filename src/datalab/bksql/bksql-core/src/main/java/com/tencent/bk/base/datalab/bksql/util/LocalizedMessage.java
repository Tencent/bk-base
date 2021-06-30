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

import com.google.common.base.Preconditions;
import java.io.File;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;

public class LocalizedMessage {

    private final String messageKey;
    private final Object[] args;
    private final Class<?> sourceClass;
    private final Locale locale;

    public LocalizedMessage(String messageKey, Object[] args, Class<?> sourceClass, Locale locale) {
        this.messageKey = messageKey;
        this.args = args;
        this.sourceClass = sourceClass;
        this.locale = locale;
    }

    private static String getResourceBundleName(Class<?> sourceClass) {
        return String.format("%s/lm_%s",
                sourceClass.getPackage().getName().replace('.', File.separatorChar),
                sourceClass.getSimpleName());
    }

    public String getMessage() {
        ResourceBundle bundle = ResourceBundle.getBundle(
                getResourceBundleName(sourceClass),
                locale
        );
        Preconditions.checkArgument(bundle.containsKey(messageKey),
                "message key not found: " + messageKey);
        String string = bundle.getString(messageKey);
        return MessageFormat.format(string, args);
    }

}

