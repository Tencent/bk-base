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

package com.tencent.bk.base.datalab.queryengine.server.handler;

import com.tencent.bk.base.datalab.bksql.rest.error.LocaleHolder;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.MessageSource;
import org.springframework.context.NoSuchMessageException;
import org.springframework.stereotype.Component;

@Component
public class MessageSourceHandler {

    @Autowired
    private MessageSource messageSource;

    /**
     * 获取本地化消息输出
     *
     * @param messageKey 消息 Key
     * @return 本地化消息
     */
    public String getMessage(String messageKey) {
        return getMessage(messageKey, LocaleHolder.instance().get(), null);
    }

    /**
     * 获取本地化消息输出
     *
     * @param messageKey 消息 Key
     * @param args 消息参数
     * @return 本地化消息
     */
    public String getMessage(String messageKey, Object... args) {
        return getMessage(messageKey, LocaleHolder.instance().get(), args);
    }


    /**
     * 获取本地化消息输出
     *
     * @param messageKey 消息 Key
     * @param locale 语言环境
     * @return 本地化消息
     */
    public String getMessage(String messageKey, Locale locale) {
        return getMessage(messageKey, locale, null);
    }

    /**
     * 获取本地化消息输出
     *
     * @param messageKey 消息 Key
     * @param locale 语言环境
     * @param args 消息参数
     * @return 本地化消息
     */
    public String getMessage(String messageKey, Locale locale, Object... args) {
        String localMessage;
        try {
            localMessage = messageSource
                    .getMessage(messageKey, args, locale);
        } catch (NoSuchMessageException e) {
            localMessage = messageKey;
        }
        return localMessage;
    }
}
