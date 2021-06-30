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

package com.tencent.bk.base.datalab.queryengine.server.util;

import com.tencent.bk.base.datalab.queryengine.server.handler.MessageSourceHandler;
import org.apache.commons.lang.StringUtils;

public class MessageLocalizedUtil {

    private static MessageSourceHandler messageSourceHandler;

    static {
        messageSourceHandler = SpringBeanUtil.getBean(MessageSourceHandler.class);
    }

    /**
     * 消息本地化
     *
     * @param messageKey 消息 Key
     * @param args 参数列表
     * @return 本地化消息
     */
    public static String getMessage(String messageKey, Object[] args) {
        String localeMessage;
        try {
            if (args != null) {
                localeMessage = messageSourceHandler.getMessage(messageKey, args);
            } else {
                localeMessage = messageSourceHandler.getMessage(messageKey);
            }
        } catch (Exception ex) {
            localeMessage = messageKey;
        }
        localeMessage = StringUtils.isBlank(localeMessage) ? messageKey : localeMessage;
        return localeMessage;
    }

    /**
     * 消息本地化
     *
     * @param messageKey 消息 Key
     * @return 本地化消息
     */
    public static String getMessage(String messageKey) {
        return getMessage(messageKey, null);
    }
}
