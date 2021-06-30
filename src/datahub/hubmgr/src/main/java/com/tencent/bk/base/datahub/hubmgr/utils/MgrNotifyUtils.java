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

package com.tencent.bk.base.datahub.hubmgr.utils;

import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ALARM_SERVICES_LIST;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DEFAULT_ALARM_SERVICES_LIST;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DEFAULT_FATAL_ALARM_TYPE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DEFAULT_MINOR_ALARM_TYPE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.DEFAULT_ORDINARY_ALARM_TYPE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.FATAL_ALARM_TYPE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.MINOR_ALARM_TYPE;
import static com.tencent.bk.base.datahub.hubmgr.MgrConsts.ORDINARY_ALARM_TYPE;

import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import com.tencent.bk.base.datahub.databus.commons.utils.NotifyUtils;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MgrNotifyUtils {

    private static final Logger log = LoggerFactory.getLogger(MgrNotifyUtils.class);
    public static Set<String> whiteList = Stream.of(
            DatabusProps.getInstance().getOrDefault(ALARM_SERVICES_LIST, DEFAULT_ALARM_SERVICES_LIST).split(",")
    ).collect(Collectors.toSet());

    public static boolean blockSendAlert(Class<?> c) {
        return !whiteList.contains(c.getSimpleName());
    }

    /**
     * 低优先级告警, 发送邮件
     *
     * @param receivers 接收人
     * @param message   消息内容
     */
    public static void sendWarningInfo(Class<?> c, String receivers, String message) {
        if (blockSendAlert(c)) {
            return;
        }

        DatabusProps props = DatabusProps.getInstance();
        String[] sendTypes = props.getOrDefault(MINOR_ALARM_TYPE, DEFAULT_MINOR_ALARM_TYPE).split(",");
        for (String sendType : sendTypes) {
            NotifyUtils.sendMsg(receivers, message, sendType);
        }
    }

    /**
     * 普通优先级告警发送微信消息
     *
     * @param receivers 接收人
     * @param message   消息内容
     */
    public static void sendOrdinaryAlert(Class<?> c, String receivers, String message) {
        if (blockSendAlert(c)) {
            return;
        }

        DatabusProps props = DatabusProps.getInstance();
        String[] sendTypes = props.getOrDefault(ORDINARY_ALARM_TYPE, DEFAULT_ORDINARY_ALARM_TYPE).split(",");
        for (String sendType : sendTypes) {
            NotifyUtils.sendMsg(receivers, message, sendType);
        }
    }

    /**
     * 致命告警 默认发送微信消息，并电话通知接收人
     *
     * @param receivers 接收人
     * @param message   消息内容
     */
    public static void sendFatalAlert(Class<?> c, String receivers, String message) {
        if (blockSendAlert(c)) {
            return;
        }

        DatabusProps props = DatabusProps.getInstance();
        String[] sendTypes = props.getOrDefault(FATAL_ALARM_TYPE, DEFAULT_FATAL_ALARM_TYPE).split(",");
        for (String sendType : sendTypes) {
            NotifyUtils.sendMsg(receivers, message, sendType);
        }
    }

}
