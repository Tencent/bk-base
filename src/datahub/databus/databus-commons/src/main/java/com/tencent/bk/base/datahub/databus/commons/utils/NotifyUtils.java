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

package com.tencent.bk.base.datahub.databus.commons.utils;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.DatabusProps;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class NotifyUtils {

    private static final Logger log = LoggerFactory.getLogger(NotifyUtils.class);

    /**
     * 发送微信消息
     *
     * @param receivers 接收人
     * @param message 消息内容
     */
    public static void sendWechat(String receivers, String message) {
        sendMsg(receivers, message, Consts.WECHAT);
    }

    /**
     * 发送微信消息，并电话通知接收人
     *
     * @param receivers 接收人
     * @param message 消息内容
     */
    public static void sendWechatAndPhone(String receivers, String message) {
        sendMsg(receivers, message, Consts.WECHAT);
        sendMsg(receivers, message, Consts.PHONE);
    }

    /**
     * 发送短信通知
     *
     * @param receivers 接收人
     * @param message 消息内容
     */
    public static void sendSms(String receivers, String message) {
        sendMsg(receivers, message, Consts.SMS);
    }

    /**
     * 发送邮件通知
     *
     * @param receivers 接收人
     * @param message 消息内容
     */
    public static void sendMail(String receivers, String message) {
        sendMsg(receivers, message, Consts.MAIL);
    }

    /**
     * 发送通知消息
     *
     * @param receivers 接收人
     * @param message 消息内容
     * @param notifyWay 通知方式
     * @return 是否成功通知
     */
    public static boolean sendMsg(String receivers, String message, String notifyWay) {
        String appCode = DatabusProps.getInstance().getOrDefault(Consts.ADMIN_APP_CODE, "data");
        String appSecret = DatabusProps.getInstance().getOrDefault(Consts.ADMIN_APP_SECRET, "");
        String url = DatabusProps.getInstance().getOrDefault(Consts.ADMIN_NOTIFY_URL, "");
        String sender = DatabusProps.getInstance().getOrDefault(Consts.ADMIN_NOTIFY_SENDER, "admin");

        if (StringUtils.isBlank(url) || StringUtils.isBlank(receivers) || StringUtils.isBlank(message)) {
            LogUtils.warn(log, "empty args, unable to send notification. {} {} {}", notifyWay, receivers, message);
            return false;
        } else {
            Map<String, String> params = new HashMap<>();
            params.put("bk_app_code", appCode);
            params.put("bk_app_secret", appSecret);
            params.put("bk_username", sender);
            params.put("receiver", receivers);
            params.put("message", message);
            params.put("notify_way", notifyWay);

            return HttpUtils.postAndCheck(url, params);
        }
    }

}
