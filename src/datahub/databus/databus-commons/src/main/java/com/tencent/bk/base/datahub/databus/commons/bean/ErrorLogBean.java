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

package com.tencent.bk.base.datahub.databus.commons.bean;

import com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.commons.lang3.StringUtils;

public class ErrorLogBean implements IdBean {

    // bean的属性
    private String uid;
    private String extra;
    private String message;
    private long time;

    /**
     * 构造函数
     */
    public ErrorLogBean() {
        time = System.currentTimeMillis() / 1000; // 时间戳，秒
    }

    /**
     * 构造函数
     *
     * @param uid uid
     * @param extra 补充信息
     * @param message 消息
     */
    public ErrorLogBean(String uid, String extra, String message) {
        this.uid = uid;
        this.extra = extra;
        this.message = message;
        time = System.currentTimeMillis() / 1000; // 时间戳，秒
    }

    // getter & setter

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    @JsonProperty("extra_info")
    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }


    /**
     * toString方法
     *
     * @return 字符串
     */
    @Override
    public String toString() {
        return String.format("[uid: %s, extra: %s, message: %s, time: %s]", uid, extra, message, time);
    }

    /**
     * 转换为tsdb中的fields所需的字符串
     *
     * @return 转换为tsdb fields字符串
     */
    public String toTsdbFields() {
        return String.format("uid=\"%s\",message=\"%s\"", StringUtils.replace(uid, " ", "\\ "),
                StringUtils.replace(message, " ", "\\ "));
    }
}
