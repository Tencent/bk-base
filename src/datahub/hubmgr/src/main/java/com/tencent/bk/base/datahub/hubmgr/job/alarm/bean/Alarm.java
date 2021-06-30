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

package com.tencent.bk.base.datahub.hubmgr.job.alarm.bean;

public class Alarm {

    //告警名称
    private String name;
    //告警类型，微信，电话, 短信
    private String type;
    //告警范围
    private String scope;
    //告警描述
    private String description;
    //当前状态
    private String state;
    //告警阀值
    private float warnLimit;
    //发送人
    private String transmitList;
    //发送时间
    private String dateTime;


    public static Alarm build() {
        return new Alarm();
    }

    /**
     * 告警
     */
    public void alarm() {

    }

    public String getDateTime() {
        return dateTime;
    }

    public Alarm setDateTime(String dateTime) {
        this.dateTime = dateTime;
        return this;
    }

    public String getName() {
        return name;
    }

    public Alarm setName(String name) {
        this.name = name;
        return this;
    }

    public String getState() {
        return state;
    }

    public Alarm setState(String state) {
        this.state = state;
        return this;
    }

    public String getType() {
        return type;
    }

    public Alarm setType(String type) {
        this.type = type;
        return this;
    }

    public String getScope() {
        return scope;
    }

    public Alarm setScope(String scope) {
        this.scope = scope;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Alarm setDescription(String description) {
        this.description = description;
        return this;
    }

    public float getWarnLimit() {
        return warnLimit;
    }

    public Alarm setWarnLimit(float warnLimit) {
        this.warnLimit = warnLimit;
        return this;
    }

    public String getTransmitList() {
        return transmitList;
    }

    public Alarm setTransmitList(String transmitList) {
        this.transmitList = transmitList;
        return this;
    }
}
