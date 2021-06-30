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

package com.tencent.bk.base.dataflow.spark;

public class BatchTimeDelta {
    private int month;
    private int week;
    private int day;
    private int hour;

    /**
     * 代表一个时间段，可以与BatchTimeStamp进行加减计算
     * @param timeStr 时间段参数，例如1M+1H
     */
    public BatchTimeDelta(String timeStr) {
        String[] arraySplitWithPlus = timeStr.split("\\+");
        for (String itemPlus  : arraySplitWithPlus) {
            String[] arraySplitWithMinus = itemPlus.split("-");
            int minusCount = 0;
            for (String itemMinus : arraySplitWithMinus) {
                if (minusCount == 0 && "".equals(itemMinus)) {
                    minusCount++;
                    continue;
                } else if (minusCount == 0 && !"".equals(itemMinus)) {
                    this.setTimeWithStringFormat(itemMinus, false);
                } else {
                    this.setTimeWithStringFormat(itemMinus, true);
                }
                minusCount++;
            }
        }
    }

    public BatchTimeDelta(int month, int week, int day, int hour) {
        this.month = month;
        this.week = week;
        this.day = day;
        this.hour = hour;
    }

    public BatchTimeDelta() {
        this.month = 0;
        this.week = 0;
        this.day = 0;
        this.hour = 0;
    }

    private void setTimeWithStringFormat(String str, boolean isMinus) {
        if (str.toUpperCase().endsWith("M")) {
            this.month = this.parseSignIntStr(str.substring(0, str.length() - 1), isMinus);
        } else if (str.toUpperCase().endsWith("W")) {
            this.week = this.parseSignIntStr(str.substring(0, str.length() - 1), isMinus);
        } else if (str.toUpperCase().endsWith("D")) {
            this.day = this.parseSignIntStr(str.substring(0, str.length() - 1), isMinus);
        } else {
            this.hour = this.parseSignIntStr(str.substring(0, str.length() - 1), isMinus);
        }
    }

    private int parseSignIntStr(String str, boolean isMinus) {
        try {
            if (isMinus) {
                return -Integer.parseInt(str);
            } else {
                return Integer.parseInt(str);
            }

        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }

    public int getMonth() {
        return month;
    }

    public int getWeek() {
        return week;
    }

    public int getDay() {
        return day;
    }

    public int getHour() {
        return hour;
    }

    public BatchTimeDelta plus(BatchTimeDelta timeDelta) {
        int tmpMonth = this.month + timeDelta.month;
        int tmpWeek = this.week + timeDelta.week;
        int tmpDay = this.day + timeDelta.day;
        int tmpHour = this.hour + timeDelta.hour;
        return new BatchTimeDelta(tmpMonth, tmpWeek, tmpDay, tmpHour);
    }

    public BatchTimeDelta minus(BatchTimeDelta timeDelta) {
        int tmpMonth = this.month - timeDelta.month;
        int tmpWeek = this.week - timeDelta.week;
        int tmpDay = this.day - timeDelta.day;
        int tmpHour = this.hour - timeDelta.hour;
        return new BatchTimeDelta(tmpMonth, tmpWeek, tmpDay, tmpHour);
    }

    public long getHourExceptMonth() {
        return this.week * 7 * 24L + this.day * 24L + this.hour;
    }
}
