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

package com.tencent.bk.base.dataflow.core.metric;

import java.util.HashMap;
import java.util.Map;

public class MetricDataStatics {

    private Map<String, Long> tags;
    private Long totalCnt;
    private Long totalCntIncrement;
    private Long checkpointDrop;
    private Map<String, Long> checkpointDroptags;

    public Map<String, Long> getCheckpointDroptags() {
        return checkpointDroptags;
    }

    public void setCheckpointDroptags(Map<String, Long> checkpointDroptags) {
        this.checkpointDroptags = checkpointDroptags;
    }

    public Map<String, Long> getTags() {
        return tags;
    }

    public void setTags(Map<String, Long> tags) {
        this.tags = tags;
    }

    public Long getTotalCnt() {
        return totalCnt;
    }

    public void setTotalCnt(Long totalCnt) {
        this.totalCnt = totalCnt;
    }

    public Long getTotalCntIncrement() {
        return totalCntIncrement;
    }

    public void setTotalCntIncrement(Long totalCntIncrement) {
        this.totalCntIncrement = totalCntIncrement;
    }

    public void setCheckpointDrop(Long checkpointDrop) {
        this.checkpointDrop = checkpointDrop;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("tags", tags);
        map.put("total_cnt", totalCnt);
        map.put("total_cnt_increment", totalCntIncrement);
        if (checkpointDrop != null && checkpointDroptags != null) {
            map.put("ckp_drop", checkpointDrop);
            map.put("ckp_drop_tags", checkpointDroptags);
        }
        return map;
    }
}
