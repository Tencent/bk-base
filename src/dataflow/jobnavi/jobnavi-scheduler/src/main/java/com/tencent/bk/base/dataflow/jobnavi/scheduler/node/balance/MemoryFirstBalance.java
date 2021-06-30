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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.node.balance;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.conf.Constants;
import com.tencent.bk.base.dataflow.jobnavi.node.RunnerInfo;
import com.tencent.bk.base.dataflow.jobnavi.util.conf.Configuration;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MemoryFirstBalance extends AbstractNodeBalance {

    private final int highLevelAmount;

    public MemoryFirstBalance(Configuration conf) {
        super(conf);
        highLevelAmount = conf.getInt(Constants.JOBNAVI_SCHEDULER_NODE_BALANCE_MEMORY_HIGH_LEVEL_AMOUNT,
                Constants.JOBNAVI_SCHEDULER_NODE_BALANCE_MEMORY_HIGH_LEVEL_AMOUNT_DEFAULT);
        if (highLevelAmount < 1) {
            throw new IllegalArgumentException(
                    Constants.JOBNAVI_SCHEDULER_NODE_BALANCE_MEMORY_HIGH_LEVEL_AMOUNT + " must large 1.");
        }
    }

    @Override
    public RunnerInfo getBestRunner(List<RunnerInfo> availableRunners) {
        List<RunnerInfo> bestRunners = getHighLevelRunners(availableRunners);
        SecureRandom random = new SecureRandom();
        int index = random.nextInt(bestRunners.size());
        return bestRunners.get(index);
    }


    private List<RunnerInfo> getHighLevelRunners(List<RunnerInfo> availableRunners) {
        if (highLevelAmount > availableRunners.size()) {
            return availableRunners;
        }

        RunnerInfo[] runnerInfos = availableRunners.toArray(new RunnerInfo[availableRunners.size()]);
        Arrays.sort(runnerInfos, new Comparator<RunnerInfo>() {
            @Override
            public int compare(RunnerInfo o1, RunnerInfo o2) {
                if (o1.getMemoryUsage() > o2.getMemoryUsage()) {
                    return 1;
                } else if (o1.getMemoryUsage().equals(o2.getMemoryUsage())) {
                    return 0;
                } else {
                    return -1;
                }
            }
        });
        List<RunnerInfo> highLevels = new ArrayList<>();
        highLevels.addAll(Arrays.asList(runnerInfos).subList(0, highLevelAmount));
        return highLevels;
    }
}
