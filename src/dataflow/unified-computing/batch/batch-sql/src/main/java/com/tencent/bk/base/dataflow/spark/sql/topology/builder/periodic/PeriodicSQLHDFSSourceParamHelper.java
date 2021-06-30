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

package com.tencent.bk.base.dataflow.spark.sql.topology.builder.periodic;

import com.tencent.bk.base.dataflow.spark.topology.PathConstructor;
import com.tencent.bk.base.dataflow.spark.utils.CommonUtil$;
import com.tencent.bk.base.dataflow.core.conf.ConstantVar;
import com.tencent.bk.base.dataflow.spark.sql.topology.helper.TopoBuilderScalaHelper$;
import scala.Tuple3;

import java.util.List;
import java.util.Map;

public class PeriodicSQLHDFSSourceParamHelper {
    private int startTime;
    private int endTime;
    private ConstantVar.PeriodUnit timeUnit;
    private boolean isManaged;
    private boolean isAccumulate;
    private long scheduleTimeInHour;
    private PathConstructor pathConstructor;
    private String processingId;
    private String type;

    /**
     * 周期性SQL上游hdfs参数解析
     * @param info hdfs相关参数
     * @param pathConstructor 路径构建
     * @param processingId 结果表id
     */
    public PeriodicSQLHDFSSourceParamHelper(Map<String, Object> info,
            PathConstructor pathConstructor, String processingId) {
        this.isManaged = ((Double) info.getOrDefault("is_managed", 1.0)).intValue() == 1;
        this.isAccumulate = (boolean)info.get("accumulate");
        this.scheduleTimeInHour = CommonUtil$.MODULE$.roundScheduleTimeStampToHour((long)info.get("schedule_time"));
        this.pathConstructor = pathConstructor;
        this.processingId = processingId;
        this.type = info.get("type").toString();
        if (isAccumulate) {
            this.startTime = (int) info.get("data_start");
            this.endTime = (int) info.get("data_end");
        } else {
            int windowSize = ((Double) info.get("window_size")).intValue();
            int windowDelay = ((Double) info.get("window_delay")).intValue();
            this.timeUnit = ConstantVar.PeriodUnit.valueOf(((String) info.get("window_size_period")).toLowerCase());
            this.startTime = windowDelay;
            this.endTime = windowDelay + windowSize;
        }
    }

    /**
     * 构建输入路径
     * @param rootPath 输入根路径
     * @return 输入路径列表
     */
    public Tuple3<List<String>, Object, Object> buildPathAndTimeRangeInput(String rootPath) {
        Tuple3 pathTuple3;
        if (isAccumulate) {
            // tdw应该不会用到累加窗口
            pathTuple3 =  TopoBuilderScalaHelper$.MODULE$.buildAccInputPathsAsJava(
                    startTime,
                    endTime,
                    this.scheduleTimeInHour,
                    rootPath,
                    this.pathConstructor);
        } else {
            pathTuple3 = TopoBuilderScalaHelper$.MODULE$.buildWindowInputPathsAsJava(
                    this.processingId,
                    startTime,
                    endTime,
                    this.scheduleTimeInHour,
                    this.type,
                    this.timeUnit,
                    rootPath,
                    this.pathConstructor,
                    isManaged);
        }
        return pathTuple3;
    }
}
