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

package com.tencent.bk.base.dataflow.flink.streaming.function.base;


import com.tencent.bk.base.dataflow.core.function.base.udf.AbstractFunctionFactory;
import com.tencent.bk.base.dataflow.core.function.base.udf.UDFExceptions.RegisterException;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkAddOnlineCnt;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkAddOs;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkContainsSubstring;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkCurDate;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkCurTime;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkDivision;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkFromMillisec;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkFromUnixTime;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkGameappidToPlat;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkGroupConcat;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkINetAToN;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkINetNToA;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkInstr;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkIntOverflow;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkLastInstr;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkLeft;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkLeftTrim;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkLength;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkLocate;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkMid;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkNow;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkRegExpExtract;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkRegexpReplace;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkReplace;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkRight;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkRightTrim;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkSplitFieldToRecords;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkSplitIndex;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkSubstring;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkSubstringIndex;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkTransFieldsToRecords;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkTruncate;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkUnixTimestamp;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkUnixtimeDiff;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkUtcToLocal;
import com.tencent.bk.base.dataflow.flink.streaming.function.FlinkZipFieldsToRecords;
import com.tencent.bk.base.dataflow.flink.streaming.runtime.FlinkStreamingRuntime;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;

public class FlinkFunctionFactory extends AbstractFunctionFactory<IFlinkFunction> {

    /**
     * flink tableEnv
     */
    private final StreamTableEnvironment tableEnv;
    
    // 与内置函数重名的UDF类会被覆盖，所以不要重名
    // 原生支持的函数(大小写不敏感)
    // 以下注册flink已实现的udf(大小写敏感)
    {
        functions.put("add_onlinecnt", FlinkAddOnlineCnt.class);
        functions.put("add_os", FlinkAddOs.class);
        functions.put("contains_substring", FlinkContainsSubstring.class);
        functions.put("curdate", FlinkCurDate.class);
        functions.put("now", FlinkNow.class);
        functions.put("curtime", FlinkCurTime.class);
        functions.put("from_millisec", FlinkFromMillisec.class);
        functions.put("from_unixtime", FlinkFromUnixTime.class);
        functions.put("gameappid_to_plat", FlinkGameappidToPlat.class);
        functions.put("inet_aton", FlinkINetAToN.class);
        functions.put("inet_ntoa", FlinkINetNToA.class);
        functions.put("int_overflow", FlinkIntOverflow.class);
        functions.put("instr", FlinkInstr.class);
        functions.put("last_instr", FlinkLastInstr.class);
        functions.put("left", FlinkLeft.class);
        functions.put("length", FlinkLength.class);
        functions.put("locate", FlinkLocate.class);
        functions.put("ltrim", FlinkLeftTrim.class);
        functions.put("mid", FlinkMid.class);
        functions.put("regexp_replace", FlinkRegexpReplace.class);
        functions.put("right", FlinkRight.class);
        functions.put("rtrim", FlinkRightTrim.class);
        functions.put("split_index", FlinkSplitIndex.class);
        functions.put("substring_index", FlinkSubstringIndex.class);
        functions.put("unixtime_diff", FlinkUnixtimeDiff.class);
        functions.put("unix_timestamp", FlinkUnixTimestamp.class);
        functions.put("bkdata_division", FlinkDivision.class);
        functions.put("udf_regexp_extract", FlinkRegExpExtract.class);
        // 一对多
        functions.put("trans_fields_to_records", FlinkTransFieldsToRecords.class);
        functions.put("split_field_to_records", FlinkSplitFieldToRecords.class);
        functions.put("zip", FlinkZipFieldsToRecords.class);

        // 由BKSQL适配
        functions.put("udf_replace", FlinkReplace.class);
        functions.put("udf_substring", FlinkSubstring.class);
        functions.put("udf_truncate", FlinkTruncate.class);

        // udaf
        functions.put("GROUP_CONCAT", FlinkGroupConcat.class);

        // utc时间转本地时区时间，解决flink utc时区问题
        timeZoneFunctions.put("utc_to_local", FlinkUtcToLocal.class);
    }

    /**
     * 有参构造函数.
     *
     * @param runtime
     */
    public FlinkFunctionFactory(final FlinkStreamingRuntime runtime) {
        this.tableEnv = runtime.getTableEnv();
        this.timeZone = runtime.getTopology().getTimeZone();
    }

    /**
     * 构造参数，供flink sql调用
     *
     * @param tableEnv table env
     */
    public FlinkFunctionFactory(StreamTableEnvironment tableEnv) {
        this.tableEnv = tableEnv;
    }

    /**
     * 注册udf函数
     *
     * @param functionName
     * @throws RegisterException
     */
    @Override
    public void register(final String functionName) throws RegisterException {
        IFlinkFunction udfInstance = buildUDF(functionName);
        // 可以改为直接引用udfInstance的通用名称
        if (udfInstance instanceof ScalarFunction) {
            tableEnv.registerFunction(functionName, (ScalarFunction) udfInstance);
        } else if (udfInstance instanceof TableFunction) {
            tableEnv.registerFunction(functionName, (TableFunction) udfInstance);
        } else if (udfInstance instanceof AggregateFunction) {
            tableEnv.registerFunction(functionName, (AggregateFunction) udfInstance);
        } else {
            throw new RegisterException(String.format("不支持的flink udf类型: %s", udfInstance));
        }
    }
}
