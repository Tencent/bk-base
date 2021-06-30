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

package com.tencent.bk.base.datahub.databus.pipe;


import com.tencent.bk.base.datahub.databus.pipe.utils.JsonUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class EtlParseDetails {

    private Map<String, Object> nodes;
    private Map<String, String> errors;
    private List<String> errorMessage;
    private Map<String, Map<String, Object>> outputType;

    public EtlParseDetails() {
        nodes = new HashMap<>();
        errors = new HashMap<>();
        errorMessage = new ArrayList<>();
        outputType = new HashMap<>();
    }

    /**
     * 设置节点的解析结果
     *
     * @param nodeLable 节点的label
     * @param result 节点的解析结果
     */
    public void setNodeDetail(String nodeLable, Object result) {
        // 如果多次调用此方法，说明此节点前有遍历节点，需将结果数据转换为list类型
        if (nodes.containsKey(nodeLable)) {
            if (nodes.get(nodeLable) instanceof LinkedList) {
                ((LinkedList) nodes.get(nodeLable)).add(result);
            } else {
                // 将node结果转换为list类型，并放入节点解析结果中
                LinkedList<Object> res = new LinkedList<>();
                res.add(nodes.get(nodeLable));
                res.add(result);
                // 替换原有结果
                nodes.put(nodeLable, res);
            }
        } else {
            nodes.put(nodeLable, result);
        }
    }

    /**
     * 设置节点的错误消息
     *
     * @param nodeLabel 节点的label
     * @param errorMsg 节点的报错信息
     */
    public void setNodeErrorMsg(String nodeLabel, String errorMsg) {
        errors.put(nodeLabel, errorMsg);
    }

    /**
     * 设置节点输出的类型
     *
     * @param nodeLabel 节点的label
     * @param type 节点输出的数据类型
     * @param value 节点输出的数据
     */
    public void setNodeOutput(String nodeLabel, String type, Object value) {
        if (!outputType.containsKey(nodeLabel)) {
            // 节点的输出类型只能设置一次，避免在iterate节点后存在多次调用，覆盖了输出值
            Map<String, Object> val = new HashMap<>();
            val.put(EtlConsts.TYPE, type);
            val.put(EtlConsts.VALUE, value);
            outputType.put(nodeLabel, val);
        }
    }

    /**
     * 设置解析错误的提示信息
     *
     * @param errMsg 时间解析错误提示信息
     */
    public void addErrorMessage(String errMsg) {
        errorMessage.add(errMsg);
    }

    /**
     * 获取节点解析的详情
     *
     * @return 节点解析详情, map结构
     */
    Map<String, Object> getNodeDetails() {
        return Collections.unmodifiableMap(nodes);
    }

    /**
     * 获取节点解析的异常信息
     *
     * @return 节点解析的异常信息, map结构
     */
    Map<String, String> getErrors() {
        return Collections.unmodifiableMap(errors);
    }

    /**
     * 获取清洗解析时的错误异常信息
     *
     * @return 错误异常信息，列表
     */
    String getErrorMessage() {
        return errorMessage.size() > 0 ? errorMessage.get(0) : "";
    }

    /**
     * 获取节点的输出类型信息
     *
     * @return 节点输出数据的类型信息，map结构
     */
    Map<String, Map<String, Object>> getNodeOutputType() {
        return Collections.unmodifiableMap(outputType);
    }

    /**
     * 将结果集转换为Json字符串返回
     *
     * @return Etl解析的结果集, json格式字符串
     */
    @Override
    public String toString() {
        Map<String, Object> result = new HashMap<>();
        result.put(EtlConsts.NODES, nodes);
        result.put(EtlConsts.ERRORS, errors);
        result.put(EtlConsts.ERROR_MESSAGE, errorMessage);
        result.put(EtlConsts.OUTPUT_TYPE, outputType);
        try {
            return JsonUtils.writeValueAsString(result);
        } catch (IOException e) {
            return "{}";
        }
    }


}
