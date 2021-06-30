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

package com.tencent.bk.base.datahub.databus.commons;

import com.tencent.bk.base.datahub.databus.commons.convert.Converter;
import com.tencent.bk.base.datahub.databus.commons.convert.ConverterFactory;
import com.tencent.bk.base.datahub.databus.commons.transfer.Transfer;
import com.tencent.bk.base.datahub.databus.commons.transfer.TransferFactory;
import com.tencent.bk.base.datahub.databus.commons.utils.LogUtils;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class BkDatabusContext {

    private static final Logger log = LoggerFactory.getLogger(BkDatabusContext.class);
    /**
     * 结果表
     */
    private String rtId;
    /**
     * connector 名称
     */
    private String name;
    /**
     * 数据ID
     */
    private int dataId;

    /**
     * The id of the instance that invokes this sink.
     *
     * @return the instance id
     */
    private int instanceId;

    /**
     * Get the number of instances that invoke this sink.
     */
    private int numInstances;

    /**
     * Get a list of all input topics
     */
    private Collection<String> inputTopics;

    /**
     * Get the output topic of the source
     */
    private String outputTopic;

    private String msgType;

    private String converterClass;

    /**
     * 数据转换器（清洗）
     */
    private Converter converter;
    /**
     * task/connector 相关配置
     */
    private TaskContext taskContext;

    private Transfer transfer;

    private Supplier<Map<String, String>> rtInfoGetter;

    private BiConsumer<String, String> stateHolder;

    private Function<String, String> stateGetter;

    private BkDatabusContext(Builder builder) {
        this.rtId = builder.rtId;
        this.name = builder.name;
        this.dataId = builder.dataId;
        this.instanceId = builder.instanceId;
        this.numInstances = builder.numInstances;
        this.inputTopics = builder.inputTopics;
        this.outputTopic = builder.outputTopic;
        this.msgType = builder.msgType;
        this.converterClass = builder.converterClass == null ? "" : builder.converterClass;
        this.rtInfoGetter = builder.rtInfoGetter;
        if (null != this.rtInfoGetter) {
            this.taskContext = new TaskContext(this.rtInfoGetter.get());
            if (StringUtils.isNotBlank(msgType)) {
                taskContext.setSourceMsgType(msgType);
            }
            this.converter = ConverterFactory.getInstance().createConverter(taskContext);
            LogUtils.info(log, "created converter {} for msgType {} ,rtId {}", converter.getClass(), msgType, rtId);
        }
        this.transfer = TransferFactory.genConvert(converterClass);
        this.stateHolder = builder.stateHolder;
        this.stateGetter = builder.stateGetter;
    }


    public void putState(String key, String value) {
        Objects.requireNonNull(stateHolder, "state holder is not set!");
        stateHolder.accept(key, value);
    }

    public String getState(String key) {
        Objects.requireNonNull(stateGetter, "state holder is not set!");
        return stateGetter.apply(key);
    }

    /**
     * 刷新rt对应的task配置
     */
    public void refreshContext() {
        Objects.requireNonNull(rtInfoGetter, "RtInfoGetter is not set");
        Objects.requireNonNull(taskContext, "TaskContext is not init");
        try {
            Map<String, String> rtInfo = this.rtInfoGetter.get();
            if (rtInfo != null && !rtInfo.isEmpty()) {
                TaskContext context = new TaskContext(rtInfo);
                // 对比etl conf、columns等,检查是否需要重新创建converter对象
                if (!this.taskContext.equals(context)) {
                    LogUtils.info(log, "task context has changed, update it. {}", rtInfo);
                    context.setSourceMsgType(this.taskContext.getSourceMsgType());
                    this.taskContext = context;
                    this.converter = ConverterFactory.getInstance().createConverter(taskContext);
                    LogUtils.info(log, "Refresh context recreated converter {} for msgType {} ,rtId {}",
                            converter.getClass(), taskContext.getSourceMsgType(), rtId);
                }
            }
        } catch (Exception ignore) {
            LogUtils.warn(log, "failed to refresh task context due to exception for " + rtId, ignore);
        }
    }

    public String getRtId() {
        return rtId;
    }

    public String getName() {
        return name;
    }

    public int getDataId() {
        return dataId;
    }

    public Converter getConverter() {
        return converter;
    }

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public Transfer getTransfer() {
        return transfer;
    }

    public int getInstanceId() {
        return instanceId;
    }

    public int getNumInstances() {
        return numInstances;
    }

    public Collection<String> getInputTopics() {
        return inputTopics;
    }

    public String getOutputTopic() {
        return outputTopic;
    }

    public static class Builder {

        private String rtId;
        private String name;
        private int dataId;
        private int instanceId;
        private int numInstances;
        private Collection<String> inputTopics;
        private String outputTopic;
        private String msgType;
        private String converterClass;
        private Supplier<Map<String, String>> rtInfoGetter;
        private BiConsumer<String, String> stateHolder;
        private Function<String, String> stateGetter;

        private Builder() {

        }

        /**
         * 设置状态绑定
         *
         * @param stateHolder 接收状态
         * @param stateGetter 获取状态
         * @return BkDatabusContext 引用
         */
        public Builder setStateHolder(BiConsumer<String, String> stateHolder, Function<String, String> stateGetter) {
            this.stateGetter = stateGetter;
            this.stateHolder = stateHolder;
            return this;
        }

        public Builder setRtId(String rtId) {
            this.rtId = rtId;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setDataId(int dataId) {
            this.dataId = dataId;
            return this;
        }

        public Builder setInstanceId(int instanceId) {
            this.instanceId = instanceId;
            return this;
        }

        public Builder setNumInstances(int numInstances) {
            this.numInstances = numInstances;
            return this;
        }

        public Builder setInputTopics(Collection<String> inputTopics) {
            this.inputTopics = inputTopics;
            return this;
        }

        public Builder setOutputTopic(String outputTopic) {
            this.outputTopic = outputTopic;
            return this;
        }

        public Builder setMsgType(String msgType) {
            this.msgType = msgType;
            return this;
        }

        public Builder setConverterClass(String converterClass) {
            this.converterClass = converterClass;
            return this;
        }

        public Builder setRtInfoGetter(Supplier<Map<String, String>> rtInfoGetter) {
            this.rtInfoGetter = rtInfoGetter;
            return this;
        }

        public BkDatabusContext build() {
            return new BkDatabusContext(this);
        }

    }

    public static Builder builder() {
        return new Builder();
    }
}
