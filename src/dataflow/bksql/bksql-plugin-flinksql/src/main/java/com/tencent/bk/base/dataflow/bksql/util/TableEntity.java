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

package com.tencent.bk.base.dataflow.bksql.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public final class TableEntity {
    private final String id;
    private final String name;
    private final Set<String> parents;
    private final Window window;
    private final Processor processor;
    private final List<Field> fields;

    private TableEntity(String id,
                        String name,
                        Set<String> parents,
                        Window window,
                        Processor processor,
                        List<Field> fields) {
        Preconditions.checkNotNull(id);
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(parents);
        Preconditions.checkNotNull(processor);
        Preconditions.checkNotNull(fields);
        this.id = id;
        this.name = name;
        this.parents = parents;
        this.window = window;
        this.processor = processor;
        this.fields = fields;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public Set<String> getParents() {
        return parents;
    }

    public Window getWindow() {
        return window;
    }

    public Processor getProcessor() {
        return processor;
    }

    public List<Field> getFields() {
        return fields;
    }

    public static class Window {
        private final String type;
        private final int countFreq;
        private final int waitingTime;
        private final int length;
        private final int sessionGap;
        private final int expiredTime;
        private boolean allowedLateness;
        private int latenessTime;
        private int latenessCountFreq;

        private Window(String type,
                       int countFreq,
                       int waitingTime,
                       int length,
                       int sessionGap,
                       int expiredTime) {
            this.type = type;
            this.countFreq = countFreq;
            this.waitingTime = waitingTime;
            this.length = length;
            this.sessionGap = sessionGap;
            this.expiredTime = expiredTime;
        }

        public Window setLateness(boolean allowedLateness, int latenessTime, int latenessCountFreq) {
            this.allowedLateness = allowedLateness;
            this.latenessTime = latenessTime;
            this.latenessCountFreq = latenessCountFreq;
            return this;
        }

        public String getType() {
            return type;
        }

        @JsonProperty("count_freq")
        public int getCountFreq() {
            return countFreq;
        }

        @JsonProperty("waiting_time")
        public int getWaitingTime() {
            return waitingTime;
        }

        public int getLength() {
            return length;
        }

        @JsonProperty("session_gap")
        public int getSessionGap() {
            return sessionGap;
        }

        @JsonProperty("expired_time")
        public int getExpiredTime() {
            return expiredTime;
        }

        @JsonProperty("allowed_lateness")
        public boolean isAllowedLateness() {
            return allowedLateness;
        }

        @JsonProperty("lateness_time")
        public int getLatenessTime() {
            return latenessTime;
        }

        @JsonProperty("lateness_count_freq")
        public int getLatenessCountFreq() {
            return latenessCountFreq;
        }

        public static class Builder {
            private final TableEntity.Builder parent;

            private String type = null;
            private int countFreq = 0;
            private int waitingTime = 0;
            private int length = 0;
            private int sessionGap = 0;
            private int expiredTime = 0;
            private boolean allowedLateness = false;
            private int latenessTime = 0;
            private int latenessCountFreq = 0;

            private Builder(TableEntity.Builder parent) {
                this.parent = parent;
            }

            public Builder type(String type) {
                this.type = type;
                return this;
            }

            public Builder countFreq(int countFreq) {
                this.countFreq = countFreq;
                return this;
            }

            public Builder waitingTime(int waitingTime) {
                this.waitingTime = waitingTime;
                return this;
            }

            public Builder length(int length) {
                this.length = length;
                return this;
            }

            public Builder sessionGap(int sessionGap) {
                this.sessionGap = sessionGap;
                return this;
            }

            public Builder expiredTime(int expiredTime) {
                this.expiredTime = expiredTime;
                return this;
            }

            public Builder allowedLateness(boolean allowedLateness) {
                this.allowedLateness = allowedLateness;
                return this;
            }

            public Builder latenessTime(int latenessTime) {
                this.latenessTime = latenessTime;
                return this;
            }

            public Builder latenessCountFreq(int latenessCountFreq) {
                this.latenessCountFreq = latenessCountFreq;
                return this;
            }

            public TableEntity.Builder create() {
                Window window = new Window(type, countFreq, waitingTime, length, sessionGap, expiredTime)
                        .setLateness(allowedLateness, latenessTime, latenessCountFreq);
                return parent.window(window);
            }

        }
    }

    public static class Processor {
        private final String processorType;
        private final String processorArgs;

        public Processor(String processorType, String processorArgs) {
            this.processorType = processorType;
            this.processorArgs = processorArgs;
        }

        @JsonProperty("processor_type")
        public String getProcessorType() {
            return processorType;
        }

        @JsonProperty("processor_args")
        public String getProcessorArgs() {
            return processorArgs;
        }
    }

    public static class Field {
        private final String field;
        private final String type;
        private final String origin;
        private final String description;
        private final boolean dimension;

        public Field(String field, String type, String origin, String description, boolean dimension) {
            this.field = field;
            this.type = type;
            this.origin = origin;
            this.description = description;
            this.dimension = dimension;
        }

        public String getField() {
            return field;
        }

        public String getType() {
            return type;
        }

        public String getOrigin() {
            return origin;
        }

        public String getDescription() {
            return description;
        }

        @JsonProperty("is_dimension")
        public boolean getDimension() {
            return dimension;
        }
    }

    public static class Builder {
        private String id;
        private String name;
        private Set<String> parents = new HashSet<>();
        private Window window;
        private Processor processor;
        private List<Field> fields = new ArrayList<>();

        private Builder() {
        }

        public Builder id(String id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder addParent(String parent) {
            parents.add(parent);
            return this;
        }

        public Set<String> getParents() {
            return parents;
        }

        private Builder window(Window window) {
            this.window = window;
            return this;
        }

        public Window.Builder window() {
            return new Window.Builder(this);
        }

        public Builder processor(Processor processor) {
            this.processor = processor;
            return this;
        }

        public Builder addField(Field field) {
            fields.add(field);
            return this;
        }

        public TableEntity create() {
            return new TableEntity(id, name, parents, window, processor, fields);
        }
    }
}
