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

package com.tencent.bk.base.dataflow.flink.streaming.checkpoint;

import com.tencent.bk.base.dataflow.flink.streaming.checkpoint.types.AbstractCheckpointKey;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CheckpointValue implements Serializable {

    private Map<AbstractCheckpointKey, OutputCheckpoint> updatedCheckpoints = new HashMap<>();

    public void setUpdatedCheckpoints(AbstractCheckpointKey checkpointKey, OutputCheckpoint updatedCheckpoint) {
        updatedCheckpoints.put(checkpointKey, updatedCheckpoint);
    }

    public OutputCheckpoint getUpdatedCheckpoint(AbstractCheckpointKey checkpointKey) {
        return updatedCheckpoints.get(checkpointKey);
    }

    public Set<Map.Entry<AbstractCheckpointKey, OutputCheckpoint>> iterable() {
        return updatedCheckpoints.entrySet();
    }

    public static class OutputCheckpoint implements Comparable<OutputCheckpoint>, Serializable {

        private static final long serialVersionUID = 1L;

        private Long pointValue = -1L;
        private Integer index = null;

        /**
         * 创建OutputCheckpoint
         *
         * @param str db的值
         * @return
         */
        public static OutputCheckpoint buildFromDbStr(String str) {
            OutputCheckpoint outputCheckpoint = new OutputCheckpoint();
            if (null != str) {
                String[] values = str.split("_");
                if (values.length > 0) {
                    outputCheckpoint.setPointValue(Long.valueOf(values[0]));
                }
                if (values.length > 1) {
                    outputCheckpoint.setIndex(Integer.valueOf(values[1]));
                }
            }
            return outputCheckpoint;
        }

        public Long getPointValue() {
            return pointValue;
        }

        public void setPointValue(Long value) {
            this.pointValue = value;
        }

        public Integer getIndex() {
            return index;
        }

        public void setIndex(Integer index) {
            this.index = index;
        }

        /**
         * OutputCheckpoint减法处理
         *
         * @param subtraction 减去值
         * @return OutputCheckpoint
         */
        public OutputCheckpoint subtraction(int subtraction) {
            OutputCheckpoint outputCheckpoint = new OutputCheckpoint();
            outputCheckpoint.pointValue = this.getPointValue();
            outputCheckpoint.index = this.getIndex();
            if (null != outputCheckpoint.index) {
                // index暂时不进行加减处理
                // outputCheckpoint.index -= subtraction;
            } else {
                outputCheckpoint.pointValue -= subtraction;
            }
            return outputCheckpoint;
        }

        /**
         * 生成存储DB的符号字段
         *
         * @return 如果是offset值格式为：[offset]_[avroIndex];如果是时间为：10位的时间戳
         */
        public String toDbValue() {
            if (null != index && index >= 0) {
                return pointValue + "_" + index;
            }
            return String.valueOf(pointValue);
        }

        @Override
        public int compareTo(OutputCheckpoint obj) {
            if (this.getPointValue() > obj.getPointValue()) {
                return 1;
            } else if (this.getPointValue().equals(obj.getPointValue())) {
                if (null != this.getIndex() && null != obj.getIndex()) {
                    if (this.getIndex() > obj.getIndex()) {
                        return 1;
                    } else if (this.getIndex().equals(obj.getIndex())) {
                        return 0;
                    } else {
                        /*当this小于obj时返回负数*/
                        return -1;
                    }
                } else {
                    return 0;
                }
            } else {
                /*当this小于obj时返回负数*/
                return -1;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof OutputCheckpoint) {
                OutputCheckpoint checkpointObj = (OutputCheckpoint) obj;
                return this.getPointValue().equals(checkpointObj.getPointValue())
                        && null != this.getIndex() && null != checkpointObj.getIndex()
                        && this.getIndex().equals(checkpointObj.getIndex());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return (index == null ? 0 : index.hashCode()) + pointValue.hashCode() * 13;
        }

        @Override
        public String toString() {
            return "{pointValue=" + pointValue + ", index=" + index + "}";
        }
    }
}
