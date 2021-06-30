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

package com.tencent.bk.base.dataflow.core.pipeline;

import com.tencent.bk.base.dataflow.core.topo.Topology;
import com.tencent.bk.base.dataflow.core.runtime.AbstractDefaultRuntime;
import com.tencent.bk.base.dataflow.core.runtime.IRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDefaultPipeline<TopologyT extends Topology, RuntimeT extends AbstractDefaultRuntime>
        implements IPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractDefaultPipeline.class);
    private Topology topology;
    private IRuntime runtime;

    public AbstractDefaultPipeline(Topology topology) {
        this.topology = topology;
        this.runtime = this.createRuntime();
        LOGGER.info("The topology is: " + topology.serialize());
    }

    public TopologyT getTopology() {
        return (TopologyT) topology;
    }

    public RuntimeT getRuntime() {
        return (RuntimeT) runtime;
    }

    public abstract RuntimeT createRuntime();

    public void prepare() {
    }

    public abstract void source();

    public abstract void transform();

    public abstract void sink();
}
