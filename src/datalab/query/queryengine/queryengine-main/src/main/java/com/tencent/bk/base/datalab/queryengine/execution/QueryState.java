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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.bk.base.datalab.queryengine.execution;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

import java.util.Set;
import java.util.stream.Stream;

public enum QueryState {
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED(false),
    /**
     * Query is waiting for the required resources (beta).
     */
    WAITING_FOR_RESOURCES(false),
    /**
     * Query is being planned.
     */
    PLANNING(false),
    /**
     * Query execution is being started.
     */
    STARTING(false),
    /**
     * Query has at least one running task.
     */
    RUNNING(false),
    /**
     * Query is finishing (e.g. commit for autocommit queries)
     */
    FINISHING(false),
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED(true),
    /**
     * Query execution failed.
     */
    FAILED(true);

    public static final Set<QueryState> TERMINAL_QUERY_STATES = Stream.of(QueryState.values())
            .filter(QueryState::isDone)
            .collect(toImmutableSet());

    private final boolean doneState;

    QueryState(boolean doneState) {
        this.doneState = doneState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone() {
        return doneState;
    }
}
