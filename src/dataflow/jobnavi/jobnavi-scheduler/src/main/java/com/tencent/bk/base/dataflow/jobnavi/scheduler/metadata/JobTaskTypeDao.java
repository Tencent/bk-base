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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata;

import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import java.util.List;
import java.util.Map;

public interface JobTaskTypeDao {

    void addTaskType(TaskType taskType) throws NaviException;

    void deleteTaskType(String typeId, String typeTag) throws NaviException;

    Map<String, Map<String, TaskType>> listTaskType() throws NaviException;

    TaskType queryTaskType(String typeId, String typeTag, String nodeLabel) throws NaviException;

    void addTaskTypeTagAlias(String typeId, String typeTag, String alias, String description)
            throws NaviException;

    void deleteTaskTypeTagAlias(String typeId, String typeTag, String alias) throws NaviException;

    List<String> queryTaskTypeTagAlias(String typeId, String typeTag) throws NaviException;

    void addTaskTypeDefaultTag(String typeId, String nodeLabel, String defaultTag) throws NaviException;

    void deleteTaskTypeDefaultTag(String typeId, String nodeLabel) throws NaviException;

    Map<String, String> listTaskTypeDefaultTag() throws NaviException;

    String queryTaskTypeDefaultTag(String typeId, String nodeLabel) throws NaviException;
}
