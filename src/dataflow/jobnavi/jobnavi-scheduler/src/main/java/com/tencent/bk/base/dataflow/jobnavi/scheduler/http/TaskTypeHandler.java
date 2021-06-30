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

package com.tencent.bk.base.dataflow.jobnavi.scheduler.http;

import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.AbstractJobDao;
import com.tencent.bk.base.dataflow.jobnavi.scheduler.metadata.MetaDataManager;
import com.tencent.bk.base.dataflow.jobnavi.state.Language;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskMode;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskType;
import com.tencent.bk.base.dataflow.jobnavi.util.http.AbstractHttpHandler;
import com.tencent.bk.base.dataflow.jobnavi.util.http.HttpReturnCode;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.glassfish.grizzly.http.server.Request;
import org.glassfish.grizzly.http.server.Response;

public class TaskTypeHandler extends AbstractHttpHandler {

    private static final Logger logger = Logger.getLogger(TaskTypeHandler.class);

    @Override
    public void doPost(Request request, Response response) throws Exception {
        Map<String, Object> params = JsonUtils.readMap(request.getInputStream());
        String operate = (String) params.get("operate");
        AbstractJobDao dao = MetaDataManager.getJobDao();
        try {
            String typeId = (String) params.get("type_id");
            String tag = (String) params.get("tag");
            switch (operate) {
                case "create_type":
                    TaskType taskType = new TaskType();
                    taskType.setMain((String) params.get("main"));
                    taskType.setEnv((String) params.get("env"));
                    taskType.setSysEnv((String) params.get("sys_env"));
                    taskType.setLanguage(Language.valueOf((String) params.get("language")));
                    taskType.setTaskMode(TaskMode.valueOf((String) params.get("task_mode")));
                    taskType.setRecoverable(params.get("recoverable") != null && (boolean) params.get("recoverable"));
                    taskType.setCreatedBy((String) params.get("created_by"));
                    taskType.setDescription((String) params.get("description"));
                    dao.addTaskType(taskType);
                    break;
                case "delete_type":
                    dao.deleteTaskType(typeId, tag);
                    break;
                case "create_tag_alias":
                    String alias = (String) params.get("alias");
                    String description = (String) params.get("description");
                    dao.addTaskTypeTagAlias(typeId, tag, alias, description);
                    break;
                case "delete_tag_alias":
                    alias = (String) params.get("alias");
                    dao.deleteTaskTypeTagAlias(typeId, tag, alias);
                    break;
                case "create_default_tag":
                    String nodeLabel = (String) params.get("node_label");
                    String defaultTag = (String) params.get("default_tag");
                    dao.addTaskTypeDefaultTag(typeId, nodeLabel, defaultTag);
                    break;
                case "delete_default_tag":
                    nodeLabel = (String) params.get("node_label");
                    dao.deleteTaskTypeDefaultTag(typeId, nodeLabel);
                    break;
                default:
                    throw new IllegalArgumentException("operate " + operate + " is not support.");
            }
        } catch (Throwable e) {
            logger.error(e);
            writeData(false, e.getMessage(), response);
        }
        writeData(true, "", response);
    }

    @Override
    public void doGet(Request request, Response response) throws Exception {
        String operate = request.getParameter("operate");
        AbstractJobDao dao = MetaDataManager.getJobDao();
        try {
            switch (operate) {
                case "list_type":
                    Map<String, Map<String, TaskType>> taskTypeInfo = dao.listTaskType();
                    List<Map<String, Object>> typeList = new ArrayList<>();
                    for (Map.Entry<String, Map<String, TaskType>> typeEntry : taskTypeInfo.entrySet()) {
                        for (Map.Entry<String, TaskType> tagEntry : typeEntry.getValue().entrySet()) {
                            String tag = tagEntry.getKey();
                            TaskType taskType = tagEntry.getValue();
                            if (tag.equals(taskType.getTag())) {
                                Map<String, Object> data = new HashMap<>();
                                data.put("type_id", taskType.getName());
                                data.put("tag", taskType.getTag());
                                data.put("main", taskType.getMain());
                                data.put("description", taskType.getDescription());
                                data.put("env", taskType.getEnv());
                                data.put("sys_env", taskType.getSysEnv());
                                data.put("language", taskType.getLanguage());
                                data.put("task_mode", taskType.getTaskMode());
                                data.put("recoverable", taskType.isRecoverable());
                                typeList.add(data);
                            }
                        }
                    }
                    writeData(true, "list task type succeeded", HttpReturnCode.DEFAULT,
                            JsonUtils.writeValueAsString(typeList), response);
                    break;
                case "retrieve_tag_alias":
                    String typeId = request.getParameter("type_id");
                    String tag = request.getParameter("tag");
                    List<String> aliasList = dao.queryTaskTypeTagAlias(typeId, tag);
                    writeData(true, "list task tag alias succeeded", HttpReturnCode.DEFAULT,
                            JsonUtils.writeValueAsString(aliasList), response);
                    break;
                case "retrieve_default_tag":
                    typeId = request.getParameter("type_id");
                    String nodeLabel = request.getParameter("node_label");
                    String defaultTag = dao.queryTaskTypeDefaultTag(typeId, nodeLabel);
                    writeData(true, "list task tag alias succeeded", HttpReturnCode.DEFAULT, defaultTag, response);
                    break;
                default:
                    throw new IllegalArgumentException("operate " + operate + " is not support.");
            }
        } catch (Throwable e) {
            logger.error(e);
            writeData(false, e.getMessage(), response);
        }
    }
}
