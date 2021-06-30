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

package com.tencent.bk.base.dataflow.jobnavi.workflow;

import com.tencent.bk.base.dataflow.jobnavi.workflow.executor.AbstractNodeExecutor;
import com.tencent.bk.base.dataflow.jobnavi.workflow.executor.ExecutorFactory;
import com.tencent.bk.base.dataflow.jobnavi.workflow.executor.SubtaskExecutor;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.AbstractExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExecutionGraph;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.ExpStateExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.execution.SubtaskExecutionNode;
import com.tencent.bk.base.dataflow.jobnavi.workflow.optimize.OptimizeGraphStrategy;
import com.tencent.bk.base.dataflow.jobnavi.api.AbstractJobNaviTask;
import com.tencent.bk.base.dataflow.jobnavi.exception.NaviException;
import com.tencent.bk.base.dataflow.jobnavi.logging.ThreadLoggingFactory;
import com.tencent.bk.base.dataflow.jobnavi.state.TaskStatus;
import com.tencent.bk.base.dataflow.jobnavi.state.event.EventListener;
import com.tencent.bk.base.dataflow.jobnavi.state.event.TaskEvent;
import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.workflow.graph.job.JobGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;


public class WorkflowTask extends AbstractJobNaviTask {

    private static final Logger logger = ThreadLoggingFactory.getLogger(WorkflowTask.class);

    private Map<String, TaskStatus> status = new ConcurrentHashMap<>();
    private Map<String, List<String>> resultList = new ConcurrentHashMap<>();
    private Map<String, List<String>> stateInputList = new ConcurrentHashMap<>();
    private Map<String, Long> execIdMap = new ConcurrentHashMap<>();
    private final Map<String, Thread> taskMonitorThreads = new ConcurrentHashMap<>();
    private boolean isFinish = false;

    private ExecutionGraph executionGraph;
    private Long scheduleTime;
    private String scheduleId;
    private final Map<String, Object> savePoints = new HashMap<>();

    public void updateNodeStatus(String id, TaskStatus status) {
        this.status.put(id, status);
    }

    public String getScheduleId() {
        return scheduleId;
    }

    public Map<String, Long> getExecIdMap() {
        return execIdMap;
    }

    public void setExecIdMap(Map<String, Long> execIdMap) {
        this.execIdMap = execIdMap;
    }

    public Map<String, List<String>> getStateInputList() {
        return stateInputList;
    }

    public void setStateInputList(Map<String, List<String>> stateInputList) {
        this.stateInputList = stateInputList;
    }

    public boolean isFinish() {
        return isFinish;
    }

    public void setFinish(boolean isFinish) {
        this.isFinish = isFinish;
    }

    public Map<String, TaskStatus> getStatus() {
        return status;
    }

    public void setStatus(Map<String, TaskStatus> status) {
        this.status = status;
    }

    public Map<String, List<String>> getResultList() {
        return resultList;
    }

    public void setResultList(Map<String, List<String>> resultList) {
        this.resultList = resultList;
    }

    public Long getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public ExecutionGraph getExecutionGraph() {
        return executionGraph;
    }

    public void addMonitorThread(String id, Thread thread) {
        taskMonitorThreads.put(id, thread);
    }

    public void removeMonitorThread(String id) {
        taskMonitorThreads.remove(id);
    }

    @Override
    public void startTask(TaskEvent event) throws Throwable {
        scheduleId = event.getContext().getTaskInfo().getScheduleId();
        scheduleTime = event.getContext().getTaskInfo().getScheduleTime();
        String extraInfo = event.getContext().getTaskInfo().getExtraInfo();
        startGraph(extraInfo);
        while (!this.isFinish) {
            Thread.sleep(1000);
        }
        WorkFlowTaskResult result = getWorkflowResult();
        logger.info("workflow status is : " + result);
        delSavepoint();
        savePoints.clear();
        if (result.getTaskStatus() == TaskStatus.failed) {
            throw new NaviException(result.getErrorInfo());
        }
    }

    /**
     * start graph
     *
     * @param extraInfo
     * @throws Exception
     */
    public void startGraph(String extraInfo) throws Exception {
        if (getSavepoint() != null) {
            logger.info("start from savePoints");
            recoveryFromSavePoints(getSavepoint());
            startFromSavePoints();
            checkFinish();
        } else {
            logger.info("start from a new graph");
            JobGraph jobGraph = generateGraph(extraInfo);
            optimizeGraph(jobGraph);
            logger.info("create executionGraph successfully");
            if (executionGraph.getHead() == null) {
                throw new NaviException("can't find head subtask");
            }
            for (AbstractExecutionNode node : executionGraph.getHead()) {
                startNode(node);
            }
        }
    }

    private void recoveryFromSavePoints(Map<String, String> values) {
        String workflowTask = values.get("workflowTask");
        Map<String, Object> workflowMap = JsonUtils.readMap(workflowTask);
        Map<String, Object> executionGraphJson = (Map<String, Object>) workflowMap.get("executeGraph");
        Map<String, Object> nodeMapIndex = (Map<String, Object>) executionGraphJson.get("nodeMap");
        Map<String, AbstractExecutionNode> nodeMap = new HashMap<>();
        Map<String, List<String>> parentMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : nodeMapIndex.entrySet()) {
            Map<String, Object> nodeJson = (Map<String, Object>) entry.getValue();
            String id = (String) nodeJson.get("id");
            List<String> parents = new ArrayList<>();
            if (nodeJson.get("parents") != null) {
                parents = (List<String>) nodeJson.get("parents");
            }
            if (nodeJson.get("triggerRule") != null) {
                ExpStateExecutionNode expStateExecutionNode = new ExpStateExecutionNode();
                expStateExecutionNode.setId((String) nodeJson.get("id"));
                expStateExecutionNode.setTriggerRule(TriggerRule.valueOf((String) nodeJson.get("triggerRule")));
                expStateExecutionNode.setStateContent((Map<String, List<String>>) nodeJson.get("stateContent"));
                nodeMap.put(id, expStateExecutionNode);
            } else {
                SubtaskExecutionNode subtaskExecutionNode = new SubtaskExecutionNode();
                subtaskExecutionNode.setId((String) nodeJson.get("id"));
                subtaskExecutionNode.setTypeId((String) nodeJson.get("typeId"));
                if (nodeJson.get("subtaskInfo") != null) {
                    subtaskExecutionNode.setSubtaskInfo((String) nodeJson.get("subtaskInfo"));
                } else {
                    subtaskExecutionNode.setSubtaskInfo("");
                }
                if (nodeJson.get("scheduleId") != null) {
                    subtaskExecutionNode.setScheduleId((String) nodeJson.get("scheduleId"));
                } else {
                    subtaskExecutionNode.setScheduleId("");
                }
                if (nodeJson.get("retryQuota") != null) {
                    subtaskExecutionNode.setRetryQuota((Integer) nodeJson.get("retryQuota"));
                } else {
                    subtaskExecutionNode.setRetryQuota(0);
                }
                if (nodeJson.get("retryInterval") != null) {
                    subtaskExecutionNode.setRetryInterval((String) nodeJson.get("retryInterval"));
                } else {
                    subtaskExecutionNode.setRetryInterval("");
                }
                nodeMap.put(id, subtaskExecutionNode);
            }

            if (!parents.isEmpty()) {
                parentMap.put(id, parents);
            }
        }
        for (Map.Entry<String, List<String>> entry : parentMap.entrySet()) {
            addParents(entry.getKey(), entry.getValue(), nodeMap);
        }
        this.executionGraph = new ExecutionGraph();
        this.executionGraph.setNodeMap(nodeMap);
        List<AbstractExecutionNode> head = new ArrayList<>();
        for (Map.Entry<String, AbstractExecutionNode> entry : nodeMap.entrySet()) {
            if (entry.getValue().getParents().isEmpty()) {
                head.add(entry.getValue());
            }
        }
        this.executionGraph.setHead(head);
        if (executionGraphJson.get("configuration") != null) {
            this.executionGraph.setConfiguration((Map<String, Object>) executionGraphJson.get("configuration"));
        }
        if (executionGraphJson.get("parameter") != null) {
            this.executionGraph.setParameter((Map<String, Object>) executionGraphJson.get("parameter"));
        }
        Map<String, String> tmpStatus = (Map<String, String>) workflowMap.get("status");
        if (tmpStatus != null) {
            for (Map.Entry<String, String> entry : tmpStatus.entrySet()) {
                status.put(entry.getKey(), TaskStatus.values(entry.getValue()));
            }
        }
        resultList = (Map<String, List<String>>) workflowMap.get("resultList");
        stateInputList = (Map<String, List<String>>) workflowMap.get("stateInputList");
        Map<String, Integer> tmpExecIdMap = (Map<String, Integer>) workflowMap.get("execIdMap");
        if (tmpExecIdMap != null) {
            for (Map.Entry<String, Integer> entry : tmpExecIdMap.entrySet()) {
                execIdMap.put(entry.getKey(), entry.getValue().longValue());
            }
        }
    }

    public void checkFinish() {
        for (Map.Entry<String, TaskStatus> entry : status.entrySet()) {
            logger.info("status of node:" + entry.getKey() + " is " + entry.getValue());
            if (entry.getValue() == TaskStatus.preparing || entry.getValue() == TaskStatus.running
                    || entry.getValue() == TaskStatus.none) {
                return;
            }
        }
        logger.info("WorkflowTask [" + scheduleId + "]  finished");
        this.isFinish = true;
    }

    private void startFromSavePoints() throws Exception {
        for (AbstractExecutionNode node : executionGraph.getHead()) {
            logger.info("head:" + node.getId());
            checkForSavePoints(node);
        }
        for (AbstractExecutionNode node : executionGraph.getSavepoint()) {
            if (node instanceof SubtaskExecutionNode) {
                if (execIdMap.containsKey(node.getId())) {
                    SubtaskExecutor subtaskExecutor = new SubtaskExecutor(node);
                    subtaskExecutor.recovery(this, execIdMap.get(node.getId()));
                } else {
                    boolean start = true;
                    for (AbstractExecutionNode parentsNode : node.getParents()) {
                        if (resultList.get(parentsNode.getId()).isEmpty() || !resultList.get(parentsNode.getId())
                                .contains(node.getId())) {
                            start = false;
                            break;
                        }
                    }
                    if (start) {
                        startNode(node);
                    } else {
                        subtaskStartNext(node.getId(), TaskStatus.disabled);
                    }
                }
            } else if (node instanceof ExpStateExecutionNode) {
                startNode(node);
            }
        }
    }

    public JobGraph generateGraph(String info) throws NaviException {
        JobGraph jobGraph = new JobGraph();
        jobGraph.parseJson(info);
        logger.info("create jobGraph successfully");
        return jobGraph;
    }

    public void optimizeGraph(JobGraph jobGraph) throws Exception {
        OptimizeGraphStrategy optimizeGraphStrategy = new OptimizeGraphStrategy(jobGraph, this);
        this.executionGraph = optimizeGraphStrategy.optimizeGraph();
        this.savePoints.put("executeGraph", executionGraph.toHttpJson());
        initializeStatus();
    }

    private void initializeStatus() {
        for (Map.Entry<String, AbstractExecutionNode> entry : executionGraph.getNodeMap().entrySet()) {
            if (entry.getValue() instanceof SubtaskExecutionNode) {
                this.status.put(entry.getKey(), TaskStatus.none);
            }
        }
    }

    public void addStateInputList(String id, String inputId) {
        //mark state checking input node (parent subtask node)
        List<String> inputList;
        if (!stateInputList.containsKey(id)) {
            inputList = new ArrayList<>();
        } else {
            inputList = stateInputList.get(id);
        }
        inputList.add(inputId);
        stateInputList.put(id, inputList);
    }

    public synchronized boolean checkTrigger(String id) {
        if (!stateInputList.containsKey(id)) {
            return false;
        } else {
            for (AbstractExecutionNode node : executionGraph.getNodeMap().get(id).getParents()) {
                if (node instanceof SubtaskExecutionNode && !stateInputList.get(id).contains(node.getId())) {
                    return false;
                }
            }
            return true;
        }
    }

    public synchronized void updateExecIdMap(String nodeId, Long execId) {
        this.execIdMap.put(nodeId, execId);
    }

    /**
     * start child node of subtask node
     *
     * @param id
     * @param status
     */
    public synchronized void subtaskStartNext(String id, TaskStatus status) {
        updateNodeStatus(id, status);
        checkTaskStatus(status);
        logger.info("finish subtask: " + id + " ,status is " + status.name());
        try {
            doWorkflowSavePoints();
        } catch (Exception e) {
            logger.error("fail to do savepoint", e);
        }
        logger.info("try to get children node");
        List<AbstractExecutionNode> children = executionGraph.getNodeMap().get(id).getChildren();
        if (children.isEmpty()) {
            //reach leaf node, check workflow status
            checkFinish();
        } else {
            for (AbstractExecutionNode child : children) {
                try {
                    logger.info("start next node " + child.getId());
                    if (child instanceof ExpStateExecutionNode) {
                        addStateInputList(child.getId(), id);
                        try {
                            doWorkflowSavePoints();
                        } catch (Exception e) {
                            logger.error("fail to do savepoint", e);
                        }
                    }
                    startNode(child);
                } catch (Exception e) {
                    logger.error("can't start node " + child.getId(), e);
                }
            }
        }
    }

    private void checkTaskStatus(TaskStatus status) {
        if (status == TaskStatus.killed) {
            //kill the whole workflow when killed event received
            for (Thread d : taskMonitorThreads.values()) {
                d.interrupt();
            }
            isFinish = true;
        }
    }

    /**
     * start child node of state node
     *
     * @param id
     * @param stateResultList
     * @throws Exception
     */
    public synchronized void stateStartNext(String id, List<String> stateResultList) throws Exception {
        logger.info("Finish state node " + id);
        if (stateResultList.isEmpty()) {
            checkFinish();
        } else {
            resultList.put(id, stateResultList);
        }
        try {
            doWorkflowSavePoints();
        } catch (Exception e) {
            logger.error("fail to do savepoint", e);
        }
        for (AbstractExecutionNode node : this.executionGraph.getNodeMap().get(id).getChildren()) {
            if (node instanceof SubtaskExecutionNode && !stateResultList.contains(node.getId())) {
                //parent nodes result does not satisfy dependence
                subtaskStartNext(node.getId(), TaskStatus.disabled);
            } else {
                startNode(node);
            }
        }
    }

    private void checkForSavePoints(AbstractExecutionNode node) {
        if (node instanceof SubtaskExecutionNode) {
            if (status.get(node.getId()) != TaskStatus.none && status.get(node.getId()) != TaskStatus.preparing
                    && status.get(node.getId()) != TaskStatus.running
                    && !(status.get(node.getId()) == TaskStatus.failed
                    && ((SubtaskExecutionNode) node).getRetryQuota() > 0)) {
                List<AbstractExecutionNode> children = node.getChildren();
                if (!children.isEmpty()) {
                    for (AbstractExecutionNode childNode : children) {
                        checkForSavePoints(childNode);
                    }
                }
            } else {
                //start from subtask node which is not finished
                List<AbstractExecutionNode> savePoints = this.executionGraph.getSavepoint();
                logger.info("add node:" + node.getId() + " to save points");
                savePoints.add(node);
            }
        } else if (node instanceof ExpStateExecutionNode) {
            if (resultList.containsKey(node.getId()) || checkTrigger(node.getId())) {
                List<AbstractExecutionNode> children = node.getChildren();
                if (!children.isEmpty()) {
                    for (AbstractExecutionNode childNode : children) {
                        checkForSavePoints(childNode);
                    }
                }
            } else {
                //start from state node which is not check
                List<AbstractExecutionNode> savePoints = this.executionGraph.getSavepoint();
                logger.info("add node:" + node.getId() + " to save points");
                savePoints.add(node);
            }
        }
    }

    /**
     * start execution node
     *
     * @param node
     * @throws Exception
     */
    public void startNode(AbstractExecutionNode node) throws Exception {
        //TODO: if all the nodes can't start,the process will be running forever
        try {
            logger.info("start node " + node.getId());
            AbstractNodeExecutor nodeExecutor = ExecutorFactory.getNodeExecutor(node);
            logger.info("find executor:" + nodeExecutor.getClass().getSimpleName());
            if (!nodeExecutor.ifTrigger(this)) {
                logger.info("can't start node");
                return;
            }
            logger.info("can start node");
            nodeExecutor.start(this);
        } catch (Exception e) {
            logger.error("Failed to start node, detail:" + e);
            setFinish(true);
            for (Map.Entry<String, TaskStatus> entry : this.status.entrySet()) {
                entry.setValue(TaskStatus.failed);
            }
        }
    }

    private WorkFlowTaskResult getWorkflowResult() {
        WorkFlowTaskResult result = new WorkFlowTaskResult();
        for (Map.Entry<String, TaskStatus> entry : this.status.entrySet()) {
            logger.info("node:" + entry.getKey() + ", status:" + entry.getValue());
            if (entry.getValue() == TaskStatus.killed) {
                result.setTaskStatus(TaskStatus.failed);
                result.setErrorInfo("workflow execute failed because " + entry.getKey() + " has been killed.");
                return result;
            }

            if (entry.getValue() == TaskStatus.failed) {
                if (executionGraph.getNodeMap().get(entry.getKey()).getChildren() == null) {
                    result.setTaskStatus(TaskStatus.failed);
                    result.setErrorInfo("workflow execute failed because " + entry.getKey() + " failed.");
                } else {
                    boolean triggerChild = false;
                    for (AbstractExecutionNode node : executionGraph.getNodeMap().get(entry.getKey()).getChildren()) {
                        if (resultList.containsKey(node.getId())) {
                            //satisfied child dependence
                            triggerChild = true;
                            break;
                        }
                    }
                    if (!triggerChild) {
                        result.setTaskStatus(TaskStatus.failed);
                        result.setErrorInfo("workflow execute failed because " + entry.getKey() + " failed.");
                    }
                }
            }
        }
        return result;
    }

    public void doWorkflowSavePoints() throws Exception {
        this.savePoints.put("executeGraph", executionGraph.toHttpJson());
        this.savePoints.put("status", this.status);
        this.savePoints.put("resultList", this.resultList);
        this.savePoints.put("stateInputList", this.stateInputList);
        this.savePoints.put("execIdMap", this.execIdMap);
        Map<String, String> workflowSavePoints = new HashMap<>();
        workflowSavePoints.put("workflowTask", JsonUtils.writeValueAsString(this.savePoints));
        doSavepoint(workflowSavePoints);
    }

    private void addParents(String nodeId, List<String> parents, Map<String, AbstractExecutionNode> nodeMap) {
        List<AbstractExecutionNode> parentsList = new ArrayList<>();
        for (String id : parents) {
            List<AbstractExecutionNode> childrenList = nodeMap.get(id).getChildren();
            childrenList.add(nodeMap.get(nodeId));
            nodeMap.get(id).setChildren(childrenList);
            parentsList.add(nodeMap.get(id));
        }
        nodeMap.get(nodeId).setParents(parentsList);

    }

    @Override
    public EventListener getEventListener(String eventName) {
        return null;
    }
}
