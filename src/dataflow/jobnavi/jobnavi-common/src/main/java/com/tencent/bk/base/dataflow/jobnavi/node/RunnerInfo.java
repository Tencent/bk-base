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

package com.tencent.bk.base.dataflow.jobnavi.node;

import com.tencent.bk.base.dataflow.jobnavi.util.http.JsonUtils;
import com.tencent.bk.base.dataflow.jobnavi.state.event.DecommissionEventResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RunnerInfo {

    private String runnerId;
    private Double cpuUsage;
    private Double cpuAvgLoad;
    private Long memory;
    private Double memoryUsage;
    private String host;
    private Integer port;
    private Integer healthPort;
    private Integer maxTaskNum;
    private Integer taskNum;
    private Integer maxThreadTaskNum;
    private Integer taskThreadNum;
    private Set<Long> execIdSet;
    private RunnerStatus status = RunnerStatus.lost;
    private Map<Long, DecommissionEventResult> decommissioningTasks;
    private Set<String> labelSet;

    public String getRunnerId() {
        //use Runner host as Runner ID if Runner ID is null
        return ((runnerId != null) && !runnerId.isEmpty()) ? runnerId : host;
    }

    public void setRunnerId(String runnerId) {
        this.runnerId = runnerId;
    }

    public Double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(Double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }

    public Double getCpuAvgLoad() {
        return cpuAvgLoad;
    }

    public void setCpuAvgLoad(Double cpuAvgLoad) {
        this.cpuAvgLoad = cpuAvgLoad;
    }

    public Double getMemoryUsage() {
        return memoryUsage;
    }

    public void setMemoryUsage(Double memoryUsage) {
        this.memoryUsage = memoryUsage;
    }

    public Long getMemory() {
        return memory;
    }

    /**
     * set Memory, unit is MB
     *
     * @param memory
     */
    public void setMemory(Long memory) {
        this.memory = memory;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public Integer getHealthPort() {
        return healthPort;
    }

    public void setHealthPort(Integer healthPort) {
        this.healthPort = healthPort;
    }

    public Integer getMaxTaskNum() {
        return maxTaskNum;
    }

    public void setMaxTaskNum(Integer maxTaskNum) {
        this.maxTaskNum = maxTaskNum;
    }

    public Integer getTaskNum() {
        return taskNum;
    }

    public void setTaskNum(Integer taskNum) {
        this.taskNum = taskNum;
    }

    public Integer getMaxThreadTaskNum() {
        return maxThreadTaskNum;
    }

    public void setMaxThreadTaskNum(Integer maxThreadTaskNum) {
        this.maxThreadTaskNum = maxThreadTaskNum;
    }

    public Integer getTaskThreadNum() {
        return taskThreadNum;
    }

    public void setTaskThreadNum(Integer taskThreadNum) {
        this.taskThreadNum = taskThreadNum;
    }

    public Set<Long> getExecIdSet() {
        return execIdSet;
    }

    public void setExecIdSet(Set<Long> execIdSet) {
        this.execIdSet = execIdSet;
    }

    public RunnerStatus getStatus() {
        return status;
    }

    public void setStatus(RunnerStatus status) {
        this.status = status;
    }

    public Map<Long, DecommissionEventResult> getDecommissioningTasks() {
        return decommissioningTasks;
    }

    public void setDecommissioningTasks(Map<Long, DecommissionEventResult> decommissioningTasks) {
        this.decommissioningTasks = decommissioningTasks;
    }

    public Set<String> getLabelSet() {
        return labelSet;
    }

    public void setLabelSet(Set<String> labelSet) {
        this.labelSet = labelSet;
    }

    /**
     * parse runner info from json string
     *
     * @param params
     */
    public void parseJson(Map<String, Object> params) {
        if (params.get("runnerId") != null) {
            runnerId = (String) params.get("runnerId");
        }
        if (params.get("cpuUsage") != null) {
            cpuUsage = (Double) params.get("cpuUsage");
        }
        if (params.get("cpuAvgLoad") != null) {
            cpuAvgLoad = (Double) params.get("cpuAvgLoad");
        }
        if (params.get("memory") != null) {
            memory = Long.parseLong(params.get("memory").toString());
        }
        if (params.get("memoryUsage") != null) {
            memoryUsage = (Double) params.get("memoryUsage");
        }
        if (params.get("host") != null) {
            host = (String) params.get("host");
        }
        if (params.get("port") != null) {
            port = (Integer) params.get("port");
        }
        if (params.get("healthPort") != null) {
            healthPort = (Integer) params.get("healthPort");
        }
        if (params.get("maxTaskNum") != null) {
            maxTaskNum = (Integer) params.get("maxTaskNum");
        }
        if (params.get("taskNum") != null) {
            taskNum = (Integer) params.get("taskNum");
        }
        if (params.get("maxThreadTaskNum") != null) {
            maxThreadTaskNum = (Integer) params.get("maxThreadTaskNum");
        }
        if (params.get("taskThreadNum") != null) {
            taskThreadNum = (Integer) params.get("taskThreadNum");
        }
        if (params.get("execIdSet") != null) {
            List<Object> execIdList = (List<Object>) params.get("execIdSet");
            execIdSet = new HashSet<>();
            for (Object execId : execIdList) {
                execIdSet.add(Long.parseLong(execId.toString()));
            }
        }
        if (params.get("status") != null) {
            status = RunnerStatus.valueOf((String) params.get("status"));
        }
        if (params.get("decommissioningTasks") != null) {
            decommissioningTasks = (Map<Long, DecommissionEventResult>) params.get("decommissioningTasks");
        }
        if (params.get("labelSet") != null) {
            List<Object> labelList = (List<Object>) params.get("labelSet");
            labelSet = new HashSet<>();
            for (Object label : labelList) {
                labelSet.add((String) label);
            }
        }
    }

    public String getQuotaString() {
        return "runnerId:" + getRunnerId() + " "
                + "host:" + host + " "
                + "port:" + port + " "
                + "status:" + status + " "
                + "cpuUsage:" + cpuUsage + " "
                + "cpuAvgLoad:" + cpuAvgLoad + " "
                + "memory:" + memory + " "
                + "memoryUsage:" + memoryUsage + " "
                + "maxTaskNum:" + maxTaskNum + " "
                + "taskNum:" + taskNum + " "
                + "maxThreadTaskNum:" + maxThreadTaskNum + " "
                + "taskThreadNum:" + taskThreadNum + " "
                + "labelSet:" + labelSet;
    }

    /**
     * get runner info digest
     *
     * @return runner info digest
     */
    public Map<String, Object> digest() {
        Map<String, Object> digest = new HashMap<>();
        digest.put("runnerId", getRunnerId());
        digest.put("host", host);
        digest.put("port", port);
        digest.put("status", status);
        digest.put("cpuUsage", cpuUsage);
        digest.put("cpuAvgLoad", cpuAvgLoad);
        digest.put("memory", memory);
        digest.put("memoryUsage", memoryUsage);
        digest.put("taskNum", taskNum);
        digest.put("maxTaskNum", maxTaskNum);
        digest.put("threadTaskNum", taskThreadNum);
        digest.put("maxThreadTaskNum", maxThreadTaskNum);
        digest.put("decommissioningTaskNumb", decommissioningTasks.size());
        digest.put("timestamp", System.currentTimeMillis());
        digest.put("labelSet", getLabelSet());
        return digest;
    }

    public String toJson() {
        return JsonUtils.writeValueAsString(this);
    }
}
