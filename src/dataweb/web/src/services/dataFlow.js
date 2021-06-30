/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

/* eslint-disable no-param-reassign */
import Meta from '../controller/meta';
import DataflowV2 from '../controller/Dataflow.v2.js';
import { getProjectList } from './meta';
import { bkRequest } from '@/common/js/ajax';

const meta = new Meta();

/** 根据项目ID获取任务信息
 * @param (query): project_id
 */
const getTaskByProjectId = {
  url: '/v3/dataflow/flow/flows/',
};

/** 根据项目ID获取任务信息
 * @param (params): fid
 */
const getFlowInfoByFlowId = {
  url: '/v3/dataflow/flow/flows/:fid/',
};

/** 获取项目统计信息
 * @param (query) project_id
 */
const getProjectSummary = {
  url: '/v3/dataflow/flow/flows/projects/count/',
};

/** 获取关联任务
 * @pram (params): rtid
 */
const listRelaTaskByRtid = {
  // url: '/result_tables/:rtid/list_rela_by_rtid/'
  url: '/v3/dataflow/flow/flows/list_rela_by_rtid/',
};

/** 获取流程画布信息 */
const getFlowGraph = {
  url: 'flows/:flowId/get_graph/',
  callback(res) {
    if (res.result) {
      Object.assign(
        res.data.locations,
        res.data.locations.map((loc) => {
          const copyLoc = JSON.parse(JSON.stringify(loc));
          loc.config = copyLoc.node_config;
          loc.name = copyLoc.node_name;
          loc.type = copyLoc.node_type;
          // eslint-disable-next-line prefer-destructuring
          loc.output = copyLoc.result_table_ids[0];
          return loc;
        }),
      );
    }
    return res;
  },
};

/** 获取当前项目下面有权限的业务列表 */
const getBizListByProjectId = {
  url: '/v3/auth/projects/:pid/bizs/',
  callback(res, options) {
    return Object.assign(res, {
      data: meta.formatBizsList(res.data, options),
    });
  },
};

/** 获取指定项目下面有权限的业务数据 */
const getBizResultList = {
  url: '/projects/:pid/list_rt_as_source/?source_type=:source_type&bk_biz_id=:bk_biz_id',
};

/** 批量启动项目下任务 */
const batchStartTaskList = {
  url: 'v3/dataflow/flow/flows/multi_start/',
  method: 'POST',
};

/** 批量停止项目下任务 */
const batchStopTaskList = {
  url: '/v3/dataflow/flow/flows/multi_stop/',
  method: 'POST',
};

/** 批量删除项目下任务 */
const batchDeleteTaskList = {
  url: '/v3/dataflow/flow/flows/multi_destroy/',
  method: 'delete',
};

/**
 * 获取计算节点信息
 * @query result_table_id
 */
const getProcessingNodes = {
  url: '/v3/dataflow/flow/nodes/processing_nodes/',
};

/**
 * 申请补算任务
 *  @apiParam {list} node_ids
    @apiParam {bool} with_child
    @apiParam {string} start_time
    @apiParam {string} end_time
 */
const applyComplementTask = {
  url: 'v3/dataflow/flow/flows/:fid/custom_calculates/apply/',
  method: 'POST',
};

/* 撤销补算任务的申请 */

const canceleApplication = {
  url: 'v3/dataflow/flow/flows/:fid/custom_calculates/cancel/',
  method: 'POST',
};

/* 提交补算任务 */

const submitComplementTask = {
  url: 'v3/dataflow/flow/flows/:fid/custom_calculates/start/',
  method: 'POST',
};

/* 停止补算任务*/

const stopComplementTask = {
  url: 'v3/dataflow/flow/flows/:fid/custom_calculates/stop/',
  method: 'POST',
};

/**
 * 获取计算集群组
 * @param (params): fid
 */
const getComputClusterList = {
  url: 'v3/dataflow/flow/flows/:fid/cluster_groups/',
};

/** 获取连线规则 */
const getLinkRulesConfig = {
  url: '/v3/dataflow/flow/flows/link_rules_info/',
};

/** 获取连线规则（V2版本） */
const getNodeLinkRulesV2 = {
  url: '/v3/dataflow/flow/node_link_rules_v2/',
};

/** 获取节点配置 */
const getNodeTypeConfigs = {
  url: '/v3/dataflow/flow/node_type_config_v2/',
  callback(resp) {
    resp.data = resp.data && new DataflowV2(resp.data).setNodeConfig(resp.data);
    return resp;
  },
};

/** 获取执行历史 */
const getDeployHistory = {
  url: '/v3/dataflow/flow/flows/:flowId/deploy_data/',
};

/** 导出dataflow */
const exportDataFlow = {
  url: '/v3/dataflow/flow/flows/:fid/export/',
};

/** 导入dataflow */
const importDataFlow = {
  url: '/v3/dataflow/flow/flows/:fid/create',
  method: 'POST',
};

/** 获取字段类型 */
const getFieldTypeConfig = {
  url: '/v3/dataflow/flow/field_type_configs/',
};

/** 获取sdk节点的输出类型列表 */
const getSdkTypeList = getFieldTypeConfig;

/** 获取画布信息 | 节点基础信息、连线信息 */
const getGraphInfo = {
  url: '/v3/dataflow/flow/flows/:fid/versions/draft/',
  callback(res) {
    if (res.result) {
      const flowId = res.data && res.data.locations.length && res.data.locations[0].flow_id;
      return bkRequest
        .httpRequest('dataFlow/getCatchData', {
          query: {
            key: `graph_data_${flowId}`,
          },
        })
        .then((cacheData) => {
          if (cacheData.result) {
            const formatData = DataflowV2.formatGraphSchema(res);
            const nodeIds = formatData.data.locations.map(node => node.id);
            cacheData.data.locations.forEach((node) => {
              if (!nodeIds.includes(node.id)) {
                formatData.data.locations.push(node);
                nodeIds.push(node.id);
              }
            });
            cacheData.data.lines.forEach((line) => {
              const existLine = formatData.data.lines
                .find(item => item.source.id === line.source.id && item.target.id === line.target.id);
              if (!existLine && nodeIds.includes(line.target.id) && nodeIds.includes(line.source.id)) {
                formatData.data.lines.push(line);
              }
            });
            return Object.assign(formatData);
          }
          return DataflowV2.formatGraphSchema(res);
        });
    }
    return DataflowV2.formatGraphSchema(res);
  },
};

/**
 * 获取节点信息
 */
const getNodeConfigInfo = {
  url: '/v3/dataflow/flow/flows/:fid/nodes/:node_id/',
};

/** 提交数据补齐
 *  @apiParamExample {json}
        {
            "target_schedule_time": "2018-12-12 10:00:00",
            "source_schedule_time": "2018-12-12 11:00:00"
        }
 */
const submitDataMakeUp = {
  url: '/v3/dataflow/flow/flows/:fid/nodes/:nid/data_makeup/',
  method: 'POST',
};

/** 离线计算节点查询执行记录
*   @apiParamExample {json}
        {
            "start_time": "2018-12-12 11:00:00",
            "end_time": "2018-12-12 11:00:00",
        }
*/
const getExecuteRecord = {
  url: '/v3/dataflow/flow/flows/:fid/nodes/:nid/data_makeup/status_list/',
};

/** 查看补齐结果
 * @apiParamExample {json}
        {
            "schedule_time": "2018-12-12 11:00:00"
        }
    @apiSuccessExample {json} 成功返回
        {
            "allowed_data_makeup": True
        }
 */
const getMakeUpResult = {
  url: '/v3/dataflow/flow/flows/:fid/nodes/:nid/data_makeup/check_execution/',
};

/** 根据SQL自动生成自依赖字段 */
const autoGenerateSQL = {
  url: '/v3/dataflow/batch/data_makeup/sql_column/',
  method: 'POST',
};

/**
 * 获取输入rt的lastMessage
 * @query result_table_id
 * @query node_type
 */
const getRtLastMsg = {
  url: '/v3/dataflow/flow/get_latest_msg/',
};

/**
 * 获取节点编辑器的示例代码
 * @query programming_language(java/python)
 * @query node_type
 */
const getDefaultCode = {
  url: '/v3/dataflow/flow/code_frame/',
};
/** 获取我的项目列表
 */
const getMyProjectList = {
  url: 'projects/list_my_projects/',
};

/** 创建新任务
 * @param (query): project_id
 */
const createNewTask = {
  url: '/v3/dataflow/flow/flows/',
  method: 'POST',
};

/** 删除flow */
const deleteTask = {
  url: 'v3/dataflow/flow/flows/:flowId/',
  method: 'DELETE',
};
/**
 * 获取ModelApp节点的输入模型列表
 */
const getInputModelList = {
  url: '/v3/dataflow/modeling/models/get_user_model_release/',
};

/**
 * 获取ModelApp节点的输入模型列表
 */
const getMyModelList = {
  url: '/v3/dataflow/modeling/models/get_update_model_release/',
};

/**
 * 创建新的mlsql模型
 */

const createModel = {
  url: '/v3/dataflow/modeling/models/',
  method: 'POST',
};

/**
 * 更新mlsql模型
 */
const updateModel = {
  url: '/v3/dataflow/modeling/models/:model_name/',
  method: 'PUT',
};

/**
 * 查看mlsql模型发布结果
 * @query task_id: 创建/更新模型对应的task_id
 */
const getModelReleaseResult = {
  url: '/v3/dataflow/modeling/models/release_result/',
};

/**
 * 查看MLSQL节点更新状态
 * @query processing_id
 * @query model_id
 */
const getModelUpdateStatus = {
  url: '/v3/dataflow/modeling/models/check_model_update/',
};

/**
 * 更新模型配置
 * @param processing_id
 * @param model_id
 */
const updateModelConfig = {
  url: 'v3/dataflow/modeling/models/update_model_info/',
  method: 'POST',
};
/**
 * 检查是否符合模型发布条件
 * @params last_sql: sql
 */
const releaseInspection = {
  url: '/v3/dataflow/modeling/models/release_inspection/',
  method: 'POST',
};

const checkModelingName = {
  url: '/v3/dataflow/modeling/models/:modelName/',
  callback(res) {
    return res.data && res.data.length === 0;
  },
};

/** 获取已发布模型列表
 * @query project_id
 */
const getReleasedModelList = {
  url: '/v3/datamanage/datamodel/instances/released_models/',
};

/** 获取模型发布版本对应的字段映射列表
 * @query model_id
 * @query version_id
 */
const getModelApplication = {
  url: '/v3/datamanage/datamodel/instances/fields_mappings/',
};

/**
 * 获取实例指标可用统计口径列表
 */

const calculationAtomList = {
  url: '/v3/datamanage/datamodel/instances/:model_instance_id/indicators/calculation_atoms/',
};

/**
 * 数据模型实例指标可选字段列表
 */

const indicatorFieldsList = {
  url: '/v3/datamanage/datamodel/instances/:model_instance_id/indicators/optional_fields/',
};

/**
 * 指标节点信息接口
 * @query result_table_id
 */

const getIndicatorInformation = {
  url: '/v3/datamanage/datamodel/instances/by_table/',
};

/**
 * 数据模型字段加工校验
 */

const dataModelSqlVerify = {
  url: '/v3/datamanage/datamodel/sql_verify/column_process/',
  method: 'POST',
};

/**
 * 数据模型根据勾选节点，自动生成节点配置
 */
const autoGenerateNodes = {
  url: '/v3/datamanage/datamodel/instances/init/',
};

/**
 * 数据模型节点tree
 */
const indicatorMenuTree = {
  url: '/v3/datamanage/datamodel/instances/indicators/menu/',
};

/** 保存缓存数据 */
const setCatchData = {
  url: '/cache/',
  method: 'POST',
};

/** 获取缓存数据 */
const getCatchData = {
  url: '/cache/',
};

/** 获取实验列表数据 */
const getExperimentalList = {
  url: 'v3/dataflow/modeling/experiments/:modelId',
};

const updateNode = {
  url: '/v3/dataflow/flow/flows/:flowId/nodes/:nodeId/',
  method: 'PUT',
};

const createNode = {
  url: '/v3/dataflow/flow/flows/:flowId}/nodes/',
  methods: 'POST',
};

export {
  getMyModelList,
  getModelUpdateStatus,
  updateModelConfig,
  checkModelingName,
  releaseInspection,
  createModel,
  updateModel,
  getModelReleaseResult,
  getInputModelList,
  getProjectList,
  getTaskByProjectId,
  getProjectSummary,
  getBizListByProjectId,
  getBizResultList,
  batchStartTaskList,
  batchStopTaskList,
  batchDeleteTaskList,
  getFlowInfoByFlowId,
  getFlowGraph,
  getProcessingNodes,
  applyComplementTask,
  canceleApplication,
  submitComplementTask,
  stopComplementTask,
  listRelaTaskByRtid,
  getComputClusterList,
  getLinkRulesConfig,
  getDeployHistory,
  exportDataFlow,
  importDataFlow,
  getNodeLinkRulesV2,
  getSdkTypeList,
  getNodeTypeConfigs,
  submitDataMakeUp,
  getExecuteRecord,
  getMakeUpResult,
  autoGenerateSQL,
  getGraphInfo,
  getNodeConfigInfo,
  getRtLastMsg,
  getDefaultCode,
  getMyProjectList,
  createNewTask,
  deleteTask,
  getFieldTypeConfig,
  getReleasedModelList,
  getModelApplication,
  calculationAtomList,
  indicatorFieldsList,
  getIndicatorInformation,
  dataModelSqlVerify,
  autoGenerateNodes,
  indicatorMenuTree,
  setCatchData,
  getCatchData,
  getExperimentalList,
  updateNode,
  createNode,
};
