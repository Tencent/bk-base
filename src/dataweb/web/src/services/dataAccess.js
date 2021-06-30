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
const meta = new Meta();
/** 获取接入详情 */
const getAccessDetails = {
  url: 'v3/access/deploy_plan/:raw_data_id/',
  method: 'get',
};

/** 查询部署状态统计全量信息*/
const getAccessSummary = {
  url: '/v3/access/collectorhub/:raw_data_id/summary/',
  method: 'get',
};

// 获取接入对象的状态统计
const getAccessObjectSummary = Object.assign({}, getAccessSummary, {
  callback: (res, option) => {
    const deployPlanId = option.params.deploy_plan_id;
    const matchedRightItem = (res.data.deploy_plans || []).find(item => item.deploy_plan_id === deployPlanId);

    return Object.assign({}, res, {
      data: matchedRightItem || {},
    });
  },
});

// 获取接入对象的模块的状态统计
const getAccessModuleSummary = Object.assign({}, getAccessSummary, {
  callback: (res, option) => {
    const deployPlanId = option.params.deploy_plan_id;
    const matchedRightItem = (res.data.deploy_plans || [])
      .filter(item => item.module && item.deploy_plan_id === deployPlanId);

    return Object.assign({}, res, {
      data: (matchedRightItem && matchedRightItem[0]) || {},
    });
  },
});

// 刷新获取模块中某个对象的运行状态信息
const getModuleStatusInfo = Object.assign({}, getAccessSummary, {
  callback: (res, option) => {
    const deployPlanId = option.params.deploy_plan_id;
    const instanceId = option.params.bk_instance_id;
    const matchedRightItem = (res.data.deploy_plans || [])
      .find(item => item.module && item.deploy_plan_id === deployPlanId);

    return Object.assign({}, res, {
      data: matchedRightItem && matchedRightItem.module.find(item => item.bk_instance_id === instanceId),
    });
  },
});

// 获取接入对象的IP的状态统计
const getIpStatusInfo = Object.assign({}, getAccessSummary, {
  callback: (res, option) => {
    const deployPlanId = option.params.deploy_plan_id;
    const matchedRightItem = (res.data.deploy_plans || [])
      .filter(item => !item.module && item.deploy_plan_id === deployPlanId);

    return Object.assign({}, res, {
      data: (matchedRightItem && matchedRightItem[0]) || {},
    });
  },
});

/** 查询部署状态 */
const queryAccessStatus = {
  url: 'v3/access/collectorhub/:raw_data_id/status/',
};

/** 接入变更历史 */
const getDeployHistory = {
  url: '/v3/access/deploy_plan/:raw_data_id/history/',
};

/** 查询历史部署状态 */
const getDeployStatusHistory = {
  url: '/v3/access/collectorhub/:raw_data_id/status/history/',
};

/** 获取字符编码 */
const getEncodeList = {
  url: '/v3/meta/encoding_configs/',
};

/** 根据access_scenario_id获取数据来源列表 */
const getSourceList = {
  url: 'v3/access/source/:access_scenario_id/',
};

/** 获取数据场景列表 */
const getScenarioList = {
  url: '/v3/access/scenario/',
};

/** 获取全部业务 */
const getMineBizList = {
  url: '/bizs/list_mine_biz/',
  callback(res, options) {
    return Object.assign(res, {
      data: meta.formatBizsList(res.data, options),
    });
  },
};

/** 获取数据源分类 */
const getCategoryConfigs = {
  url: '/v3/meta/dm_category_configs/',
  callback(res) {
    return Object.assign(res, {
      data: (res.data || []).sort((a, b) => a.id - b.id),
    });
  },
};

/** 查询部署计划 */
const getDeployPlan = {
  url: '/v3/access/deploy_plan/:raw_data_id/',
};

/** 数据库测试 */
const getDbCheck = {
  url: '/v3/access/collector/:data_scenario/check/',
  method: 'POST',
};

/** 根据bk_biz_id获取数据结构列表ODM  */
const getDataStruct = {
  url: 'v3/access/collector/data_struct/:bk_biz_id/',
};

/** 获取时间格式 */
const getTimeFormat = {
  url: '/v3/meta/time_format_configs/',
};

/** 根据bk_biz_id和name获取数据结构列表 */
const getDataStructDetail = {
  url: 'v3/access/collector/data_struct/:bk_biz_id/tables/',
};

/** 灯塔模块：获取事件名称 */
const getEventList = {
  url: 'v3/access/collector/app_key/',
};

/** TQOS 获取所有ID列表 */
const getTSODList = {
  url: '/v3/access/collector/tqos/',
};

/** 获取分隔符列表 */
const getDelimiterList = {
  url: '/v3/access/delimiter/',
};

/** 获取数据库类型 */
const getDBTypelists = {
  url: '/v3/access/db_type/',
};

/** 获取数据接入列表，服务器分页
 * @param {Int} (query) bk_biz_id
 * @param {Boolean} (query) show_display=1 是否显示
 * @param {Int} (query) page 当前页
 * @param {Int} (query) page_size 每页数据
 * @param {String} (query) data_source 数据来源
 * @param {String} (query) data_scenario 接入场景
 * @param {String} (query) data_cayegory 数据分类
 */
const getAccessList = {
  url: 'v3/access/rawdata/mine/',
};

/** 检查IP状态
 * @param {String} biz_id
 * @param {Int} bk_cloud_id
 * @param {Array} ips:[ip1,ip2]
 */
const checkIpsStatus = {
  url: '/bizs/:biz_id/check_ips_status/',
  method: 'post',
};

/** 获取告警列表 */
const getWarningList = {
  url: '/v3/datamanage/dmonitor/alert_details/',
};

/**
 * 采集器启动
 */
const startCollector = {
  method: 'post',
  url: '/v3/access/collectorhub/:raw_data_id/start/',
};

/** 获取当前接入场景下面Host配置
 * @param (query) data_scenario
 */
const getHostConfig = {
  url: '/v3/access/host_config/',
};

/** 获取数据监控和任务监控的通知方式
 */
const getMonitorNotifyWays = {
  url: '/v3/datamanage/dmonitor/notify_ways/',
};

/** 根据用户输入的topic,获取当前接入场景下面的tid列表
 * @param (query) topic
 */
const getTidList = {
  url: '/v3/access/tdw/tid/',
  callback(res) {
    res.data.forEach((item) => {
      item.description = `${item.tid}(${item.description})`;
    });
    return res;
  },
};

/** 接入详情页和监控告警页用来获取alert_config_ids参数的接口
 * @param raw_data_id
 */
const getAlertConfigIds = {
  url: '/v3/datamanage/dmonitor/alert_configs/rawdata/:raw_data_id/',
};

/** 标签格式化 */
const formatTagData = function (tags, tagList, type, isMultiple) {
  tags[tagList].forEach((tag) => {
    tag.active = false;
    tag.multiple = isMultiple;
    tag[tagList] && formatTagData(tag, tagList, type, isMultiple);
  });
};

/** 获取标签列表
 * @param top 默认展示多少条
 */
const getDataTagList = {
  url: '/datamart/datamap/tag/sort_count_for_access/',
  callback(res) {
    if (!res.result) return res;
    res.data.overall_top_tag_list && formatTagData(res.data, 'overall_top_tag_list', 'tag_type', true);
    res.data.tag_list.forEach((tagBlock) => {
      const { multiple } = tagBlock.action;
      tagBlock.multiple = multiple;
      if (tagBlock.title) {
        tagBlock.label = `${tagBlock.title}：${tagBlock.description}`;
      } else {
        tagBlock.label = tagBlock.description;
      }
      tagBlock.data = Array.from(tagBlock.tag_list);
      tagBlock.data.forEach((tagList) => {
        tagList.expand = false;
        formatTagData(tagList, 'sub_top_list', 'tag_type', multiple); // 格式化sub_top_list标签
        formatTagData(tagList, 'sub_list', 'tag_type', multiple); // 格式化sub_list标签
        tagList.backupTopList = JSON.parse(JSON.stringify(tagList.sub_top_list));
      });
    });
    res.data.overall_top_tag_list.forEach((tag) => {
      tag.multiple = true;
    });
    return res;
  },
};
/** 删除数据源
 * @param raw_data_id
 * @param bk_biz_id
 * @param bk_app_code
 * @param appenv
 */
const deleteRawData = {
  url: '/v3/access/deploy_plan/:raw_data_id/',
  method: 'DELETE',
};

/** 删除清洗任务
 * @param raw_processing_id
 * @param bk_app_code
 * @param processing_id
 */
const deleteCleanTask = {
  url: '/v3/databus/cleans/:processing_id/',
  method: 'DELETE',
};

/** 主要用来获取多个清洗RT的时候从data第一层级里的groups获取每个rt对应的告警数量alert_count
 * @query alert_config_ids
 * @query dimensions
 * @query group
 */
const getAlertCount = {
  url: 'v3/datamanage/dmonitor/alert_details/summary/',
};

/** 数据集成里数据入库详情的入库数据质量-获取输出量
 * @query data_set_ids
 * @query start_time
 * @query end_time
 * @query storages
 */
const getDepotOutput = {
  url: 'v3/datamanage/dmonitor/metrics/output_count/',
};

/** 数据集成里数据入库详情的入库数据质量-获取输出量
 * @query data_set_ids
 * @query start_time
 * @query end_time
 * @query storages
 */
const getDepotInput = {
  url: 'v3/datamanage/dmonitor/metrics/input_count/',
};

/** 数据集成里数据入库详情的入库数据质量-获取数据时间延迟数据
 * @query data_set_ids
 * @query start_time
 * @query end_time
 * @query storages
 */
const getDataTimeDelay = {
  url: 'v3/datamanage/dmonitor/metrics/process_time_delay/',
};
/**
 * 获取可编辑Appcode白名单
 */
const getEditAbleAppCode = {
  url: '/v3/access/rawdata/editable_apps/',
};

/** 根据数据源获取业务id */
const getBizIdBySourceId = {
  url: '/v3/access/rawdata/:sourceId/',
};

/** 根据集群获取游戏区
 * @query cluster_name
 */
const getGameZoneList = {
  url: '/v3/storekit/tcaplus/get_zone_info/',
};

const deployPlan = {
  url: '/v3/access/deploy_plan/',
  method: 'POST',
};

const getDeployPlanList = {
  url: '/v3/access/deploy_plan/',
};

const getSourceBizList = {
  url: 'v3/access/rawdata/',
};

const tdeClusterList = {
  url: '/v3/access/collector/tdm/clusters/',
};
export {
  getGameZoneList,
  getTidList,
  getAccessDetails,
  getAccessSummary,
  getAccessObjectSummary,
  getAccessModuleSummary,
  getModuleStatusInfo,
  getIpStatusInfo,
  queryAccessStatus,
  getDeployHistory,
  getDeployStatusHistory,
  getEncodeList,
  getSourceList,
  getScenarioList,
  getMineBizList,
  getCategoryConfigs,
  getDeployPlan,
  getDbCheck,
  getDataStruct,
  getTimeFormat,
  getDataStructDetail,
  getEventList,
  getTSODList,
  getDelimiterList,
  getAccessList,
  checkIpsStatus,
  getWarningList,
  startCollector,
  getHostConfig,
  getDBTypelists,
  getMonitorNotifyWays,
  getAlertConfigIds,
  getDataTagList,
  deleteRawData,
  deleteCleanTask,
  getAlertCount,
  getDepotOutput,
  getDepotInput,
  getDataTimeDelay,
  getEditAbleAppCode,
  getBizIdBySourceId,
  deployPlan,
  getDeployPlanList,
  getSourceBizList,
  tdeClusterList,
};
