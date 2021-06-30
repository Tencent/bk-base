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

import utils from '@/pages/DataGraph/Common/utils.js';
import { deepClone } from '@/common/js/Utils.ts';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';
import { Component, Prop, Ref, Watch } from 'vue-property-decorator';
import { ICalculationAtomsResults } from '../../Interface/indexDesign';
import DataDetailFold from '../Steps/ChildNodes/DataDetailFold.vue';
import { DataModelManageBase } from '../../Controller/DataModelManageBase';
import { IndexTree } from '../Common/index';

export interface IModelType {
  master_table: string;
  fact_table: string;
  dimension_table: string;
  calculation_atom: string;
  indicator: string;
}

/**
 * 格式化子节点
 * @param {Array} nodes 节点
 * @param calculationFormula
 * @returns 返回格式化后的节点数组
 */
const getChildNodes = (nodes: any[] = [], calculationFormula: string) => {
  const resNodes: any[] = [];
  nodes.forEach(child => {
    Object.assign(child, { displayName: `${child.indicatorName}（${child.indicatorAlias}）` });
    const { indicatorAlias, indicatorName } = child;
    Object.assign(child, { calculationFormula });
    const nodeName = `${indicatorAlias}（${indicatorName}）`;
    const grandSonNode = {
      name: indicatorName,
      displayName: nodeName,
      alias: indicatorAlias,
      // type为indicator类型
      type: child.type,
      children: [],
      icon: 'icon-quota',
      id: indicatorName,
      sourceData: child,
      count: child.indicatorCount,
    };
    if (child.subIndicators && child.subIndicators.length) {
      grandSonNode.children = getChildNodes(child.subIndicators, calculationFormula);
    }
    resNodes.push(grandSonNode);
  });
  return resNodes;
};

@Component({
  components: {
    DataTable,
    DataDetailFold,
    IndexTree,
  },
})
export default class IndexDesignView extends DataModelManageBase {
  @Prop({ required: true }) public readonly data!: Array<object>;

  @Ref() public readonly indexTree!: IndexTree;

  windowTypeMap = {
    scroll: $t('滚动窗口'),
    accumulate: $t('累加窗口'),
    slide: $t('滑动窗口'),
    accumulate_by_hour: $t('按小时累加窗口'),
    fixed: $t('固定窗口'),
  };

  // 表格最多显示数
  calcPageSize = 10;
  searchText = '';
  isLoading = false;
  isIndexLoading = false;
  indexDetailNameIcons: IModelType = {
    master_table: 'icon-fact-model',
    fact_table: 'icon-fact-model',
    dimension_table: 'icon-dimension-model',
    calculation_atom: 'icon-statistic-caliber',
    indicator: 'icon-quota',
  };

  treeData: any[] = [];
  activeNode: any = null;
  indexList: any[] = [];

  unitMap = {
    s: $t('秒'),
    min: $t('分钟'),
    h: $t('小时'),
    d: $t('天'),
    w: $t('周'),
    m: $t('月'),
  };

  // 表格头部的信息会依赖此数据变化，每次只要更改这个数据值来改变表格头部信息
  tableInfoValueMap = {
    type: '',
    name: '',
  };

  headerTop = 0;
  calculationAtomProp = {};
  localData: any = {};
  isOpen = false;

  get maxHeight() {
    if (this.tableInfoValueMap.type === 'indicator') {
      const height = this.isOpen ? 538 : 570;
      return height;
    }
    return 630;
  }

  get tableInfoData() {
    const { data = {} } = this.activeNode || {};
    return {
      displayName: data.displayName,
      data: data.sourceData,
    };
  }

  // 统计口径列表
  get calculationAtomTableData() {
    if (this.tableInfoValueMap.type !== 'master_table') {
      return this.localData;
    }
    return this.localData.filter(item => {
      return item.calculationAtomName.includes(this.searchText)
                || item.calculationAtomAlias.includes(this.searchText);
    });
  }

  // 指标列表
  get indexTableData() {
    if (this.tableInfoValueMap.type !== 'calculation_atom') {
      return this.indexList;
    }
    return this.indexList.filter(item => {
      return item.indicatorName.includes(this.searchText) || item.indicatorAlias.includes(this.searchText);
    });
  }

  @Watch('data', { immediate: true, deep: true })
  handleChangeData() {
    this.localData = deepClone(this.data);
    this.initData();
  }

  public getIcon(type: string) {
    if (type === 'master_table') {
      const modeType = this.activeModelTabItem.modelType || 'master_table';
      return this.indexDetailNameIcons[modeType];
    }
    return this.indexDetailNameIcons[type];
  }

  public handleChangeStatus(status: boolean) {
    this.isOpen = status;
  }

  public initData() {
    const { name, displayName, modelType } = this.activeModelTabItem;
    const showName = `${displayName}（${name}）`;
    const rootNode = {
      name: name,
      displayName: showName,
      alias: displayName,
      count: 0,
      type: 'master_table',
      icon: this.indexDetailNameIcons[modelType] || 'icon-fact-model',
      id: `model_id_${this.modelId}`,
      children: [],
    };
    this.tableInfoValueMap = {
      type: 'master_table',
      name,
    };
    this.treeData = [rootNode];
    this.handleSetCalculationAtoms();
  }

  getUnitFirstWord(unit: string) {
    return unit.split('')[0].toLowerCase();
  }

  scrollEvent(e) {
    this.headerTop = e.target.scrollTop;
  }

  // 表格上方搜索框来搜索指标或统计口径
  searchTextChange() {
    if (this.tableInfoValueMap.type === 'master_table') {
      this.localData.filter((item: ICalculationAtomsResults) => {
        return (
          item.calculationAtomName.includes(this.searchText)
                    || item.calculationAtomAlias.includes(this.searchText)
        );
      });
    }
  }

  // 查看统计口径下的指标列表
  handleIndexDetail(data: ICalculationAtomsResults) {
    this.tableInfoValueMap = {
      type: 'calculation_atom',
      name: data.calculationAtomName,
    };
    this.indexTree.handleSetTreeSelected(data.calculationAtomName);
    this.getIndicatorList(data.indicators);
  }

  // 指标点击事件回调
  goToIndexDetail(indicatorName: string) {
    this.tableInfoValueMap.type = 'indicator';
    this.tableInfoValueMap.name = indicatorName;
    this.indexTree.handleSetTreeSelected(indicatorName);
  }

  goToCalculationAtomDetail(row) {
    this.tableInfoValueMap.type = 'calculation_atom';
    this.tableInfoValueMap.name = row.calculationAtomName;
    this.indexTree.handleSetTreeSelected(row.calculationAtomName);
    // 获取统计口径下指标列表
    this.getIndicatorList(row.indicators);
  }

  /** 节点点击事件回调 */
  handleNodeClick(node: any) {
    const { data = {} } = node;
    this.tableInfoValueMap = {
      type: data.type,
      name: data.name,
    };
    this.activeNode = node;
    this.calcPageSize = 10;
    this.isOpen = false;
    if (data.type === 'calculation_atom') {
      // 根据统计口径名称获取指标列表，在二级树形
      this.getIndicatorList(data.sourceData.indicators);
    } else if (data.type === 'master_table') {
      // 一级树形
      !this.localData.length && this.handleSetCalculationAtoms();
    } else if (data.type === 'indicator') {
      this.calculationAtomProp = data;
      this.getIndicatorList(data.sourceData.subIndicators);
    }
  }

  get indicatorDetailData() {
    const aggregationFieldsStr = this.getAggregationFieldsStr({
      aggregationFieldsAlias: this.tableInfoData.data.aggregationFieldsAlias,
      aggregationFields: this.tableInfoData.data.aggregationFields,
    });
    const { schedulingContent } = this.tableInfoData.data.schedulingContent;
    const commonParams = [
      {
        label: $t('指标统计口径'),
        value: `${this.tableInfoData.data.calculationAtomAlias}
                （${this.tableInfoData.data.calculationAtomName}）`,
      },
      {
        label: $t('口径聚合逻辑'),
        value: this.tableInfoData.data.calculationFormula || '--',
      },
      {
        label: $t('聚合字段'),
        value: aggregationFieldsStr || '--',
      },
      {
        label: $t('过滤条件'),
        value: this.tableInfoData.data.filterFormulaStrippedComment || '--',
      },
      {
        groupDataList: [
          {
            label: $t('计算类型'),
            value: this.tableInfoData.data.schedulingType === 'stream' ? $t('实时计算') : $t('离线计算'),
          },
          {
            label: $t('窗口类型'),
            value: this.windowTypeMap[schedulingContent.windowType],
          },
        ],
      },
    ];

    let params1 = [];

    if (['scroll', 'slide', 'accumulate'].includes(schedulingContent.windowType)) {
      // 滚动窗口、滑动窗口、累加窗口共用参数
      params1 = [
        {
          groupDataList: [
            {
              label: $t('统计频率'),
              value: schedulingContent.countFreq + $t('秒'),
            },
            {
              label: $t('窗口长度'),
              value: schedulingContent.windowTime + $t('分钟'),
              isHidden: schedulingContent.windowType === 'scroll',
            },
            {
              label: $t('等待时间'),
              value: schedulingContent.waitingTime + $t('秒'),
            },
          ],
        },
        {
          groupDataList: [
            {
              label: $t('依赖计算延迟数据'),
              value: schedulingContent.windowLateness.allowedLateness ? $t('是') : $t('否'),
            },
            {
              label: $t('延迟时间'),
              value: schedulingContent.windowLateness.latenessTime + $t('小时'),
              isHidden: !schedulingContent.windowLateness.allowedLateness,
            },
            {
              label: $t('统计频率'),
              value: schedulingContent.windowLateness.latenessCountFreq + $t('秒'),
              isHidden: !schedulingContent.windowLateness.allowedLateness,
            },
          ],
        },
      ];
    } else {
      // 固定窗口、按小时累加窗口共用参数
      const dependencyRule = {
        no_failed: $t('无失败'),
        all_finished: $t('全部成功'),
        at_least_one_finished: $t('一次成功'),
      };
      let dataStartList: {
        id: number;
        name: string;
      }[] = [];
      let dataEndList: {
        id: number;
        name: string;
      }[] = [];
      if (schedulingContent.windowType === 'accumulate_by_hour') {
        dataStartList = utils.getTimeList();
        dataEndList = utils.getTimeList('59');
      }
      const freqIndex = this.getUnitFirstWord(schedulingContent.schedulePeriod);
      params1 = [
        {
          groupDataList: [
            {
              label: $t('统计频率'),
              value: `${schedulingContent.countFreq}${this.unitMap[freqIndex]}`,
            },
            {
              label: $t('统计延迟'),
              value: schedulingContent.windowType === 'fixed'
                ? schedulingContent.fixedDelay + $t('小时')
                : schedulingContent.delay + $t('小时'),
            },
            {
              label: $t('窗口长度'),
              value: schedulingContent.formatWindowSize
                    + this.unitMap[schedulingContent.formatWindowSizeUnit],
              isHidden: schedulingContent.windowType !== 'fixed',
            },
            {
              label: $t('窗口起点'),
              value: dataStartList.find(child => child.id === schedulingContent.dataStart)
                ?.name,
              isHidden: schedulingContent.windowType === 'fixed',
            },
            {
              label: $t('窗口终点'),
              value: dataEndList.find(child => child.id === schedulingContent.dataEnd)?.name,
              isHidden: schedulingContent.windowType === 'fixed',
            },
          ],
        },
        {
          label: $t('依赖策略'),
          value: dependencyRule[schedulingContent.unifiedConfig?.dependencyRule],
        },
        {
          groupDataList: [
            {
              label: $t('调度失败重试'),
              value: schedulingContent.advanced.recoveryEnable ? $t('是') : $t('否'),
            },
            {
              label: $t('重试次数'),
              value: schedulingContent.advanced.recoveryTimes + $t('小时'),
              isHidden: !schedulingContent.advanced.recoveryEnable,
            },
            {
              label: $t('调度间隔'),
              value: parseInt(schedulingContent.advanced.recoveryInterval) + $t('分钟'),
              isHidden: !schedulingContent.advanced.recoveryEnable,
            },
          ],
        },
      ];
    }
    return [
      ...commonParams,
      ...params1,
      {
        groupDataList: [
          {
            label: $t('更新人'),
            value: this.tableInfoData.data.updatedBy,
          },
          {
            label: $t('更新时间'),
            value: this.tableInfoData.data.updatedAt,
          },
        ],
      },
    ];
  }

  // 获取指标列表
  getIndicatorList(list: any[] = []) {
    this.indexList = list;
    this.indexList.forEach(item => {
      this.$set(item, 'displayName', `${item.indicatorName}（${item.indicatorAlias}）`);
    });
    this.indexList.length
            && this.indexList.forEach(item => {
              this.$set(item, 'aggregationFieldsStr', this.getAggregationFieldsStr(item));
            });
  }

  getAggregationFieldsStr(item) {
    // 聚合字段（aggregationFieldsStr），首先用aggregationFieldsAlias，不存在用aggregationFields
    let str = '';
    let arr: any[];
    if (item.aggregationFieldsAlias?.length) {
      arr = item.aggregationFieldsAlias;
    } else if (item.aggregationFields?.length) {
      arr = item.aggregationFields;
    }
    if (arr?.length) {
      arr.forEach(child => {
        str += child + '，';
      });
    }
    return str.slice(0, str.length - 1) || '--';
  }

  // 获取统计口径列表
  handleSetCalculationAtoms() {
    this.treeData[0].children = [];
    this.localData.forEach(item => {
      this.$set(item, 'displayName', `${item.calculationAtomName}（${item.calculationAtomAlias}）`);
    });

    // 总指标数
    let totalCount = 0;
    this.localData.forEach(item => {
      const { calculationAtomName, calculationAtomAlias, type, indicatorCount } = item;
      totalCount += indicatorCount;
      const nodeName = `${calculationAtomAlias}（${calculationAtomName}）`;
      const childNode = {
        name: calculationAtomName,
        displayName: nodeName,
        alias: calculationAtomAlias,
        type,
        count: indicatorCount,
        sourceData: item,
        icon: 'icon-statistic-caliber',
        id: calculationAtomName,
        children: [],
      };
      if (item.indicators && item.indicators.length) {
        childNode.children.push(...getChildNodes(item.indicators, item.calculationFormulaStrippedComment));
      }
      this.treeData[0].count = totalCount;
      this.treeData[0].children.push(childNode);
    });
  }

  handleGoEditIndexDesign(name: string, row) {
    // 激活指标步骤
    this.DataModelTabManage.updateTabActiveStep(3);
    this.$router.push({
      name: 'dataModelEdit',
      params: Object.assign({}, this.routeParams, { open: { name, row } }),
      query: this.routeQuery,
    });
  }
}
