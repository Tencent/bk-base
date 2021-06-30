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

import operationGroup from '@/bkdata-ui/components/operationGroup/operationGroup.vue';
import { showMsg } from '@/common/js/util';
import utils from '@/pages/DataGraph/Common/utils.js';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { Component, Inject, Ref, Watch } from 'vue-property-decorator';
import {
  calculationAtomDetail,
  confirmModelIndicators,
  deleteCalculationAtom,
  deleteIndicator,
  getCalculationAtoms,
  getIndicatorList,
} from '../../Api/index';
import {
  ICalculationAtomDetail,
  ICalculationAtomDetailData,
  ICalculationAtomsData,
  ICalculationAtomsResults,
  IIndicatorDetail,
} from '../../Interface/indexDesign';
import { IndexTree } from '../Common/index';
import AddCaliber from './ChildNodes/AddCaliber';
import DataDetailFold from './ChildNodes/DataDetailFold.vue';
import { DataModelManage } from './IStepsManage';

/**
 * 获取指标类型节点树数据
 * @param nodes
 * @param calculationFormula
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
      id: `indicatorId-${child.indicatorId}`,
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
    operationGroup,
    AddCaliber,
    DataTable,
    AddIndicator: () => import('./ChildNodes/AddIndicator.vue'),
    DataDetailFold,
    IndexTree,
  },
})
export default class IndexDesign extends DataModelManage.IStepsManage {
  @Ref() public readonly indexTree!: IndexTree;
  @Ref() public readonly detailsFold!: DataDetailFold;

  public appHeight: number = window.innerHeight;

  // 窗口类型
  public windowTypeMap = {
    scroll: $t('滚动窗口'),
    accumulate: $t('累加窗口'),
    slide: $t('滑动窗口'),
    accumulate_by_hour: $t('按小时累加窗口'),
    fixed: $t('固定窗口'),
  };

  // 统计口径详情请求loading
  public isAllLoading = false;

  // 统计口径的添加方式
  public addMethod = 'quote';

  // 是否增加指标
  public isAddIndicator = false;

  // 当前模型的相关信息
  @Inject('activeTabItem')
  public activeTabItem!: function;

  public searchText = '';

  public isLoading = false;

  public isIndexLoading = false;

  public calculationAtomsInfo = {
    step_id: 1,
    results: [],
  };

  public indexDetailNameIcons = {
    master_table: 'icon-fact-model',
    fact_table: 'icon-fact-model',
    dimension_table: 'icon-dimension-model',
    calculation_atom: 'icon-statistic-caliber',
    indicator: 'icon-quota',
  };

  public treeData: any[] = [];

  public activeNode: any = null;

  public indexInfo = {
    step_id: 1,
    results: [],
  };

  // 操作弹框相关信息
  public dialogInfo = {
    isShow: false,
    buttonText: '',
    title: '',
    data: '',
    buttonLoading: false,
  };

  //  统计口径相关信息
  public calculationAtomDetailData: ICalculationAtomDetailData = {};

  // 单位map
  public unitMap = {
    s: $t('秒'),
    min: $t('分钟'),
    h: $t('小时'),
    d: $t('天'),
    w: $t('周'),
    m: $t('月'),
  };

  // 表格头部的信息会依赖此数据变化，每次只要更改这个数据值来改变表格头部信息
  public tableInfoValueMap = {
    type: '',
    name: '',
  };

  public headerTop = 0;

  public calculationAtomProp = {};

  public isOpen = false;
  public detailsFoldHeight = 0;

  @Watch('tableInfoValueMap.type')
  public handleTypeChange(value: string | undefined, old: string | undefined) {
    if (value === 'indicator' && value !== old) {
      this.getFoldHeight();
    }
  }

  @Watch('isOpen')
  public handleOpenChange() {
    if (this.tableInfoValueMap.type === 'indicator') {
      this.getFoldHeight();
    }
  }

  /**
     * 获取 table maxHeight
     */
  get maxHeight() {
    if (this.tableInfoValueMap.type === 'indicator') {
      const occupyHeight = 421;
      return this.appHeight - occupyHeight - this.detailsFoldHeight;
    }
    return this.appHeight - 451;
  }

  get tableInfoData() {
    const { data = {} } = this.activeNode || {};
    return {
      displayName: data.displayName,
      data: data.sourceData,
    };
  }

  /**
     * 统计口径列表
     */
  get calculationAtomTableData() {
    if (this.tableInfoValueMap.type !== 'master_table') {
      return this.calculationAtomsInfo.results;
    }
    return this.calculationAtomsInfo.results.filter(item => {
      return (
        item.calculationAtomName.includes(this.searchText)
                || item.calculationAtomAlias.includes(this.searchText)
                || item.calculationFormulaStrippedComment.includes(this.searchText)
                || item.updatedBy.includes(this.searchText)
      );
    });
  }

  /**
     * 指标列表
     */
  get indexTableData() {
    return this.indexInfo.results.filter(item => {
      return (
        item.indicatorName.includes(this.searchText)
                || item.indicatorAlias.includes(this.searchText)
                || item.aggregationFieldsStr.includes(this.searchText)
                || item.filterFormulaStrippedComment.includes(this.searchText)
                || item.updatedBy.includes(this.searchText)
      );
    });
  }

  /**
     * 添加按钮的文本
     */
  get addButtonText() {
    if (this.tableInfoValueMap.type === 'master_table') {
      return $t('指标统计口径');
    } else {
      return $t('指标');
    }
  }

  get isCalculationDelete() {
    if (this.tableInfoValueMap.type === 'calculation_atom') {
      return !this.tableInfoData.data.deletable;
    }
    return false;
  }

  get isIndicatorDelete() {
    if (this.tableInfoValueMap.type === 'indicator') {
      return !this.tableInfoData.data.deletable;
    }
    return false;
  }

  /**
     * 某个模型下所有的聚合逻辑列表
     */
  get aggregationLogicList() {
    return this.calculationAtomTableData.length
      ? this.calculationAtomTableData.map((item: ICalculationAtomsResults) => item.calculationFormula)
      : [];
  }

  /**
     * 获取删除 tips
     */
  get getTableInfoDeleTips() {
    if (this.tableInfoValueMap.type === 'calculation_atom') {
      return this.getCalculationAtomsTips(this.tableInfoData.data);
    }
    if (this.tableInfoValueMap.type === 'indicator' && !this.tableInfoData.data.deletable) {
      return this.$t('父指标无法被删除');
    }
    return {
      disabled: true,
    };
  }

  /**
     * 获取编辑 tips
     */
  get getTableInfoEditTips() {
    if (this.tableInfoValueMap.type === 'calculation_atom' && !this.tableInfoData.data.editable) {
      return {
        content: $t('该指标统计口径引用自数据集市'),
        disabled: false,
      };
    }
    return {
      disabled: true,
    };
  }

  get searchPlaceholder() {
    return this.tableInfoValueMap.type === 'master_table'
      ? this.$t('请输入指标统计口径名称、聚合逻辑、更新人')
      : this.$t('请输入指标名称、聚合字段、过滤条件、更新人');
  }

  /**
     * 获取指标详情内容高度
     */
  public getFoldHeight() {
    this.$nextTick(() => {
      const el = this.detailsFold.$el;
      const style = window.getComputedStyle(el, null);
      const height = parseInt(style.getPropertyValue('height'));
      const marginTop = parseInt(style.getPropertyValue('margin-top'));
      this.detailsFoldHeight = height + marginTop;
    });
  }

  /**
     * 获取指标删除 tips
     * @param row
     */
  public handleDeleteIndicatorTips(row) {
    if (!row.deletable) {
      return this.$t('父指标无法被删除');
    }
    return {
      disabled: true,
    };
  }

  /**
     * 获取内容展示 icon
     * @param type
     */
  public getIcon(type: string) {
    if (type === 'master_table') {
      const modeType = this.activeModelTabItem?.modelType || 'master_table';
      return this.indexDetailNameIcons[modeType];
    }
    return this.indexDetailNameIcons[type];
  }

  /**
     * 控制折叠状态
     * @param status
     */
  public handleChangeStatus(status: boolean) {
    this.isOpen = status;
  }

  /**
     * 重新设置内容 height
     */
  public handleResetHeight() {
    this.appHeight = window.innerHeight;
    if (this.tableInfoValueMap.type === 'indicator') {
      this.getFoldHeight();
    }
  }

  public async created() {
    window.addEventListener('resize', this.handleResetHeight);
    // 初始化根节点
    const { name, displayName } = this.activeTabItem();
    const showName = `${displayName}（${name}）`;
    this.tableInfoValueMap = {
      type: 'master_table',
      name,
    };
    const rootNode = {
      name,
      displayName: showName,
      alias: displayName,
      count: 0,
      type: 'master_table',
      icon: this.indexDetailNameIcons[this.activeModelTabItem?.modelType] || 'icon-fact-model',
      id: `model_id_${this.modelId}`,
      children: [],
    };
    this.treeData = [rootNode];
    this.initPreNextManage();
    await this.getCalculationAtoms();

    const timer = setTimeout(() => {
      // 查看态跳转打开对应编辑侧栏
      const { open } = this.routeParams;
      if (open) {
        const { name, row } = open;
        if (name && row) {
          const editFn = name === 'calculationAtom' ? this.calculationAtomDetail : this.editIndicator;
          editFn(row);
          this.appendRouter(Object.assign(this.routeParams, { open: undefined }));
        }
      }
      clearTimeout(timer);
    }, 200);
  }

  public beforeDestroy() {
    window.removeEventListener('resize', this.handleResetHeight);
  }

  /**
     * 获取单位第一个字母
     * @param unit
     */
  public getUnitFirstWord(unit: string) {
    return unit.split('')[0].toLowerCase();
  }

  /**
     * 控制滑动事件
     * @param e
     */
  public scrollEvent(e) {
    this.headerTop = e.target.scrollTop;
  }

  /**
     * 统计口径表格的编辑按钮tips
     * @param content
     */
  public getCalculationAtomsEditTips(content: string) {
    return {
      content,
      disabled: !content,
    };
  }

  /**
     * 获取统计口径列表删除按钮的tips
     * @param data
     */
  public getCalculationAtomsTips(data) {
    function getParams(content = '') {
      return {
        content,
        disabled: !content,
      };
    }
    if (data.isAppliedByIndicators && data.isQuotedByOtherModels) {
      return getParams($t('统计口径同时被指标和其他统计口径引用！'));
    }
    if (data.isAppliedByIndicators) {
      return getParams($t('统计口径正在被指标应用！'));
    }
    if (data.isQuotedByOtherModels) {
      return getParams($t('统计口径正在被其他统计口径引用！'));
    }
    return getParams();
  }

  /**
     * 创建指标之后需要刷新tree和指标列表
     */
  public updateIndexInfo({ name, isChildIndicator }) {
    this.getCalculationAtoms();
    this.getIndicatorList(name, isChildIndicator);
  }

  /**
     * 编辑操作
     */
  public editOption() {
    if (this.tableInfoValueMap.type === 'calculation_atom') {
      this.calculationAtomDetail(this.tableInfoData.data);
    } else if (this.tableInfoValueMap.type === 'indicator') {
      this.editIndicator(this.tableInfoData.data);
    }
  }

  /**
     * 表格上方搜索框来搜索指标或统计口径
     */
  public searchTextChange() {
    if (this.tableInfoValueMap.type === 'master_table') {
      this.calculationAtomsInfo.results.filter((item: ICalculationAtomsResults) => {
        return (
          item.calculationAtomName.includes(this.searchText)
                    || item.calculationAtomAlias.includes(this.searchText)
        );
      });
    }
  }

  /**
     * 查看统计口径下的指标列表
     * @param data
     */
  public handleIndexDetail(data: ICalculationAtomsResults) {
    this.tableInfoValueMap = {
      type: 'calculation_atom',
      name: data.calculationAtomName,
    };
    this.indexTree.handleSetTreeSelected(data.calculationAtomName);
    this.getIndicatorList(data.calculationAtomName);
  }

  /**
     * 重置弹窗信息
     */
  public reSetDialogInfo() {
    this.dialogInfo = {
      isShow: false,
      buttonText: '',
      title: '',
      data: '',
      buttonLoading: false,
    };
  }

  /**
     * 节点树触发统计口径/指标编辑
     * @param data
     */
  public changeIndexType(data: object) {
    this.tableInfoValueMap = {
      type: data.type,
      name: data.name,
    };
    this.indexTree.handleSetTreeSelected(data.id);
    if (this.tableInfoValueMap.type === 'master_table') {
      this.openAddCaliber();
    } else {
      this.calculationAtomProp = this.tableInfoData.data;
      this.getIndicatorList(data.name, this.tableInfoValueMap.type === 'indicator');
      this.openAddIndicatorSlide();
    }
  }

  /**
     * 表头新增指标按钮方法
     */
  public addIncatior() {
    this.calculationAtomProp = this.tableInfoData.data;
    this.openAddIndicatorSlide();
  }

  /**
     * 新增指标
     * @param calculationAtomProp
     */
  public addIndex(calculationAtomProp) {
    this.calculationAtomProp = calculationAtomProp;
    this.openAddIndicatorSlide();
  }

  /**
     * 打开统计口径侧边栏
     */
  public openAddCaliber() {
    this.addMethod = 'quote';
    this.$refs.addCaliber.isShow = true;
    this.sendUserActionData({ name: '添加【指标统计口径】' });
  }

  /**
     * 打开创建指标侧边栏
     */
  public openAddIndicatorSlide() {
    this.sendUserActionData({ name: '添加【指标】' });
    this.$nextTick(() => {
      this.$refs.addIndicator.init();
    });
  }

  /**
     * 展示指标详情
     * @param indicatorName
     */
  public goToIndexDetail(indicatorName: string) {
    this.tableInfoValueMap.type = 'indicator';
    this.tableInfoValueMap.name = indicatorName;
    this.indexTree.handleSetTreeSelected(indicatorName);
  }

  /**
     * 展示统计口径详情
     * @param calculationAtomName
     */
  public goToCalculationAtomDetail(calculationAtomName: string) {
    this.tableInfoValueMap.type = 'calculation_atom';
    this.tableInfoValueMap.name = calculationAtomName;
    this.indexTree.handleSetTreeSelected(calculationAtomName);
    // 获取统计口径下指标列表
    this.getIndicatorList(calculationAtomName);
  }

  /**
     * 处理节点点击事件
     * @param node
     */
  public handleNodeClick(node: any) {
    const { data = {} } = node;
    this.tableInfoValueMap = {
      type: data.type,
      name: data.name,
    };
    this.activeNode = node;
    this.isOpen = false;
    if (data.type === 'calculation_atom') {
      // 根据统计口径名称获取指标列表，在二级树形
      this.getIndicatorList(data.sourceData.calculationAtomName);
    } else if (data.type === 'master_table') {
      // 一级树形
      !this.calculationAtomsInfo.results.length && this.getCalculationAtoms();
    } else if (data.type === 'indicator') {
      this.calculationAtomProp = data;
      this.getIndicatorList(data.name, true);
    }
  }

  /**
     * 获取字表详情信息
     */
  get indicatorDetailData() {
    const aggregationFieldsStr = this.getAggregationFieldsStr({
      aggregationFieldsAlias: this.tableInfoData.data.aggregationFieldsAlias,
      aggregationFields: this.tableInfoData.data.aggregationFields,
    });
    const {
      calculationAtomAlias,
      calculationAtomName,
      calculationFormula,
      filterFormulaStrippedComment
    } = this.tableInfoData.data;

    const { schedulingContent } = this.tableInfoData.data.schedulingContent;
    const commonParams = [
      {
        label: $t('指标统计口径'),
        value: `${calculationAtomAlias}（${calculationAtomName}）`,
      },
      {
        label: $t('口径聚合逻辑'),
        value: calculationFormula || '--',
      },
      {
        label: $t('聚合字段'),
        value: aggregationFieldsStr || '--',
      },
      {
        label: $t('过滤条件'),
        value: filterFormulaStrippedComment || '--',
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
              value: schedulingContent.windowLateness.allowedLateness
                ? $t('是') : $t('否'),
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
      let dataStartList: Array<{
        id: number;
        name: string;
      }> = [];
      let dataEndList: Array<{
        id: number;
        name: string;
      }> = [];
      if (schedulingContent.windowType === 'accumulate_by_hour') {
        dataStartList = utils.getTimeList();
        dataEndList = utils.getTimeList('59');
      }
      params1 = [
        {
          groupDataList: [
            {
              label: $t('统计频率'),
              value:
                                schedulingContent.countFreq
                                + this.unitMap[this.getUnitFirstWord(schedulingContent.schedulePeriod)],
            },
            {
              label: $t('统计延迟'),
              value:
                                schedulingContent.windowType === 'fixed'
                                  ? schedulingContent.fixedDelay + $t('小时')
                                  : schedulingContent.delay + $t('小时'),
            },
            {
              label: $t('窗口长度'),
              value:
                                schedulingContent.formatWindowSize
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
          value: dependencyRule[schedulingContent?.unifiedConfig?.dependencyRule],
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

  /**
     * 表格上方删除指标功能
     */
  public deleteIndex() {
    if (this.tableInfoValueMap.type === 'calculation_atom') {
      if (this.isCalculationDelete) {
        return;
      }
      this.handleDelete(this.tableInfoValueMap.type, this.tableInfoData.data.calculationAtomName);
    } else if (this.tableInfoValueMap.type === 'indicator') {
      if (this.isIndicatorDelete) {
        return;
      }
      this.handleDelete(this.tableInfoValueMap.type, this.tableInfoData.data.indicatorName);
    }
  }

  /**
     * 弹框确认方法
     */
  public confirm() {
    if (this.dialogInfo.type === 'indicator') {
      this.deleteIndicator(this.dialogInfo.data);
    } else {
      this.deleteCalculationAtom(this.dialogInfo.data);
    }
  }

  /**
     * 删除指标
     * @param indicatorName
     * @param deletable
     */
  public handleDeleteIndicator(indicatorName: string, deletable = true) {
    if (!deletable) {
      return;
    }
    this.handleDelete('indicator', indicatorName);
  }

  /**
     * 删除统计口径
     * @param calculationAtomName
     * @param deletable
     */
  public handleDeleteCalculation(calculationAtomName: string, deletable = true) {
    this.handleDelete('calculation_atom', calculationAtomName, deletable);
  }

  /**
     * 打开删除提示弹窗
     * @param type
     * @param data
     * @param deletable
     */
  public handleDelete(type: string, data: string, deletable = true) {
    if (!deletable) {
      return;
    }

    const deleteInfoMap = {
      master_table: $t('确认删除该统计口径?'),
      calculation_atom: $t('确认删除该统计口径?'),
      indicator: $t('确认删除该指标?'),
    };
    const deleteApi = type === 'indicator' ? this.deleteIndicator : this.deleteCalculationAtom;
    this.$bkInfo({
      title: deleteInfoMap[type],
      subTitle: this.$t('删除操作无法撤回'),
      extCls: 'bk-dialog-sub-header-center',
      closeIcon: true,
      confirmLoading: true,
      confirmFn: async () => {
        await deleteApi(data);
      },
    });
  }

  /**
     * 编辑指标
     * @param data
     */
  public editIndicator(data) {
    this.calculationAtomProp = data;
    this.$refs.addIndicator && this.$refs.addIndicator.getIndicatorDetail(data.indicatorName);
  }

  /**
     * 删除指标
     * @param indicator_name
     */
  public deleteIndicator(indicator_name: string) {
    this.dialogInfo.buttonLoading = true;
    deleteIndicator(indicator_name, Number(this.modelId))
      .then(res => {
        if (res.validateResult()) {
          showMsg(this.$t('删除该指标成功!'), 'success', { delay: 1500 });
          this.getCalculationAtoms();
          if (this.dialogInfo.type === 'calculation_atom') {
            this.getIndicatorList(this.tableInfoData.data.calculationAtomName);
          } else {
            const { name } = this.activeTabItem();
            this.tableInfoValueMap = {
              type: 'master_table',
              name,
            };
            this.indexTree.handleSetTreeSelected(this.treeData[0].id);
          }
          // 刷新可以引用的统计口径列表
          this.$refs.addCaliber.getCalculationAtoms();
          this.reSetDialogInfo();
          this.handleUpdatePublishStatus(res.data.publish_status);
        }
      })
      ['finally'](() => {
        this.dialogInfo.buttonLoading = false;
      });
  }

  /**
     * 获取指标列表
     * @param name
     * @param isChildIndicator
     */
  public getIndicatorList(name: string | undefined, isChildIndicator = false) {
    let calculationAtomName;
    let parentIndicatorName;
    isChildIndicator ? (parentIndicatorName = name) : (calculationAtomName = name);
    this.isIndexLoading = true;
    this.addMethod = 'quote';
    getIndicatorList(calculationAtomName, this.$route.params.modelId, parentIndicatorName)
      .then(res => {
        const instance = new BKHttpResponse<IIndicatorDetail>(res);
        instance.setData(this, 'indexInfo');
        this.indexInfo.results.forEach(item => {
          this.$set(item, 'displayName', `${item.indicatorName}（${item.indicatorAlias}）`);
        });
        this.indexInfo.results.length
                    && this.indexInfo.results.forEach(item => {
                      this.$set(item, 'aggregationFieldsStr', this.getAggregationFieldsStr(item));
                    });
      })
      ['finally'](() => {
        this.isIndexLoading = false;
      });
  }

  /**
     * 获取聚合字段信息
     * @param item
     */
  public getAggregationFieldsStr(item) {
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

  /**
     * 删除统计口径
     * @param calculation_atom_name
     */
  public deleteCalculationAtom(calculation_atom_name: string) {
    this.dialogInfo.buttonLoading = true;
    return deleteCalculationAtom(calculation_atom_name, this.$route.params.modelId)
      .then(res => {
        if (res.validateResult()) {
          showMsg(this.$t('删除该统计口径成功!'), 'success', { delay: 1500 });
          this.getCalculationAtoms();
          this.reSetDialogInfo();
          const { name } = this.activeTabItem();
          this.tableInfoValueMap = {
            type: 'master_table',
            name,
          };
          this.indexTree.handleSetTreeSelected(this.treeData[0].id);
          this.handleUpdatePublishStatus(res.data.publish_status);
        }
      })
      ['finally'](() => {
        this.dialogInfo.buttonLoading = false;
      });
  }

  /**
     * 获取统计口径详情
     * @param data
     */
  public calculationAtomDetail(data) {
    if (!data.editable) {
      return;
    }
    this.isAllLoading = true;

    // 打开统计口径侧边栏
    this.openAddCaliber();

    calculationAtomDetail(data.calculationAtomName)
      .then(res => {
        if (res.validateResult()) {
          const instance = new BKHttpResponse<ICalculationAtomDetail>(res, false);
          instance.setData(this, 'calculationAtomDetailData');
          this.addMethod = data.calculationAtomType;
          this.$refs.addCaliber.mode = 'edit';
        }
      })
      ['finally'](() => {
        this.isAllLoading = false;
      });
  }

  /**
     * 获取统计口径列表
     */
  public getCalculationAtoms() {
    this.isLoading = true;
    getCalculationAtoms(this.$route.params.modelId, true)
      .then(res => {
        if (res.validateResult()) {
          // 事实表没有统计口径不允许下一步
          if (this.activeModelTabItem?.modelType === 'fact_table') {
            this.$parent.nextBtnDisabled = !res.data.results.length;
          }

          this.treeData[0].children = [];
          const instance = new BKHttpResponse<ICalculationAtomsData>(res);
          instance.setData(this, 'calculationAtomsInfo');
          this.calculationAtomsInfo.results.forEach(item => {
            this.$set(item, 'displayName', `${item.calculationAtomName}（${item.calculationAtomAlias}）`);
          });

          // 总指标数
          let totalCount = 0;
          this.calculationAtomsInfo.results.forEach(item => {
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
              const linkNodes = getChildNodes(item.indicators, item.calculationFormulaStrippedComment);
              childNode.children.push(...linkNodes);
            }
            this.treeData[0].count = totalCount;
            this.treeData[0].children.push(childNode);
          });
          // 更新激活节点信息
          const timer = setTimeout(() => {
            const nodeId = this.activeNode && this.activeNode.id;
            if (nodeId) {
              const newNode = this.indexTree.handleGetNodeById(nodeId);
              this.activeNode = newNode;
            }
            clearTimeout(timer);
          }, 300);
        }
      })
      ['finally'](() => {
        this.isLoading = false;
      });
  }

  /**
     * 点击下一步方法
     */
  public nextStepClick() {
    confirmModelIndicators(this.modelId).then(res => {
      if (res.validateResult(null, null, false)) {
        const activeTabItem = this.DataModelTabManage.getActiveItem()[0];
        activeTabItem.lastStep = res.data.step_id;
        this.DataModelTabManage.updateTabItem(activeTabItem);
        this.DataModelTabManage.dispatchEvent('updateModel', [
          {
            model_id: this.modelId,
            step_id: res.data.step_id,
          },
        ]);
      }
    });
  }

  /**
     * 更新模型发布状态
     * @param status
     */
  public handleUpdatePublishStatus(status: string) {
    const activeTabItem = this.DataModelTabManage.getActiveItem()[0];
    activeTabItem.publishStatus = status;
    this.DataModelTabManage.updateTabItem(activeTabItem);
    this.DataModelTabManage.dispatchEvent('updateModel', [
      {
        model_id: this.modelId,
        publish_status: status,
      },
    ]);
  }
}
