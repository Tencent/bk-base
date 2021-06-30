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

import { exitFullscreen, fullScreen, isFullScreen, showMsg } from '@/common/js/util.js';
import NodeHeader from '@/pages/DataGraph/Graph/Children/components/NodeHeader.vue';
import Layout from '@/pages/DataGraph/Graph/Children/components/TribleColumnLayout.vue';
import { getMasterTableInfo } from '@/pages/DataModelManage/Api/index';
import { IDataModelManage } from '@/pages/DataModelManage/Interface/index';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { Component, Prop, Provide, Vue, Watch } from 'vue-property-decorator';
import { createIndicator, editIndicator, getIndicatorDetail, getIndicatorFields } from '../../../Api/index';
import { ICalculationAtomsResults, ICreateIndexParams } from '../../../Interface/indexDesign';
import IndexConfig from './IndexConfig.vue';
import IndicatorInput from './IndicatorInput.vue';
import IndicatorOutput from './IndicatorOutput.vue';

/*** 添加指标口径 */
@Component({
  components: {
    NodeHeader,
    IndicatorOutput,
    IndexConfig,
    Layout,
    IndicatorInput,
    GraphNode: () => import('@/pages/DataGraph/Graph/Graph.node.vue'),
  },
})
export default class AddIndicator extends Vue {
  @Prop({ default: () => ({}) }) indexInfo: object;
  @Prop({ default: () => [] }) calculationAtomsList: ICalculationAtomsResults[];

  @Provide()
  handleSliderClose = this.closeSlider;

  slideWidth = 600;

  params: ICreateIndexParams = {
    model_id: this.$route.params.modelId,
    indicator_name: '',
    indicator_alias: '',
    description: '',
    calculation_atom_name: '',
    aggregation_fields: [],
    filter_formula: '-- WHERE 条件语句 \n-- 示例：where os=\'andrioid\' AND plat=\'qq\' \n',
    scheduling_content: {},
    parent_indicator_name: null,
    scheduling_type: '',
  };

  isLoading = false;

  isMainLoading = false;

  // 节点类型 实时stream 离线batch
  nodeType = 'stream';

  // 模型相关的字段列表
  fieldsList: object[] = [];

  // 模型相关的信息
  masterTableInfo: IDataModelManage.IMasterTableInfo = {
    fields: [],
  };

  // 区分保存还是修改指标，false为保存
  isSaveNode = false;

  isShow = false;

  // 是否增加指标
  isAddIndicator = false;

  childCompParams = {
    indexConfig: {},
    dataOutput: {},
  };

  // 编辑状态初始节点类型
  initNodeType = '';

  isFullScreen = false;

  fullScreenHandle() {
    const ele = document.getElementsByClassName('bk-sideslider-wrapper')[0];
    isFullScreen() ? exitFullscreen() : fullScreen(ele);
    this.isFullScreen = !isFullScreen();
  }

  indicatorFields: any[] = [];

  fieldCategoryMap = {
    dimension: this.$t('维度'),
    measure: this.$t('度量'),
  };

  get modelId() {
    return Number(this.$route.params.modelId);
  }

  get isRestrictedEdit() {
    return this.isSaveNode && this.indexInfo.hasSubIndicators;
  }

  get isChildIndicator() {
    if (this.isSaveNode) {
      return !!this.indexInfo.parentIndicatorName;
    }
    return this.indexInfo?.type === 'indicator';
  }

  get fieldInfo() {
    let name = `${this.indexInfo.indicatorAlias} (${this.indexInfo.indicatorName})`;
    if (this.isChildIndicator && this.isSaveNode) {
      name = `${this.indexInfo.parentIndicatorAlias} (${this.indexInfo.parentIndicatorName})`;
    }
    return [
      {
        key: name,
        fields: this.isChildIndicator
          ? this.indicatorFields.map(item => {
            return {
              name: item.fieldName,
              type: item.fieldType,
              iconName: item.fieldCategory,
            };
          })
          : this.fieldsList,
      },
    ];
  }

  // 聚合字段列表
  get aggregateFieldList() {
    return (this.masterTableInfo.fields || [])
      .filter(item => item.fieldCategory === 'dimension')
      .map(child => {
        child.displayName = `${child.fieldName}（${child.fieldAlias}）`;
        return child;
      });
  }

  get configAggregateFieldList() {
    if (this.isChildIndicator) {
      return this.indicatorFields
        .filter(item => item.fieldCategory !== 'measure')
        .map(item => Object.assign(item, { displayName: `${item.fieldName}（${item.fieldAlias}）` }));
    }
    return this.aggregateFieldList;
  }

  get calculationFormula() {
    return this.indexInfo.calculationFormulaStrippedComment || this.indexInfo.calculationFormula || '';
  }

  get helpDocUrl() {
    return this.$store.getters['docs/getPaths'].helpDocRoot;
  }

  get curCalculationAtom() {
    return this.calculationAtomsList.find(atom => atom.calculationAtomName === this.params.calculation_atom_name);
  }

  get aggregationFields() {
    return this.aggregateFieldList.filter(field => this.params.aggregation_fields.includes(field.fieldName));
  }

  get indicatorTableFields() {
    const fields = [];
    if (this.curCalculationAtom) {
      const {
        calculationAtomName: fieldName,
        calculationAtomAlias: fieldAlias,
        fieldType,
        description,
      } = this.curCalculationAtom;
      fields.push({ fieldName, fieldAlias, fieldType, description, fieldCategory: this.$t('度量') });
    }
    for (const field of this.aggregationFields) {
      const { fieldName, fieldAlias, fieldType, fieldCategory, description } = field;
      fields.push({
        fieldName,
        fieldAlias,
        fieldType,
        description,
        fieldCategory: this.fieldCategoryMap[fieldCategory],
      });
    }
    return fields;
  }

  @Watch('params.parent_indicator_name', { immediate: true })
  handleGetIndicatorFields(name: string) {
    if (this.isChildIndicator && this.isShow) {
      getIndicatorFields(name, this.modelId).then(res => {
        res.setDataFn(data => {
          this.indicatorFields = data.fields || [];
        });
      });
    }
  }

  mounted() {
    this.getMasterTableInfo();
    this.slideWidth = document.body.clientWidth * 0.95;
  }

  changeIndicatorParams(indicatorParams: object) {
    this.params = Object.assign({}, this.params, indicatorParams);
  }

  init() {
    this.isShow = true;
    if (this.isChildIndicator) {
      this.params.parent_indicator_name = this.indexInfo?.indicatorName;
    }
    if (this.indexInfo?.calculationAtomName) {
      // 默认选中指标统计口径
      this.params.calculation_atom_name = this.indexInfo.calculationAtomName;
    }
  }

  reSetParams() {
    this.nodeType = 'stream';
    this.initNodeType = '';
    this.isSaveNode = false;
    this.params = {
      model_id: this.$route.params.modelId,
      indicator_name: '',
      indicator_alias: '',
      description: '',
      calculation_atom_name: '',
      aggregation_fields: [],
      filter_formula: '-- WHERE 条件语句 \n-- 示例：where os=\'andrioid\' AND plat=\'qq\' \n',
      scheduling_content: {},
      parent_indicator_name: null,
      scheduling_type: '',
    };
  }

  // 获取指标详情
  getIndicatorDetail(indicatorName: string) {
    this.isMainLoading = true;
    this.isShow = true;
    getIndicatorDetail(indicatorName, undefined, this.modelId)
      .then(res => {
        if (res.validateResult()) {
          this.params = Object.assign({}, this.params, res.data);
          this.initNodeType = this.params.scheduling_type;
          this.isSaveNode = true;
          this.nodeType = this.params.scheduling_type;
        }
      })
      ['finally'](() => {
        this.isMainLoading = false;
      });
  }

  closeSlider() {
    this.$emit('closeSlider');
  }

  handleBtnSave() {
    this.params.scheduling_type = this.nodeType;
    const childComp = [this.$refs.indexConfig, this.$refs.dataOutput];
    Promise.all(childComp.map(item => item.validateFormData())).then(res => {
      if (res.every(item => item)) {
        this.isSaveNode ? this.editIndicator() : this.createIndicator();
      }
    });
  }

  handlebtnClose() {
    this.isShow = false;
  }

  // 获取主表详情
  getMasterTableInfo() {
    getMasterTableInfo(this.$route.params.modelId).then(res => {
      const instance = new BKHttpResponse<IDataModelManage.IMasterTableInfo>(res);
      instance.setData(this, 'masterTableInfo');
      if (res.validateResult()) {
        this.fieldsList = this.masterTableInfo.fields.map(item => {
          return {
            name: item.fieldName,
            type: item.fieldType,
            iconName: item.fieldCategory,
          };
        });
      }
    });
  }

  // 新建指标
  createIndicator() {
    this.isLoading = true;
    this.$emit('sendReport');
    createIndicator(this.params).then(res => {
      if (res.validateResult()) {
        showMsg(this.$t('创建指标成功!'), 'success', { delay: 1500 });
        this.isShow = false;
        // 刷新统计口径下指标列表
        this.$emit('updateIndexList', {
          name: this.isChildIndicator
            ? this.params.parent_indicator_name
            : this.indexInfo.calculationAtomName,
          isChildIndicator: this.isChildIndicator,
        });
        this.$emit('updatePublishStatus', res.data.publish_status);
        this.reSetParams();
      }
      this.isLoading = false;
    });
  }

  // 编辑指标
  editIndicator() {
    this.isLoading = true;
    const params = Object.assign({}, this.params, { indicator_id: this.indexInfo.indicatorId });
    editIndicator(params).then(res => {
      if (res.validateResult()) {
        showMsg(this.$t('修改指标成功!'), 'success', { delay: 1500 });
        this.isShow = false;
        // 刷新统计口径下指标列表
        this.$emit('updateIndexList', {
          name: this.isChildIndicator
            ? this.params.parent_indicator_name
            : this.indexInfo.calculationAtomName,
          isChildIndicator: this.isChildIndicator,
        });
        this.$emit('updatePublishStatus', res.data.publish_status);
        this.reSetParams();
      }
      this.isLoading = false;
    });
  }
}
