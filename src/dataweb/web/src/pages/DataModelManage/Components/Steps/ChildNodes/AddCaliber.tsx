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

import { showMsg } from '@/common/js/util.js';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { Component, Prop, Vue, Watch } from 'vue-property-decorator';
import {
  createCalculationAtom,
  editCalculationAtom,
  getCalculationAtomsCanBeQuoted,
  quoteCalculationAtom,
} from '../../../Api/index';
import {
  ICalculationAtomsCanBeQuoted,
  ICalculationAtomsCanBeQuoteResult,
  ICalculationAtomsResults,
  IQuoteCalculationAtom,
  IQuoteCalculationAtomData,
} from '../../../Interface/indexDesign';
import CommonFormCondition from './CommonFormCondition.vue';

/*** 添加指标口径 */
@Component({
  components: {
    DataTable,
    CommonFormCondition,
  },
})
export default class AddCaliber extends Vue {
  @Prop({ default: () => ({}) }) calculationAtomDetailData: Object;

  @Prop({ default: 'quote' }) addMethod: string;

  @Prop({ default: false }) isAllLoading: boolean;

  @Prop({ default: () => [] }) aggregationLogicList: Array<ICalculationAtomsResults>;

  appHeight: number = window.innerHeight;

  // 模式（created、edit）
  mode = 'create';

  // 侧边栏是否展开
  isShow = false;

  isLoading = false;

  isSubmitLoading = false;

  groupSetting = {
    selected: 'quote',
  };

  // 选取的指标
  selectedIndex: ICalculationAtomsResults[] = [];

  calculationAtomsInfo = {
    step_id: 1,
    results: [],
  };

  isSelectedAll = false;

  calcPageSize = 10;

  get isEdit() {
    return this.mode === 'edit';
  }

  get isRestrictedEdit() {
    return this.isEdit && this.calculationAtomDetailData.key_params_editable === false;
  }

  get title() {
    return this.mode === 'create' ? $t('添加指标统计口径') : $t('编辑指标统计口径');
  }

  @Watch('addMethod', { immediate: true })
  onAddMethodChanged(val: string) {
    this.groupSetting.selected = val;
  }

  handleResetHeight() {
    this.appHeight = window.innerHeight;
  }

  created() {
    window.addEventListener('resize', this.handleResetHeight);
    this.getCalculationAtoms();
  }

  beforeDestroy() {
    window.removeEventListener('resize', this.handleResetHeight);
  }

  changeCheckBox(isChecked: boolean, data: ICalculationAtomsResults) {
    data.isChecked = isChecked;
    this.isSelectedAll = this.calculationAtomsInfo.results.every(
      (item: ICalculationAtomsCanBeQuoteResult) => item.isChecked
    );
  }

  selectAll(data: ICalculationAtomsResults[]) {
    this.calculationAtomsInfo.results.forEach((item: ICalculationAtomsCanBeQuoteResult) => {
      this.$set(item, 'isChecked', data.length ? true : false);
    });
  }
  onChange(flag: boolean) {
    this.isSelectedAll = flag;
    this.calculationAtomsInfo.results.forEach((item: ICalculationAtomsCanBeQuoteResult) => {
      this.$set(item, 'isChecked', flag);
    });
  }

  renderHeader(h) {
    // 旧的babel环境jsx不支持v-model指令,所以在jsx里尽量不用v-model
    return (
      <bkdata-checkbox
        trueValue={true}
        falseValue={false}
        onChange={this.onChange}
        value={this.isSelectedAll}
      ></bkdata-checkbox>
    );
  }

  quoteCalculationAtomResult: IQuoteCalculationAtomData = {};

  reSetData() {
    this.mode = 'create';
    this.$emit('reSetData');
    this.calculationAtomsInfo.results.forEach((item: ICalculationAtomsCanBeQuoteResult) => {
      item.isChecked = false;
    });
  }

  // 获取统计口径列表
  public getCalculationAtoms() {
    this.isLoading = true;
    getCalculationAtomsCanBeQuoted(this.$route.params.modelId)
      .then(res => {
        if (res.validateResult()) {
          const instance = new BKHttpResponse<ICalculationAtomsCanBeQuoted>(res);
          instance.setData(this, 'calculationAtomsInfo');
          if (this.calculationAtomsInfo.results.length) {
            this.calculationAtomsInfo.results.forEach((item: ICalculationAtomsCanBeQuoteResult) => {
              this.$set(item, 'isChecked', false);
            });
          } else {
            this.groupSetting.selected = 'create';
          }
        }
      })
      ['finally'](() => {
        this.isLoading = false;
      });
  }

  public handleSubmit(params: object) {
    const { field_type, description, calculation_atom_name, calculation_atom_alias, calculation_content } = params;
    if (this.mode === 'create') {
      this.createCalculationAtom(
        field_type,
        description,
        calculation_atom_name,
        calculation_atom_alias,
        calculation_content
      );
    } else {
      this.editCalculationAtom(
        field_type,
        description,
        calculation_atom_name,
        calculation_atom_alias,
        calculation_content
      );
    }
  }

  createCalculationAtom(
    field_type: string,
    description: string,
    calculation_atom_name: string,
    calculation_atom_alias: string,
    calculation_content: object
  ) {
    this.isSubmitLoading = true;
    this.$emit('sendReport');
    createCalculationAtom(
      this.$route.params.modelId,
      field_type,
      description,
      calculation_atom_name,
      calculation_atom_alias,
      calculation_content
    )
      .then(res => {
        if (res.validateResult()) {
          this.$emit('updatePublishStatus', res.data.publish_status);
          this.handleShowMsg(this.$t('添加指标统计口径成功！'));
        }
      })
      ['finally'](() => {
        this.isSubmitLoading = false;
      });
  }

  editCalculationAtom(
    field_type: string,
    description: string,
    calculation_atom_name: string,
    calculation_atom_alias: string,
    calculation_content: object
  ) {
    this.isSubmitLoading = true;
    editCalculationAtom(
      this.$route.params.modelId,
      field_type,
      description,
      calculation_atom_name,
      calculation_atom_alias,
      calculation_content
    )
      .then(res => {
        if (res.validateResult()) {
          this.$emit('updatePublishStatus', res.data.publish_status);
          this.handleShowMsg(this.$t('编辑指标统计口径成功！'));
        }
      })
      ['finally'](() => {
        this.isSubmitLoading = false;
      });
  }

  public quoteCalculationAtom() {
    const calculationAtomNames = this.calculationAtomsInfo.results
      .filter((item: ICalculationAtomsCanBeQuoteResult) => item.isChecked)
      .map((item: ICalculationAtomsCanBeQuoteResult) => item.calculationAtomName);
    if (!calculationAtomNames.length) {
      showMsg($t('请选中需要引用的指标统计口径！'), 'warning');
      return;
    }
    this.isSubmitLoading = true;
    quoteCalculationAtom(this.$route.params.modelId, calculationAtomNames)
      .then(res => {
        if (res.validateResult()) {
          const instance = new BKHttpResponse<IQuoteCalculationAtom>(res);
          instance.setData(this, 'quoteCalculationAtomResult');
          this.handleShowMsg(this.$t('添加指标统计口径成功！'));
          this.$emit('updatePublishStatus', res.data.publish_status);
          calculationAtomNames.forEach(name => {
            this.calculationAtomsInfo.results.splice(
              this.calculationAtomsInfo.results.indexOf(
                this.calculationAtomsInfo.results.find(item => item.calculationAtomName === name)
              ),
              1
            );
          });
        }
      })
      ['finally'](() => {
        this.isSubmitLoading = false;
      });
  }

  public handleShowMsg(text: string) {
    showMsg(text, 'success', { delay: 1500 });
    this.isShow = false;
    this.$emit('reFresh');
  }
}
