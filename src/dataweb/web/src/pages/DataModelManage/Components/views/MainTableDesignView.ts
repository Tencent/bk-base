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

import { Component, Prop } from 'vue-property-decorator';
import { IDataModelManage } from '../../Interface/index';
import { CDataModelManage } from '../../Controller/MasterTable';
import { DataModelManageBase } from '../../Controller/DataModelManageBase';
import { getMasterTableInfo, getFieldContraintConfigs } from '../../Api';
import { ConditionSelector } from '../Common/index';
import { FieldProcessingLogic } from '../Steps/ChildNodes/index';

@Component({
  components: { ConditionSelector, FieldProcessingLogic },
})
export default class MainTableDesignView extends DataModelManageBase {
  @Prop({ required: true }) public readonly data!: object;

  get masterTableInfo() {
    return this.data.masterTableInfo || {};
  }
  get fieldContraintConfigList() {
    return this.data.fieldContraintConfigList || [];
  }
  get activeModelTabItem() {
    return this.DataModelTabManage.getActiveItem()[0];
  }
  get fields() {
    const fields = this.masterTableInfo.fields || [];
    for (let i = 0; i < fields.length; i++) {
      const cur = fields[i];
      const next = fields[i + 1];
      if (cur?.isExtendedField && next?.isExtendedField) {
        cur.isExtendedFieldCls = true;
      }
    }
    return fields;
  }

  public fieldCategoryMap = {
    measure: this.$t('度量'),
    dimension: this.$t('维度'),
  };
  public fieldCleanContentSlider = {
    isShow: false,
    title: '',
    field: new CDataModelManage.MstTbField({}),
  };

  public rowClassName({ row }) {
    // if (row.fieldName === '__time__') return 'is-inner-row'
    return row?.isExtendedField ? 'is-extended-row' : '';
  }

  public cellClassName({ row, column, rowIndex, columnIndex }) {
    return columnIndex === 0 && row?.isExtendedFieldCls ? 'cell-border-bottom-none' : '';
  }

  public getConstraintList(row: IDataModelManage.IMasterTableField) {
    return this.fieldContraintConfigList.filter(cfg => cfg.allowFieldType.some(t => t === row.fieldType));
  }

  /**
   * 点击字段加工逻辑弹出侧栏
   */
  public handleFieldProcessingLogic(row: IDataModelManage.IMasterTableField) {
    this.fieldCleanContentSlider.field = row;
    this.fieldCleanContentSlider.title = `${row.fieldName} (${row.fieldAlias})`;
    this.fieldCleanContentSlider.isShow = true;
  }
}
