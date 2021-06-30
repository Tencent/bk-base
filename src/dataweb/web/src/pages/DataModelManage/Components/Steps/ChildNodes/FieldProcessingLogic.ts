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

import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import { getServeFormData } from '@/common/js/Utils';
import { verifyFieldProcessingLogic } from '../../../Api/index';
import { CDataModelManage, MstTbField } from '../../../Controller/MasterTable';
import { IDataModelManage } from '../../../Interface/index';
import Monaco from '@/components/monaco';
import popContainer from '@/components/popContainer';

/*** 字段加工逻辑 */
@Component({
  components: { Monaco, popContainer },
})
export default class FieldProcessingLogic extends Vue {
  @Prop({ default: () => ({}) }) readonly activeModel!: object;
  @Prop({ default: () => [] }) readonly fieldList!: IDataModelManage.IMasterTableField[];
  @Prop({ default: () => new CDataModelManage.MstTbField({}) }) field!: IDataModelManage.IMasterTableField;
  @Prop({ default: true }) readonly editable!: boolean;

  @Ref() public readonly clearConfirmPop!: VNode;

  get options() {
    return {
      fontSize: '14px',
      readOnly: !this.editable,
    };
  }

  get fieldCleanContent() {
    return this.field.fieldCleanContent || {};
  }

  get scopeFieldList() {
    return this.fieldList.map(item => {
      let fieldCleanContent = this.field.uid === item.uid
        ? {
          cleanOption: this.type,
          cleanContent: this.code,
        }
        : item.fieldCleanContent || { cleanOption: '', cleanContent: '' };
      if (!this.code) {
        fieldCleanContent = {};
      }
      return {
        fieldName: item.fieldName,
        fieldType: item.fieldType,
        fieldCleanContent,
      };
    });
  }

  get verifyParams() {
    return {
      tableName: this.activeModel.name,
      verifyFields: [this.field.fieldName],
      scopeFieldList: this.scopeFieldList,
    };
  }

  public monacoSetting: object = {
    tools: {
      guidUrl: this.$store.getters['docs/getPaths'].realtimeSqlRule,
      toolList: {
        font_size: true,
        full_screen: true,
        event_fullscreen_default: true,
        editor_fold: false,
        format_sql: true,
      },
      title: this.$t('SQL编辑器'),
    },
  };

  public defaultCode =
        `-- 功能：通过 if()、CASE WHEN 等方式加工字段\n-- 
    示例 1：字段映射\n/**\n-- case\n--           
    when country=\'香港\' then \'中国香港\'\n--           
    when country=\'澳门\' then \'中国澳门\' \n--           
    when country=\'台湾\' then \'中国台湾\' \n--           
    else country \n-- end as country\n**/\n-- 
    示例 2：结果数据表中不存在数据模型中定义的字段\n-- 2 as uid_type\n`;

  public code = '';

  public type = 'SQL';

  public errorMessage = '';

  public completeSql = '';

  public isVerifying = false;

  public originFieldsDict = {};

  @Watch('field', { immediate: true, deep: true })
  initData() {
    this.code = this.fieldCleanContent.cleanContent || this.defaultCode;
  }

  @Emit('on-save')
  handleSave() {
    const res = {
      fieldCleanContent: {
        cleanContent: this.code,
        cleanOption: this.type,
      },
      field: this.field,
      originFieldsDict: this.originFieldsDict[this.field.fieldName],
    };
    if (!this.code) {
      res.fieldCleanContent = null;
    }
    return res;
  }

  @Emit('on-cancel')
  handleClose() { }

  public handleChangeCode(content: string) {
    this.code = content;
    this.errorMessage = '';
    this.completeSql = '';
  }

  public handleShowConfirm(e) {
    this.clearConfirmPop.handlePopShow(e, { placement: 'top-end', hideOnClick: false, maxWidth: '280px' });
  }

  public handleHideConfirm() {
    this.clearConfirmPop && this.clearConfirmPop.handlePopHidden();
  }

  public handleSubmitClear() {
    this.code = '';
    this.errorMessage = '';
    this.completeSql = '';
    this.handleSave();
    this.handleHideConfirm();
  }

  public handleVerifyCode() {
    this.isVerifying = true;
    const { table_name, verify_fields, scope_field_list } = getServeFormData(this.verifyParams);
    verifyFieldProcessingLogic(table_name, verify_fields, scope_field_list)
      .then(res => {
        if (res.result) {
          this.originFieldsDict = res.data?.origin_fields_dict || {};
          this.handleSave();
        } else {
          [this.errorMessage, this.completeSql] = res.message.split(' <br /> ');
        }
      })
      ['finally'](() => {
        this.isVerifying = false;
      });
  }
}
