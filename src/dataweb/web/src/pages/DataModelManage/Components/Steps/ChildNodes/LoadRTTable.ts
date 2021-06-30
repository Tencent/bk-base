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

import { TsRouteParams } from '@/common/ts/tsVueBase';
import { getBizListByProjectId, getAuthResultTablesByProject } from '@/api/common';
import { getResultTablesFields } from '../../../Api/index';
import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import applyBiz from '@/pages/userCenter/components/applyBizData';

/*** 加载结果表的结构 */
@Component({
  components: { applyBiz },
})
export default class LoadRTTable extends TsRouteParams {
  get projectId() {
    return this.routeQuery.project_id;
  }

  public appHeight: number = window.innerHeight;

  public isBizLoading = false;

  public isResultTableLoading = false;

  public isTableLoading = false;

  public isShowApply = false;

  public formData = {
    bizId: '',
    resultTableId: '',
  };

  public bizList = [];

  public resultTableList = [];

  public resultTableDetail = [];

  public loadFields: any[] = [];

  public created() {
    window.addEventListener('resize', this.handleResetHeight);
    this.getBizListByProjectId();
  }

  public beforeDestroy() {
    window.removeEventListener('resize', this.handleResetHeight);
  }

  public handleResetHeight() {
    this.appHeight = window.innerHeight;
  }

  @Emit('save-table')
  public confirm() {
    // this.$emit('updateResultTable', this.resultTableDetail)
    // this.$emit('update:isLoadRtTableSliderShow', false)
    return this.loadFields;
  }

  @Emit('cancel-table')
  public cancel() {
    // this.$emit('update:isLoadRtTableSliderShow', false)
    return false;
  }

  public getBizListByProjectId() {
    this.isBizLoading = true;
    getBizListByProjectId({ pid: this.projectId })
      .then(res => {
        res.setData(this, 'bizList');
      })
      ['finally'](() => {
        this.isBizLoading = false;
      });
  }

  public getAuthResultTablesByProject(bkBizId: string) {
    this.isResultTableLoading = true;
    getAuthResultTablesByProject({ project_id: this.projectId }, { bk_biz_id: bkBizId })
      .then(res => {
        res.setData(this, 'resultTableList');
      })
      ['finally'](() => {
        this.isResultTableLoading = false;
      });
  }

  public getResultTablesFields(result_table_id: string) {
    this.isTableLoading = true;
    getResultTablesFields(result_table_id)
      .then(res => {
        res.setData(this, 'resultTableDetail');
      })
      ['finally'](() => {
        this.isTableLoading = false;
      });
  }

  public resultTableChange(resultTableId: string) {
    this.getResultTablesFields(resultTableId);
  }

  public bizChange(bkBizId: string) {
    this.formData.resultTableId = '';
    this.resultTableDetail = [];
    this.getAuthResultTablesByProject(bkBizId);
  }

  public handleShowApply() {
    this.isShowApply = true;
    this.$nextTick(() => {
      const projectId = this.routeQuery.project_id && this.routeQuery.project_id.toString();
      this.$refs.applyBiz.show(
        {
          project_id: projectId,
        },
        ''
      );
    });
  }

  public handleCloseApply() {
    this.isShowApply = false;
  }

  public handleDddDataId() {
    const url = this.$router.resolve({ name: 'createDataid' });
    window.open(url.href, '_blank');
  }

  public handleSelectonChange(selection: any[]) {
    this.loadFields = selection;
  }
}
