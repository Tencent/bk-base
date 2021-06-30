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

import { Component, Prop, Vue } from 'vue-property-decorator';
import HeaderType from '@/pages/datamart/common/components/HeaderType.vue';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';
import { getFixRuleConfig } from '@/pages/datamart/Api/DataQuality';
// 数据接口
import { IQualityOverviewInfo, IQualityOverviewData } from '@/pages/datamart/InterFace/DataDict/DataQualityFix.ts';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { dataTypeIds } from '@/pages/datamart/common/config';

@Component({
  components: {
    HeaderType,
    DataTable,
  },
})
export default class DataFix extends Vue {
  @Prop({ default: '' }) processingType: string;

  isLoading = false;

  calcPageSize = 10;

  tableData: IQualityOverviewData[] = [];

  get OperateName(): string {
    return this.tableData.length ? this.$t('查看修正详情') : this.$t('创建修正任务');
  }

  get dataId(): string {
    return this.$route.query[dataTypeIds[this.$route.query.dataType]];
  }

  mounted() {
    this.getFixRuleConfig();
  }

  operateCorrectInfo(): void {
    this.$router.push('/routehub/?rtid=' + this.dataId);
  }

  getFixRuleConfig(): void {
    this.isLoading = true;
    getFixRuleConfig(this.dataId).then(res => {
      console.log(res);
      if (res.result && res.data) {
        const instance = new BKHttpResponse<IQualityOverviewInfo>(res, false);
        instance.setData(this, 'tableData');
      }
      this.isLoading = false;
    });
  }
}
