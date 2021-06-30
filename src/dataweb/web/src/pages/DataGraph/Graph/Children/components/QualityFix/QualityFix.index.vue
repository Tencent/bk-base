

<!--
  - Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
  - Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
  - BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
  -
  - License for BK-BASE 蓝鲸基础平台:
  - -------------------------------------------------------------------
  -
  - Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
  - documentation files (the "Software"), to deal in the Software without restriction, including without limitation
  - the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
  - and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
  - The above copyright notice and this permission notice shall be included in all copies or substantial
  - portions of the Software.
  -
  - THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
  - LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
  - NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
  - WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  - SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
  -->

<template>
  <section class="quality-fix-container bk-scroll-y">
    <QualityFixConfig
      :correctFieldList="correctFieldList"
      v-bind="$attrs"
      :correct_configs.sync="correctConfig"
      :tableSize="tableSize" />
    <QualityFixOperate :correctFieldList="correctFieldList"
      v-bind="$attrs"
      :correct_configs="correctConfig" />
  </section>
</template>

<script lang="ts">
import { Component, Vue, Prop } from 'vue-property-decorator';
import { getResultTables } from '@/pages/datamart/Api/DataQuality';
// 数据接口
import { IResultInfo, IResultInfoData } from '@/pages/datamart/InterFace/DataQuality';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { showMsg } from '@/common/js/util';
import QualityFixConfig from './QualityFixConfig.vue';
import QualityFixOperate from './QualityFixOperate/QualityFixOperate.index.vue';

@Component({
  components: {
    QualityFixConfig,
    QualityFixOperate,
  },
})
export default class QualityFixIndex extends Vue {
  @Prop({ default: 'normal-table' }) tableSize: string;
  @Prop({ default: () => [] }) correctConfig: any;

  isLoading = false;

  calcPageSize = 10;

  tableData: Array<any> = [];

  correctFieldList: IResultInfoData[] = [];

  mounted() {
    // 修正字段下拉列表
    this.getResultTables();
  }

  getResultTables() {
    getResultTables(this.$attrs.dataSetId).then(res => {
      if (res.result && res.data) {
        res.data.forEach(item => {
          item.field_alias = `${item.field_name}（${item.field_alias}）`;
        });
        const instance = new BKHttpResponse<IResultInfo>(res, false);
        instance.setData(this, 'correctFieldList');
      }
    });
  }
}
</script>

<style lang="scss" scoped>
.quality-fix-container {
  height: 100%;
}
</style>
