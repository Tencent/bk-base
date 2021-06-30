

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
  <div>
    <bkdata-collapse v-model="activeName"
      extCls="collapse-background-style">
      <bkdata-collapse-item v-for="(item, index) in standardContent"
        :key="index"
        :name="`${index}`">
        <div>
          {{ `${contentType[item.standard_info.standard_content_type]}:${item.standard_info.standard_content_name}` }}
        </div>
        <div slot="content"
          class="f13">
          <ContentTable :standardInfo="item.standard_info"
            :tableData="item.standard_fields"
            :isShowCalcSet="item.standard_info.standard_content_type === 'indicator'" />
        </div>
      </bkdata-collapse-item>
    </bkdata-collapse>
  </div>
</template>
<script>
import ContentTable from './ContentTable';
import Bus from '@/common/js/bus.js';

export default {
  components: {
    ContentTable,
  },
  props: {
    standardContent: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      activeName: '0',
      contentType: {
        detaildata: this.$t('明细指标'),
        indicator: this.$t('原子指标'),
      },
    };
  },
  mounted() {
    Bus.$on('openCollapse', id => {
      this.activeName = [
        this.standardContent.findIndex(item => {
          return item.standard_info.id === id;
        }),
      ];
    });
  },
  methods: {
    getCalcShow(data) {
      // 类型为原子指标并且计算配置相关属性有值的情况展示
      return Object.keys(data.standard_info.window_period).length > 0
        && data.standard_info.standard_content_sql
        && data.standard_info.standard_content_type === 'indicator';
    },
  },
};
</script>
