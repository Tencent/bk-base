

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
  <Layout
    :crumbName="[
      { name: $t('标准列表'), to: '/data-mart/data-standard' },
      {
        name: `${$t('标准详情')}(${standardDetail.basic_info.standard_name})`,
      },
    ]"
    :withMargin="isSmallScreen">
    <div v-bkloading="{ isLoading: isLoading }"
      class="standard-detail-container">
      <StandardBasicInfo :standardDetail="standardDetail" />
      <StandardContent :treeId="treeId"
        :standardContent="standardDetail" />
    </div>
  </Layout>
</template>

<script>
import Layout from '@/components/global/layout';
import StandardBasicInfo from './StandardBasicInfo';
import StandardContent from './StandardContent';

export default {
  components: {
    Layout,
    StandardBasicInfo,
    StandardContent,
  },
  data() {
    return {
      standardDetail: {
        basic_info: {
          tag_list: [],
          standard_name: '',
          detaildata_count: 0,
          indicator_count: 0,
        },
        version_info: {},
        standard_content: [],
      },
      id: '',
      isLoading: true,
      treeId: 0,
    };
  },
  computed: {
    isSmallScreen() {
      return document.body.clientWidth < 1441;
    },
  },
  mounted() {
    this.id = this.$route.query.id;
    this.treeId = this.$route.params.treeId;
    this.getDetailData();
  },
  methods: {
    getDetailData() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('dataStandard/getStandardDetail', {
          query: {
            dm_standard_config_id: this.id,
          },
        })
        .then(res => {
          if (res.result) {
            if (!Object.keys(res.data).length) return;
            this.standardDetail = res.data;
            const sortObj = {
              detaildata: 2,
              indicator: 1,
            };
            this.standardDetail.standard_content.sort(
              (a, b) => sortObj[b.standard_info.standard_content_type] - sortObj[a.standard_info.standard_content_type]
            );
            this.standardDetail.standard_content.forEach(item => {
              item.standard_info['standard_name'] = this.standardDetail.basic_info.standard_name;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .layout-content {
  &.with-margin {
    margin: 0 50px;
  }
}
.standard-detail-container {
  margin: 0 auto;
  width: 1600px;
}
</style>
