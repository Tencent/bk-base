

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
  <div class="dict-mart-wrapper">
    <keep-alive :include="keepAliveRoute">
      <router-view v-if="keepAliveRoute.includes($route.name)"
        :selectedParams.sync="selectedParams"
        :tagParam.sync="tagParam"
        @changeAdvanceParams="changeAdvanceParams" />
    </keep-alive>
    <router-view v-if="!keepAliveRoute.includes($route.name)" />
  </div>
</template>

<script>
import Bus from '@/common/js/bus.js';

const cacheRoute = ['DataDictionary', 'DataSearchResult', 'DataMap'];

export default {
  data() {
    return {
      keepAliveRoute: [...cacheRoute],
      selectedParams: {
        project_id: '',
        biz_id: '',
        channelKeyWord: 'bk_data',
      },
      tagParam: {
        selectedTags: [],
        searchTest: '',
        advanceSearch: {
          heat_operate: '',
          heat_score: '',
          asset_value_operate: '',
          asset_value_score: '',
          storage_capacity_operate: '',
          storage_capacity: '',
          range_operate: '',
          range_score: '',
          importance_operate: '',
          importance_score: '',
          assetvalue_to_cost: '',
          assetvalue_to_cost_operate: '',
          storage_type: '',
          created_by: '',
          me_type: '',
          standard_name: '',
          tag_code: '',
          standard_content_id: '',
          standard_content_name: '',
          cal_type: []
        },
      },
    };
  },
  watch: {
    '$route.name'() {
      if (this.$route.name === 'DataDictionary') {
        this.tagParam = {
          selectedTags: [],
          searchTest: '',
          advanceSearch: {
            heat_operate: '',
            heat_score: '',
            asset_value_operate: '',
            asset_value_score: '',
            storage_capacity_operate: '',
            storage_capacity: '',
            range_operate: '',
            range_score: '',
            importance_operate: '',
            importance_score: '',
            assetvalue_to_cost: '',
            assetvalue_to_cost_operate: '',
            storage_type: '',
            created_by: '',
            me_type: '',
            standard_name: '',
            tag_code: '',
            standard_content_id: '',
            standard_content_name: '',
            cal_type: []
          },
        };
      }
    }
  },
  mounted() {
    // 数据地图和数据字典需要点击面包屑一级菜单刷新本页面，将本组件从动态路由里去掉再加入
    Bus.$on('dataMarketReflshPage', () => {
      const index = this.keepAliveRoute.findIndex(item => item === this.$route.name);
      index >= 0 && this.keepAliveRoute.splice(index, 1);
      this.$nextTick(() => {
        this.keepAliveRoute = [...cacheRoute];
      });
    });
  },
  methods: {
    changeAdvanceParams(params) {
      this.selectedParams = params.selectedParams;
      this.tagParam = params.tagParam;
    },
  },
};
</script>

<style lang="scss" scoped>
.dict-mart-wrapper {
  height: 100%;
}
</style>
