

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
  <div class="data-inventory-wrap">
    <Layout :crumbName="[{ name: $t('数据字典'), to: '/data-mart/data-dictionary' }]">
      <template slot="defineTitle">
        <a href="javascript:void(0);"
          class="crumb-item with-router fl">
          {{ $t('数据标准') }}
        </a>
        <span class="crumb-desc">{{ $t('对数据进行标准化') }}</span>
      </template>
      <div class="page-main">
        <div class="search-bar">
          <TagComBinedSearch ref="tag"
            v-model="tagParam"
            :tagTypes="[$t('来源标签'), $t('数据描述')]"
            :width="'100%'"
            :placeholder="$t('请输入数据标准名')"
            :isShowSearch="false"
            @choseTag="choseTag" />
        </div>
        <StandardTable ref="standaradTable" />
      </div>
    </Layout>
  </div>
</template>

<script>
import { postMethodWarning } from '@/common/js/util';
import Layout from '../../components/global/layout';
import TagComBinedSearch from './DataDictionaryComp/TagComBinedSearch';
import { mapGetters } from 'vuex';
import StandardTable from '@/pages/datamart/DataStandardComp/StandardTable';
import { checkParams } from '@/pages/datamart/common/utils.js';

export default {
  name: 'DataStandard',
  components: {
    Layout,
    TagComBinedSearch,
    StandardTable,
  },
  provide() {
    return {
      selectedParams: this.selectedParams,
      tagParam: this.tagParam,
      getTags: this.getTags,
      handerSafeVal: this.handerSafeVal,
      getParams: this.getParams,
    };
  },
  data() {
    return {
      selectedParams: {
        project_id: '',
        biz_id: '',
        channelKeyWord: 'bk_data',
      },
      tagList: [],
      tagParam: {
        selectedTags: [],
        searchTest: '',
      },
      stInsSearchText: null,
    };
  },
  computed: {
    ...mapGetters({
      flatTags: 'dataTag/getAllFlatTags',
    }),
  },
  watch: {
    selectedParams: {
      deep: true,
      handler() {
        this.updateData();
      },
    },
    'tagParam.searchTest'(val) {
      if (!val.length) {
        clearTimeout(this.stInsSearchText);
        this.updateData();
        return;
      }
      this.dealSearchText(val);
    },
    'tagParam.selectedTags': {
      deep: true,
      handler() {
        this.updateData();
      },
    },
  },
  methods: {
    getParams() {
      return {
        bk_biz_id: this.handerSafeVal(this.selectedParams.biz_id),
        project_id: this.handerSafeVal(this.selectedParams.project_id),
        tag_ids: this.getTags(),
        keyword: this.tagParam.searchTest,
        tag_code: 'virtual_data_mart',
        me_type: 'tag',
        has_standard: 1,
        cal_type: ['standard'],
        data_set_type: 'all',
        page: 1,
        page_size: 10,
        platform: this.handerSafeVal(this.selectedParams.channelKeyWord) || 'all',
      };
    },
    dealSearchText(val) {
      this.stInsSearchText && clearTimeout(this.stInsSearchText);
      this.stInsSearchText = setTimeout(() => {
        if (checkParams(val)) {
          this.updateData();
        } else {
          postMethodWarning('搜索关键字至少要3个字符', 'error');
        }
      }, 1000);
    },
    updateData() {
      this.$refs.standaradTable.updateData(this.getParams());
    },
    handerSafeVal(value) {
      return value || null;
    },
    getTags() {
      return this.tagParam.selectedTags.map(item => item.tag_code);
    },
    choseTag(val) {
      this.tagList = [];
      val.forEach(item => {
        this.tagList.push(item.tag_code);
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.data-inventory-wrap {
  height: 100%;
}

.crumb-item {
  font-weight: 500;
  height: 40px;
  line-height: 40px;
  background: #efefef;
  color: #1a1b2d;
  font-size: 14px;
}
.crumb-desc {
  margin-left: 20px;
  padding-top: 1px;
  font-size: 12px;
  color: rgb(160, 157, 157);
  font-weight: normal;
}
.page-main {
  margin-bottom: 50px;
}
.min-width {
  min-width: 1290px;
}
.search-bar {
  margin-bottom: 20px;
  .tag-wrap {
    margin: 0;
  }
}
</style>
