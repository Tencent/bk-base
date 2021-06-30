

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
  <div class="dictionary-home">
    <Layout :crumbName="[{ name: $t('数据字典'), to: '/data-mart/data-dictionary' }]">
      <template slot="defineTitle">
        <a href="javascript:void(0);"
          class="crumb-item with-router fl"
          @click.stop="reflshPage('DataDictionary')">
          {{ $t('数据字典') }}
        </a>
        <span class="crumb-desc">{{ $t('对数据进行检索_详情查看及血缘关系追溯') }}</span>
      </template>
      <div class="page-main">
        <div class="search-bar">
          <ConditionSelect v-model="selectedParamsValue" />
        </div>
        <TagComBinedSearch ref="tag"
          v-model="tagParamValue"
          :isShowAdvanceSearch="true"
          :width="'900px'"
          :size="'big'"
          :placeholder="$t('请输入数据集名_表名_描述')"
          @choseTag="choseTag"
          @linkToSeartResult="linkToSeartResult"
          @historySearch="historySearch"
          v-on="$listeners" />
      </div>
      <DictOveriew v-if="$modules.isActive('lifecycle')"
        :isInitRequest="true" />
    </Layout>
  </div>
</template>

<script>
import Layout from '../../components/global/layout';
import ConditionSelect from './DataDictionaryComp/ConditionSelect';
import TagComBinedSearch from './DataDictionaryComp/TagComBinedSearch';
import DictOveriew from './DataDictionaryComp/DictOveriew';
import Bus from '@/common/js/bus.js';

export default {
  name: 'DataDictionary',
  components: {
    Layout,
    ConditionSelect,
    TagComBinedSearch,
    DictOveriew,
  },
  props: {
    selectedParams: {
      type: Object,
      default: () => ({}),
    },
    tagParam: {
      type: Object,
      default: () => ({
        selectedTags: [],
        searchTest: '',
        advanceSearch: {},
      }),
    },
  },
  data() {
    return {
      tagList: [],
    };
  },
  computed: {
    tagParamValue: {
      get() {
        return this.tagParam;
      },
      set(val) {
        Object.assign(this, 'tagParam', val);
      },
    },
    selectedParamsValue: {
      get() {
        return this.selectedParams;
      },
      set(value) {
        Object.assign(this, 'selectedParams', val);
      },
    },
  },
  watch: {
    '$route.name': {
      immediate: true,
      handler(newVal, oldVal) {
        // 搜索结果页跳转回预览页，需要回填数据
        if (oldVal === 'DataSearchResult') {
          this.$refs.tag.backData(this.tagParam.advanceSearch);
        }
      },
    },
  },
  activated() {
    this.$refs.tag.getDataTagListFromServer(true, true);
  },
  methods: {
    reflshPage(routeName) {
      Bus.$emit('dataMarketReflshPage');
    },
    historySearch(backFillParams, params) {
      // 数据所在系统、项目、业务回填
      for (const key in backFillParams.selectedParams) {
        // eslint-disable-next-line vue/no-mutating-props
        this.selectedParams[key] = backFillParams.selectedParams[key];
      }
      this.$router.push({
        name: 'DataSearchResult',
        params: {
          historySearchParams: {
            params,
            backFillParams,
          },
        },
      });
    },
    linkToSeartResult() {
      this.$router.push({
        name: 'DataSearchResult',
        params: {
          advanceSearchParams: {
            selectedParams: this.selectedParams,
            tagParam: this.tagParam,
          },
        },
      });
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
.search-bar {
  margin-bottom: 40px;
  display: flex;
  justify-content: flex-end;
}
.dictionary-home {
  height: 100%;
}
</style>
