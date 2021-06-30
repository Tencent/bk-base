

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
          class="crumb-item with-router fl"
          @click.stop="reflshPage('DataDictionary')">
          {{ $t('数据盘点') }}
        </a>
        <span class="crumb-desc">{{ $t('盘点数据资产_评估数据价值') }}</span>
      </template>
      <div class="page-main"
        :class="[activeTab === 'dataValue' ? 'min-width' : '']">
        <div class="search-bar">
          <ConditionSelect v-model="selectedParamsValue"
            class="fr"
            @choseChannel="choseChannel"
            @choseBiz="choseBiz"
            @choseProject="choseProject" />
          <TagComBinedSearch ref="tag"
            v-model="tagParamValue"
            :isShowAdvanceSearch="true"
            :isShowSearch="false"
            :width="'100%'"
            @choseTag="choseTag"
            @linkToSeartResult="updateParamsTip" />
        </div>
        <InventoryDataChart ref="chartContainer"
          @tabChange="tabChange" />
      </div>
    </Layout>
  </div>
</template>

<script>
import { confirmMsg, postMethodWarning } from '@/common/js/util';
import Layout from '../../components/global/layout';
import ConditionSelect from './DataDictionaryComp/ConditionSelect';
import TagComBinedSearch from './DataDictionaryComp/TagComBinedSearch';
import InventoryDataChart from './DataDictionaryComp/InventoryDataChart';
import Bus from '@/common/js/bus.js';
import { mapGetters } from 'vuex';
import { checkParams } from '@/pages/datamart/common/utils.js';

export default {
  name: 'DataDictionaryInventory',
  components: {
    Layout,
    ConditionSelect,
    TagComBinedSearch,
    InventoryDataChart,
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
  props: {
    selectedParams: {
      type: Object,
      default: () => ({
        project_id: '',
        biz_id: '',
        channelKeyWord: 'bk_data',
      }),
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
      bizId: '',
      projectId: '',
      channelKeyWord: '',
      searchText: '',
      projPlacehoder: this.$t('按项目筛选'),
      tagList: [],
      stInsSearchText: null,
      activeTab: '',
    };
  },
  computed: {
    ...mapGetters({
      flatTags: 'dataTag/getAllFlatTags',
    }),
    tagParamValue: {
      get() {
        return this.tagParam;
      },
      set(val) {
        // eslint-disable-next-line vue/no-mutating-props
        this.tagParam = val;
      },
    },
    selectedParamsValue: {
      get() {
        return this.selectedParams;
      },
      set(value) {
        // eslint-disable-next-line vue/no-mutating-props
        this.selectedParams = value;
      },
    },
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
  mounted() {
    Bus.$on('changeTags', label => {
      // eslint-disable-next-line vue/no-mutating-props
      this.tagParam.selectedTags = [];
      // 图表组件修改标签
      for (let i = 0; i < this.flatTags.length; i++) {
        const index = this.flatTags[i].findIndex(child => child.tag_alias === label);
        if (index > -1) {
          if (!this.tagParam.selectedTags.find(d => d.tag_alias === label)) {
            // eslint-disable-next-line vue/no-mutating-props
            this.tagParam.selectedTags.push(this.flatTags[i][index]);
          }
          break;
        }
      }
    });
  },
  methods: {
    updateParamsTip() {
      this.updateData();
    },
    getParams() {
      const advanceSearchParams = this.tagParam.advanceSearch;
      if (Object.keys(advanceSearchParams).length) {
        // 数据标准高级搜索
        for (const key in advanceSearchParams) {
          if (!advanceSearchParams[key] && advanceSearchParams[key] !== 0) {
            delete advanceSearchParams[key];
          }
        }
      }
      return Object.assign(
        {},
        {
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
        },
        advanceSearchParams
      );
    },
    tabChange(tab) {
      this.activeTab = tab;
    },
    dealSearchText(val) {
      this.stInsSearchText && clearTimeout(this.stInsSearchText);
      this.stInsSearchText = setTimeout(() => {
        if (checkParams(val)) {
          this.updateData();
        } else {
          postMethodWarning(this.$t('搜索关键字至少要3个字符'), 'error');
        }
      }, 1000);
    },
    updateData() {
      this.$refs.chartContainer.updateData();
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
    choseChannel(val) {
      this.channelKeyWord = val;
    },
    choseBiz(val) {
      this.bizId = val;
    },
    choseProject(val) {
      this.projectId = val;
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
