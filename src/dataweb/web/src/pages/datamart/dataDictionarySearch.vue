

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
  <div class="search-result-container">
    <Layout :crumbName="[{ name: $t('数据字典'), to: '/data-mart/data-dictionary' }]">
      <template slot="defineTitle">
        <a href="javascript:void(0);"
          class="crumb-item with-router fl"
          @click.stop="goBack()">
          {{ $t('数据字典') }}
        </a>
        <span class="crumb-desc">{{ $t('对数据进行检索_详情查看及血缘关系追溯') }}</span>
      </template>
      <div class="page-main">
        <div class="click-style">
          <Tips v-if="conditionText.length && $modules.isActive('lifecycle')"
            :text="conditionText"
            :size="'small'"
            class="mb10"
            @tipsClick="tipsClick" />
        </div>
        <div class="search-bar">
          <ConditionSelect v-model="selectedParamsValue"
            class="fr"
            :isDisabledProj="isDisabledProj"
            :projPlacehoder="projPlacehoder"
            :isTdwDisabled="isTdwDisabled" />
          <TagComBinedSearch ref="tag"
            v-model="tagParamValue"
            :placeholder="$t('请输入数据集名_表名_描述')"
            :isShowSearch="false"
            :isShowAdvanceSearch="true"
            :width="'100%'"
            @linkToSeartResult="updateParamsTip"
            @historySearch="historySearch"
            @change="fixDataSys"
            v-on="$listeners" />
        </div>
      </div>
      <DictData
        ref="dictData"
        :isShowStandard="isShowStandard"
        :nodeTreeData="nodeTreeData"
        :platform="selectedParams.channelKeyWord"
        :bizId="selectedParams.biz_id"
        :projectId="selectedParams.project_id"
        :keyword="tagParam.searchTest"
        :labelList="tagList"
        :advanceStandardParams="advanceStandardParams"
        :historySearchParams="historySearchParams"
        @projDisabled="projDisabled"
        @reOpenRequest="isRequestDictData = true"
        @cancelHistorySearch="historySearchParams = {}"
        @clearNodeTree="nodeTreeData = []" />
    </Layout>
  </div>
</template>

<script>
import { postMethodWarning } from '@/common/js/util';
import Layout from '../../components/global/layout';
import ConditionSelect from './DataDictionaryComp/ConditionSelect';
import TagComBinedSearch from './DataDictionaryComp/TagComBinedSearch';
import DictData from './DataDict/DictData';
import { mapGetters } from 'vuex';
import Tips from '@/components/TipsInfo/TipsInfo.vue';
import { isNumber } from './common/utils';
import { dataValueOperateMap } from '@/pages/datamart/common/config';
import { checkParams } from '@/pages/datamart/common/utils.js';
import { deepClone } from '@/common/js/Utils.ts';

export default {
  name: 'DataSearchResult',
  components: {
    Layout,
    ConditionSelect,
    TagComBinedSearch,
    DictData,
    Tips,
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
      bizId: '',
      projectId: '',
      channelKeyWord: '',
      search_text: '',
      isDisabledProj: false,
      isShowStandard: false,
      nodeTreeData: [],
      projPlacehoder: this.$t('按项目筛选'),
      tagList: [],
      stInsSearchText: null,
      standardParamsObj: {
        storage_type: this.$t('存储类型'),
        tag_code: this.$t('数据标准'),
        created_by: this.$t('创建者'),
        created_at_start: this.$t('创建时间'),
        heat_score: this.$t('热度评分'),
        range_score: this.$t('广度评分'),
        standard_content_name: this.$t('标准内容'),
        asset_value_score: this.$t('价值评分'),
        storage_capacity: this.$t('存储成本'),
        importance_score: this.$t('重要度评分'),
        assetvalue_to_cost: this.$t('收益比'),
      },
      conditionText: '',
      historySearchParams: {},
      isRequestDictData: true,
      isTdwDisabled: false,
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
    advanceStandardParams() {
      return deepClone(this.tagParam.advanceSearch);
    }
  },
  watch: {
    flatTags(val) {
      if (this.$route.params.dataMapParams) {
        this.getTagDetail();
      }
    },
    '$route.name': {
      immediate: true,
      handler() {
        this.isShowStandard = false;
        this.$nextTick(() => {
          this.standardJumphandle();
          if (this.$route.params.dataMapParams) {
            // 数据地图跳转到数据字典的数据回填
            // 表格上部标签
            if (this.flatTags.length) {
              // 标签全量数组存在，就遍历获取标签详情，否则在flatTags的监听事件里获取
              this.getTagDetail();
            }
            this.nodeTreeData = this.$route.params.parentNodes || [];
            this.isShowStandard = this.$route.params.dataMapParams.isShowStandard;
            this.$refs.dictData.dataType = 'all';
            this.$refs.dictData.searchType = [];
            // eslint-disable-next-line vue/no-mutating-props
            this.selectedParams = {
              project_id: this.$route.params.dataMapParams.project_id,
              biz_id: this.$route.params.dataMapParams.biz_id,
              channelKeyWord: 'bk_data',
            };
            this.$refs.dictData.dataType = this.$route.params.nodeType;
            if (this.$refs.dictData.dataType === 'standard_table') {
              this.$refs.dictData.dataType = 'result_table';
              this.$refs.dictData.getStandardParams();
            }
            if (this.$route.params.nodeId) {
              this.$refs.dictData.tag_code = this.$route.params.nodeId;
            }
            this.getDictData();
          }
          const historySearchParams = this.$route.params.historySearchParams;
          if (historySearchParams) {
            this.handleHistoryData(historySearchParams.backFillParams, historySearchParams.params);
          }
        });
      },
    },
    selectedParams: {
      deep: true,
      handler() {
        if (!this.isRequestDictData) return;
        this.getDictData();
      },
    },
    'tagParam.searchTest'(val) {
      if (!val.length) {
        clearTimeout(this.stInsSearchText);
        if (!this.isRequestDictData) return;
        this.getDictData();
      }
      val = val.trim();
      this.dealSearchText(this.tagParam.searchTest);
    },
    'tagParam.selectedTags': {
      immediate: true,
      handler(val) {
        if (!this.isRequestDictData) return;
        this.dealTags(val);
      },
    }
  },
  activated() {
    // 字典预览页跳转搜索结果页需要携带高级搜索参数
    if (this.$route.params.advanceSearchParams) {
      const { selectedParams, tagParam } = this.$route.params.advanceSearchParams;
      this.$emit('update:selectedParams', selectedParams);
      this.$emit('update:tagParam', tagParam);
      this.$nextTick(() => {
        this.$refs.dictData.getDictData();
        if (Object.keys(this.tagParamValue.advanceSearch).length) {
          this.getConditionText(this.tagParamValue.advanceSearch);
        }
      });
    }
  },
  methods: {
    fixDataSys() {
      this.$refs.dictData.fixDataSys();
    },
    handleHistoryData(backFillParams, params) {
      this.isRequestDictData = false;
      this.historySearchParams = params;
      this.getConditionText(this.tagParamValue.advanceSearch);
      this.dealTags(this.tagParamValue.selectedTags);
    },
    historySearch(backFillParams, params) {
      // 历史搜索跳转到搜索页
      this.handleHistoryData(backFillParams, params);
    },
    tipsClick() {
      this.$refs.tag.showSearchMenu('search');
    },
    updateParamsTip() {
      // 高级搜索跳转到搜索页
      this.getConditionText(this.tagParamValue.advanceSearch);
      this.dealSearchText(this.tagParamValue.searchTest.trim());
    },
    getConditionText(data) {
      // eslint-disable-next-line max-len
      let fields = ['storage_type', 'tag_code', 'created_at_start', 'created_by', 'standard_content_name', 'heat_score', 'range_score', 'asset_value_score', 'storage_capacity', 'importance_score', 'assetvalue_to_cost'];
      if (!isNumber(data['tag_code'])) {
        fields = fields.filter(item => item !== 'tag_code');
      }
      const rateCondition = {
        eq: this.$t('等于_'),
        ge: this.$t('大于等于'),
        le: this.$t('小于等于'),
        gt: this.$t('大于'),
        lt: this.$t('小于'),
      };
      const fieldsValue = {
        storage_type: data['storage_type'],
        created_at_start: `${data['created_at_start']} - ${data['created_at_end']}`,
        tag_code: data['standard_name'],
        created_by: data['created_by'],
        standard_content_name: data['standard_content_name'],
      };
      this.conditionText = '';
      for (const key in data) {
        if (fields.includes(key) && (data[key] || data[key].length)) {
          if (Object.values(dataValueOperateMap).includes(key)) {
            const operate = Object.keys(dataValueOperateMap)
              .find(item => dataValueOperateMap[item] === key);
            if (rateCondition[data[operate]]) {
              this.conditionText += `${this.standardParamsObj[key]}：
                            ${rateCondition[data[operate]]} ${data[key]}；`;
              if (key === 'storage_capacity') {
                // 存储成本需要在最后加上单位MB
                this.conditionText = `${this.conditionText.slice(0, this.conditionText.length - 1)}MB；`;
              }
            }
          } else {
            this.conditionText += `${this.standardParamsObj[key]}：${fieldsValue[key]}；`;
          }
        }
      }
    },
    standardJumphandle() {
      // 从数据标准带条件跳转
      if (!this.$route.params.standard_version_id) return;
      // eslint-disable-next-line vue/no-mutating-props
      this.tagParamValue.advanceSearch['tag_code'] = this.$route.params.standard_version_id;
      // eslint-disable-next-line vue/no-mutating-props
      this.tagParamValue.advanceSearch['me_type'] = 'standard';
      // eslint-disable-next-line vue/no-mutating-props
      this.tagParamValue.advanceSearch['cal_type'] = ['standard', 'only_standard'];
      // 标准内容、标准名称的展示
      const standardInfo = ['standard_name', 'standard_content_name', 'standard_content_id'];
      for (const key in this.$route.params) {
        if (standardInfo.includes(key) && this.$route.params[key]) {
          // eslint-disable-next-line vue/no-mutating-props
          this.tagParamValue.advanceSearch[key] = this.$route.params[key];
        }
      }
      this.getConditionText(this.tagParamValue.advanceSearch);

      this.$nextTick(() => {
        this.$refs.dictData.searchType = this.tagParamValue.advanceSearch['cal_type'];
        this.$refs.dictData.getDictData(true);
      });
    },
    getTagDetail() {
      // eslint-disable-next-line vue/no-mutating-props
      this.tagParamValue.selectedTags = [];
      this.$route.params.dataMapParams.chosen_tag_list.forEach(alias => {
        this.flatTags.forEach(item => {
          const index = item.findIndex(child => child.tag_code.toUpperCase() === alias.toUpperCase());
          if (index > -1) {
            // eslint-disable-next-line vue/no-mutating-props
            this.tagParamValue.selectedTags.push(item[index]);
          }
        });
      });
    },
    dealTags(val) {
      this.tagList = [];
      val.forEach(item => {
        this.tagList.push(item.tag_code);
      });
      this.getDictData();
    },
    dealSearchText(val) {
      this.stInsSearchText && clearTimeout(this.stInsSearchText);
      this.stInsSearchText = setTimeout(() => {
        if (checkParams(val)) {
          if (!this.isRequestDictData) return;
          this.getDictData();
        } else {
          postMethodWarning(this.$t('搜索关键字至少要3个字符'), 'error');
        }
      }, 1000);
    },
    getDictData() {
      this.$nextTick(() => {
        this.$refs.dictData.getDictData(true);
      });
    },
    goBack() {
      this.$router.push({
        name: 'DataDictionary',
        params: {},
      });
      this.conditionText = '';
      this.$emit('update:selectedParams', {
        project_id: '',
        biz_id: '',
        channelKeyWord: this.selectedParams.channelKeyWord || '',
      });
      this.$emit('update:tagParam', {
        selectedTags: [],
        searchTest: '',
        advanceSearch: {
          tag_code: '',
          created_by: '',
          created_at_start: '',
          created_at_end: '',
          standard_name: '',
          storage_type: '',
          heat_operate: '',
          heat_score: '',
          range_operate: '',
          range_score: '',
          asset_value_operate: '',
          asset_value_score: '',
          importance_operate: '',
          importance_score: '',
          assetvalue_to_cost_operate: '',
          assetvalue_to_cost: '',
          storage_capacity_operate: '',
          storage_capacity: '',
          standard_content_id: '',
          standard_content_name: '',
        },
      });
    },
    projDisabled(val) {
      // 数据源无法选中tdw
      this.isTdwDisabled = val;
      if (val) {
        this.projectId = '';
        this.projPlacehoder = this.$t('数据源无法按项目筛选');
      } else {
        this.projPlacehoder = this.$t('按项目筛选');
      }
      this.isDisabledProj = val;
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .layout-content {
  padding-top: 10px !important;
}
.search-result-container {
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
  margin-bottom: 20px;
  .click-style {
    cursor: pointer;
  }
}
.search-bar {
  margin-bottom: 20px;
  .tag-wrap {
    margin: 0;
  }
}
</style>
