

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
  <div class="tag-container">
    <div class="tag-wrap"
      :style="{ width: `${width}` }">
      <!-- 上方输入框样式，展示所选tag -->
      <div class="data-tag">
        <div
          class="data-tag-inputs"
          :class="{
            'focus-style': isEyeCatching,
            'eye-catching': size === 'big',
            'show-border-radius': isShowBorderRadius,
          }">
          <i class="icon-search" />
          <div class="data-tag-collection">
            <span v-for="(tag, index) in value.selectedTags"
              :key="index"
              class="tag">
              {{ tag[tagName] }}
              <span class="icon-close"
                @click="removeTag(tag)" />
            </span>
            <input
              v-model="value.searchTest"
              type="text"
              maxlength="128"
              :class="['tag-input', { 'tag-big-size': !value.selectedTags.length }]"
              :placeholder="placeholder"
              @focus="isEyeCatching = true"
              @blur="isEyeCatching = false"
              @keyup.enter="goToSeartResult()">
          </div>
          <div v-if="value.searchTest.length || value.selectedTags.length"
            class="data-tag-icon-group">
            <span class="icon-close-circle-shape"
              :title="$t('清空')"
              @click="clearAll" />
          </div>
        </div>
        <bkdata-button v-if="isShowSearch"
          extCls="jump-search"
          :theme="'primary'"
          :title="$t('搜索')"
          @click="goToSeartResult()">
          {{ $t('搜索') }}
        </bkdata-button>
        <div v-if="isShowAdvcance"
          class="search-config">
          <a href="javascript:void(0);"
            @click="showSearchMenu('search')">
            {{ $t('高级搜索') }}
          </a>
          <a v-bk-tooltips="tipContent()"
            href="javascript:void(0);"
            @click="showSearchMenu('history')">
            {{ $t('历史搜索') }}
          </a>
        </div>
      </div>
      <ul class="hot-label">
        <li v-for="(label, index) in getOverallTags"
          :key="index"
          class="label-more"
          :title="label.tag_alias"
          :class="{ 'label-clicked': hasIncludesSelected(label) }"
          @click="updateLabel(label)">
          {{ label.tag_alias }}
        </li>
        <span class="label-more font-weight700"
          @click="focusHandle">
          {{ $t('更多') }}<i class="icon-angle-double-right" />
        </span>
      </ul>
      <DataTag ref="dataTag"
        v-model="selectedTagsValue"
        :data="getDataTagList"
        :focusMode="false"
        :cancelHandle="tagCancelHandle"
        :confirmHandle="tagConfirmHandle"
        :placeholder="$t('非必填_准确的数据描述能帮助系统提取标签_提升数据分析的准确性')" />
    </div>
    <transition v-if="isShowAdvcance"
      name="fade">
      <AdvanceSearch ref="adSearch"
        v-model="advanceSearchValue"
        :tab="activeTab"
        :isShowAdvancedSearch="isShowAdvancedSearch"
        @confirm="confirmSearch"
        @close="showSearchMenu"
        @tabChange="tabChange"
        @historySearch="historySearch"
        @getHistoryCount="getHistoryCount"
        v-on="$listeners" />
    </transition>
  </div>
</template>

<script>
import mixin from '@/components/dataTag/tagMixin.js';
import { mapGetters } from 'vuex';
import DataTag from '@/components/dataTag/DataTag.vue';
import AdvanceSearch from './AdvanceSearch';
import { checkParams } from '@/pages/datamart/common/utils.js';

export default {
  components: {
    DataTag,
    AdvanceSearch,
  },
  mixins: [mixin],
  props: {
    value: {
      type: Object,
      default: () => {
        return {
          selectedTags: [],
          searchTest: '',
          advanceSearch: {},
        };
      },
    },
    overiewTagList: {
      type: Array,
      default: () => [],
    },
    isShowSearch: {
      type: Boolean,
      default: true,
    },
    width: {
      type: String,
      default: '800px',
    },
    size: {
      type: String,
      default: '',
    },
    isShowBorderRadius: {
      type: Boolean,
      default: false,
    },
    tagTypes: {
      type: Array,
      default: () => [],
    },
    isShowAdvanceSearch: {
      type: Boolean,
      default: false,
    },
    placeholder: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      text: '',
      tags: [],
      backupTagData: [], // 标签列表缓存,
      tagConfig: {
        subList: 'sub_list',
        tagName: 'tag_alias',
        subTopList: 'sub_top_list',
        tagId: 'tag_i',
      },
      tagShow: false,
      udfVisiable: false,
      visiable: false,
      sheetVal: '',
      outputZh: '',
      outputEn: '',
      output: {},
      showMonaco: false,
      scope: {},
      tagList: [
        {
          label: this.$t('业务标签_表达业务含义'),
          multiple: true,
          data: [],
        },
        {
          label: this.$t('公共类非业务标签'),
          multiple: false,
          data: [],
        },
      ],
      tagKeysMap: {
        subList: 'sub_list',
        tagName: this.$i18n.locale === 'en' ? 'tag_code' : 'tag_alias',
        subTopList: 'sub_top_list',
        tagId: 'tag_code',
      },
      backupTags: [],
      searchText: '',
      isEyeCatching: false,
      isShowAdvancedSearch: false,
      activeTab: '',
      historyCount: 0,
    };
  },
  computed: {
    selectedTagsValue: {
      get() {
        return this.value.selectedTags;
      },
      set(val) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.selectedTags = val;
      },
    },
    advanceSearchValue: {
      get() {
        return this.value.advanceSearch;
      },
      set(val) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.advanceSearch = val;
      },
    },
    dynamicPlaceholder() {
      if (this.value.selectedTags.length) {
        return this.$t('点击编辑标签');
      }
      return this.placeholder;
    },
    ...mapGetters({
      getDataTagList: 'dataTag/getTagList',
      getMultipleTagsGroup: 'dataTag/getMultipleTagsGroup',
      getSingleTagsGroup: 'dataTag/getSingleTagsGroup',
      flatTags: 'dataTag/getAllFlatTags',
      getOverallTags: 'dataTag/getOverallTags',
    }),
    tagName() {
      return this.tagKeysMap.tagName;
    },
    isShowAdvcance() {
      return this.isShowAdvanceSearch && this.$modules.isActive('lifecycle');
    },
  },
  watch: {
    'value.selectedTags'(val) {
      if (val.length) {
        this.isEyeCatching = true;
        const arr = [];
        val.forEach(item => {
          arr.push(item.tag_code);
        });
      }
    },
  },
  mounted() {
    this.getDataTagListFromServer(true, true);
    this.$refs.adSearch && this.$refs.adSearch.getSearchHistory();
  },
  methods: {
    backData() {
      this.$refs.adSearch.backFillData(this.value.advanceSearch);
    },
    getEnName(row) {
      if (this.$i18n.locale === 'en') {
        return row.tag_code;
      }
      return row.tag_alias;
    },
    tipContent() {
      return {
        placement: 'top',
        content: this.historyCount ? `最近${this.historyCount}条历史记录` : this.$t('暂无历史搜索记录'),
        boundary: 'window',
      };
    },
    getHistoryCount(count) {
      this.historyCount = count;
    },
    fillBackData(backFillParams) {
      // eslint-disable-next-line vue/no-mutating-props
      this.value.selectedTags = [];
      if (backFillParams.tagParam.selectedTags.length) {
        this.flatTags.forEach(item => {
          item.forEach(child => {
            const index = this.value.selectedTags.findIndex(item => item.tag_code === child.tag_code);
            if (index < 0 && backFillParams.tagParam.selectedTags.includes(child.tag_code)) {
              // eslint-disable-next-line vue/no-mutating-props
              this.value.selectedTags.push(child);
            }
          });
        });
      }
      // eslint-disable-next-line vue/no-mutating-props
      this.value.searchTest = backFillParams.tagParam.searchTest;
    },
    historySearch(backFillParams, params) {
      // 标签和搜索关键词回填
      this.fillBackData(backFillParams);
      backFillParams.tagParam.selectedTags = this.value.selectedTags;
      this.$nextTick(() => {
        this.$emit('historySearch', backFillParams, params);
      });
    },
    tabChange(tab) {
      this.activeTab = tab;
    },
    showSearchMenu(tab = 'search') {
      if (this.isShowAdvancedSearch && this.activeTab && this.activeTab !== tab) {
        this.activeTab = tab;
        return;
      }
      this.activeTab = tab;
      this.isShowAdvancedSearch = !this.isShowAdvancedSearch;
    },
    goToSeartResult() {
      if (!checkParams(this.value.searchTest)) return;
      // 在高级搜索组件父级搜索时，需要触发确定事件
      if (Object.keys(this.value.advanceSearch).some(item => this.value.advanceSearch[item])) {
        this.$refs.adSearch.confirm();
        return;
      }
      // if (!this.isShowSearch) return
      this.$emit('linkToSeartResult');
    },
    confirmSearch() {
      this.$emit('linkToSeartResult');
    },
    initFlagStatus() {
      this.$refs.dataTag.resetStatus();
      this.value.selectedTags.forEach(item => {
        item.active = true;
      });
    },
    changeLabelStatus() {
      let index = 0;
      for (const item of this.value.selectedTags) {
        for (let i = 0; i < this.flatTags.length; i++) {
          index = this.flatTags[i].findIndex(child => child.tag_id === item.tag_id);
          if (index > -1) {
            this.$refs.dataTag.changeTagStatus(item, i);
            break;
          }
        }
      }
    },
    hasIncludesSelected(item) {
      return this.value.selectedTags.findIndex(child => child.tag_id === item.tag_id) > -1;
    },
    updateLabel(tag) {
      if (this.value.selectedTags.findIndex(item => item.tag_id === tag.tag_id) > -1) {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.selectedTags = this.value.selectedTags.filter(item => item.tag_id !== tag.tag_id);
      } else {
        // eslint-disable-next-line vue/no-mutating-props
        this.value.selectedTags.push(tag);
      }
      this.initFlagStatus();
      this.changeLabelStatus();
    },
    removeTag(tag) {
      this.initFlagStatus();
      this.$refs.dataTag.removeTag(tag);
      this.changeLabelStatus();
    },
    clearAll() {
      this.$refs.dataTag.clearAll();
      // eslint-disable-next-line vue/no-mutating-props
      this.value.searchTest = '';
      // eslint-disable-next-line vue/no-mutating-props
      this.value.selectedTags = [];
      this.$emit('input', this.value);
      this.isEyeCatching = false;
    },
    focusHandle() {
      const list = this.getDataTagList;
      this.$refs.dataTag.switchFullScreen(true);
      this.backupTagData = JSON.parse(JSON.stringify(list));
      this.backupTags = JSON.parse(JSON.stringify(this.value.selectedTags));
    },
    tagCancelHandle() {
      this.$refs.dataTag.switchFullScreen(false);
      this.$refs.dataTag.text = ''; // 清空input输入内容
      // eslint-disable-next-line vue/no-mutating-props
      this.value.selectedTags = this.backupTags;
      this.backupTagData[0].data.length && this.$store.commit('dataTag/setDataTagList', this.backupTagData);
    },
    tagConfirmHandle() {
      this.$refs.dataTag.switchFullScreen(false);
      this.$refs.dataTag.text = ''; // 清空input输入内容
      this.backupTagData = [];
      this.backupTags = [];
    },
  },
};
</script>

<style lang="scss" scoped>
.fade-enter-active,
.fade-leave-active {
  z-index: 900;
  transition: height 5s;
}
.fade-enter, .fade-leave-to /* .fade-leave-active below version 2.1.8 */ {
  height: 0;
  z-index: -1;
  padding: 0 !important;
}
.tag-container {
  flex: 1;
  .tag-wrap {
    margin: 0 auto;
    margin-bottom: 20px;
    .data-tag {
      max-width: 1000px;
      display: flex;
      align-items: center;
      padding-top: 1px;
      ::v-deep .jump-search {
        height: 50px;
        width: 120px;
        font-size: 18px;
      }
      .search-config {
        display: flex;
        margin-left: 5px;
        a {
          margin: 0 5px;
          white-space: nowrap;
          font-size: 12px;
        }
        ::v-deep button {
          // border: 1px solid white;
          // color: #3a84ff;
          &:first-child {
            margin: 0 5px;
          }
        }
      }
      .data-tag-inputs {
        width: 100%;
        border: 1px solid #c4c6cc;
        min-height: 30px;
        padding: 0 20px 0 34px;
        position: relative;
        display: flex;
        justify-content: space-between;
        align-items: center;
        &.no-margin {
          margin: 0;
        }
        &.display-input {
          height: 32px;
          min-height: unset;
          padding-left: 2px !important;
          padding-top: 4px !important;
          .data-tag-icon-group {
            top: 50%;
            transform: translateY(-50%);
            .icon-close-circle-shape {
              display: block;
            }
          }
        }
        .icon-search {
          position: absolute;
          left: 10px;
        }
        .data-tag-collection {
          width: 100%;
          .tag-input {
            margin: 0 -1px 0 5px;
            border: none;
            padding: 0;
            width: 156px;
            outline: none;
            line-height: 28px;
            font-size: 12px;
            height: 28px;
            &.tag-big-size {
              width: 100%;
            }
            &::placeholder {
              color: #c4c6cc;
            }
          }
          .tag {
            height: 100%;
            padding: 2px 5px;
            border-radius: 2px;
            background: #e1ecff;
            display: inline-block;
            font-size: 12px;
            text-align: center;
            color: #3a84ff;
            margin-right: 4px;
            margin-bottom: 4px;
            margin-top: 4px;
          }
          .single-tag {
            background: #fdf6ec;
            color: #faad14;
          }
          .icon-close {
            color: #63656e;
            display: inline-block;
            cursor: pointer;
          }
        }
        .data-tag-icon-group {
          cursor: pointer;
          position: absolute;
          right: 6px;
          top: 50%;
          transform: translateY(-50%);
          .icon-close-circle-shape {
            display: block;
            color: #c4c6cc;
          }
        }
      }
      .focus-style {
        border-color: #4791ff;
      }
      .eye-catching {
        min-height: 50px;
        padding: 0 10px 0 34px;
      }
      .show-border-radius {
        border-radius: 50px;
      }
    }
    .hot-label {
      margin: 0 auto;
      margin-top: 10px;
      display: flex;
      flex-wrap: wrap;
      .label-more {
        display: inline-block;
        text-overflow: ellipsis;
        overflow: hidden;
        white-space: nowrap;
        font-size: 12px;
        line-height: 14px;
        padding: 3px 8px;
        border-radius: 2px;
        cursor: pointer;
        margin: 5px 10px 5px 0;
        background: #efefef;
      }
      .label-clicked {
        background: #e1ecff;
        color: #3a84ff;
      }
      .font-weight700 {
        // font-weight: 700;
        background-color: #fafafa;
        color: #a09d9d;
        margin-right: 0;
      }
    }
  }
}
</style>
