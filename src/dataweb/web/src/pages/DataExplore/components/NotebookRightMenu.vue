

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
  <div class="notebook-right-content">
    <div class="notebook-header">
      <bkdata-select v-model="selectedLanguage"
        :clearable="false"
        style="width: 130px"
        :popoverWidth="270">
        <bkdata-option
          v-for="option in languageList"
          :id="option.name"
          :key="option.name"
          :name="option.disName"
          :loading="loading"
          :fontSize="'medium'"
          @selected="paramsChange" />
      </bkdata-select>
    </div>
    <div class="right-menu notebook-right-menu">
      <bkdata-tab :class="['right-tab']"
        type="unborder-card"
        :active.sync="tabActived">
        <bkdata-tab-panel name="RT"
          :label="$t('结果数据表')" />
        <bkdata-tab-panel name="ST"
          :label="$t('原始数据源')" />
        <bkdata-tab-panel name="sql">
          <template slot="label">
            <i v-bk-tooltips="getSQLToolTips()"
              class="bk-icon icon-fx" />
          </template>
        </bkdata-tab-panel>
        <bkdata-tab-panel name="alModel">
          <template slot="label">
            <i v-bk-tooltips="tooltip($t('算法模型'))"
              class="bk-icon icon-algorithm-2" />
          </template>
        </bkdata-tab-panel>
        <bkdata-tab-panel name="buildInLibrary">
          <template slot="label">
            <i v-bk-tooltips="tooltip($t('内置库'))"
              class="bk-icon icon-storehouse-3" />
          </template>
        </bkdata-tab-panel>
      </bkdata-tab>
      <ResultTableList :isQueryPanel="false"
        :tabActived="tabActived"
        :isNotebook="true">
        <div v-if="tabActived === 'ST'"
          class="model-list"
          style="height: 100%">
          <SQLFunc
            :infoMap="modelInfoMap"
            :list="modelList"
            :loading="modelLoading"
            type="alModel"
            :keyList="originData.keyList"
            @changeModelName="changeModelName" />
        </div>
        <div v-show="tabActived === 'buildInLibrary'"
          class="build-in-library"
          style="height: 100%">
          <BuildInLibrary :languageList="languageList"
            :selectedLanguage="selectedLanguage" />
        </div>
        <div v-show="tabActived === 'sql'"
          class="sql-list"
          style="height: 100%">
          <SQLFunc :infoMap="sqlInfoMap"
            :list="sqlList"
            :loading="sqlLoading" />
        </div>
        <div v-show="tabActived === 'alModel'"
          class="model-list"
          style="height: 100%">
          <SQLFunc
            :infoMap="modelInfoMap"
            :list="modelList"
            :loading="modelLoading"
            type="alModel"
            :keyList="originData.keyList"
            @changeModelName="changeModelName" />
        </div>
      </ResultTableList>
    </div>

    <!-- 编辑器状态 -->
    <div
      v-if="languageList.length > 1"
      v-bk-tooltips="{ content: $t('编辑器状态'), position: 'top' }"
      class="status-float">
      <i :class="['icon-circle-shape', 'bk-icon', statusIconClass]" />
      {{ readyStatus }}
    </div>
  </div>
</template>
<script>
import ResultTableList from './ResultTableList';
import BuildInLibrary from './BuildInLibrary';
import SQLFunc from './SQL';
import mixin from './mixin/modelPanel.mixin';
import sqlMixin from './mixin/sqlPanel.mixin';
import common from './mixin/common';
export default {
  components: {
    ResultTableList,
    BuildInLibrary,
    SQLFunc,
  },
  mixins: [mixin, sqlMixin, common],
  props: {
    activeNotebook: Object,
  },
  data() {
    return {
      languageList: [],
      selectedLanguage: 'Python3',
      loading: false,
      readyStatus: 'Ready',
      tabActived: 'RT',
    };
  },
  computed: {
    statusIconClass() {
      switch (this.readyStatus) {
        case 'Loading':
          return 'loading-color';
        case 'Faild':
          return 'faild-color';
        default:
          return 'success-color';
      }
    },
  },
  watch: {
    'activeNotebook.kernel_type'(val) {
      this.selectedLanguage = val;
    },
  },
  mounted() {
    this.selectedLanguage = this.activeNotebook.kernel_type;
    this.loading = true;
    this.bkRequest.httpRequest('dataExplore/getNotebookLanguage').then(data => {
      const originData = data.data;
      Object.keys(originData).forEach(language => {
        const languageType = {
          name: language,
          disName: `${this.$t('内核')}: ${language}`,
          content: originData[language].libs,
          order: originData[language].libs_order,
        };
        this.languageList.push(languageType);
      });
      this.loading = false;
    });
  },
  methods: {
    tooltip(content) {
      return {
        content: content,
        interactive: false,
      };
    },
    paramsChange(language) {
      this.loading = true;
      this.readyStatus = 'Loading';
      this.bkRequest
        .httpRequest('dataExplore/switchKernel', {
          params: {
            kernel_type: language,
            notebook_id: this.activeNotebook.notebook_id,
          },
        })
        .then(data => {
          if (data.result) {
            this.$emit('refreshNotebook');
            this.readyStatus = 'Ready';
          } else {
            this.readyStatus = 'Faild';
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.notebook-right-content {
  height: 100%;
  border: 1px solid #dcdee5;
  border-top: none;
  width: 322px;
  .notebook-header {
    justify-content: center;
    overflow: hidden;
    margin-top: 10px;
    ::v-deep .bk-select {
      border: none;
      box-shadow: none;
      line-height: 25px;
      height: 25px;
      &.is-focus {
        background-color: #f0f1f5;
      }
      .bk-select-name {
        height: 25px;
      }
      .icon-angle-down {
        top: 3px;
      }
    }
    &::after {
      content: '';
      display: block;
      clear: both;
      width: calc(100% + 20px);
      margin-top: 9px;
      margin-left: -10px;
      border-bottom: 1px solid #dcdee5;
    }
  }
  .right-menu {
    ::v-deep .bk-tab-label-list {
      li:nth-of-type(-n + 2) {
        padding: 0;
        min-width: 96px;
      }
      li:nth-of-type(3) {
        margin-left: 5px;
        .bk-tab-label::before {
          content: '';
          display: inline-block;
          width: 1px;
          height: 20px;
          background: #dcdee5;
          position: absolute;
          left: -5px;
          top: 50%;
          transform: translateY(-50%);
        }
      }
      li:nth-of-type(n + 3) {
        min-width: 40px;
        padding: 0;
      }
    }
  }
  ::v-deep .notebook-right-menu .right-menu-wrapper {
    height: calc(100% - 106px);
  }
  .status-float {
    cursor: pointer;
    outline: none;
    height: 30px;
    width: 76px;
    background-color: #fff;
    border-radius: 2px;
    border: 1px solid #c4c6cc;
    position: fixed;
    right: 19%;
    bottom: 50px;
    color: #63656e;
    font-size: 12px;
    display: flex;
    justify-content: center;
    align-items: center;
    .bk-icon {
      margin-right: 5px;
      &.success-color {
        color: #2dcb56;
      }
      &.loading-color {
        color: #ff9c01;
      }
      &.faild-color {
        color: #ea3636;
      }
    }
  }
  ::v-deep .icon-fx:before,
  .icon-algorithm-2:before,
  .icon-storehouse-3:before {
    font-size: 16px;
  }
}
</style>
