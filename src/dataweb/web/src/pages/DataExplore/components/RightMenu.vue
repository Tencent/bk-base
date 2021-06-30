/** 数据探索 - 右栏 - 模式公共仓库 */


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
  <div class="right-menu">
    <bkdata-tab class="right-tab"
      type="unborder-card"
      :active.sync="tabActived">
      <bkdata-tab-panel name="RT"
        :label="$t('结果数据表')" />
      <bkdata-tab-panel name="sql"
        extCls="fx-tab">
        <template slot="label">
          <i v-bk-tooltips="getSQLToolTips()"
            class="bk-icon icon-fx" />
        </template>
      </bkdata-tab-panel>
    </bkdata-tab>
    <ResultTableList :isQueryPanel="isQueryPanel"
      :tabActived="tabActived"
      :isNotebook="false">
      <div v-show="tabActived === 'sql'"
        class="sql-list"
        style="height: 100%">
        <SQLFunc :infoMap="sqlInfoMap"
          :list="sqlList"
          :loading="sqlLoading" />
      </div>
    </ResultTableList>
  </div>
</template>
<script>
import SQLFunc from './SQL';
import ResultTableList from './ResultTableList';
import mixin from './mixin/sqlPanel.mixin';
import common from './mixin/common';
export default {
  components: {
    SQLFunc,
    ResultTableList,
  },
  mixins: [mixin, common],
  props: {
    isQueryPanel: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      tabActived: 'RT',
    };
  },
};
</script>
<style lang="scss" scoped>
.right-menu {
  width: 100%;
  height: 100%;
  user-select: none;
  .right-tab {
    ::v-deep {
      .bk-tab-header {
        height: 48px;
        background-image: linear-gradient(transparent 47px, #dcdee5 0);
        text-align: center;
      }
      .bk-tab-section {
        height: calc(100% - 48px);
        padding: 0;
        .bk-tab-content {
          height: 100%;
        }
      }
      .bk-tab-label-list {
        display: flex;
        justify-content: space-between;
        width: 100%;
        .bk-tab-label-item {
          line-height: 48px;
        }
        li {
          padding: 0 16px;
          height: 48px;
        }
        .is-last {
          min-width: 40px;
          padding: 0;
          &::before {
            display: inline-block;
            content: '';
            height: 20px;
            width: 1px;
            background: #dcdee5;
            position: absolute;
            left: 0px;
            top: 50%;
            transform: translateY(-50%);
          }
        }
      }

      &.query-right-tab .bk-tab-label-item {
        padding: 0;
        margin: 0 10px;
      }
    }
  }
}
</style>
