

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
  <div class="data-query-container">
    <div v-show="!isEmpty"
      class="query-main">
      <div class="tab-header">
        <span
          v-if="isSqlView || isEmptyRtId"
          :class="{ active: compActiveName === 'sqlQuery' }"
          @click="handleTabChange('sqlQuery')">
          {{ $t('SQL查询') }}
        </span>
        <span
          v-if="isEsView || isEmptyRtId"
          :class="{ active: compActiveName === 'esQuery' }"
          @click="handleTabChange('esQuery')">
          {{ $t('全文检索') }}
        </span>
      </div>
      <div class="tab-body">
        <template v-if="compActiveName === 'sqlQuery'">
          <SqlQuery :activeType="activeType"
            :formData="activeItem"
            :resultTable="resultTable" />
        </template>
        <template v-if="compActiveName === 'esQuery'">
          <EsQuery :resultTable="resultTable" />
        </template>
      </div>
    </div>
    <!-- <div class="Masking"></div> -->
    <template v-if="isEmpty">
      <div class="noMain">
        <img alt
          src="../../common/images/no-data@2x.png">
        <p>{{ $t('请先选择结果数据表') }}</p>
      </div>
    </template>
  </div>
</template>
<script>
// import Layout from '@/components/global/layout';
import { mapState, mapGetters } from 'vuex';
import SqlQuery from './Components/SqlQuery';
import EsQuery from './Components/EsQuery';
export default {
  components: { SqlQuery, EsQuery },
  data() {
    return {
      currentActive: '',
    };
  },
  computed: {
    ...mapState({
      isEsView: state => state.dataQuery.hsitory.isEsView,
      isSqlView: state => state.dataQuery.hsitory.isSqlView,
      activeItem: state => state.dataQuery.hsitory.activeItem,
      activeType: state => state.dataQuery.hsitory.clusterType,
      resultTable: state => state.dataQuery.resultTable,
      activeTabName: state => state.dataQuery.hsitory.activeTabName,
    }),
    isEmpty() {
      return false; // !this.activeItem || (!this.isEsView && !this.isSqlView)
    },

    isEmptyRtId() {
      return !this.activeItem || (!this.isEsView && !this.isSqlView);
    },

    compActiveName() {
      return this.activeTabName || 'sqlQuery';
    },
  },
  methods: {
    handleTabChange(val) {
      this.$store.commit('dataQuery/setActiveTabName', val);
      this.$store.commit('dataQuery/setSearchLoading', false);
    },
  },
};
</script>

<style lang="scss">
.data-query-container {
  .query-main {
    .tab-header {
      height: 48px;
      background: #fafbfd;
      border: solid 1px #ddd;
      border-left: none;
      display: flex;
      justify-content: flex-start;
      align-items: center;

      span {
        height: 48px;
        line-height: 48px;
        padding: 0 20px;
        cursor: pointer;

        &.active {
          color: #3a84ff;
          background: #fff;
          border-right: solid 1px #ddd;
          border-left: solid 1px #ddd;
        }

        &:first-child {
          &.active {
            border-left: none;
          }
        }
      }
    }
  }
  //   .Masking {
  //     width: calc(100% - 318px);
  //     height: 20px;
  //     position: fixed;
  //     top: 108px;
  //     // left: 320px;
  //     background: #fff;
  //     z-index: 4;
  //     min-width: 882px;
  //   }
  .noMain {
    text-align: center;
    position: absolute;
    left: 50%;
    top: 50%;
    transform: translate(-50%, -50%);
    p {
      margin-top: 22px;
      font-size: 18px;
      color: #c3cdd7;
      font-weight: bold;
    }
  }
}
</style>
