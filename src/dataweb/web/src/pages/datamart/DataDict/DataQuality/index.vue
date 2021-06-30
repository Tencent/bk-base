

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
  <div>
    <bkdata-tab :active.sync="active"
      type="unborder-card">
      <template slot="setting">
        <i class="icon-light-line bk-text-warning"
          :title="$t('指引')"
          @click="isShow = !isShow" />
      </template>
      <bkdata-tab-panel v-for="(panel, index) in panels"
        v-bind="panel"
        :key="index" />
    </bkdata-tab>
    <DataQualityOverview v-show="['buriedPoint', 'dataAnalyze'].includes(active)"
      :tipsInfo="tipsInfo"
      @refresh="refresh"
      @cancelRefresh="cancelRefresh" />
    <buriedPoint v-show="active === 'buriedPoint'"
      ref="dataQuality"
      @updateDataInfo="updateDataInfo" />
    <dataAnalyze v-show="active === 'dataAnalyze'"
      ref="dataAnalyze" />
    <template v-if="hasAuth">
      <DataFix v-show="active === 'dataFix'"
        :processingType="processingType" />
      <QualityAduit v-show="active === 'qualityAduit'" />
    </template>
    <bkdata-dialog v-model="isShow"
      :extCls="``"
      :hasHeader="true"
      :closeIcon="true"
      :maskClose="false"
      :width="'1250'"
      :showFooter="false">
      <div v-show="isShow"
        class="guide-container">
        <div class="line" />
        <div class="guide-item">
          <div class="index-container">
            <span class="icon icon-quality-index" />
            <div class="index-name">
              {{ $t('质量指标') }}
            </div>
          </div>
          <div class="index-explain">
            <p>{{ $t('构建各项数据质量指标') }}</p>
            <p>{{ $t('系统内置了多项质量指标') }}</p>
            <p>{{ $t('其中_主要分为两类') }}：</p>
            <ul>
              <li>
                <a href="javascript:void(0);"
                  class="bk-text-primary"
                  @click="changeTab('buriedPoint')">
                  {{ $t('数据埋点') }}（{{ $t('数据流质量') }}）
                </a>
              </li>
              <li>
                <a href="javascript:void(0);"
                  class="bk-text-primary"
                  @click="changeTab('dataAnalyze')">
                  {{ $t('数据剖析') }}（{{ $t('数据内容质量') }}）
                </a>
              </li>
            </ul>
          </div>
        </div>
        <div class="guide-item">
          <div class="index-container">
            <span class="icon icon-audit-pass" />
            <div class="index-name">
              {{ $t('质量审核') }}
            </div>
          </div>
          <div class="index-explain">
            <p>{{ $t('利用数据质量检测规则') }}</p>
            <p>{{ $t('系统内置了多项规则模板') }}</p>
            <div>
              {{ $t('具体规则配置_参见') }}
              <!-- <a href="javascript:void(0);" class="bk-text-primary"> -->
              {{ $t('帮助文档') }}
              <!-- </a> -->
            </div>
          </div>
        </div>
        <div class="guide-item">
          <div class="index-container">
            <span class="icon icon-quality-fix" />
            <div class="index-name">
              {{ $t('质量修正') }}
            </div>
          </div>
          <div class="index-explain">
            <p>{{ $t('针对质量较差的数据进行修正') }}</p>
            <p>{{ $t('系统内置了多项修正规则') }}</p>
            <p>{{ $t('用户也可以进行自定义配置') }}</p>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import DataQualityOverview from './DataQualityOverview';
import BuriedPoint from './DataFlowQuality/index';
import DataAnalyze from './DataAnalyze/index';
import QualityAduit from './QualityAduit/QualityAduit.index';
import DataFix from './DataFix/DataFix.index';

export default {
  components: {
    DataQualityOverview,
    BuriedPoint,
    DataAnalyze,
    QualityAduit,
    DataFix,
  },
  props: {
    processingType: {
      type: String,
      default: '',
    },
  },
  data() {
    return {
      tipsInfo: {},
      tab: 'status',
      panels: [
        { name: 'buriedPoint', label: this.$t('数据埋点') },
        { name: 'dataAnalyze', label: this.$t('数据剖析') },
      ],
      active: 'buriedPoint',
      isShow: false,
      hasAuth: false,
    };
  },
  mounted() {
    this.checkUserAuth();
  },
  methods: {
    checkUserAuth() {
      this.bkRequest
        .httpRequest('auth/checkUserAuth', {
          params: {
            action_id: 'result_table.update_data',
            object_id: this.$route.query.result_table_id,
            user_id: this.$store.getters.getUserName,
          },
        })
        .then(res => {
          if (res.result && res.data) {
            this.hasAuth = true;
            this.panels.push(
              ...[
                { name: 'qualityAduit', label: this.$t('质量审核') },
                { name: 'dataFix', label: this.$t('质量修正') },
              ]
            );
          } else {
            this.hasAuth = false;
          }
        })
        ['catch'](e => {
          this.hasAuth = false;
        });
    },
    updateDataInfo(type, data) {
      this.tipsInfo[type] = data;
    },
    refresh(timeout) {
      for (const key in this.$refs) {
        this.$refs[key].pullData(timeout);
      }
    },
    cancelRefresh() {
      for (const key in this.$refs) {
        this.$refs[key].cancelRefresh();
      }
    },
    changeTab(tab) {
      this.active = tab;
      this.isShow = false;
    },
  },
};
</script>

<style lang="scss" scoped>
.icon-light-line {
  display: inline-block;
  width: 20px;
  height: 20px;
  margin-right: 5px;
  font-size: 20px;
  cursor: pointer;
}
.bk-tab {
  ::v-deep .bk-tab-header {
    background-color: white;
    border-color: white;
    .bk-tab-label-wrapper {
      display: flex;
      justify-content: center;
      .bk-tab-label {
        font-size: 13px;
      }
    }
  }
}
.guide-container {
  position: relative;
  display: flex;
  justify-content: space-between;
  .line {
    position: absolute;
    top: 15px;
    width: 100%;
    height: 0;
    width: 850px;
    transform: translate(-50%, 0%);
    left: 50%;
    right: 50%;
    border-top: 1px solid #3a84ff;
    z-index: 0;
  }
  .guide-item {
    width: 30%;
    display: flex;
    flex-direction: column;
    justify-content: space-between;
    align-items: center;
    .index-container {
      display: flex;
      flex-direction: column;
      .icon {
        background-color: white;
        font-size: 38px;
        z-index: 1;
      }
      .index-name {
        font-size: 16px;
        font-weight: 550;
      }
    }
    .index-explain {
      border-radius: 10px;
      border: 1px solid #ddd;
      padding: 15px 10px;
      box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
      line-height: 26px;
      font-size: 14px;
      margin-top: 10px;
      min-height: 165px;
      height: 100%;
      ul > li {
        list-style: outside;
        margin-left: 20px;
      }
    }
  }
}
</style>
