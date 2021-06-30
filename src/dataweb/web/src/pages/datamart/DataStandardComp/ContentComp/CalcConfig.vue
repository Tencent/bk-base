

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
    <bkdata-sideslider :isShow.sync="isShow"
      extCls="slide-style"
      :quickClose="true"
      :width="580">
      <div slot="header"
        class="head-container">
        <i class="icon-cog" />
        <div class="head-text">
          <div class="title">
            {{ $t('计算配置') }}
          </div>
          <i18n path="标准计算配置"
            tag="p">
            <span class="primary-color"
              place="name">
              {{ standardInfo.standard_content_name }}
            </span>
          </i18n>
        </div>
      </div>
      <div slot="content">
        <form class="bk-form"
          :class="{ width100: $i18n.locale !== 'en' }">
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('加工来源') }}:</label>
            <div class="bk-form-content">
              {{ getDefaultValue(standardInfo.parent_name) }}
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('窗口类型') }}:</label>
            <div class="bk-form-content">
              {{ getDefaultValue($i18n.locale === 'en'
                ? standardInfo.window_period.window_type
                : standardInfo.window_period.window_type_alias) }}
            </div>
          </div>
          <div v-if="isShowWindow"
            class="bk-form-item">
            <label class="bk-label pr15">{{ $t('窗口示例') }}:</label>
            <div class="bk-form-content">
              <ScrollWindowBar v-if="standardInfo.window_period.window_type"
                :type="standardInfo.window_period.window_type" />
            </div>
          </div>
          <div v-if="isShowWindow"
            class="bk-form-item">
            <label class="bk-label pr15">{{ $t('统计频率') }}:</label>
            <div class="bk-form-content">
              {{ getDefaultValue(standardInfo.window_period.count_freq_alias) }}
            </div>
          </div>
          <!-- 滚动窗口只有“统计频率和等待时间” -->
          <div v-if="!['scroll', 'none'].includes(standardInfo.window_period.window_type)"
            class="bk-form-item">
            <label class="bk-label pr15">{{ $t('窗口长度') }}:</label>
            <div class="bk-form-content">
              {{ getDefaultValue(standardInfo.window_period.window_time) }}
            </div>
          </div>
          <div v-if="isShowWindow"
            class="bk-form-item">
            <label class="bk-label pr15">{{ $t('等待时间') }}:</label>
            <div class="bk-form-content">
              {{ standardInfo.window_period.waiting_time ? standardInfo.window_period.waiting_time_alias : $t('无') }}
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('过滤条件') }}:</label>
            <div class="bk-form-content">
              {{ ['where', 'none'].includes(standardInfo.filter_cond)
                || !standardInfo.filter_cond ? $t('无')
                : standardInfo.filter_cond.replace(/where/, '') }}
            </div>
          </div>
          <div class="bk-form-item">
            <label class="bk-label pr15">{{ $t('计算SQL') }}:</label>
            <div class="bk-form-content height450 pt20">
              <Monaco
                :code="code"
                :tools="{
                  guidUrl: $store.getters['docs/getPaths'].querySqlRule,
                  toolList: {
                    view_data: true,
                    full_screen: true,
                    event_fullscreen_default: true,
                  },
                }" />
            </div>
          </div>
        </form>
      </div>
    </bkdata-sideslider>
  </div>
</template>

<script>
import ScrollWindowBar from '@/pages/DataGraph/Graph/Components/ScrollWindowBar';
import Monaco from '@/components/monaco';

export default {
  components: {
    ScrollWindowBar,
    Monaco,
  },
  props: {
    code: {
      type: String,
      default: '',
    },
    standardInfo: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      isShow: false,
      windowType: {},
    };
  },
  computed: {
    isShowWindow() {
      if (this.standardInfo.window_period) {
        return this.standardInfo.window_period.window_type !== 'none';
      }
      return true;
    },
  },
  methods: {
    getDefaultValue(value) {
      return value || this.$t('无');
    },
  },
};
</script>

<style lang="scss" scoped>
.bk-form {
  padding: 20px;
  .bk-form-item {
    .no-float {
      float: inherit;
    }
    .bk-form-content {
      margin-left: 100px;
    }
    .height450 {
      width: 500px;
      height: 450px;
      margin-left: 24px;
    }
  }
}
.width100 {
  .bk-label {
    width: 100px;
  }
}
::v-deep .slide-style {
  .bk-sideslider-header {
    height: auto !important;
    .bk-sideslider-title {
      padding: 0 0 0 30px !important;
      height: auto !important;
      .head-container {
        display: flex;
        line-height: normal;
        font-size: 12px;
        .icon-cog {
          float: left;
          font-size: 22px;
          width: 22px;
          margin: 20px 0 0 10px;
        }
        .head-text {
          width: calc(100% - 32px);
          padding: 10px;
          .title {
            font-size: 16px;
            line-height: 28px;
            margin-bottom: 10px;
          }
          p {
            font-size: 14px;
            color: #979ba5;
            font-weight: normal;
            .primary-color {
              color: #3a84ff;
            }
          }
        }
      }
    }
  }
}
</style>
