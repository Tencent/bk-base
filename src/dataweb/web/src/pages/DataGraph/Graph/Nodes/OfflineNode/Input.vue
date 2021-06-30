

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
  <Card :show.sync="syncShow"
    :title="title"
    :panels="panels"
    :color="syncParams.color"
    :currentTab.sync="currentTab">
    <div class="content"
      style="backgroundcolor: #fff">
      <div v-if="currentTab === 'config'"
        class="config-tab-content">
        <!-- <InfoTips text="窗口类型影响当前节点加载上有数据的行为，详见" size="small" :link="{ text: '文档' }"></InfoTips> -->
        <bkdata-alert type="info">
          <div slot="title">
            {{ $t('窗口类型影响当前节点加载上有数据的行为_详见') }}
            <a>{{ $t('文档') }}</a>
          </div>
        </bkdata-alert>
        <div v-if="openBtn"
          class="bk-button-group">
          <bkdata-button v-for="(button, index) in windowTypes"
            :key="index"
            v-bk-tooltips.bottom="button.tipsText"
            class="bottom-middle"
            :disabled="button.disabled"
            :class="syncParams.window_type === button.value ? 'is-selected' : ''"
            size="small"
            @click="syncParams.window_type = button.value">
            {{ button.name }}
          </bkdata-button>
        </div>
        <div v-else
          class="bk-button-group">
          <bkdata-button v-for="(button, index) in windowTypes"
            :key="index"
            :disabled="button.disabled"
            :class="syncParams.window_type === button.value ? 'is-selected' : ''"
            size="small"
            @click="syncParams.window_type = button.value">
            {{ button.name }}
          </bkdata-button>
        </div>
        <bkdata-form v-if="isWindowSizeShow"
          ref="base-form"
          formType="vertical"
          :model="syncParams">
          <window-form-layout
            layout="double"
            mainFormType="input"
            :label="windowSizeLabel.label"
            :labelDesc="windowSizeLabel.desc"
            :mainParam.sync="syncParams.window_size"
            :minorParam.sync="syncParams.window_size_unit"
            :minorSelectList="windowSizeUnitList"
            @minor-selected="autoSetAccumulateTime"
            @main-input="setInputParams" />
        </bkdata-form>
        <div v-if="syncParams.window_type === 'all'"
          class="full-data-tips">
          <i class="icon-exclamation-circle-delete" />
          <p class="tips-text">
            <span>{{ $t('上游是静态节点提示') }}</span>
            <span>{{ $t('本节点全量数据提示') }}</span>
          </p>
        </div>
        <AdvancedPanel v-if="syncParams.window_type !== 'all'">
          <bkdata-form ref="advanced-form"
            formType="vertical">
            <window-form-layout label="依赖策略"
              labelDesc="依赖策略"
              :mainParam.sync="syncParams.dependency_rule"
              :mainSelectList="ruleList"
              @main-selected="formSelectedHandle('dependency_rule')" />
            <window-form-layout
              layout="dri"
              label="窗口偏移"
              labelDesc="窗口偏移"
              offsetTip="往左(-)"
              :mainParam.sync="syncParams.window_offset"
              :minorParam.sync="syncParams.window_offset_unit"
              :minorSelectList="windowSizeUnitList"
              @main-input="formSelectedHandle('window_offset')"
              @minor-selected="formSelectedHandle('window_offset_unit')" />
            <template v-if="syncParams.window_type === 'accumulate'">
              <window-form-layout
                layout="dri"
                label="累加起始相对时间"
                labelDesc="累加起始相对时间"
                offsetTip="往右(+)"
                :mainParam.sync="syncParams.window_start_offset"
                :minorParam.sync="syncParams.window_start_offset_unit"
                :minorSelectList="windowSizeUnitList"
                @main-input="formSelectedHandle('window_start_offset')"
                @minor-selected="formSelectedHandle('window_start_offset_unit')" />
              <window-form-layout
                layout="dri"
                label="累加结束相对时间"
                labelDesc="累加起始相对时间"
                offsetTip="往右(+)"
                :mainParam.sync="syncParams.window_end_offset"
                :minorParam.sync="syncParams.window_end_offset_unit"
                :minorSelectList="windowSizeUnitList"
                @main-input="formSelectedHandle('window_end_offset')"
                @minor-selected="formSelectedHandle('window_end_offset_unit')" />
              <window-form-layout
                v-if="showAccumulateForm"
                layout="single"
                label="首次累加基准值"
                labelDesc="首次累加基准值"
                mainFormType="date"
                :mainInputProps="{
                  'time-picker-options': {
                    steps: [1, 60, 60],
                  },
                }"
                :mainParam.sync="syncParams.accumulate_start_time"
                @date-pick="formSelectedHandle('accumulate_start_time')" />
            </template>
          </bkdata-form>
        </AdvancedPanel>
      </div>
      <div v-show="currentTab === 'field'"
        class="field-tab-content">
        <FieldList :showTitle="false"
          :resultList="parentResultTables"
          :params.sync="originParams"
          :domain="title" />
      </div>
      <div class="recent-tab-content" />
    </div>
  </Card>
</template>

<script lang="ts">
import FieldList from '@/pages/DataGraph/Graph/Children/components/Datainput.vue';
import Card from '@/pages/DataGraph/Graph/Nodes/NodesComponents/Card.tsx';
import InfoTips from '@/components/TipsInfo/TipsInfo.vue';
import AdvancedPanel from '../NodesComponents/AdvancedPanel.tsx';
import { Component, Prop, PropSync, Watch, Vue, InjectReactive } from 'vue-property-decorator';
import { windowComposeFormItem, windowFormItem, offsetTip } from '../NodesComponents/BaseStyleComponent';
import windowFormLayout from '../NodesComponents/WindowForm.Base';
import moment from 'moment';
import gundong from '@/common/images/dataFlow/gundong.png';
import huadong from '@/common/images/dataFlow/huadong.png';
import leijia from '@/common/images/dataFlow/leijia.png';
import bkStorage from '@/common/js/bkStorage.js';
import Bus from '@/common/js/bus.js';

@Component({
  components: {
    Card,
    InfoTips,
    windowFormLayout,
    AdvancedPanel,
    windowComposeFormItem,
    windowFormItem,
    offsetTip,
    FieldList,
  },
})
export default class OfflineInput extends Vue {
  @InjectReactive() parentResultTables: Array<object>;

  @Prop({ default: '' }) title: string;
  @Prop({ default: '' }) parentNodeType: string;
  @Prop({ default: () => ({}) }) originParams: Object;
  @PropSync('show', { default: false }) syncShow: boolean;
  @PropSync('params', { default: () => ({}) }) syncParams: Object;

  @Watch('syncShow')
  onShowChanged(val: boolean) {
    this.$emit('showChange', {
      title: this.title,
      value: val,
    });
  }

  @Watch('params.window_type')
  onWindowTypeChange(val: string) {
    if (Object.keys(this.backupWindowInfo[val]).length === 0) {
      this.backupWindowInfo[val] = JSON.parse(JSON.stringify(this.syncParams));
      return;
    }
    Object.keys(this.backupWindowInfo[val]).forEach(key => {
      if (key !== 'window_type') {
        this.syncParams[key] = this.backupWindowInfo[val][key];
      }
    });
  }

  backupWindowInfo: any = {
    scroll: {},
    slide: {},
    accumulate: {},
    whole: {},
  };
  currentTab: string = 'config';
  panels: Array<object> = [
    { name: 'config', label: this.$t('配置') },
    { name: 'field', label: this.$t('字段类型') },
    { name: 'recent', label: this.$t('最近数据'), disabled: true },
  ];
  windowTypes: Array<object> = [
    {
      name: this.$t('滚动窗口'),
      value: 'scroll',
      disabled: this.parentNodeType === 'unified_kv_source',
      tipsText: `
      <div class="tips-text">
        <div class="tips-title">【滚动窗口】窗口长度 = 调度周期</div>
        <img src="${gundong}"/>
        <div class="tips-content">
          <div>比如：</div>
          <div>
            <p>调度周期为1天，窗口长度为1天</p>
            <p>每天0点加载昨天 00：00~23：59 合计1天的数据</p>
          </div>
        </div>
      </div>
      `,
    },
    {
      name: this.$t('滑动窗口'),
      value: 'slide',
      disabled: this.parentNodeType === 'unified_kv_source',
      tipsText: `
      <div class="tips-text">
        <div class="tips-title">【滑动窗口】可以自由配置窗口长度</div>
        <img src="${huadong}"/>
        <div class="tips-content">
          <div>比如：</div>
          <div>
            <p>调度周期为1天，窗口长度为6小时</p>
            <p>每天0点加载昨天 18：00~23：59 合计6小时的数据</p>
          </div>
        </div>
      </div>
      `,
    },
    {
      name: this.$t('累加窗口'),
      value: 'accumulate',
      disabled: this.parentNodeType === 'unified_kv_source',
      tipsText: `
      <div class="tips-text">
        <div class="tips-title">【累加窗口】在累加范围内，每次启动任务加载：起始时间~调度时间内的所有数据</div>
        <img src="${leijia}"/>
        <div class="tips-content">
          <div>比如：</div>
          <div>
            <p>调度周期为1小时，累加时间范围为1天</p>
            <p>每天1点加载 00：00~00：59 的数据</p>
            <p>每天2点加载 00：00~01：59 的数据</p>
            <p>每天3点加载 00：00~02：59 的数据...</p>
          </div>
        </div>
      </div>
      `,
    },
    {
      name: this.$t('全量数据'),
      value: 'whole',
      disabled: this.parentNodeType !== 'unified_kv_source',
    },
  ];

  windowConfig: any = {
    windowSize: '1',
    windowSizeUnit: 'hour',
    dependencyRule: 'all_finished',
  };

  timeList: Array<object> = [
    { name: this.$t('月'), id: 'month' },
    { name: this.$t('天'), id: 'day' },
  ];

  windowSizeUnitList: Array<object> = [
    {
      value: 'hour',
      name: this.$t('小时'),
    },
    {
      value: 'day',
      name: this.$t('天'),
    },
    {
      value: 'week',
      name: this.$t('周'),
    },
    {
      value: 'month',
      name: this.$t('月'),
    },
  ];

  ruleList: Array<object> = [
    {
      value: 'no_failed',
      name: this.$t('无失败'),
    },
    {
      value: 'all_finished',
      name: this.$t('全部成功'),
    },
    {
      value: 'at_least_one_finished',
      name: this.$t('一次成功'),
    },
  ];

  openBtn: boolean = bkStorage.get('nodeHelpMode') || false;
  mounted() {
    Bus.$on('offlineNodeHelp', (helpMode: boolean) => {
      return (this.openBtn = helpMode);
    });
  }

  get showAccumulateForm() {
    return this.syncParams.window_type === 'accumulate';
  }

  get windowSizeLabel() {
    return this.syncParams.window_type === 'slide'
      ? {
        label: this.$t('窗口长度'),
        desc: this.$t('窗口长度'),
      }
      : {
        label: this.$t('数据累加范围'),
        desc: this.$t('数据累加范围'),
      };
  }

  get isWindowSizeShow() {
    return this.syncParams.window_type === 'slide' || this.syncParams.window_type === 'accumulate';
  }

  formSelectedHandle(key: string, value: string | number | undefined = undefined) {
    const type = this.syncParams.window_type;
    this.backupWindowInfo[type][key] = value === undefined ? this.syncParams[key] : value;
  }

  setInputParams() {
    this.syncParams.window_start_offset = '0';
    this.syncParams.window_start_offset_unit = 'hour';
    this.syncParams.window_end_offset = this.syncParams.window_size;
    this.syncParams.window_end_offset_unit = this.syncParams.window_size_unit;
    this.formSelectedHandle('window_size');
    this.formSelectedHandle('window_end_offset');
  }

  autoSetAccumulateTime(val: string) {
    this.syncParams.window_end_offset_unit = val;
    this.formSelectedHandle('window_size_unit');
    this.formSelectedHandle('window_end_offset_unit');
    switch (val) {
      case 'month':
        this.syncParams.accumulate_start_time = moment().startOf('month')
          .format('YYYY-MM-DD HH:mm:ss');
        break;
      case 'week':
        this.syncParams.accumulate_start_time = moment().startOf('week')
          .add(1, 'day')
          .format('YYYY-MM-DD HH:mm:ss');
        break;
      case 'day':
        this.syncParams.accumulate_start_time = moment().startOf('day')
          .format('YYYY-MM-DD HH:mm:ss');
        break;
      default:
        this.syncParams.accumulate_start_time = moment().set({ minute: 0, second: 0 })
          .format('YYYY-MM-DD HH:mm:ss');
        break;
    }
  }
}
</script>

<style lang="scss" scoped>
.content {
  .config-tab-content {
    padding: 20px;
  }
  .bk-button-group {
    display: flex;
    margin-top: 16px;
    /deep/ button {
      flex: 1;
      cursor: pointer;
    }
  }
  .full-data-tips {
    display: flex;
    justify-content: center;
    color: #979ba5;
    margin-top: 20px;
    font-size: 14px;
    line-height: 16px;
  }
}
</style>
<style lang="scss">
.tips-text {
  width: 346px;
  font-size: 12px;
  padding: 10px;
  .tips-title {
    font-size: 12px;
    font-family: PingFangSC, PingFangSC-Regular;
    font-weight: 400;
    color: #dcdee5;
    line-height: 17px;
  }
  img {
    margin: 20px 0;
  }
  .tips-content {
    display: flex;
    div {
      font-size: 12px;
      font-family: PingFangSC, PingFangSC-Regular;
      font-weight: 400;
      color: #c4c6cc;
      line-height: 17px;
    }
  }
}
</style>
