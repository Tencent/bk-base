

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
    <div class="bk-code-edit">
      <Monaco
        :code="params.config.sql"
        :tools="{
          guidUrl: $store.getters['docs/getPaths'].realtimeSqlRule,
          toolList: { view_data: true, full_screen: true, event_fullscreen_default: true },
        }"
        height="100%"
        @codeChange="getValue"
        @mounted="onMounted"
        @onViewDataFormatClick="showFieldTable">
        <div v-show="isFieldTableShow"
          class="field-table bk-scroll-x">
          <div class="bk-form-item result-table-select m10">
            <label class="fl bk-label pr15">{{ $t('结果数据表') }}</label>
            <div class="bk-form-content pr10">
              <bkdata-selector
                :popoverOptions="{ appendTo: 'parent' }"
                :list="rtIds"
                :selected.sync="output"
                :settingKey="'id'"
                :displayKey="'id'"
                @item-selected="changeResultTableList" />
            </div>
          </div>
          <div v-bkloading="{ isLoading: loading }"
            class="bkdata-table-small">
            <bkdata-table :data="mySqlTable"
              :headerCellStyle="{ background: '#fff' }">
              <bkdata-table-column :label="$t('字段名称')"
                prop="name" />
              <bkdata-table-column :label="$t('类型')"
                prop="type" />
              <bkdata-table-column :label="$t('描述')"
                prop="des" />
            </bkdata-table>
          </div>
        </div>
      </Monaco>
    </div>
    <SqlEditCollapseBtn />
    <bkdata-tab :active="activeTab"
      :class="'node-editing'">
      <bkdata-tab-panel :label="$t('节点配置')"
        name="config">
        <div id="validate_form2"
          class="bk-form">
          <div v-bkloading="{ isLoading: loading }"
            class="acompute-node-edit node">
            <div class="content-up">
              <!-- <h3 class="title">{{ $t('实时计算节点') }}</h3> -->
              <div class="bk-form-item">
                <label class="bk-label">
                  {{ $t('节点名称') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content flex-column">
                  <bkdata-input
                    v-model="params.config.name"
                    :class="{ 'bk-form-input-error': validata.name.status }"
                    :placeholder="$t('节点的中文名')"
                    :maxlength="50"
                    name="validation_name"
                    type="text"
                    @keyup="checkDataDes" />
                  <div v-show="validata.name.status"
                    class="help-block"
                    v-text="validata.name.errorMsg" />
                </div>
              </div>
            </div>
            <div class="content-up pt30">
              <div class="bk-form-item sideslider-select">
                <label class="bk-label">{{ $t('数据输入') }}</label>
                <div class="bk-form-content flex-column source-origin">
                  <span
                    v-for="(item, index) in parentConfig"
                    :key="index"
                    :title="`${item.result_table_ids[0]}(${item.node_name})`"
                    class="bk-form-input data-src">
                    {{ item.result_table_ids[0] }}({{ item.node_name }})
                  </span>
                  <i v-bk-tooltips="$t('来自上一节点的输出')"
                    class="bk-icon icon-question-circle" />
                </div>
              </div>
              <div class="bk-form-item cal-label">
                <label class="bk-label">
                  {{ $t('数据输出') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <div class="biz_id">
                    <bkdata-selector
                      :disabled="selfConfig.hasOwnProperty('node_id')"
                      :displayKey="'name'"
                      :list="bizList"
                      :placeholder="$t('请选择')"
                      :selected.sync="params.config.bk_biz_id"
                      :settingKey="'biz_id'" />
                  </div>
                  <div v-if="selfConfig.hasOwnProperty('node_id')"
                    class="bk-form-content output">
                    <bkdata-input
                      v-model="params.config.table_name"
                      :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                      :title="params.config.table_name"
                      disabled
                      style="width: 100%"
                      :maxlength="50"
                      name="validation_name"
                      type="text" />
                  </div>
                  <div v-else
                    class="bk-form-content output">
                    <bkdata-input
                      v-model="params.config.table_name"
                      :class="{ 'bk-form-input-error': validata.table_name.status }"
                      :placeholder="$t('由英文字母_下划线和数字组成_且字母开头')"
                      :title="params.config.table_name"
                      :maxlength="50"
                      name="validation_name"
                      type="text"
                      @keyup="checkDataFormat" />
                  </div>
                </div>
                <div
                  :title="params.config.bk_biz_id + '_' + params.config.table_name"
                  class="bk-form-content table-name">
                  {{ params.config.bk_biz_id }}_{{ params.config.table_name }}
                  <i v-bk-tooltips="$t('数据输出ID_作为下一个节点的数据输入')"
                    class="bk-icon icon-question-circle">
                    <!-- <span>{{ $t('数据输出ID_作为下一个节点的数据输入') }}</span> -->
                  </i>
                </div>
                <div
                  v-show="validata.table_name.status"
                  class="help-block"
                  style="margin-left: 125px"
                  v-text="validata.table_name.errorMsg" />
              </div>
              <div class="bk-form-item">
                <label class="bk-label">
                  {{ $t('输出中文名') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-input
                    v-model="params.config.output_name"
                    :placeholder="$t('输出中文名')"
                    :maxlength="50"
                    name="validation_name"
                    type="text" />
                </div>
              </div>
            </div>
            <div class="content-down">
              <div class="bk-form-item window-type">
                <label class="bk-label">
                  {{ $t('窗口类型') }}
                  <span class="required">*</span>
                </label>
                <div :class="{ English: $t('窗口类型') !== '窗口类型' }"
                  class="bk-form-content">
                  <bkdata-radio-group v-model="params.config.window_type">
                    <bkdata-radio value="none">
                      {{ $t('无窗口') }}
                    </bkdata-radio>
                    <bkdata-radio :value="'scroll'">
                      {{ $t('滚动窗口') }}
                    </bkdata-radio>
                    <bkdata-radio :value="'slide'">
                      {{ $t('滑动窗口') }}
                    </bkdata-radio>
                    <bkdata-radio :value="'accumulate'">
                      {{ $t('累加窗口') }}
                    </bkdata-radio>
                    <bkdata-radio v-if="$modules.isActive('session_window')"
                      :value="'session'">
                      {{ $t('会话窗口') }}
                    </bkdata-radio>
                  </bkdata-radio-group>
                </div>
              </div>
              <div
                v-if="params.config.window_type !== 'none'"
                class="bk-form-item indow-example-form"
                ext-cls="form-content-auto-width">
                <label class="bk-label">
                  {{ $t('窗口示例') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <ScrollWindowBar :type="windowType" />
                </div>
              </div>
              <div
                v-if="params.config.window_type !== 'none' && params.config.window_type !== 'session'"
                class="bk-form-item time-select">
                <label class="bk-label">
                  {{ $t('统计频率') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :list="countFreqList"
                    :selected.sync="params.config.count_freq"
                    :settingKey="'id'"
                    :displayKey="'name'"
                    @visible-toggle="divScrollBottom" />
                </div>
              </div>
              <div
                v-if="params.config.window_type === 'slide'
                  || params.config.window_type === 'accumulate'"
                class="bk-form-item time-select">
                <label class="bk-label">
                  {{ $t('窗口长度') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :list="windowInterval"
                    :selected.sync="params.config.window_time"
                    :settingKey="'id'"
                    :displayKey="'name'"
                    @visible-toggle="divScrollBottom" />
                </div>
              </div>
              <div v-if="params.config.window_type === 'session'"
                class="bk-form-item time-select">
                <label class="bk-label">
                  {{ $t('间隔时间') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :displayKey="'name'"
                    :list="waitTime"
                    :placeholder="$t('请选择')"
                    :selected.sync="params.config.session_gap"
                    :settingKey="'id'" />
                </div>
              </div>
              <div v-if="params.config.window_type !== 'none'"
                class="bk-form-item time-select">
                <label class="bk-label">
                  {{ $t('等待时间') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :displayKey="'name'"
                    :list="waitTime"
                    :placeholder="$t('请选择')"
                    :selected.sync="params.config.waiting_time"
                    :settingKey="'id'"
                    @visible-toggle="divScrollBottom" />
                </div>
              </div>
              <div v-if="params.config.window_type === 'session'"
                class="bk-form-item time-select">
                <label class="bk-label">
                  {{ $t('过期时间') }}
                  <span class="required">*</span>
                </label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :displayKey="'name'"
                    :list="expiredTimeList"
                    :placeholder="$t('请选择')"
                    :selected.sync="params.config.expired_time"
                    :settingKey="'id'" />
                </div>
              </div>
              <RealtimeDealy
                v-if="params.config.window_type !== 'none'"
                v-model="params.config.window_lateness"
                :windowType="params.config.window_type" />
            </div>
          </div>
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel :label="`${$t('关联任务')}(${referTaskLength})`"
        name="referTasks">
        <ReferTasks v-model="referTaskLength"
          :rtid="currentRtid"
          :nodeId="selfConfig.node_id" />
      </bkdata-tab-panel>
    </bkdata-tab>
  </div>
</template>

<script>
import Monaco from '@/components/monaco';
import ReferTasks from './components/ReferTask';
import SqlEditCollapseBtn from './components/SqlEditCollapseBtn';
import ScrollWindowBar from '../Components/ScrollWindowBar';
import childMixin from './config/child.global.mixin.js';
import RealtimeDealy from './components/Realtime.delay';
export default {
  components: {
    Monaco,
    ReferTasks,
    SqlEditCollapseBtn,
    ScrollWindowBar,
    RealtimeDealy,
  },
  mixins: [childMixin],
  props: {
    nodeType: {
      type: String,
      defalut: '',
    },
  },
  data() {
    return {
      activeTab: 'config',
      referTaskLength: 0,
      fieldLoading: false,
      output: '',
      buttonStatus: {
        isDisabled: false,
      },
      isFieldTableShow: false, // 数据格式表格是否显示
      loading: true,
      defineFields: [], // 自定义提示字段
      sqlError: false, // sql报错窗口是否显示
      isExpand: true, // sql编辑器是否展开
      sqlErrorMsg: '', // sql错误提示
      mySqlTable: [], // 字段表
      selfConfig: '', // 自身配置
      parentConfig: '', // 本地保存父节点config
      isFullScreen: false, // 是否全屏
      validata: {
        table_name: {
          status: false,
          errorMsg: '',
        },
        name: {
          status: false,
          errorMsg: '',
        },
      },
      sql: '', // sql 字段
      params: {
        node_type: this.nodeType,
        config: {
          output_name: '',
          table_name: '',
          name: '',
          bk_biz_id: 0,
          sql: '',
          window_type: 'none',
          count_freq: 30,
          window_time: 10,
          waiting_time: 0,
          session_gap: 0,
          expired_time: 0,
          window_lateness: {
            allowed_lateness: false,
            lateness_time: 1,
            lateness_count_freq: 60,
          },
        },
        from_links: [],
        frontend_info: [],
      },
      bizList: [], // 父节点业务类型
      timer: '',
    };
  },
  computed: {
    waitTime() {
      const times = [0, 10, 30, 60, 180, 300, 600];
      return this.timeCalculator(times, '秒');
    },
    /** 窗口长度 */
    windowInterval() {
      const windowType = this.params.config.window_type;
      let times = [];
      if (windowType === 'slide') {
        times = [2, 10, 30, 45, 60, 1440];
      } else {
        times = [10, 30, 45, 60, 1440];
      }
      return this.timeCalculator(times, '分钟', { id: 1440, name: '24' + this.$t('小时') });
    },
    countFreqList() {
      const times = [30, 60, 180, 300, 600];
      return this.timeCalculator(times, '秒');
    },
    expiredTimeList() {
      const times = [0, 1, 3, 5, 10, 30];
      return this.timeCalculator(times, '分钟');
    },
    windowType() {
      const type = this.params.config.window_type;
      if (type === 'scroll' || type === 'session') {
        return 'scroll';
      }

      return type;
    },
    currentRtid() {
      return (this.selfConfig.result_table_ids && this.selfConfig.result_table_ids[0]) || '';
    },
    rtIds() {
      let arr = [];
      if (Object.keys(this.parentConfig).length) {
        this.parentConfig.forEach(config => {
          config.result_table_ids.forEach(rtid => {
            arr.push({
              id: rtid,
              name: rtid,
            });
          });
        });
      }
      return arr;
    },
  },
  watch: {
    'params.config.bk_biz_id': {
      immediate: true,
      handler: function (value) {
        if (value) {
          this.parentConfig.forEach(item => {
            if (item.bk_biz_id === value || item.config.bk_biz_id === value) {
              this.params.config.bk_biz_name = this.getBizNameByBizId(item.bk_biz_id);
            }
          });
        }
      },
    },
    'params.config.window_type'(value, oldValue) {
      if (oldValue === 'slide' && value === 'accumulate') {
        this.params.config.window_time = 10;
      }
    },
  },
  mounted() {},
  methods: {
    timeCalculator(times, unit, special = { id: null, name: null }) {
      const timeOptions = times.map(time => {
        const name = time === 0 ? this.$t('无') : time.toString() + this.$t(unit);
        const obj = time === special.id ? special : { id: time, name: name };
        return obj;
      });
      return timeOptions;
    },
    /*
     * 跳转至指南页
     */
    jumpToGuide() {
      if (['openpaas', 'tgdp'].includes(window.BKBASE_Global.runVersion)) {
        window.open(window.BKBASE_Global.helpDocUrl + 'index.html');
      } else {
        window.open(window.BKBASE_Global.helpDocUrl + 'docs/features/tasks/423-han-shu-ku.html');
      }
    },
    /*
     * @改变结果数据表字段
     */
    changeResultTableList(value) {
      this.output = value;
      this.getResultList(true, this.output);
    },
    /*
     * @改变按钮的状态
     * */
    changeButtonStatus() {
      this.buttonStatus.isDisabled = false;
    },
    /*
     *   下拉滚动
     * */
    divScrollBottom() {
      let div = document.getElementById('validate_form2');
      setTimeout(function () {
        div.scrollTop = div.offsetHeight + 100;
      }, 200);
    },
    /*
     * 显示数据格式表格
     * */
    showFieldTable() {
      this.isFieldTableShow = !this.isFieldTableShow;
    },
    /*
     *   检验计算名称
     */
    checkDataFormat() {
      let val = this.params.config.table_name.trim();
      if (this.validator.validWordFormat(val)) {
        this.validata.table_name.status = false;
      } else {
        this.validata.table_name.status = true;
        if (val.length === 0) {
          this.validata.table_name.errorMsg = this.validator.message.required;
        } else {
          this.validata.table_name.errorMsg = this.validator.message.wordFormat;
        }
      }
    },
    /*
     *  校验计算描述
     */
    checkDataDes(e) {
      let val = this.params.config.name.trim();
      if (val.length < 51 && val.length !== 0) {
        this.validata.name.status = false;
      } else {
        this.validata.name.status = true;
        if (val.length === 0) {
          this.validata.name.errorMsg = this.validator.message.required;
        } else {
          this.validata.name.errorMsg = this.validator.message.max50;
        }
      }
    },
    /*
     * @初始化报错信息
     */
    initVali() {
      this.validata = {
        table_name: {
          status: false,
          errorMsg: '',
        },
        name: {
          status: false,
          errorMsg: '',
        },
      };
    },
    /*
                codemiror初始化
            */
    init() {
      this.validata.table_name.status = false;
      this.validata.name.status = false;
    },
    /*
     *   sql错误提示
     */
    sqlErrorFunc(msg) {
      this.sqlErrorMsg = msg;
      this.sqlError = true;
    },
    /*
                数据回填
            */
    setConfigBack(self, source, fl, option = {}) {
      this.activeTab = option.elType === 'referTask' ? 'referTasks' : 'config';
      this.initVali();
      this.selfConfig = self;
      this.parentConfig = source;
      this.sqlError = false;
      this.sqlErrorMsg = '';
      this.output = this.parentConfig[0].result_table_ids[0];
      this.params.frontend_info = self.frontend_info;
      this.params.from_links = fl;
      if (!option.isLoad) {
        this.getResultList();
      } else {
        this.loading = false;
      }
      this.bizList = [];
      for (let i = 0; i < this.parentConfig.length; i++) {
        let bizId = this.parentConfig[i].bk_biz_id || this.parentConfig[i].config.bk_biz_id;
        if (!this.bizList.some(item => item.biz_id === bizId)) {
          this.bizList.push({
            name: this.getBizNameByBizId(bizId),
            biz_id: bizId,
          });
        }
      }

      if (this.bizList.length) {
        this.params.config.bk_biz_id = this.bizList[0].biz_id;
      }
      // this.judgeBizIsEqual()

      // 对于没有 config 节点，初始化 config
      if (self.node_config === undefined) {
        self.node_config = this.initConfig();
      } else {
        this.cleanConfig(self.node_config);
      }

      if (self.hasOwnProperty('node_id') || self.isCopy) {
        for (let key in self.node_config) {
          if (self.node_config[key] || self.node_config[key] === 0) {
            this.params.config[key] = self.node_config[key];
          }
        }
      } else {
        // this.params.config = this.initConfig()
      }
      // 将 self.node_config 与 params.config 绑定，未保存节点下次打开节点，也可以看到上一次的修改
      this.init();
    },
    /*
     * 默认 config 配置
     */
    initConfig() {
      return {
        output_name: '',
        table_name: '',
        name: '',
        sql: '',
        bk_biz_id: this.bizList[0].biz_id,
        window_type: 'none',
        count_freq: 30,
        window_time: 10,
        waiting_time: 0,
      };
    },
    /*
     * 规整存在的 config 配置，与节点配置页面适配
     */
    cleanConfig(config) {
      if (config.count_freq === null) {
        config.count_freq = 30;
      }
      if (config.window_time === null) {
        config.window_time = 10;
      }
      if (config.waiting_time === null) {
        config.waiting_time = 0;
      }
      return config;
    },
    /*
     *   判断业务类型是否重复
     */
    // judgeBizIsEqual() {
    //     for (let i = 0; i < this.bizList.length; i++) {
    //         for (let j = 0; j < this.bizList.length; j++) {
    //             if (this.bizList[i].biz_id === this.bizList[j].biz_id && i !== j) {
    //                 this.bizList.splice(i, 1)
    //             }
    //         }
    //     }
    // },
    /*
                编辑器全屏
            */
    fullScreen() {
      this.isFullScreen = !this.isFullScreen;
    },
    /*
                获取结果字段表
            */
    getResultList(isDropListChange = false) {
      this.fieldLoading = true;
      this.loading = true;
      this.bkRequest
        .httpRequest('meta/getSQLFields', {
          params: {
            rtId: this.output,
          },
        })
        .then(res => {
          if (res.result) {
            this.sql = '';
            this.mySqlTable.splice(0);
            this.defineFields = [];
            for (let i = 0; i < res.data.fields_detail.length; i++) {
              let temp = {
                name: res.data.fields_detail[i].name,
                type: res.data.fields_detail[i].type,
                des: res.data.fields_detail[i].description,
              };
              // 添加编辑器自定义字段
              let defineFiedl = {
                value: res.data.fields_detail[i].name,
                caption: res.data.fields_detail[i].name,
                meta: res.data.fields_detail[i].name,
                type: 'local',
                score: 1, // 让test排在最上面
              };
              this.defineFields.push(defineFiedl);

              this.mySqlTable.push(temp);
            }
            // 添加编辑器自定义字段
            for (let i = 0; i < this.parentConfig.length; i++) {
              this.defineFields.push({
                value: this.parentConfig[i].table_name,
                caption: this.parentConfig[i].table_name,
                meta: this.parentConfig[i].table_name,
                type: 'local',
                score: 1, // 让test排在最上面
              });
            }
            for (let i = 0; i < res.data.fields_detail.length; i++) {
              this.sql += res.data.fields_detail[i].name + ', ';
              if (i % 3 === 0 && i !== 0) {
                this.sql += '\n    ';
              }
            }
            let index = this.sql.lastIndexOf(',');
            this.sql = this.sql.slice(0, index);
            if (!this.sql) {
              this.sql = '*';
            }
            let output = this.parentConfig[0].result_table_ids[0];
            if (!isDropListChange && !this.selfConfig.hasOwnProperty('node_id')) {
              this.params.config.sql = 'select ' + this.sql + '\nfrom ' + output;
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.fieldLoading = false;
          this.loading = false;
        });
    },
    /*
                关闭侧边弹框
            */
    closeSider() {
      this.buttonStatus.isDisabled = false;
      this.loading = true;
      this.$emit('closeSider');
      this.isFullScreen = false;
    },
    /*
                获取参数
            */
    getValue(content) {
      this.params.config.sql = content;
    },
    validateFormData() {
      this.buttonStatus.isDisabled = true;
      this.checkDataDes();
      this.checkDataFormat();
      let result = [];
      for (var key in this.validata) {
        result.push(this.validata[key].status);
        result.forEach(function (item, index) {
          if (!item) {
            result.splice(index, 1);
          }
        });
      }
      if (result.length === 0) {
        this.params.config.from_result_table_ids = this.getFromResultTableIds(this.parentConfig);
        if (this.selfConfig.hasOwnProperty('node_id')) {
          this.params.flow_id = this.$route.params.fid;
        } else {
          if (!this.params.config.bk_biz_id) {
            this.params.config.bk_biz_id = this.parentConfig[0].bk_biz_id
                        || this.parentConfig[0].config.bk_biz_id;
          }
        }
      } else {
        this.buttonStatus.isDisabled = false;
        return false;
      }
      return true;
    },
    onMounted(editor) {
      this.editor = editor;
    },
  },
};
</script>
<style lang="scss" scoped>
.icon-question-circle {
  position: absolute;
  right: 0;
  top: 50%;
  transform: translate(calc(100% + 10px), -50%);
}
.node-editing {
  .help-block {
    color: red;
  }
}
.bk-label {
  position: relative;
  .required {
    color: red;
    position: absolute;
    right: 18px;
  }
}
.bk-form-content {
  .biz_id {
    width: 30%;
  }

  .bk-form-input {
    &.data-src {
      display: inline-block;
      line-height: 32px;
      overflow: hidden;
      white-space: nowrap;
      text-overflow: ellipsis;

      &:first-child {
        margin-right: 5px;
      }

      &:last-child {
        margin-right: 0px;
      }
    }
  }

  .bk-form-content {
    &.output {
      margin-left: 5px !important;
      width: calc(70% - 5px);
      height: 32px;
    }
  }

  &.table-name {
    margin-top: 5px;
    line-height: 28px;
    height: 28px;
    color: rgb(255, 255, 255);
    margin-left: 150px;
    padding-left: 10px;
    background: rgb(58, 132, 255);
    border-radius: 2px;
    overflow: hidden;
  }
}
</style>
