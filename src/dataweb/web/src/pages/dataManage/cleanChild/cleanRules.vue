

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
  <div class="clean-rules">
    <div class="header">
      <p class="title"
        @click="isContentShow = !isContentShow">
        {{ $t('清洗配置规则') }}
      </p>
      <span class="fr collspan"
        @click="isContentShow = !isContentShow">
        <i
          :class="{
            'bk-icon': true,
            'collspan-icon': true,
            'icon-angle-down': !isContentShow,
            'icon-angle-up': isContentShow,
          }" />
      </span>
      <span class="fr guide pr10"
        @click="toHelpWord">
        <i :title="$t('清洗算子指南')"
          class="bk-icon icon-question-circle" />
        {{ $t('清洗算子指南') }}
      </span>
      <span class="fr btn-default-template"
        @click="handleGetDefaultTemplate">
        {{ $t('拉取默认模板') }}
      </span>
      <div class="fr">
        <div v-if="isEdit">
          <bkdata-dropdown-menu>
            <span
              id="dataCleanModel"
              slot="dropdown-trigger"
              v-bk-tooltips="saveModelConfig"
              class="fr btn-default-template dropdown-trigger-text"
              :class="{ readonly: isTemplateChange }">
              {{ $t('保存模板') }}
            </span>
            <div slot="dropdown-content">
              <ul slot="dropdown-content"
                class="bk-dropdown-list">
                <li>
                  <a href="javascript:;"
                    @click="handleTemplateClick('json')">
                    {{ $t('保存json模板') }}
                  </a>
                </li>
                <li>
                  <a href="javascript:;"
                    @click="handleTemplateClick('api')">
                    {{ $t('保存api模板') }}
                  </a>
                </li>
              </ul>
            </div>
          </bkdata-dropdown-menu>
        </div>
      </div>
      <span v-if="!isEdit"
        class="fr btn-default-template"
        @click="importJsonTemplate">
        <input
          id="fileImportJsonTemplate"
          style="display: none"
          accept=".json"
          type="file"
          name=""
          @change="handleFileChange">
        {{ $t('导入模板') }}
      </span>
    </div>
    <table v-show="isContentShow"
      class="bk-table has-table-bordered header-table">
      <thead>
        <tr>
          <th width="50">
            {{ $t('序号') }}
          </th>
          <th width="156">
            {{ $t('数据输入') }}
          </th>
          <th width="156">
            {{ $t('清洗算子') }}
          </th>
          <th>{{ $t('算子参数') }}</th>
          <th width="125">
            {{ $t('操作') }}
          </th>
        </tr>
      </thead>
      <tbody>
        <template v-for="(item, index) in cleanRuleListAddComponent">
          <tr
            :key="item.label"
            :class="{ 'debug-error': item.debugError === 'failure' }"
            @click.stop="e => item.debugError === 'failure' && showLineError(index, item.errorContent, true, e)">
            <td>{{ index + 1 }}</td>
            <td>
              <div :class="{ error: error['cleanRuleList#' + index + '#dataInput'] }"
                class="table-select data-input">
                <bkdata-selector
                  :class="index === 0 ? 'data-input-selector-disabled' : ''"
                  :disabled="index === 0"
                  :list="index === 0 ? dataInput.FirstList : dynamicInputList(index)"
                  :placeholder="dataInput.placehoder"
                  :selected.sync="item.dataInput"
                  @item-selected="
                    (...v) => {
                      refreshRecommendedParamWhileSelecting(v, index);
                    }
                  " />
                <div v-show="error['cleanRuleList#' + index + '#dataInput']"
                  class="error-msg">
                  {{ error['cleanRuleList#' + index + '#dataInput'] }}
                </div>
              </div>
            </td>
            <td>
              <div
                :class="{ error: error['cleanRuleList#' + index + '#operator'] }"
                class="table-select cleaning-operator">
                <bkdata-selector
                  v-bkloading="{ isLoading: item.loadingDataInput }"
                  hasChildren="auto"
                  :isLoading="item.loadingDataInput"
                  :list="item.recommendList ? item.recommendList : cleaningOperator.list"
                  :placeholder="cleaningOperator.placehoder"
                  :selected="item.operator"
                  @item-selected="
                    (id, vText) => {
                      changeItemOperator(index, id);
                    }
                  "
                  @visible-toggle="
                    _ => {
                      refreshRecommendedParamWhileSelecting(_, index);
                    }
                  " />
                <div v-show="error['cleanRuleList#' + index + '#operator']"
                  class="error-msg">
                  {{ error['cleanRuleList#' + index + '#operator'] }}
                </div>
              </div>
            </td>
            <td class="operator-td"
              :class="{ active: focusNum === index }">
              <div class="component-wrapper">
                <component
                  :is="item.currentComponent"
                  ref="calcComp"
                  :data-id-set="dataIdSet"
                  :highlightIndex="0"
                  :index="index"
                  :label="item.label"
                  :operatorParam="item.operatorParam"
                  @focus="rulesFocus"
                  @operatorParam="changParams" />
                <div>
                  <template v-if="index < errorIndex">
                    <span v-if="item.debugError === 'success'"
                      class="bk-icon icon-check-circle-shape success" />
                    <span
                      v-if="item.debugError === 'failure'"
                      :title="$t('点击显示错误')"
                      class="bk-icon icon-exclamation-circle-shape failure pointer"
                      @click="showLineError(index, item.errorContent)" />
                  </template>
                </div>
              </div>
            </td>
            <td :ref="'operating' + index"
              class="operating-button">
              <span
                :class="{ 'is-loading': item.loading }"
                class="bk-icon-button tool-button"
                @click="debug(item, index)">
                <i :title="$t('调试至当前')"
                  class="bk-icon icon-debug" />
              </span>
              <span class="bk-icon-button"
                @click="addRule(item, index + 1)">
                <i :title="$t('添加')"
                  class="bk-icon icon-plus" />
              </span>
              <span v-if="index !== 0"
                class="bk-icon-button"
                @click="deleteRule(item, index)">
                <i :title="$t('删除')"
                  class="bk-icon icon-delete bk-icon-new" />
              </span>
            </td>
          </tr>
        </template>
      </tbody>
    </table>
    <v-info-tip v-if="tip.isShow"
      v-clickoutside="closeTip"
      :direction="'top'"
      :dom="infoTip.dom1"
      @closeTip="closeTip">
      <div class="tip-content">
        <p class="title">
          {{ $t('数据输入') }}【{{ tip.inputName }}】
        </p>
        <div class="content-wrapper">
          <p>{{ tip.inputData }}</p>
        </div>
      </div>
    </v-info-tip>
    <v-info-tip
      v-if="nowErrorShow"
      v-clickoutside="(nowErrorShow = false)"
      :direction="'top'"
      :dom="infoTip.dom3"
      @closeTip="nowErrorShow = false">
      <div class="tip-content warning">
        <p class="title">
          {{ $t('错误信息') }}
        </p>
        <div class="content-wrapper">
          <p>{{ nowErrorContent }}</p>
        </div>
      </div>
    </v-info-tip>
    <v-info-tip
      v-if="tip.isShow"
      v-clickoutside="closeTip"
      :direction="'bottom'"
      :dom="infoTip.dom2"
      @closeTip="closeTip">
      <div class="tip-content">
        <p v-if="tip.resultName === null"
          class="title">
          {{ $t('结果数据') }}
        </p>
        <p v-else
          class="title">
          {{ $t('中间结果输出') }}【{{ tip.resultName }}】
        </p>
        <div v-if="tip.tipType === 'paragraph'"
          class="content-wrapper">
          <p>{{ tip.result }}</p>
        </div>
        <div class="content-wrapper tip-table">
          <table v-if="tip.tipType === 'table'"
            class="bk-table has-table-bordered">
            <template v-if="tip.result">
              <thead>
                <tr>
                  <th v-for="(value, key) of tip.titles"
                    :key="key">
                    {{ value }}
                  </th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(value, key) of tip.result"
                  :key="key">
                  <td v-for="(item, tkey) of value"
                    :key="tkey">
                    {{ item }}
                  </td>
                </tr>
              </tbody>
            </template>
          </table>

          <table v-else-if="tip.tipType === 'iterate'"
            class="bk-table has-table-bordered">
            <template v-if="tip.result">
              <caption>
                {{
                  $t('后续步骤请按照单条数据格式进行配置')
                }}
              </caption>
              <tbody>
                <tr v-for="(value, key) of tip.result"
                  :key="key">
                  <td>{{ key }}</td>
                  <td>{{ value }}</td>
                </tr>
              </tbody>
            </template>
          </table>
        </div>
      </div>
      <div v-if="errorTip.length > 0"
        class="tip-content warning">
        <p class="title fb">
          {{ $t('错误信息') }}
        </p>
        <div class="content-wrapper">
          <p v-for="(err, ekey) in errorTip"
            :key="ekey">
            {{ $t('步骤') }}{{ err[0] + 1 }}：{{ err[1] }}
          </p>
        </div>
      </div>
    </v-info-tip>
  </div>
</template>
<script>
import vInfoTip from '@/pages/dataManage/cleanChild/infoTip';
import Json from '@/pages/dataManage/cleanChild/json';
import Regx from '@/pages/dataManage/cleanChild/regx';
import Csv from '@/pages/dataManage/cleanChild/csv';
import Url from '@/pages/dataManage/cleanChild/url';
import Iteration from '@/pages/dataManage/cleanChild/iteration';
import Split from '@/pages/dataManage/cleanChild/split';
import Replace from '@/pages/dataManage/cleanChild/replace';
import Remove from '@/pages/dataManage/cleanChild/remove';
import Access from '@/pages/dataManage/cleanChild/access';
import Assign from '@/pages/dataManage/cleanChild/assign';
import Splitkv from '@/pages/dataManage/cleanChild/splitkv';
import Zip from '@/pages/dataManage/cleanChild/zip';
import Bus from '@/common/js/bus.js';
import { JValidator } from '@/common/js/check';
import { postMethodWarning, showMsg, generateId, isObject, copyObj, confirmMsg } from '@/common/js/util.js';
import Vue from 'vue';
import clickoutside from '@/common/js/clickoutside.js';
export default {
  directives: {
    clickoutside,
  },
  components: {
    vInfoTip,
    Json,
    Regx,
    Csv,
    Url,
    Iteration,
    Split,
    Replace,
    Remove,
    Access,
    Assign,
    Splitkv,
    Zip,
  },
  props: {
    expand: {
      type: Boolean,
      default: true,
    },
    conf: {
      type: Array,
    },
    latestMsg: {
      type: String,
    },
    isReadonly: {
      type: Boolean,
      default: true,
    },
    loading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      isFieldsDataInit: false,
      isTemplateChange: false,
      isHighlightChange: false,
      isContentShow: true,
      hasRepeat: false,
      // 调试显示信息
      tip: {
        debugIndex: '',
        tipType: '',
        isShow: false,
        inputData: '', // 输入数据
        inputName: '', // 输入名称
        error: [], // 错误提示
        result: '', // 中间结果输出
        resultName: '', // 中间结果名称,
        titles: [],
      },
      infoTip: {
        dom1: '',
        dom2: '',
        dom3: '',
      },
      debugErrors: [],
      nowErrorShow: false,
      nowErrorContent: '',
      recommendCache: null,
      // 数据输入
      dataInput: {
        FirstList: [
          {
            id: 'root',
            name: this.$t('原始数据'),
          },
        ],
        placehoder: this.$t('请选择输入源'),
      },

      // 校验变量
      error: {},

      // 清洗算子列表
      cleaningOperator: {
        list: [
          {
            id: 'regex_extract',
            component: 'Regx',
            name: window.$t('正则提取'),
          },
          {
            id: 'from_json_list',
            component: 'Json',
            name: window.$t('json列表反序列化'),
          },
          {
            id: 'from_json',
            component: 'Json',
            name: window.$t('json反序列化'),
          },
          {
            id: 'csvline',
            component: 'Csv',
            name: window.$t('csv反序列化'),
          },
          {
            id: 'from_url',
            component: 'Url',
            name: window.$t('url反序列化'),
          },
          {
            id: 'iterate',
            component: 'Iteration',
            name: window.$t('遍历'),
          },
          {
            id: 'split',
            component: 'Split',
            name: window.$t('切分'),
          },
          {
            id: 'replace',
            component: 'Replace',
            name: window.$t('替换'),
          },
          {
            id: 'access',
            component: 'Access',
            name: window.$t('取值'),
          },
          {
            id: 'assign',
            component: 'Assign',
            name: window.$t('赋值'),
          },
          {
            id: 'pop',
            component: 'Json',
            name: window.$t('弹出元素'),
          },
          {
            id: 'splitkv',
            component: 'Splitkv',
            name: window.$t('KV提取'),
          },
          {
            id: 'zip',
            component: 'Zip',
            name: window.$t('ZIP'),
          },
        ],
        placehoder: this.$t('请选择算子'),
      },
      cleanRuleList: [],
      focusNum: -1, // 聚焦的行数,
      wholeDebugProcess: false,
      showCleanRule: true,
      initFinished: {
        backfill: false,
        latestMsg: false,
      },
      dataIdSet: [],
      saveModelConfig: {
        content: this.isTemplateChange ? this.$t('请先调试通过_再保存模板') : '',
        appendTo: () => document.getElementById('dataCleanModel'),
      },
    };
  },
  computed: {
    errorTip() {
      return this.tip.error.filter(element => {
        let condition = element[0] <= this.tip.debugIndex || !this.tip.debugIndex;
        return condition;
      });
    },
    errorIndex() {
      return this.cleanRuleListAddComponent.findIndex(item => item.debugError === 'failure') + 1 || 9999;
    },
    cleanRuleListAddComponent() {
      return this.cleanRuleList.map(_rule => {
        const comp = this.cleaningOperator.list.find(op => op.id === _rule.operator);
        comp && this.$set(_rule, 'currentComponent', comp.component);
        return _rule;
      });
    },
    isEdit() {
      return this.$route.name === 'edit_clean';
    },
  },
  watch: {
    expand(val) {
      this.isContentShow = val;
    },
    conf(newVal, oldVal) {
      newVal && this.initByConf(newVal);
    },
    cleanRuleList: {
      handler(newVal) {
        let fields = [];
        for (let rule of newVal) {
          if (rule.operator === 'assign' && rule.operatorParam) {
            if (rule.operatorParam.fields && !(rule.operatorParam.fields instanceof Array)) {
              rule.operatorParam.fields = [rule.operatorParam.fields];
            }
            for (let f of rule.operatorParam.fields) {
              if (f.assign_to && f.type) {
                fields.push(f);
              }
            }
          }
        }
        Vue.nextTick(() => {
          this.checkRepeatAssign();
        });
        Bus.$emit('clean.fields-update', fields);
        this.$emit('cleanFieldsChanged', newVal, this.isHighlightChange);
        this.changeSaveTemplateStatus(true);
      },
      deep: true,
    },
  },
  mounted() {
    window.addEventListener('beforeunload', this.beforeunloadFn);
    this.initByConf(this.conf);
    this.initDataIdSet();
  },
  beforeDestroy() {
    window.removeEventListener('beforeunload', this.beforeunloadFn);
  },
  methods: {
    handleTemplateClick(value) {
      if (value === 'json') {
        this.saveAsJsonTemplate();
      } else {
        this.saveAsApiTemplate();
      }
    },
    changeSaveTemplateStatus(templateChange) {
      if (!this.isFieldsDataInit) return;
      this.isTemplateChange = templateChange;
    },
    importJsonTemplate() {
      document.getElementById('fileImportJsonTemplate').click();
    },
    handleFileChange(e) {
      if (e.target.files.length) {
        const reader = new FileReader();
        reader.onload = fe => {
          this.$emit('importJsonTemplate', this.resolveImportJson(fe.target.result));
        };
        reader.readAsText(e.target.files[0]);
      }
    },
    resolveImportJson(jsonStr) {
      const res = { result: true, data: {}, message: '', code: 200 };
      try {
        const jsonObj = JSON.parse(jsonStr);
        res.data = jsonObj.template;
      } catch (e) {
        res.result = false;
        res.message = e.message;
        res.code = '500012';
      }

      return res;
    },
    saveAsJsonTemplate() {
      !this.isTemplateChange && this.$emit('saveAsJson');
    },
    saveAsApiTemplate() {
      !this.isTemplateChange && this.$emit('saveAsApi');
    },
    handleGetDefaultTemplate() {
      confirmMsg(
        this.$t('操作提示'),
        this.$t('拉取默认模板提示'),
        () => this.$emit('GetDefaultTpl'),
        () => ({}),
        { theme: 'warning' }
      );
    },
    handlerItemHover(item, index, hover) {
      this.isHighlightChange = true;
      item[`tooltip${index}`] = hover;
      this.$parent.$nextTick(() => {
        this.isHighlightChange = false;
      });
    },
    toHelpWord() {
      window.open(this.$store.getters['docs/getPaths'].cleanRule);
    },
    getHasRepeat() {
      return this.hasRepeat;
    },
    /**
     * 生成推荐信息校验存放缓存
     */
    saveRecommendedCache() {
      let cache = {
        msg: this.latestMsg,
        data: [],
      };
      for (let item of this.cleanRuleList) {
        cache.data.push({
          dataInput: item.dataInput,
          currentComponent: item.currentComponent,
          label: item.label,
          operator: item.operator,
          operatorParam: item.operatorParam,
        });
      }
      return JSON.parse(JSON.stringify(cache));
    },
    checkRecommendedCache() {
      if (this.recommendCache) {
        return JSON.stringify(this.saveRecommendedCache()) === JSON.stringify(this.recommendCache);
      } else {
        return false;
      }
    },
    async refreshRecommendedParamWhileSelecting(...v) {
      if (v[0]) {
        this.error['cleanRuleList#' + v[1] + '#dataInput'] = '';
        await this.getRecommendedParam(v[1]);
      }
    },
    /**
     * 获取推荐算子
     */
    async getRecommendedParam(clickIndex) {
      let connectedList;
      if (clickIndex === undefined || clickIndex === null) {
        connectedList = this.cleanRuleList.filter((clean, index) => {
          Vue.set(this.cleanRuleList[index], 'loadingDataInput', true);
          return this.checkCleanRule(index);
        });
      } else {
        Vue.set(this.cleanRuleList[clickIndex], 'loadingDataInput', true);
        let wrapper = this.checkSingleConnectedList(clickIndex, true);
        if (wrapper.result) {
          connectedList = wrapper.content;
        } else {
          Vue.set(this.cleanRuleList[clickIndex], 'loadingDataInput', false);
          return;
        }
      }
      let param = {
        msg: this.latestMsg,
        conf: this.getparams(connectedList),
      };
      // 校验缓存
      if (!this.checkRecommendedCache(param)) {
        await this.$store.dispatch('api/recommendationParam', param).then(resp => {
          if (resp.result) {
            for (let cleanRule in resp.data) {
              let keyIndexs = [];
              for (let i = 0; i < this.cleanRuleList.length; i++) {
                if (this.cleanRuleList[i].dataInput === cleanRule) {
                  keyIndexs.push(i);
                }
              }
              for (let keyIndex of keyIndexs) {
                this.cleanRuleList[keyIndex].recommendList = [];
                let recommendation = {
                  name: this.$t('推荐'),
                  children: [],
                };
                let other = {
                  name: this.$t('其他'),
                  children: [],
                };
                let recommendIdList = [];
                for (let item of resp.data[cleanRule]) {
                  let temp = copyObj(
                    this.cleaningOperator.list.filter(o => {
                      return o.id === item.calc_id;
                    })[0]
                  );
                  !recommendIdList.includes(temp.id) && recommendIdList.push(temp.id);
                  if (item.calc_id === 'assign') {
                    for (let f = 0; f < item.calc_params.fields.length; f++) {
                      !item.calc_params.fields[f].id && Vue.set(item.calc_params.fields[f], 'id', generateId('field'));
                      Vue.set(item.calc_params.fields[f], 'highlight', false);
                    }
                  }
                  Vue.set(temp, 'calc_param', item.calc_params);
                  !recommendation.children.some(child => child.id === temp.id) && recommendation.children.push(temp);
                }
                other.children = this.cleaningOperator.list.filter(o => {
                  return !recommendIdList.includes(o.id);
                });
                this.cleanRuleList[keyIndex].recommendList.push(recommendation);
                this.cleanRuleList[keyIndex].recommendList.push(other);
              }
            }
            this.recommendCache = this.saveRecommendedCache();
          }
        });
      }
      if (clickIndex === undefined || clickIndex === null) {
        for (let i = 0; i < this.cleanRuleList.length; i++) {
          Vue.set(this.cleanRuleList[i], 'loadingDataInput', false);
        }
      } else {
        Vue.set(this.cleanRuleList[clickIndex], 'loadingDataInput', false);
      }

      this.$nextTick(() => {
        // cleanRuleList改变后，字段初始化完成
        this.isFieldsDataInit = true;
      });
    },
    /**
     * 校验单个清洗规则
     * @param index cleanRuleList对应的索引
     */
    checkCleanRule(index) {
      let cleanRule = this.cleanRuleList[index];
      let ret = true;
      if (!this.checkItemInput(index, cleanRule.dataInput)) {
        ret = false;
      }
      if (!this.checkItemOperator(index, cleanRule.operator)) {
        ret = false;
      }
      for (let calc of this.$refs.calcComp) {
        if (calc.$attrs.label === cleanRule.label) {
          if (!calc.check()) {
            ret = false;
          }
          break;
        }
      }
      if (cleanRule.dataInput === '' || !cleanRule.dataInput) {
        return false;
      }
      if (!cleanRule.currentComponent) {
        return false;
      }
      if (!cleanRule.operatorParam) {
        return false;
      }
      return ret;
    },
    initByConf(conf) {
      if (conf.length === 0) {
        this.cleanRuleList = [];
        this.addRule();
      }
    },
    /*
     * 回填参数
     */
    backfill(conf, fields) {
      this.$set(this, 'cleanRuleList', []);
      this.$nextTick(() => {
        let list = [];
        conf.forEach(item => {
          if (item.calc_id === 'assign') {
            if (Array.isArray(item.calc_params.fields)) {
              for (let i = 0; i < item.calc_params.fields.length; i++) {
                if (item.calc_params.fields[i].id === undefined || item.calc_params.fields[i].id === null) {
                  Vue.set(item.calc_params.fields[i], 'id', generateId('field'));
                  Vue.set(item.calc_params.fields[i], 'highlight', false);
                }

                if (fields && !item.calc_params.fields[i]['field_alias']) {
                  const field = fields.find(f => f.field_name === item.calc_params.fields[i]['assign_to']);
                  field && Vue.set(item.calc_params.fields[i], 'field_alias', field.field_alias);
                }
              }
              if (item.calc_params.fields.length <= 0) {
                let tempField;
                switch (item.calc_params.assign_method) {
                  case 'index':
                  case 'key':
                    tempField = {
                      id: generateId('field'),
                      assign_to: '',
                      location: '',
                      type: '',
                      highlight: false,
                    };
                    break;
                  case 'direct':
                  default:
                    tempField = {
                      id: generateId('field'),
                      assign_to: '',
                      type: '',
                      highlight: false,
                    };
                    break;
                }
                item.calc_params.fields.push(tempField);
              }
            } else if (typeof item.calc_params.fields === 'object' && item.calc_params.fields) {
              if (item.calc_params.fields.id === undefined || item.calc_params.fields.id === null) {
                Vue.set(item.calc_params.fields, 'id', generateId('field'));
                Vue.set(item.calc_params.fields, 'highlight', false);
              }

              if (fields && !item.calc_params.fields['field_alias']) {
                const field = fields.find(f => f.field_name === item.calc_params.fields['assign_to']);
                field && Vue.set(item.calc_params.fields, 'field_alias', field.field_alias);
              }
            }
          }
          list.push({
            dataInput: item.input,
            operator: item.calc_id,
            operatorParam: item.calc_params,
            label: item.label,
            debugError: 'init',
            errorContent: '',
            recommendList: null,
            tooltip1: false,
            tooltip2: false,
            tooltip3: false,
            tooltip4: false,
          });
        });
        // this.cleanRuleList = Object.assign([], list)
        this.$set(this, 'cleanRuleList', list);
        this.$nextTick(() => {
          this.initRecommend('backfill');
        });
      });
    },
    /*
     * 改变算子参数
     */
    changParams(data, index) {
      this.cleanRuleList[index].operatorParam = data;
    },
    rulesFocus(index) {
      this.focusNum = index;
    },
    /*
     * 关闭提示
     */
    closeTip(e) {
      if (e && e.target) {
        if (e.target.closest('.tip-content') === null) {
          this.tip.isShow = false;
        }
      } else {
        this.tip.isShow = false;
      }
    },
    showLineError(index, content, isTrShow, e) {
      if (isTrShow) {
        if (!this.tip.isShow) {
          if (
            !Array.prototype.some.call(
              e.target.classList,
              c => /param-select|bk-select-name|bkdata-combobox-input|bk-icon-button|icon-plus|icon-debug/.test(c)
            )
          ) {
            this.tip.isShow = true;
          }
        }
      } else {
        if (!this.tip.isShow) {
          this.tip.isShow = true;
        }
      }
      // let parentDom = this.$refs['operating' + index][0].parentElement
      // this.infoTip.dom3 = parentDom.querySelectorAll('.operator-param')
      // this.nowErrorShow = true
      // this.nowErrorContent = content
    },
    setItemLoading(maxMaxindex) {
      if (maxMaxindex) {
        for (let i = 0; i < maxMaxindex; i++) {
          Vue.set(this.cleanRuleList[i], 'loading', true);
        }
      }
    },
    setItemUnLoading(maxMaxindex) {
      if (maxMaxindex) {
        for (let i = 0; i < maxMaxindex; i++) {
          Vue.set(this.cleanRuleList[i], 'loading', false);
        }
      }
    },
    debug(item, index, type) {
      if (type !== 'whole' && index === this.cleanRuleListAddComponent.length - 1) {
        this.wholeDebug();
      } else {
        this.nowErrorShow = false;
        this.$emit('notifyHideResultPreview');
        if (type !== 'whole') {
          this.tip.debugIndex = index;
        }
        if (this.tip.isShow) {
          this.tip.isShow = false;
        }

        if (this.latestMsg.trim() === '') {
          showMsg(this.$t('请输入一条样例数据用于调试'), 'warning');
          this.setItemUnLoading(index || this.cleanRuleList.length);
          this.$emit('syncCleanResult', [], false);
          return false;
        }

        for (let i = 0; i < this.cleanRuleList.length; i++) {
          Vue.set(this.cleanRuleList[i], 'debugError', 'init');
          Vue.set(this.cleanRuleList[i], 'errorContent', '');
        }

        let cleanRule;
        if (type === 'whole') {
          if (!this.check()) {
            this.$emit('syncCleanResult', [], false);
            this.setItemUnLoading(index || this.cleanRuleList.length);
            return false;
          }
          cleanRule = this.getparams();
        } else {
          let wrapper = this.checkSingleConnectedList(index);
          if (!wrapper.result) {
            return false;
          }
          cleanRule = this.getparams(wrapper.content);
          Vue.set(item, 'loading', true);
        }

        let params = {
          msg: this.latestMsg,
          conf: cleanRule,
          debug_by_step: type !== 'whole',
        };
        this.axios
          .post('etls/debug/', params)
          .then(res => {
            if (res.result) {
              this.showTip(res.data, index, cleanRule, type);
            } else {
              showMsg(res.message, 'error');
            }
            if (type !== 'whole') {
              Vue.set(item, 'loading', false);
            } else {
              this.setItemUnLoading(index || this.cleanRuleList.length);
              this.$emit('syncCleanResult', [], false);
            }
          })
          ['catch'](err => {
            this.$emit('syncCleanResult', [], false);
            postMethodWarning(err, 'error');
          });
      }
    },
    wholeDebug() {
      this.setItemLoading(this.cleanRuleList.length);
      this.debug(null, null, 'whole');
    },
    /**
     * 校验assign的fields是否存在重复
     */
    checkRepeatAssign() {
      if (this.cleanRuleList.length <= 0) {
        return;
      }
      let tempObj = {};
      // 第一层
      for (let i = 0; i < this.cleanRuleList.length; i++) {
        if (!this.isValidAssignFields(this.cleanRuleList[i])) {
          continue;
        }
        if (tempObj[i] === undefined) {
          tempObj[i] = new Set();
        }
        for (let j = 0; j < this.cleanRuleList[i].operatorParam.fields.length; j++) {
          //    第二层
          for (let k = 0; k < this.cleanRuleList.length; k++) {
            if (!this.isValidAssignFields(this.cleanRuleList[k])) {
              continue;
            }
            for (let m = 0; m < this.cleanRuleList[k].operatorParam.fields.length; m++) {
              let sourceField = this.cleanRuleList[k].operatorParam.fields[m].assign_to;
              let targetField = this.cleanRuleList[i].operatorParam.fields[j].assign_to;
              if (!(i === k && m === j) && sourceField === targetField && sourceField !== '') {
                tempObj[i].add(j);
              }
            }
          }
        }
      }
      let tempHasRepeat = false;
      for (let item in tempObj) {
        let s = Array.from(tempObj[item]);
        if (s.length > 0) {
          tempHasRepeat = true;
        }
        if (this.$refs.calcComp) {
          for (let calc of this.$refs.calcComp) {
            if (calc.$attrs.label === this.cleanRuleList[item].label) {
              // todo
              calc.showErrorMessage(s);
              // console.warn(`字段:${calc.$attrs.label} 重复出现`)
              break;
            }
          }
        }
      }
      this.hasRepeat = tempHasRepeat;
    },
    isValidAssignFields(cleanRule) {
      return cleanRule.operator === 'assign' && cleanRule.operatorParam && cleanRule.operatorParam.fields;
    },
    /**
     * 通过生成的 label 来获取某一步骤的清洗配置
     */
    getCleanRuleByLabel(label) {
      let targetRule = null;
      this.cleanRuleList.forEach(rule => {
        if (rule.label === label) {
          targetRule = rule;
        }
      });
      return targetRule;
    },
    getLabelIndex() {
      let mLabelIndex = {};
      this.cleanRuleList.forEach((rule, index) => {
        mLabelIndex[rule.label] = index;
      });
      return mLabelIndex;
    },
    showTip(debugData, index, cleanRule, type) {
      if (type === 'whole') {
        const mLabelIndex = this.getLabelIndex();
        const errors = Object.entries(debugData.errors);
        if (errors[0] && errors[0][0]) {
          index = mLabelIndex[errors[0][0]];
        }
      }
      if (index || index === 0 || type !== 'whole') {
        let parentDom = this.$refs['operating' + index][0].parentElement;
        this.infoTip.dom1 = parentDom.querySelectorAll('.data-input');
        this.infoTip.dom2 = parentDom.querySelectorAll('.operator-param');

        let currentRule = this.cleanRuleList[index];

        // 显示数据输入
        if (currentRule.dataInput === 'root') {
          this.tip.inputName = this.$t('原始数据');
          this.tip.inputData = this.latestMsg;
        } else {
          let _inputRule = this.getCleanRuleByLabel(currentRule.dataInput);
          this.tip.inputName = _inputRule.operatorParam.result;
          this.tip.inputData = JSON.stringify(debugData.nodes[_inputRule.label]);
        }

        // 显示数据输出
        let nodeData = debugData.nodes[currentRule.label];
        if (currentRule.operator === 'assign') {
          this.tip.resultName = null;
          this.tip.tipType = 'table';
          if (nodeData && !(nodeData instanceof Array)) {
            nodeData = [nodeData];
          }
          let nodeTitles = [];
          if (nodeData) {
            for (let title in nodeData[0]) {
              nodeTitles.push(title);
            }
          }
          this.tip.titles = nodeTitles;
          this.tip.result = nodeData;
          Vue.set(this.tip, 'assign_method', currentRule.operatorParam.assign_method);
        } else if (currentRule.operator === 'iterate') {
          this.tip.resultName = currentRule.operatorParam.result;
          this.tip.tipType = 'iterate';
          let tempNode = [];
          if (nodeData) {
            for (let node of nodeData) {
              tempNode.push(JSON.stringify(node));
            }
          }

          this.tip.result = tempNode;
        } else {
          this.tip.resultName = currentRule.operatorParam.result;
          this.tip.tipType = 'paragraph';
          this.tip.result = JSON.stringify(nodeData);
        }

        // 显示错误信息
        let mLabelIndex = this.getLabelIndex();
        this.tip.error = [];
        for (let [k, v] of Object.entries(debugData.errors)) {
          this.tip.error.push([mLabelIndex[k], v]);
        }
      }
      let mLabelIndex = this.getLabelIndex();
      let tempError = [];
      for (let [k, v] of Object.entries(debugData.errors)) {
        tempError.push([mLabelIndex[k], v]);
        for (let i = 0; i < this.cleanRuleList.length; i++) {
          if (this.cleanRuleList[i].label === k) {
            Vue.set(this.cleanRuleList[i], 'debugError', 'failure');
            Vue.set(this.cleanRuleList[i], 'errorContent', v);
            break;
          }
        }
      }
      for (let rule of cleanRule) {
        for (let i = 0; i < this.cleanRuleList.length; i++) {
          if (this.cleanRuleList[i].label === rule.label && this.cleanRuleList[i].debugError === 'init') {
            Vue.set(this.cleanRuleList[i], 'debugError', 'success');
            Vue.set(this.cleanRuleList[i], 'errorContent', '');
            break;
          }
        }
      }
      debugData.errors = tempError;

      // 显示最终清洗出来的全部字段
      if (type === 'whole') {
        this.tip.isShow = debugData.errors && debugData.errors.length;
        this.$emit('syncCleanResult', debugData, true);
      } else {
        this.tip.isShow = true;
      }
    },
    /*
     * 点击添加规则
     */
    addRule(data, index) {
      this.showCleanRule = false;
      let dataInput = this.cleanRuleList.length === 0 ? 'root' : '';
      if (this.isValidMiddleResult(data)) {
        dataInput = data.label;
      }
      let val = {
        dataInput: dataInput,
        operator: '',
        currentComponent: '',
        operatorParam: null,
        label: generateId('label'),
        debugError: 'init',
        errorContent: '',
        tooltip1: false,
        tooltip2: false,
        tooltip3: false,
        tooltip4: false,
        recommendList: null,
      };
      this.cleanRuleList.splice(index, 0, val); // (索引位置, 要删除元素的数量, 元素)
    },
    isValidMiddleResult(data) {
      return data && data.operator !== 'assign' && data.operatorParam && data.operatorParam.result;
    },
    /*
     * 点击删除规则
     */
    deleteRule(item, index) {
      this.showCleanRule = false;
      let temp = this.cleanRuleList;
      this.cleanRuleList = [];
      this.cleanRuleList = temp;
      // Bus.$emit('Delete-Clean-Rule', item)

      if (this.cleanRuleList.length > 1) {
        this.cleanRuleList.splice(index, 1);
        delete this.error[`cleanRuleList#${index}#dataInput`];
        delete this.error[`cleanRuleList#${index}#operator`];
      }
      // 清空输入
      for (let leftItem of this.cleanRuleList) {
        if (leftItem.dataInput === item.label) {
          leftItem.dataInput = '';
        }
      }
      let self = this;
      Vue.nextTick().then(function () {
        self.showCleanRule = true;
      });
    },
    getparams(connectedList) {
      if (connectedList === null || connectedList === undefined) {
        connectedList = this.cleanRuleList;
      }
      let list = [];
      connectedList.forEach((rItem, index) => {
        let item = JSON.parse(JSON.stringify(rItem));
        if (item.operatorParam && item.operatorParam.assign_method === 'direct') {
          item.operatorParam.fields = {
            assign_to: item.operatorParam.fields[0].assign_to,
            type: item.operatorParam.fields[0].type,
          };
        }
        list.push({
          input: item.dataInput,
          calc_id: item.operator,
          calc_params: item.operatorParam,
          label: item.label,
        });
      });
      return list;
    },
    check() {
      let ret = true;
      // 基本参数校验
      this.cleanRuleList.forEach((item, index) => {
        if (!this.checkItemInput(index, item.dataInput)) {
          ret = false;
        }
        if (!this.checkItemOperator(index, item.operator)) {
          ret = false;
        }
      });

      // 算子组件参数校验
      if (this.$refs.calcComp) {
        this.$refs.calcComp.forEach(comp => {
          if (!comp.check()) {
            ret = false;
          }
        });
      }
      for (let i = 0; i < this.cleanRuleList.length; i++) {
        let connectedItem = this.cleanRuleList[i];
        let appearTimes = this.countCleanRuleRepeatTimes(connectedItem);
        if (appearTimes > 1) {
          showMsg(this.$t('中间结果不可重复: ' + this.getResultByLabel(connectedItem.dataInput)), 'warning');
          return false;
        }
      }
      return ret;
    },
    /**
     * 校验生成的debug链
     * @param connectedList 生成的debug链
     * @param ignoreEnd 是否忽略最后一个
     */
    checkConnectedList(connectedList, ignoreEnd) {
      if (!connectedList) {
        return false;
      }
      if (connectedList.length <= 0) {
        return false;
      }
      let labelIndexs = this.getLabelIndex();
      for (let i = 0; i < connectedList.length; i++) {
        if (ignoreEnd && i === 0) {
          continue;
        }
        if (!this.checkCleanRule(labelIndexs[connectedList[i].label])) {
          return false;
        }
      }
      if (connectedList[connectedList.length - 1].dataInput !== 'root') {
        return false;
      }
      for (let i = 0; i < connectedList.length; i++) {
        let connectedItem = connectedList[i];
        if (ignoreEnd && i === 0) {
          continue;
        }
        // 判断是否不存在或者存在多于1项的中间结果
        let appearTimes = this.countCleanRuleRepeatTimes(connectedItem);
        if (appearTimes > 1) {
          showMsg(this.$t('中间结果不可重复: ' + this.getResultByLabel(connectedItem.dataInput)), 'warning');
          return false;
        }
      }
      return true;
    },
    /**
     * 计算链路中中间结果元素出现的次数
     */
    countCleanRuleRepeatTimes(connectedItem) {
      // 判断是否不存在或者存在多于1项的中间结果
      let appearTimes = 0;
      for (let cleanRule of this.cleanRuleList) {
        if (connectedItem.dataInput === 'root' || connectedItem.dataInput === '') {
          appearTimes = 1;
          continue;
        } else if (cleanRule.label !== connectedItem.label && cleanRule.operatorParam && connectedItem.operatorParam) {
          if (this.getResultByLabel(connectedItem.dataInput) === cleanRule.operatorParam.result) {
            appearTimes += 1;
          }
        }
      }
      return appearTimes;
    },
    getResultByLabel(label) {
      for (let clean of this.cleanRuleList) {
        if (clean.label === label && clean.operatorParam) {
          return clean.operatorParam.result;
        }
      }
      return undefined;
    },
    /**
     * 根据当前所选的index生成对应的debug链
     * @param index
     * @returns {Array}
     */
    generateConnectedList(index) {
      let connectedList = [];
      let signList = [];

      connectedList.push(this.cleanRuleList[index]);
      signList.push(this.cleanRuleList[index].label);

      // 参考广度优先搜索的做法，将子项丢进列表中后在拿出来寻找子项
      while (connectedList[connectedList.length - 1].dataInput !== 'root') {
        let midAppearTimes = 0;
        let midAppearIndex = -1;
        let inputRule = this.getCleanRuleByLabel(connectedList[connectedList.length - 1].dataInput);
        if (!inputRule) {
          return connectedList;
        }
        let op = inputRule.operatorParam;
        if (!op) {
          return connectedList;
        }
        let midResult = op.result;
        for (let i = 0; i < this.cleanRuleList.length; i++) {
          if (
            !signList.includes(this.cleanRuleList[i].label)
            && this.cleanRuleList[i].operatorParam
            && midResult === this.cleanRuleList[i].operatorParam.result
          ) {
            midAppearTimes += 1;
            midAppearIndex = i;
            break;
          }
        }
        if (midAppearTimes >= 1) {
          connectedList.push(this.cleanRuleList[midAppearIndex]);
          signList.push(this.cleanRuleList[midAppearIndex].label);
        } else {
          return connectedList;
        }
      }
      return connectedList;
    },
    checkSingleConnectedList(index, ignoreEnd) {
      let connectedList = this.generateConnectedList(index);
      let result = this.checkConnectedList(connectedList, ignoreEnd);
      if (result && ignoreEnd) {
        connectedList.splice(0, 1);
      }
      return {
        result: result,
        content: connectedList,
      };
    },
    checkItemInput(index, value) {
      let errKey = `cleanRuleList#${index}#dataInput`;
      this.$set(this.error, errKey, '');

      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(value)) {
        this.$set(this.error, errKey, validator.errMsg);
        return false;
      }
      return true;
    },
    checkItemOperator(index, value) {
      let errKey = `cleanRuleList#${index}#operator`;
      this.$set(this.error, errKey, '');

      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(value)) {
        this.$set(this.error, errKey, validator.errMsg);
        return false;
      }
      return true;
    },
    async changeItemOperator(index, value) {
      if (value !== this.cleanRuleList[index].operator) {
        this.cleanRuleList[index].operatorParam = null; // 清空operatorParam

        /** 获取动态组件，并reset参数 */
        const currentComp = this.cleaningOperator.list.find(item => item.id === value);
        this.$nextTick(() => {
          const el = (this.$refs.calcComp || []).filter(item => item.$attrs.refId === index);
          el.length && el[0].resetParams(currentComp.component);
        });

        if (this.cleanRuleList[index].recommendList) {
          for (let re of this.cleanRuleList[index].recommendList) {
            if (re.name === this.$t('推荐')) {
              for (let child of re.children) {
                if (child.id === value) {
                  let param = JSON.parse(JSON.stringify(child.calc_param));
                  if (child.id === 'assign') {
                    let result = [];
                    for (let field of param.fields instanceof Array ? param.fields : [param.fields]) {
                      if (!this.checkFieldHasRepeated(field.assign_to)) {
                        result.push(field);
                      }
                    }
                    param.fields = JSON.parse(JSON.stringify(result));
                  }
                  this.cleanRuleList[index].operatorParam = param;
                  break;
                }
              }
              break;
            }
          }
        }
        this.checkItemOperator(index, value);
      }
      this.cleanRuleList[index].operator = value;
    },
    /**
     * 校验assign的field的assign_to有没有重复
     * @param assignTo
     * @returns {boolean}
     */
    checkFieldHasRepeated(assignTo) {
      for (let cleanRule of this.cleanRuleList) {
        if (cleanRule.operator === 'assign' && cleanRule.operatorParam.fields) {
          for (let field of cleanRule.operatorParam.fields instanceof Array
            ? cleanRule.operatorParam.fields
            : [cleanRule.operatorParam.fields]) {
            if (assignTo === field.assign_to) {
              return true;
            }
          }
        }
      }
      return false;
    },
    dynamicInputList(index) {
      let results = [];
      for (let i = 0; i < this.cleanRuleList.length; i++) {
        let hasResult = false;
        if (i === index) {
          hasResult = true;
          break;
        }
        for (let j = 0; j < results.length; j++) {
          if (
            !this.cleanRuleList[i].operatorParam
            || results[j].name === this.cleanRuleList[i].operatorParam.result
            || this.cleanRuleList[i].operatorParam.assign_method === 'direct'
          ) {
            hasResult = true;
            break;
          }
        }
        if (!hasResult && this.cleanRuleList[i].operatorParam) {
          results.push({
            id: this.cleanRuleList[i].label,
            name: this.cleanRuleList[i].operatorParam.result || this.cleanRuleList[i].label,
          });
        }
      }
      return results;
    },
    beforeunloadFn(e) {
      const dialogText = '离开后，新编辑的数据将丢失';
      e.returnValue = dialogText;
      return dialogText;
    },
    changeHighlight(fieldId) {
      this.isHighlightChange = true;
      for (let i = 0; i < this.cleanRuleList.length; i++) {
        if (this.cleanRuleList[i].operatorParam && this.cleanRuleList[i].operatorParam.fields) {
          for (let j = 0; j < this.cleanRuleList[i].operatorParam.fields.length; j++) {
            if (this.cleanRuleList[i].operatorParam.fields[j].id === fieldId) {
              this.$refs.calcComp[i].changeHighlight(fieldId, true);
              break;
            }
          }
        }
      }
    },
    initRecommend(value) {
      this.initFinished[value] = true;
      if (this.initFinished.backfill && this.initFinished.latestMsg) {
        this.getRecommendedParam();
      }
    },
    initDataIdSet() {
      this.$store.dispatch('api/requestDataIdSet').then(resp => {
        if (resp.result) {
          this.dataIdSet = resp.data.filter(res => !/float/i.test(res.field_type));
        }
      });
    },
  },
};
</script>
<style lang="scss" scoped>
@import './scss/cleanRules.scss';
</style>
<style media="screen" lang="scss">
.tool-button {
  .tooltip {
    display: none;
    position: absolute;
    bottom: 30px;
    /*left: 50%;*/
    width: 100px;
    min-width: 40px;
    transform: translate(-50%, 0);
    background: #212232;
    padding: 5px 5px;
    font-size: 12px;
    color: #fff;
    border-radius: 2px;
    word-break: break-all;
    z-index: 1;
    &:after {
      content: '';
      position: absolute;
      right: 50%;
      bottom: -10px;
      border: 5px solid;
      border-color: #212232 transparent transparent transparent;
    }
  }
  &:hover {
    .tooltip {
      display: block;
    }
  }
}
.clean-rules {
  position: relative;
  margin-top: 20px;
  border: 1px solid #c3cdd7;
  box-shadow: 2px 4px 5px rgba(33, 34, 50, 0.15);
  background: #fff;
  .header {
    padding: 15px 20px;

    span.collspan {
      cursor: pointer;
      .collspan-icon {
        display: inline-block;
        height: 20px;
        line-height: 20px;
        font-size: 26px;
      }
    }

    .title {
      cursor: pointer;
      line-height: 20px;
      font-weight: bold;
      padding-left: 18px;
      position: relative;
      display: inline-block;
      margin-right: 15px;
      &:before {
        content: '';
        width: 4px;
        height: 20px;
        position: absolute;
        left: 0px;
        top: 50%;
        background: #3a84ff;
        transform: translate(0%, -50%);
      }
    }
    .guide {
      color: #3a84ff;
      cursor: pointer;
      .bk-icon {
        vertical-align: middle;
      }
    }
    .bk-dropdown-menu .bk-dropdown-content {
      top: 33px !important;
    }

    .btn-default-template {
      cursor: pointer;
      // border: solid 1px #ddd;
      // border-radius: 4px;
      padding: 0 5px;
      // background: #ddd;
      margin-right: 15px;
      color: #3a84ff;
      font-size: 14px;

      &.readonly {
        cursor: not-allowed;
        color: #c3cdd7;
      }
    }
  }
  > .bk-table {
    .line {
      position: relative;
      &:before {
        content: '';
        width: 1px;
        height: 24px;
        background: #dbe1e7;
        position: absolute;
        right: -11px;
        top: 50%;
        transform: translate(0, -50%);
      }
    }
    .table-select {
      width: 146px;
    }
    .operator-td {
      .component-wrapper {
        min-width: 700px;
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        .success {
          color: #30d878;
        }
        .failure {
          color: #ff5656;
        }
      }
      input {
        display: block;
        /* border: none; */
      }
    }
    .component-wrapper {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }
    .operator-param {
      .title {
        border: 1px solid #c3cdd7;
        padding: 9px 10px;
        line-height: 12px;
        background: #fff;
      }
      .param-select {
        min-width: 90px;
        margin-right: -1px;
        .param-title {
          padding: 0 10px;
        }
        input {
          background: #fafafa;
        }
      }
      .param-input {
        width: 130px;
      }
    }
    .delimiter {
      width: 180px;
      .delimiter-title {
        border: 1px solid #c3cdd7;
        padding: 0 9px;
        width: 90px;
        background: #fafafa;
        line-height: 30px;
        margin-right: -1px;
      }
      .delimiter-select {
        width: calc(100% - 90px);
      }
    }
    .delimiter-key {
      width: 180px;
      .delimiter-title {
        width: 90px;
        background: #fafafa;
        margin-right: -1px;
      }
      .delimiter-select {
        width: calc(100% - 90px);
        /* margin-left: -1px; */
      }
    }
    .replace {
      .replace-title {
        padding: 0 9px;
        border: 1px solid #c3cdd7;
        margin-left: -1px;
        background: #fafafa;
        line-height: 30px;
      }
      .replace-input {
        width: 85px;
        margin-left: -1px;
      }
    }
    .cleaning-operator {
      .bkdata-selector-input {
        background-color: #f4f2f2;
      }
    }
    .data-input {
      .data-input-selector-disabled {
        .bkdata-selector-icon {
          display: none;
        }
      }
    }
  }
  .tip-content {
    line-height: 18px;
    font-size: 12px;
    .content-wrapper {
      width: 100%;
      max-height: 200px;
      overflow: auto;
    }
    .title {
      margin-top: 15px;
      font-size: 14px;
      font-weight: bold;
      color: #737987;
      margin-bottom: 5px;
    }
    &.warning {
      color: #ff5656;
      .title {
        color: #ff5656;
      }
    }
    .tip-table {
      width: 100%;
      overflow: auto;
    }
    .bk-table {
      margin-top: 10px;
      th {
        background: #efefef;
      }
      td {
        word-break: break-all;
      }
      caption {
        text-align: left;
        color: #ffb400;
      }
    }
    p {
      word-break: break-all;
    }
  }
}
</style>
