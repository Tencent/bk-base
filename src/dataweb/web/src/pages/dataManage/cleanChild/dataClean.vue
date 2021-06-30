

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
  <Layout :crumbName="navis">
    <div v-bkloading="{ isLoading: loading.page }"
      :class="{ 'data-clean': true, 'without-sidebar': !isSideBarShow }">
      <div class="clean-content">
        <div class="raw-data"
          :class="{ expand: isLastMsgExpand }">
          <div v-bkloading="{ isLoading: loading.cleanRule }"
            class="raw-data-left">
            <p class="title">
              <i :title="$t('原始数据')"
                class="bk-icon icon-input" />
              {{ $t('原始数据') }}
            </p>
            <p>{{ `${$t('数据ID')}: ${calcRawDataId}` }}</p>
            <p v-bk-tooltips="{ content: diaplayName }"
              class="raw-data-names">
              {{ diaplayName }}
            </p>
          </div>
          <div class="raw-data-right fl">
            <i class="triangle-left" />
            <textarea
              ref="textArea"
              v-model="latestMsg"
              v-tooltip.notrigger="latestMsgValidate"
              :placeholder="latestPlacehoder"
              class="text-area bk-scroll-y"
              @input="validateLastMsg" />
            <i
              class="triangle-right-bottom bk-icon"
              :class="{
                'icon-angle-double-down': !isLastMsgExpand,
                'icon-angle-double-up': isLastMsgExpand,
              }"
              @click.stop="isLastMsgExpand = !isLastMsgExpand" />
          </div>
        </div>
        <v-clean-rules
          ref="cleanRules"
          v-bkloading="{ isLoading: loading.cleanRule || loading.loadMsg }"
          :conf="cleanInfo.conf"
          :loading="loading.cleanRule"
          :expand="!isLastMsgExpand"
          :latestMsg="latestMsg"
          :isReadonly="isSidebarReadonly"
          @GetDefaultTpl="handleGetDefaultTpl"
          @notifyHideResultPreview="notifyHideResultPreview"
          @syncCleanResult="syncCleanResult"
          @cleanFieldsChanged="cleanFieldsChanged"
          @saveAsJson="saveTemplate('json')"
          @saveAsApi="saveTemplate('api')"
          @importJsonTemplate="importJsonTemplate" />
        <div class="result-preview">
          <div class="header">
            <p class="title">
              {{ $t('清洗结果预览') }}
            </p>
            <div class="result-preview-btn">
              <span
                v-if="cleanResult.resultPreview"
                :class="{
                  'debug-result': true,
                  success: !isGlobalDebugFailed,
                  failed: isGlobalDebugFailed,
                }">
                {{ globalDebugResult }}
              </span>
              <bkdata-button
                :loading="resultLoading || loading.cleanRule || loading.loadMsg"
                theme="primary"
                @click="emitWholeDebug">
                {{ $t('全局调试') }}
              </bkdata-button>
            </div>
          </div>
          <div v-if="cleanResult.resultPreview"
            v-bkloading="{ isLoading: resultLoading }"
            class="result-table">
            <bkdata-table style="margin-top: 15px"
              :emptyText="$t('暂无数据')"
              :data="cleanResult.tableList">
              <template v-for="(item, index) in cleanResult.tableHead">
                <bkdata-table-column :key="index"
                  :label="item">
                  <!-- <div slot-scope="props">
                                        {{ props.row[index] }}
                                    </div> -->
                  <cell-click slot-scope="{ row }"
                    :html="false"
                    :content="row[index]" />
                </bkdata-table-column>
              </template>
            </bkdata-table>
          </div>
        </div>
        <!-- <div class="mtb-10 fr">
                    <span class="mr5 f14" v-bk-tooltips="$t('以数据字段内容做修正，从而提升数据质量')">{{ $t('数据修正') }}</span>
                    <bkdata-switcher
                        v-model="isOpenCorrect"
                    ></bkdata-switcher>
                </div>
                <DataCorrection class="clear-both" v-show="isOpenCorrect" />
                <ResultPreview class="clear-both" :btnText="$t('全局调试')" /> -->
      </div>
      <v-sidebar
        v-show="isSideBarShow"
        ref="cleanSide"
        :isLoading="loading.save"
        :referenceFormat="getResultToObj"
        :bizId="rawDetails.bk_biz_id"
        :isShow="isSideBarShow"
        :isReadonly="isSidebarReadonly"
        @notifyChangeHighlight="changeHighlight"
        @save="Save" />
    </div>
  </Layout>
</template>

<script>
import vCleanRules from '@/pages/dataManage/cleanChild/cleanRules';
import vSidebar from '@/pages/dataManage/cleanChild/cleanSidebar';
import { postMethodWarning, showMsg, generateId } from '@/common/js/util.js';
import Vue from 'vue';
import Layout from '../../../components/global/layout';
import cellClick from '@/components/cellClick/cellClick.vue';
// import DataCorrection from './DataCorrection.vue'
// import ResultPreview from './ResultPreview.vue'

export default {
  components: {
    Layout,
    vCleanRules,
    vSidebar,
    cellClick,
    // DataCorrection,
    // ResultPreview
  },
  beforeRouteLeave(to, from, next) {
    if (!this.isSubmitLeave) {
      console.log('data clean leave router');
      let res = confirm(window.gVue.$t('离开此页面_已经填写的数据将会丢失'));
      if (res) {
        next();
      } else {
        next(false);
        return;
      }
    }
    next();
  },
  data() {
    return {
      rawDetails: {},
      isLastMsgExpand: false,
      isSideBarShow: false,
      isSidebarReadonly: true,
      debugShow: false,
      rawDataId: '',
      loading: {
        loadMsg: false,
        page: false,
        save: false,
        cleanRule: false,
      },
      latestMsgValidate: {
        content: this.$t('请输入一条样例数据用于调试'),
        visible: false,
      },
      // 清洗结果预览
      cleanResult: {
        resultPreview: false, // 结果显示隐藏,
        tableHead: [],
        tableList: [],
        errors: [],
      },
      resultLoading: false,
      isSetCleanConfigBtnActive: false,
      // 最新数据控制变量
      latestMsg: '',
      isSubmitLeave: false,
      // 清洗配置信息
      cleanInfo: {
        name: '',
        dataId: 0,
        dataName: '-',
        dataScenario: '',
        dataAlias: '',
        totalDeploySummary: {
          failed: '-',
          running: '-',
          total: '-',
          success: '-',
          pending: '-',
        },
        // 具体的清洗配置
        conf: [],
      },
      focusStatus: false,
      // isOpenCorrect: false
    };
  },
  computed: {
    getResultToObj() {
      let obj = {};
      if (this.cleanResult.tableList && this.cleanResult.tableList.length) {
        this.cleanResult.tableHead.forEach((item, index) => {
          let key = item.split('(')[0];
          obj[key] = this.cleanResult.tableList[0][index];
        });
      }
      return obj;
    },
    globalDebugResult() {
      return (this.cleanResult.errors && this.cleanResult.errors.length && this.$t('调试失败')) || this.$t('调试成功');
    },
    isGlobalDebugFailed() {
      return this.cleanResult.errors && this.cleanResult.errors.length;
    },
    calcRawDataId() {
      return this.cleanInfo.dataId || this.$route.params.rawDataId;
    },
    diaplayName() {
      return `${this.cleanInfo.dataName}（${this.cleanInfo.dataAlias}）`;
    },
    navis() {
      return [
        { name: this.$t('数据源列表'), to: { name: 'data' } },
        {
          name: `${this.$t('数据详情')}(${this.calcRawDataId})`,
          to: { name: 'data_detail', params: { did: this.calcRawDataId } },
        },
        {
          name: this.$t('清洗配置列表'),
          to: { name: 'data_detail', params: { did: this.calcRawDataId, tabid: '3' } },
        },
        { name: !this.isEdit ? this.$t('新增清洗配置') : this.$route.params.rtid },
      ];
    },
    /**
     * 计算最新数据输入框 placeholder
     */
    latestPlacehoder() {
      if (this.latestMsg === '') {
        return this.$t('请输入一条样例数据用于调试');
      } else {
        return this.$t('加载中');
      }
    },
    isEdit() {
      return this.$route.name === 'edit_clean';
    },
    showDeploySummary() {
      return false; // ['log'].includes(this.cleanInfo.dataScenario)
    },
  },
  watch: {
    latestMsg(val) {
      this.isSidebarReadonly = true;
      console.log('latestMsg');
    },
    isSidebarReadonly(val) {
      !val && this.$refs.cleanRules.changeSaveTemplateStatus(false);
    },
  },
  mounted() {
    window.addEventListener('beforeunload', this.beforeunloadFn);
    this.initInfo();
  },
  beforeDestroy() {
    this.removeBeforunload();
  },
  methods: {
    saveTemplate(mode = 'json') {
      let setting = this.$refs.cleanSide.getParams();
      let cleanRule = this.$refs.cleanRules.getparams();
      let param = {};
      if (mode === 'json') {
        param = {
          start_clean: 1,
          raw_data_id: this.cleanInfo.dataId,
          result_table_name: setting.resultTable,
          result_table_name_alias: setting.cleanName,
          description: setting.description, // 页面没有参数 todo
          time_format: setting.timeFormat,
          timestamp_len: setting.timestamp_len,
          encoding:
            this.cleanInfo.encoding
            || (this.rawDetails && this.rawDetails.access_raw_data && this.rawDetails.access_raw_data.data_encoding)
            || '',
          timezone: setting.timeZone,
          time_field_name: setting.timePicked,
          fields: setting.fields,
          conf: cleanRule,
          clean_config_name: setting.clean_config_name,
          latestMsg: this.latestMsg,
        };
      } else {
        param = {
          bk_biz_id: this.rawDetails.bk_biz_id,
          bk_username: this.$store.getters.getUserName,
          clean_config_name: setting.clean_config_name,
          description: setting.description,
          fields: setting.fields,
          json_config: JSON.stringify(this.cleanInfo.rawConfig),
          raw_data_id: this.cleanInfo.dataId,
          result_table_name: setting.resultTable,
          result_table_name_alias: setting.cleanName,
        };
      }

      this.downloadTemplate(param, mode === 'json');
    },
    downloadTemplate(params, isJson) {
      function download(content, fileName, contentType) {
        if (typeof content === 'object') {
          content = JSON.stringify(content);
        }
        var a = document.createElement('a');
        var file = new Blob([content], { type: contentType });
        a.href = URL.createObjectURL(file);
        a.download = fileName;
        a.click();
      }
      download(
        isJson ? { template: params } : { ...params },
        `${params.raw_data_id}${params.result_table_name}.json`,
        'text/json'
      );
    },
    importJsonTemplate(res) {
      this.loading.save = true;
      const rid = this.$route.params.rtid;
      if (res.result) {
        this.cleanInfo.name = res.data.result_table_name_alias;
        // this.cleanInfo.dataId = res.data.raw_data_id
        this.cleanInfo.conf = res.data.conf;
        res = this.initCleanInfoFields(res);
        this.$refs.cleanSide.Backfill(res.data);
        this.latestMsg = res.data.latestMsg;
        if (res.data.conf.length > 0) {
          this.$refs.cleanRules.backfill(res.data.conf, res.data.fields);
        }
      } else {
        this.getMethodWarning(res.message, res.code);
      }
      this.loading.save = false;
    },

    async handleGetDefaultTpl() {
      this.loading.cleanRule = true;
      await this.requestTemplateInfo(this.cleanInfo.dataId);
      this.loading.cleanRule = false;
    },
    getRawdetail() {
      this.$store
        .dispatch('getDelpoyedList', {
          params: { raw_data_id: this.calcRawDataId },
          query: { show_display: 1 },
        })
        .then(res => {
          if (res.result) {
            this.rawDetails = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    cleanFieldsChanged(val, isHighlightChange) {
      !isHighlightChange && this.$set(this, 'isSidebarReadonly', true);
    },
    handleCleanBodyClick() {
      this.cleanResult.resultPreview = false;
      this.$refs.cleanRules.closeTip();
    },
    validateLastMsg(val) {
      this.latestMsgValidate.visible = val.length === 0;
    },
    collapse() {
      this.$refs.cleanSide.collapse();
    },
    showSideBar(readonly = false) {
      this.$nextTick(() => {
        this.isSideBarShow = true;
        this.$refs.cleanSide.showSideBar();
      });
      setTimeout(() => {
        this.isSidebarReadonly = readonly;
      }, 500);
    },
    emitWholeDebug() {
      this.isSetCleanConfigBtnActive = false;
      if (this.latestMsg.length === 0) {
        this.$refs.textArea.focus();
      }
      this.resultLoading = true;
      if (!this.latestMsg) {
        this.latestMsgValidate.visible = true;
      }
      this.$refs.cleanRules.wholeDebug();
    },
    changeHighlight(fieldId) {
      this.$refs.cleanRules.changeHighlight(fieldId);
    },
    /*
     * 保存
     */
    async Save() {
      let sideCheck = this.$refs.cleanSide.check();
      if (!sideCheck) {
        return false;
      }

      let ruleCheck = this.$refs.cleanRules.check();
      if (!ruleCheck) {
        return false;
      }

      let setting = this.$refs.cleanSide.getParams();
      let cleanRule = this.$refs.cleanRules.getparams();
      if (this.$refs.cleanRules.getHasRepeat()) {
        postMethodWarning('赋值字段设置重复，请检查赋值字段', 'error');
        return false;
      }
      let params = {
        start_clean: 1,
        raw_data_id: this.cleanInfo.dataId,
        result_table_name: setting.resultTable,
        result_table_name_alias: setting.cleanName,
        description: setting.description, // 页面没有参数 todo
        time_format: setting.timeFormat,
        timestamp_len: setting.timestamp_len,
        encoding:
          this.cleanInfo.encoding
          || (this.rawDetails && this.rawDetails.access_raw_data && this.rawDetails.access_raw_data.data_encoding)
          || '',
        timezone: setting.timeZone,
        time_field_name: setting.timePicked,
        fields: setting.fields,
        conf: cleanRule,
        clean_config_name: setting.clean_config_name,
      };
      this.loading.save = true;
      if (this.$route.name === 'create_clean') {
        this.createEtl(params);
      } else {
        this.updateEtl(params);
      }
    },
    updateEtl(params) {
      let payload = {
        rid: this.$route.params.rtid,
        params: params,
      };

      this.$store
        .dispatch('api/updateEtl', payload)
        .then(res => {
          if (res.result) {
            this.isSubmitLeave = true;
            this.debugShow = true;
            this.rawDataId = this.cleanInfo.dataId;
            this.$router.push({
              name: 'clean-success',
              params: {
                rawDataId: this.rawDataId,
                cleanExtraData: res.data.extra_data,
              },
            });
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.loading.save = false;
        });
    },
    async createEtl(params) {
      params.raw_data_id = this.calcRawDataId;
      await this.$store
        .dispatch('api/createEtl', params)
        .then(async res => {
          if (res.result) {
            this.isSubmitLeave = true;
            this.debugShow = true;
            this.rawDataId = this.calcRawDataId;
            this.$router.push({
              name: 'clean-success',
              params: {
                rawDataId: this.rawDataId,
              },
            });
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.loading.save = false;
        });
    },
    /*
     * 保存获取dataId,跳转清洗详情页面
     */
    async getCleanDetail() {
      this.loading.save = true;
      const rid = this.$route.params.rtid;
      await this.$store
        .dispatch('api/getEtl', { rid: rid })
        .then(res => {
          if (res.result) {
            this.cleanInfo.name = res.data.result_table_name_alias;
            this.cleanInfo.dataId = res.data.raw_data_id;
            this.cleanInfo.conf = res.data.conf;
            this.cleanInfo.rawConfig = res.data.raw_config;
            res = this.initCleanInfoFields(res);
            this.$refs.cleanSide.Backfill(res.data);
            if (res.data.conf.length > 0) {
              this.$refs.cleanRules.backfill(res.data.conf, res.data.fields);
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.save = false;
        });
    },
    /**
     * 初始化清洗内容中的assign项，把每项field加上id方便与cleanSide做数据绑定
     * @param res 请求返回的内容
     */
    initCleanInfoFields(res) {
      for (let i = 0; i < res.data.conf.length; i++) {
        if (res.data.conf[i].calc_id === 'assign') {
          if (res.data.conf[i].calc_params.assign_method === 'direct') {
            res.data.conf[i].calc_params.fields = [res.data.conf[i].calc_params.fields];
          }
          for (let j = 0; j < res.data.conf[i].calc_params.fields.length; j++) {
            let field = res.data.conf[i].calc_params.fields[j];
            for (let k = 0; k < res.data.fields.length; k++) {
              if (res.data.fields[k].field_name === field.assign_to) {
                let id = res.data.fields[k].id || generateId('field');
                Vue.set(res.data.fields[k], 'id', id);
                Vue.set(res.data.conf[i].calc_params.fields[j], 'id', id);
                Vue.set(res.data.fields[k], 'highlight', false);
              }
            }
          }
        }
      }
      return res;
    },
    /**
     * 初始化基本详情信息
     */
    async initInfo() {
      this.loading.cleanRule = true;
      if (this.$route.name === 'create_clean') {
        this.initDataIdInfo(this.calcRawDataId);
      } else {
        await this.getCleanDetail();
        this.initDataIdInfo(this.cleanInfo.dataId);
        this.isSideBarShow = false;
        this.isSidebarReadonly = true;
      }
      this.getRawdetail();
    },
    async requestTemplateInfo(dataScenario) {
      await this.$store.dispatch('api/getEtlTemplate', { param: dataScenario }).then(resp => {
        if (resp.result) {
          this.cleanInfo.conf = resp.data.conf;
          if (resp.data.conf && resp.data.conf.length > 0) {
            this.$refs.cleanRules.backfill(resp.data.conf, resp.data.fields || []);
          }
        }
      });
    },
    async initDataIdInfo(dataId) {
      let payload = {
        params: { raw_data_id: dataId },
      };
      this.$store
        .dispatch('getDelpoyedList', payload)
        .then(res => {
          if (res.result) {
            const resData = res.data;
            this.cleanInfo.dataId = resData.access_raw_data.id;
            this.cleanInfo.dataName = resData.access_raw_data.raw_data_name;
            this.cleanInfo.dataScenario = resData.access_raw_data.data_scenario;
            this.cleanInfo.dataAlias = resData.access_raw_data.raw_data_alias; // 中文
            this.getRecentlyData(dataId);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading.cleanRule = false;
          if (this.$route.name === 'create_clean') {
            this.requestTemplateInfo(this.cleanInfo.dataId);
          }
        });
    },
    /*
     * 获取原始数据
     */
    getRecentlyData(dataId) {
      this.loading.loadMsg = true;
      this.axios.get(`/v3/databus/rawdatas/${dataId}/tail/`).then(res => {
        if (res.result) {
          if (res.data.length > 0) {
            this.latestMsg = res.data[0].value;
            this.$nextTick(() => {
              this.$refs.cleanRules.initRecommend('latestMsg');
            });
          }
        }
        this.loading.loadMsg = false;
      });
    },
    syncCleanResult(data, ok) {
      if (!ok) {
        this.resultLoading = false;
        return;
      }
      this.cleanResult.resultPreview = true;
      let display = data.display;
      let tempArr = [];
      let res = data.result;
      const displayIndex = [];

      display.forEach((item, index) => {
        item === 'user_field' && tempArr.push(data.schema[index]) && displayIndex.push(index);
      });
      this.cleanResult.tableHead = tempArr;
      this.cleanResult.tableList = res.map(item => {
        const tableList = item.filter((tableItem, index) => {
          console.log(tableItem, index, displayIndex.includes(index));
          return displayIndex.includes(index);
        });
        return tableList;
      });
      this.cleanResult.errors = data.errors;
      this.resultLoading = false;
      this.isSetCleanConfigBtnActive = true;
      if (!data.errors || !data.errors.length) {
        this.showSideBar(false);
      }
    },
    notifyHideResultPreview() {
      this.cleanResult.resultPreview = false;
    },
    removeBeforunload() {
      window.removeEventListener('beforeunload', this.beforeunloadFn);
    },
  },
};
</script>

<style media="screen" lang="scss">
.data-clean {
  background: #fafafa;
  transition: all 0.5s ease 0.5s;

  &.without-sidebar {
    display: flex;
    justify-content: center;
  }

  .clean-content {
    width: calc(100% - 480px);
    padding: 20px 15px 50px;
  }
  .raw-data {
    height: 124px;
    box-shadow: 2px 4px 5px rgba(33, 34, 50, 0.15);
    border: 1px solid #c3cdd7;
    transition: all 0.3s ease;
    &.expand {
      height: 324px;
      transition: all 0.3s ease;
    }
    .raw-data-left {
      width: 230px;
      height: 100%;
      float: left;
      padding: 20px;
      background: #f5f5f5;
      font-size: 12px;

      .raw-data-names {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
    }
    .title {
      font-size: 16px;
      color: #212232;
      margin-bottom: 7px;
      /* font-weight: bold; */
      .bk-icon {
        font-size: 18px;
        color: #3a84ff;
        margin-right: 10px;
        vertical-align: -2px;
      }
    }
    .progress {
      margin-top: 10px;
      position: relative;
      .progress-list {
        float: left;
      }
      p + p {
        margin-left: 20px;
      }
      .round {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background: #c3cdd7;
        vertical-align: -2px;
        margin-right: 5px;
        &.success {
          background: #9dcb6b;
        }
        &.warning {
          background: #ff5555;
        }
      }
    }
    .raw-data-right {
      width: calc(100% - 230px);
      height: 100%;
      padding: 20px;
      position: relative;
      background: #fff;
      &.error {
        position: relative;
        .text-area {
          border-color: #f00;
        }
      }
      .error-msg {
        max-width: 300px;
        min-width: 40px;
        position: absolute;
        top: -15px;
        left: 50%;
        padding: 5px 5px;
        font-size: 12px;
        white-space: nowrap;
        background: #212232;
        color: #fff;
        border-radius: 2px;
        line-height: 14px;
        transform: translate(-50%, 0);
        &:after {
          content: '';
          position: absolute;
          /* right: 70%; */
          left: 50%;
          bottom: -10px;
          border: 5px solid;
          border-color: #212232 transparent transparent transparent;
        }
      }
    }
    .triangle-left {
      position: absolute;
      left: -8px;
      top: 24px;
      width: 0;
      height: 0;
      border-top: 7px solid transparent;
      border-right: 8px solid #fff;
      border-bottom: 7px solid transparent;
    }

    .triangle-right-bottom {
      position: absolute;
      right: 20px;
      bottom: 20px;
      font-size: 24px;
      cursor: pointer;
      transition: all 0.3s ease;
    }
    .text-area {
      width: 100%;
      height: 100%;
      resize: none;
      border-color: #c3cdd7;
      border-radius: 2px;
      outline: none;
      padding: 4px 11px;
      font-size: 12px;
      line-height: 18px;
    }
  }
  .result-preview {
    margin-top: 20px;
    border: 1px solid #c3cdd7;
    box-shadow: 2px 4px 5px rgba(33, 34, 50, 0.15);
    background: #fff;
    .result-preview-btn {
      display: flex;
      align-items: center;

      .debug-result {
        padding: 2px 15px;

        &.success {
          color: #30d878;
        }

        &.failed {
          color: #ff5656;
        }
      }
    }
    .header {
      height: 50px;
      padding-left: 20px;
      padding-right: 20px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      .title {
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
      .more {
        padding-right: 25px;
        color: #3a84ff;
        cursor: pointer;
        position: relative;
        .bk-icon {
          position: absolute;
          right: 0px;
          top: 3px;
          transform: rotate(90deg);
          transition: 0.2s ease all;
        }
        &.active {
          .bk-icon {
            transform: rotate(270deg);
          }
        }
      }
    }
    .result-table {
      overflow: auto;
      .bk-table th {
        background: #efefef;
      }
      .bk-table td {
        word-break: break-all;
      }
      .warning {
        color: #ff5656;
        margin-bottom: 15px;
        .title {
          color: #ff5656;
        }
        padding-left: 20px;
      }
    }
  }
  .mtb-10 {
    margin: 10px 0;
  }
  .clear-both {
    clear: both;
  }
  .error {
    position: relative;
    input {
      border-color: #ff0000;
    }
    .error-msg {
      max-width: 300px;
      position: absolute;
      top: -33px;
      left: 50%;
      padding: 5px 5px;
      font-size: 12px;
      white-space: nowrap;
      background: #212232;
      color: #fff;
      border-radius: 2px;
      line-height: 14px;
      transform: translate(-50%, 0);
      &:after {
        content: '';
        position: absolute;
        /* right: 70%; */
        left: 50%;
        bottom: -10px;
        border: 5px solid;
        border-color: #212232 transparent transparent transparent;
      }
    }
  }
  .setting-button {
    display: none;
  }
  @media (max-width: 1780px) {
    .clean-content {
      width: 100%;
    }
    .setting-button {
      margin-left: 5px;
      display: block;
    }
  }
  @media (max-width: 1366px) {
    .clean-content {
      width: 1180px;
      margin: 0 auto;
      padding: 20px 0 50px;
    }
  }
}
</style>
