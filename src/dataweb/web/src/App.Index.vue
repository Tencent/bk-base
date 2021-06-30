

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
  <div id="bkdata_global_container"
    v-bkloading="{ isLoading: isLoading, opacity: 0.4 }"
    class="index-container">
    <template v-if="isLoading">
      <Header :isPlaceholder="true" />
    </template>
  </div>
</template>
<script>
import Vue from 'vue';
import getRouter from './router';
import store from './store';
import App from './App';
import getI18N from './i18n/index';
import Qs from 'qs';
import components from '@/components/bkCompAlias/index.js';
import bkStorage from '@/common/js/bkStorage.js';
import directives from '@/components/bkCompAlias/directives.js';
import 'vue-directive-tooltip/css/index.css';
import bkTooltip from 'vue-directive-tooltip';
import { Combobox } from '@/components/index.js';
import { bkMessage, bkInfoBox, bkLoading, bkNotify, bkPopover } from 'bk-magic-vue';
import {
  compare,
  getMethodWarning,
  postMethodWarning,
  getValueFromObj,
  linkToCC,
  scrollView,
  toolTip,
} from '@/common/js/util';
import { ajax as _axios, axios, bkRequest } from '@/common/js/ajax';
import Header from '@/components/header/header.vue';
import initGlobalStorage from './init';
import './bkdata-ui/common/scss/base.scss';
import './bkdata-ui/common/scss/iconCool.scss';
import './bkdata-ui/common/scss/global.scss';
import './common/scss/base.scss';
import './bkdata-ui/common/scss/bkDataUi.scss';
import './bkdata-ui/common/scss/form.scss';
import VueHighlightJS from 'vue-highlightjs';
import VueHljs from 'vue-hljs-with-line-number';
import 'vue-hljs-with-line-number/line-number.css';
import 'highlight.js/styles/atom-one-light.css';
import globalConfig from './global.config';
export default {
  name: 'App.Index',
  components: { Header },
  data() {
    return {
      isLoading: false,
      timer: null,
      popConfirm: null,
      normalInterval: 1800000,
      publishInterval: 60000,
    };
  },
  watch: {
    /*
     * 监听路由变化
     */
    '$route.path': function (cur, prev) {
      _axios.cancelSources.active(prev);
    },
  },
  async mounted() {
    this.isLoading = true;
    await this.getInitConfig();
    window.gVue = this.mountAppWithRouter();
    window.$t = window.gVue.$t;

    this.initGlobalValidate();
    this.isLoading = false;
    initGlobalStorage(this.$store);
  },

  created() {
    this.mountComponents();
    this.getEnvSettings();
    this.getUserInfo();
  },
  beforeDestroy() {
    clearTimeout(this.timer);
    this.timer = null;
  },
  methods: {
    async getInitConfig() {
      Vue.prototype.$modules = {
        data: [],
        isActive: function (key) {
          return this.data.some(mod => mod.operation_id === key && mod.status === 'active');
        },
      };

      if (!window.$modules) window.$modules = Vue.prototype.$modules;
      try {
        /** 查询发布状态, 配置功能开关 */
        await this.setSwitchTimer();
      } catch (_) {
        console.log('initVueInstance Error', _);
      }
    },

    /**
     * 获取发布状态并根据发布状态处理前端相关交互
     */
    async processPublishingStatus(isInit = false) {
      this.updateModuleData(globalConfig);
      const operatorConfig = await this.bkRequest.httpRequest('auth/getPublishStatus');
      let globalConfigItems = globalConfig;

      /** 只有初始化时，对功能开关做配置，轮询不需要配置功能开关 */
      if (isInit) {
        if (operatorConfig.result) {
          /**
           * 过滤新增配置项
           */
          function filterNotExistFn(item) {
            return globalConfig.some(cfg => cfg.operation_id === item.operation_id);
          }
          const responseData = (operatorConfig.data || []);
          /** 过滤当前配置不存在的配置项 */
          const appendItems = responseData.filter(item => !filterNotExistFn(item));

          /** 根据接口返回配置项更新当前配置项 */
          const existItems = globalConfig.map(item => {
            const updateItem = responseData.find(resItem => resItem.operation_id === item.operation_id) || {};
            return Object.assign({}, item, updateItem);
          });
          globalConfigItems = [...appendItems, ...existItems];
          this.updateModuleData(globalConfigItems);
        } else {
          postMethodWarning(operatorConfig.message, 'error');
        }
      }

      /** 每次查询，都需要查看发布状态，并根据状态选择是否禁止用户操作 */
      const publishingExempt = globalConfigItems.find(item => item.operation_id === 'publishing_exempt');
      const publishingStatus = globalConfigItems.find(item => item.operation_id === 'publishing');

      if (!publishingExempt || !operatorConfig) return false; // 没有相关状态数据，直接返回
      /** 判断是否在发布并屏蔽功能 */
      if (publishingExempt.status === 'active') return false; // 具有豁免权，返回不受影响

      /**
       * 根据publishing状态，缓存状态：
       * - 0 代表第一次查询到，提示1分钟后发布
       * - 1 代表第二次查到，禁止用户任何操作
       */
      if (publishingStatus.status !== 'active') {
        if (this.popConfirm) {
          this.popConfirm.close();
          this.popConfirm = null;
        }
        bkStorage.set('publishing', 0);
        return false;
      }
      const localPublishTime = bkStorage.get('publishing');

      switch (localPublishTime) {
        case 0:
        case null:
          this.popConfirm = this.popConfirm === null
            ? this.$bkInfo({
              type: 'warning',
              showFooter: true,
              title: window.$t('即将发布'),
              subTitle: window.$t('将在1分钟后发布_请尽快保存相关操作'),
              afterLeaveFn: () => {
                this.popConfirm = null;
              },
            })
            : this.popConfirm;
          bkStorage.set('publishing', 1);
          break;
        case 1:
          /** 当popconfirm非loading状态，强行关闭 */
          if (this.popConfirm && this.popConfirm.type !== 'loading') {
            Object.assign(this.popConfirm, {
              type: 'loading',
              title: 'loading',
              showFooter: false,
              closeIcon: false,
              subTitle: window.$t('正在发布_请稍等'),
            });
            return true;
          }
          this.popConfirm =            this.popConfirm === null
            ? this.$bkInfo({
              type: 'loading',
              showFooter: false,
              title: 'loading',
              closeIcon: false,
              subTitle: window.$t('正在发布_请稍等'),
            })
            : this.popConfirm;
          break;
      }
      return true;
    },


    updateModuleData(data) {
      Vue.prototype.$modules.data = data;
      store.commit('ide/updateNodeConfig', data);
    },

    /** 设置发布状态开关定时器
     *  发布状态时：60s轮询接口； 常规状态下，30m轮询状态
     */
    async setSwitchTimer() {
      const getSwitch = async (isInit = false) => {
        const result = await this.processPublishingStatus(isInit);
        const interval = result ? this.publishInterval : this.normalInterval; // 当开始发布时，轮询间隔缩短为60s，正常状态时，轮询为30分
        this.timer = setTimeout(getSwitch, interval);
      };

      /** 初始化，第一次查询，配置功能开关 */
      await getSwitch(true);
    },
    mountAppWithRouter() {
      const router = getRouter(Vue.prototype.$modules);
      const i18n = getI18N();

      return new Vue({
        el: '#bkdata_global_container',
        store,
        router,
        i18n: i18n,
        components: { App }, // 重点！
        template: '<App/>',
      });
    },

    initGlobalValidate() {
      Vue.prototype.validator = {
        message: {
          wordFormat: window.$t('格式不正确_内容由字母_数字和下划线组成_且以字母开头'),
          max15: window.$t('格式不正确_请输入十五个以内字符'),
          max50: window.$t('格式不正确_请输入五十个以内字符'),
          required: window.$t('必填项不可为空'),
          piz: window.$t('请选择项目'),
          biz: window.$t('请选择业务'),
          results: window.$t('请选择结果数据表'),
          bluekingApp: window.$t('请选择蓝鲸APP'),
          desRequired: window.$t('用途描述必填'),
          noHost: window.$t('未选择主机'),
          project: window.$t('项目名称不能为空'),
        },
        validWordFormat(val) {
          let reg = new RegExp('^[A-Za-z][0-9a-zA-Z_]*$');
          return reg.test(val);
        },
      };
    },

    /*
         * 获取APP配置
         */
    getEnvSettings() {
      _axios.get('tools/get_env_settings/').then(res => {
        if (res.result) {
          this.$store.dispatch('activeEnvSettings', res.data);
        }
      });
    },
    getUserInfo() {
      _axios.get('tools/get_user_info/').then(res => {
        if (res.result) {
          this.username = res.data.username;
          this.$store.dispatch('updateUserName', res.data.username);
          this.$store.dispatch('updateIsAdmin', res.data.is_admin);
        } else {
          console.log('get_user_info', res);
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    /**
         * 挂载组件 & 注册扩展
         */
    mountComponents() {
      this.registerComponents(Vue);
      Vue.prototype.qs = Qs;
      Vue.prototype.$bkInfo = bkInfoBox;
      Vue.prototype.$bkMessage = option => bkMessage(Object.assign(option, { isSingleLine: false, limit: 1 }));
      Vue.prototype.$bkLoading = bkLoading;
      Vue.prototype.$bkNotify = bkNotify;

      Vue.prototype.axios = _axios;
      Vue.prototype.Axios = axios;
      Vue.prototype.bkRequest = bkRequest;

      // todo 需要去掉，公共方法不需要注册到 VUE 原型中
      Vue.prototype.routeCheck = _axios.routeCheck;
      Vue.prototype.compare = compare;
      Vue.prototype.scrollView = scrollView;
      Vue.prototype.getValueFromObj = getValueFromObj;
      Vue.prototype.toolTip = toolTip;
      Vue.prototype.getMethodWarning = getMethodWarning;
      Vue.prototype.$linkToCC = linkToCC;
    },
    /**
         * 注册组件
         */
    registerComponents(ins) {
      components.forEach(comp => {
        ins.component(comp.name.replace(/^bk-/i, 'bkdata-'), comp);
      });
      directives.forEach(directive => {
        ins.use(directive);
      });

      ins.use(Combobox);
      ins.use(bkTooltip, {
        delay: 100,
        placement: 'top',
      });

      ins.use(bkLoading);
      ins.use(VueHighlightJS);
      ins.use(bkPopover);
      ins.use(VueHljs);
    }
  },
};
</script>
<style lang="scss" scoped>
#bkdata_global_container {
  height: 100%;
}
</style>
