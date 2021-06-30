/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

import { Component, Prop, Vue, Watch } from 'vue-property-decorator';
import Mixin from './../Mixin';
import HeaderType from '@/pages/datamart/common/components/HeaderType.vue';
import alertConvergenceForm from '@/pages/dmonitorCenter/components/convergenceForm/alertConvergenceForm.vue';
import alertTriggerForm from '@/pages/dmonitorCenter/components/convergenceForm/alertTriggerForm.vue';
import receiverForm from '@/pages/dmonitorCenter/components/notifyForm/receiverForm.vue';
import RuleContentConfig from './RuleContentConfig';
import EventSelect from './EventSelect.vue';
import RuleModel from './RuleModel';
import NoData from '@/pages/datamart/DataDict/components/children/chartComponents/NoData.vue';
// api请求函数
import {
  queryQualityRuleFunc,
  queryQualityRuleIndex,
  queryQualityRuleTemplates,
  createQualityRules,
  updateQualityRules,
  getAuditEventTemp,
  getDataQualityEventTypes,
  getNotifyWayList,
} from '@/pages/datamart/Api/DataQuality';
// 数据接口
import {
  IQualityRuleFunc,
  IQualityRuleIndex,
  IQualityRuleTemplate,
  IRuleConfigFields,
  IAuditEventTemp,
  IRuleFuncData,
  IRuleIndexData,
  IRuleTemplateData,
  IAuditEventTempData,
  INotifyWayList,
  INotifyWayData,
} from '@/pages/datamart/InterFace/DataQuality';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { showMsg } from '@/common/js/util';

@Component({
  mixins: [Mixin],
  components: {
    HeaderType,
    alertConvergenceForm,
    alertTriggerForm,
    receiverForm,
    RuleContentConfig,
    EventSelect,
    RuleModel,
    NoData,
  },
})
export default class QualityRuleConfig extends Vue {
  isShowEventAdvance = false;
  isEdit = true;
  isShowAdvance = false;
  isReceiverLoading = false;
  isShowDropContent = false;
  // 区分创建规则和更新规则
  isCreated = false;
  isCoveredConfig = false;
  isChecking = false;
  isRuleTemplateLoading = false;
  isRuleIndexLoading = false;
  isRuleFuncLoading = false;
  isShowRuleModel = false;
  isRefresh = true;
  isShow = false; // 规则弹窗是否展示
  isShowTableSelect = false; // 当聚焦时，显示表格
  eventType = 'data_quality';
  modelLimitMetrics: string[] = [];
  ruleFuncList: IRuleFuncData[] = [];
  ruleIndexList: IRuleIndexData[] = [];
  ruleTemplateList: IRuleTemplateData[] = [];
  auditEventTempList: IAuditEventTempData[] = [];
  receivers: string[] = [];
  roles = [
    {
      objectClass: 'project',
      scopeId: this.$route.query.project_id,
    },
  ];
  eventTypeList = [
    {
      id: 'data_quality',
      name: this.$t('质量事件'),
    },
  ];
  eventPolarityList = [
    {
      id: 'positive',
      name: this.$t('正向'),
    },
    {
      id: 'negative',
      name: this.$t('负向'),
    },
    {
      id: 'neutral',
      name: this.$t('中性'),
    },
  ];
  influenceLevelList = [
    {
      id: 'pubblic',
      name: this.$t('公开'),
    },
    {
      id: 'private',
      name: this.$t('私有'),
    },
    {
      id: 'confidential',
      name: this.$t('机密'),
    },
  ];
  ruleContent = '';
  formData: IRuleConfigFields = {
    data_set_id: '',
    rule_name: '',
    rule_description: '',
    rule_template_id: '',
    rule_config: [
      {
        metric: {
          metric_type: '',
          metric_name: '',
        },
        function: '',
        constant: {
          constant_type: 'float',
          constant_value: 0,
        },
        operation: 'and',
      },
    ],
    event_name: '',
    event_alias: '',
    event_description: '',
    event_type: 'data_quality',
    event_sub_type: '',
    event_polarity: 'neutral',
    event_currency: '300',
    sensitivity: 'private',
    event_detail_template: '',
    notify_ways: [],
    receivers: [], // 表单中告警接收人
    convergence_config: {
      duration: 1,
      alert_threshold: 1,
      mask_time: 1,
    },
    rule_id: '',
    audit_task_status: '',
    rule_config_alias: '',
  };
  userList = [];
  rules: Record<string, any> = {
    rule_name: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    rule_template_id: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    event_name: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    event_alias: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    event_description: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    event_polarity: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    event_currency: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    sensitivity: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    event_detail_template: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
    ],
    notify_ways: [],
    receivers: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        validator: function (val: []) {
          return val.length >= 1;
        },
        message: '请选择接收人',
        trigger: 'blur',
      },
    ],
    convergence_config: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        validator: function (val: {}) {
          return Object.values(val).every(item => item);
        },
        message: '请填写收敛策略',
        trigger: 'blur',
      },
    ],
    rule_config: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        validator: this.validatorRuleContent(),
        message: '请填写规则内容',
        trigger: 'blur',
      },
    ],
  };
  alertConfigInfo: any = {
    trigger_config: {
      duration: 1,
      alert_threshold: 1,
    },
    convergence_config: {
      mask_time: 60,
    },
    receivers: [],
  };
  isNotifyLoading = false;
  isDropdownShow = false;
  // textarea组件光标位置
  selectionStart = 0;
  notifyWays: Array<INotifyWayData> = [
    {
      description: '',
      notifyWayAlias: '',
      active: false,
      notifyWay: '',
      notifyWayName: '',
      icon: '',
    },
  ];
  // 任务处于运行状态，禁止编辑
  get isAllDisabled() {
    return !this.isEdit;
  }

  // 统一从store获取user信息
  get bkUser() {
    return this.$store.getters.getUserName;
  }

  /**
   * onUserNameChange
   * @param name 
   */
  @Watch('bkUser', { immediate: true })
  onUserNameChange(name: string) {
    // 新建规则配置时，默认设置自己为告警接收人
    if (!this.isCreated) {
      this.formData.receivers = [name];
      this.receivers = [name];
    }
  }

  /**
   * onReceiversChange
   * @param val 
   */
  @Watch('receivers', { immediate: true })
  onReceiversChange(val: []) {
    this.formData.receivers = val;
  }

  // alert信息变化时，更新表单
  @Watch('alertConfigInfo', { immediate: true, deep: true })
  onAlertConfigInfoChange() {
    const { duration, alert_threshold } = this.alertConfigInfo.trigger_config;
    this.formData.convergence_config.duration = duration;
    this.formData.convergence_config.alert_threshold = alert_threshold;
    this.formData.convergence_config.mask_time = this.alertConfigInfo.convergence_config.mask_time;
  }

  /**
   * mounted
   */
  mounted() {
    this.queryQualityRuleFunc();
    this.queryQualityRuleIndex();
    this.queryQualityRuleTemplates();
    this.getAuditEventTemp();
    this.getProjectMember();
    this.getDataQualityEventTypes();
    this.getNotifyWayList();
  }

  // 获取数据平台支持的所有告警通知方式
  getNotifyWayList() {
    this.isNotifyLoading = true;
    getNotifyWayList().then(res => {
      const instance = new BKHttpResponse<INotifyWayList>(res);
      instance.setData(this, 'notifyWays');
      this.isNotifyLoading = false;
      if (this.isCreated) return;
      this.$nextTick(() => {
        this.formData.notify_ways = [];
      });
    });
  }

  // 接口拉取数据质量的事件类型
  getDataQualityEventTypes() {
    getDataQualityEventTypes('data_quality').then(res => {
      if (res.result && res.data) {
        this.eventTypeList = [];
        res.data.forEach(item => {
          this.eventTypeList.push({
            id: item.event_type_name,
            name: item.event_type_alias,
          });
        });
      }
    });
  }

  /**
   * handleRuleNameChange
   * @param ruleName 
   */
  handleRuleNameChange(ruleName: string) {
    if (!this.formData.event_alias && ruleName) {
      this.formData.event_alias = `${ruleName}的事件`;
    }
  }

  /**
   * clearReceivers
   */
  clearReceivers() {
    this.receivers = [];
  }

  // 获取所有成员名单,获取成员
  getProjectMember() {
    this.isReceiverLoading = true;
    this.axios.get('projects/list_all_user/').then(res => {
      if (res.result) {
        this.userList = res.data.map(user => ({ id: user, name: user }));
      } else {
        this.getMethodWarning(res.message, res.code);
      }
      this.isReceiverLoading = false;
    });
  }

  /**
   * handleInputChange
   * @param value 
   */
  handleInputChange(value: string) {
    this.selectionStart = this.$refs.textareaInput.$refs.textarea.selectionStart;

    if (value[this.selectionStart - 1] === '$') {
      this.$refs.dropdown.isShow = true;
      this.isDropdownShow = true;
    } else {
      this.isDropdownShow = false;
    }
  }

  /**
   * handleStopOver
   */
  handleStopOver() {
    this.selectionStart = this.$refs.textareaInput.$refs.textarea.selectionStart;
    const value = this.formData.event_detail_template[this.selectionStart - 1];
    if (value === '$') {
      this.isDropdownShow = true;
    } else {
      this.isDropdownShow = false;
    }
  }

  /**
   * triggerHandler
   * @param data 
   */
  triggerHandler(data: IAuditEventTempData) {
    const left = this.formData.event_detail_template.slice(0, this.selectionStart - 1);
    const right = this.formData.event_detail_template.slice(this.selectionStart);
    this.formData.event_detail_template = left + '${' + data.varName + '}' + right;
    this.$refs.dropdown.isShow = false;
    this.$refs.textareaInput.focus();
    this.selectionStart = 0;
  }

  /**
   * handleTempSelect
   * @param id 
   * @param data 
   */
  handleTempSelect(id: number, data: IRuleTemplateData) {
    this.formData.event_sub_type = data.template_name;
    if (this.isCreated) {
      this.$bkInfo({
        theme: 'warning',
        title: '此操作存在风险',
        subTitle: '更换模板后，原规则配置将会被覆盖，是否继续？',
        confirmFn: () => {
          this.formData.rule_config = JSON.parse(JSON.stringify(data.template_config.default_rule_config));
          this.modelLimitMetrics = data.template_config.limit_metrics;
          this.isCoveredConfig = true;
        },
      });
    } else {
      this.isCoveredConfig = true;
      this.formData.rule_config = JSON.parse(JSON.stringify(data.template_config.default_rule_config));
      this.modelLimitMetrics = data.template_config.limit_metrics;
    }
  }

  /**
   * backFillData
   * @param data 
   * @param isEdit 
   */
  backFillData(data: IRuleConfigFields, isEdit?= true) {
    this.isCreated = true;
    this.formData = data;
    // 告警接收人
    this.receivers = this.formData.receivers;
    // 收敛策略
    const { duration, alert_threshold, mask_time } = this.formData.convergence_config;
    this.alertConfigInfo.trigger_config.duration = duration;
    this.alertConfigInfo.trigger_config.alert_threshold = alert_threshold;
    this.alertConfigInfo.convergence_config.mask_time = mask_time;
    this.isEdit = isEdit;
    this.getModelLimitMetrics();
  }

  /**
   * getModelLimitMetrics
   */
  getModelLimitMetrics() {
    if (this.ruleTemplateList.length && this.formData.rule_template_id) {
      this.modelLimitMetrics = this.ruleTemplateList.find(item => item.id === this.formData.rule_template_id)
        .template_config.limit_metrics;
    }
  }

  /**
   * handleCloseRuleTemplate
   */
  handleCloseRuleTemplate() {
    this.isShowRuleModel = false;
  }

  /**
   * validatorRuleContent
   * @returns 
   */
  validatorRuleContent() {
    const self = this;
    return function (val: []) {
      return self.ruleContent.length > 0;
    };
  }

  /**
   * submitData
   */
  submitData() {
    this.formData.rule_config_alias = this.ruleContent;
    this.isChecking = true;
    this.$refs.validateForm1.validate().then(
      () => {
        if (this.isCreated) {
          this.updateQualityRules();
        } else {
          this.createQualityRules();
        }
      },
      () => {
        this.isChecking = false;
      }
    );
  }

  /**
   * eventFucus
   */
  eventFucus() {
    this.isShowTableSelect = true;
    this.eventType = 'number';
  }

  /**
   * eventLeave
   */
  eventLeave() {
    this.isShowTableSelect = false;
    this.eventType = 'textarea';
  }

  // 质量事件查看指标接口
  getAuditEventTemp() {
    getAuditEventTemp().then(res => {
      const instance = new BKHttpResponse<IAuditEventTemp>(res);
      instance.setData(this, 'auditEventTempList');
      this.$nextTick(() => {
        this.isShowDropContent = true;
      });
    });
  }

  // 获取规则配置函数列表
  public queryQualityRuleFunc() {
    this.isRuleFuncLoading = true;
    queryQualityRuleFunc(this.DataId).then(res => {
      const instance = new BKHttpResponse<IQualityRuleFunc>(res);
      instance.setData(this, 'ruleFuncList');

      this.isRuleFuncLoading = false;
    });
  }

  // 获取规则配置指标列表
  public queryQualityRuleIndex() {
    this.isRuleIndexLoading = true;
    queryQualityRuleIndex(this.DataId).then(res => {
      const instance = new BKHttpResponse<IQualityRuleIndex>(res);
      instance.setData(this, 'ruleIndexList');
      this.isRuleIndexLoading = false;
    });
  }

  // 获取规则配置模板列表
  public queryQualityRuleTemplates() {
    this.isRuleTemplateLoading = true;
    queryQualityRuleTemplates(this.DataId).then(res => {
      const instance = new BKHttpResponse<IQualityRuleTemplate>(res, false);
      instance.setData(this, 'ruleTemplateList');
      this.isRuleTemplateLoading = false;
      this.getModelLimitMetrics();
    });
  }

  /**
   * 新建规则
   */
  createQualityRules() {
    this.formData.data_set_id = this.DataId;
    createQualityRules(this.formData).then(res => {
      if (res.data) {
        showMsg('新建审核规则成功！', 'success', { delay: 2500 });
        this.$emit('close');
      }
      this.isChecking = false;
    });
  }

  /**
   * 保存规则
   */
  updateQualityRules() {
    this.formData.data_set_id = this.DataId;
    updateQualityRules(this.formData).then(res => {
      if (res.data) {
        showMsg('更新审核规则成功！', 'success', { delay: 2500 });
        this.$emit('close');
      }
      this.isChecking = false;
    });
  }
}
