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

import { Component, Vue } from 'vue-property-decorator';
import Mixin from './Mixin';
import HeaderType from '@/pages/datamart/common/components/HeaderType.vue';
import ImgCombineExplain from './components/ImgCombineExplain.vue';
import DataTable from '@/pages/datamart/common/components/DataTable.vue';
import QualityRuleConfig from './components/QualityRuleConfig';
import {
  getQualityAuditRuleTable,
  deleteQualityRules,
  startQualityRulesTask,
  stopQualityRulesTask,
} from '@/pages/datamart/Api/DataQuality';
// 数据接口
import { IRuleConfigFields, IQualityAuditRuleTable } from '@/pages/datamart/InterFace/DataQuality';
import { BKHttpResponse } from '@/common/js/BKHttpResponse';
import { showMsg } from '@/common/js/util';
import operationGroup from '@/bkdata-ui/components/operationGroup/operationGroup.vue';

const statusMap: Record<string, any> = {
  running: {
    alias: $t('运行中'),
    color: '#00c873',
  },
  waiting: {
    alias: $t('待启动'),
    color: '#63656e',
  },
  failed: {
    alias: $t('失败'),
    color: '#cb2537',
  },
};

@Component({
  mixins: [Mixin],
  components: {
    HeaderType,
    ImgCombineExplain,
    DataTable,
    QualityRuleConfig,
    operationGroup,
  },
})
export default class AuditRule extends Vue {
  isShowConfig = false;

  isLoading = true;

  tableData: Array<any> = [];

  // 操作弹框按钮背景色
  dialogTheme = 'danger';

  activeFunc = '';

  calcPageSize = 10;

  process = {
    isShow: false,
    title: '',
    buttonText: $t('删除'),
    buttonLoading: false,
    content: '',
    rule_id: '',
  };

  get statusMap() {
    return statusMap;
  }

  mounted() {
    this.getQualityAuditRuleTable();
  }

  openRuleConfig(id: string) {
    this.editAuditRule(this.tableData.find(item => item.event_id === id));
  }

  showConfig() {
    this.isShowConfig = true;
    this.$nextTick(() => {
      this.$refs.QualityRuleConfig.isShow = true;
    });
  }

  getQualityAuditRuleTable() {
    getQualityAuditRuleTable(this.DataId).then(res => {
      const instance = new BKHttpResponse<IQualityAuditRuleTable>(res, false);
      instance.setData(this, 'tableData');
      this.isLoading = false;
    });
  }

  editAuditRule(data: IRuleConfigFields) {
    // 任务正在运行中，不能直接进入编辑
    // if (data.audit_task_status === 'running') return
    this.showConfig();
    this.$nextTick(() => {
      this.$refs.QualityRuleConfig.backFillData(data, data.audit_task_status !== 'running');
    });
  }

  closeConfig() {
    this.isShowConfig = false;
    this.getQualityAuditRuleTable();
    this.$emit('changeRule');
  }

  deleteRule(data: IRuleConfigFields) {
    // 任务正在运行中，不能直接删除
    if (data.audit_task_status === 'running') return;
    this.process.isShow = true;
    this.process.title = this.$t('确认删除规则?');
    this.process.rule_id = data.rule_id;
    this.activeFunc = 'delete';
  }

  actionRule(data: IRuleConfigFields) {
    this.process.isShow = true;
    this.process.rule_id = data.rule_id;
    this.process.buttonText = this.$t('确定');
    if (data.audit_task_status === 'waiting') {
      this.dialogTheme = 'primary';
      this.activeFunc = 'start';
      this.process.title = this.$t('确认启动审核任务?');
    } else {
      this.activeFunc = 'stop';
      this.process.title = this.$t('确认停止审核任务?');
    }
  }

  stopRule(data: IRuleConfigFields) {
    this.process.isShow = true;
  }

  cancel() {
    this.activeFunc = '';
    this.process.rule_id = '';
    this.process.title = '';
    this.dialogTheme = 'danger';
  }

  getActionFunc() {
    const funcMap = {
      delete: this.deleteQualityRules,
      start: this.startQualityRulesTask,
      stop: this.stopQualityRulesTask,
    };
    return funcMap[this.activeFunc]();
  }

  // 删除规则
  deleteQualityRules() {
    this.process.buttonLoading = true;
    deleteQualityRules(this.process.rule_id).then(res => {
      if (res.result && res.data) {
        showMsg('删除规则成功！', 'success', { delay: 2500 });
        this.getQualityAuditRuleTable();
      }
      this.process.isShow = false;
      this.process.buttonLoading = false;
      this.$emit('changeRule');
    });
  }

  // 启动规则审核任务
  startQualityRulesTask() {
    this.process.buttonLoading = true;
    startQualityRulesTask(this.process.rule_id).then(res => {
      console.log('res: ', res);
      if (res.result && res.data) {
        showMsg('启动审核任务成功！', 'success', { delay: 2500 });
        this.getQualityAuditRuleTable();
        this.$emit('update');
      }
      this.process.isShow = false;
      this.process.buttonLoading = false;
    });
  }

  // 停止规则审核任务
  stopQualityRulesTask() {
    this.process.buttonLoading = true;
    stopQualityRulesTask(this.process.rule_id).then(res => {
      if (res.result && res.data) {
        showMsg('停止审核任务成功！', 'success', { delay: 2500 });
        this.getQualityAuditRuleTable();
        this.$emit('update');
      }
      this.process.isShow = false;
      this.process.buttonLoading = false;
    });
  }
}
