

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
  <Layout class="dmonitor-center"
    :crumbName="[{ name: $t('我的告警'), to: '/user-center?tab=alert' }, { name: $t('告警配置列表') }]">
    <div class="alert-config-list">
      <div class="inquire-content clearfix">
        <div class="inquire">
          <div class="target-type-select">
            <bkdata-selector :selected.sync="filterForm.alertTargetType"
              :list="filterForm.targetTypeList"
              :settingKey="'key'"
              :placeholder="$t('告警对象类型')"
              :displayKey="'name'"
              :allowClear="true" />
          </div>
        </div>
        <div v-if="filterForm.alertTargetType === 'rawdata'"
          class="inquire">
          <div class="business-select">
            <bkdata-selector :selected.sync="filterForm.bkBizId"
              :filterable="true"
              :searchable="true"
              :placeholder="filterForm.bizHoder"
              :list="filterForm.bizList"
              :settingKey="'bk_biz_id'"
              :displayKey="'bk_biz_name'"
              :allowClear="true"
              searchKey="bk_biz_name" />
          </div>
        </div>
        <div v-if="filterForm.alertTargetType === 'dataflow'"
          class="inquire">
          <div class="project-select">
            <bkdata-selector :selected.sync="filterForm.projectId"
              :filterable="true"
              :searchable="true"
              :placeholder="filterForm.projectHoder"
              :list="filterForm.projectList"
              :settingKey="'project_id'"
              :displayKey="'project_alias'"
              :allowClear="true"
              searchKey="project_alias" />
          </div>
        </div>
        <div v-if="filterForm.alertTargetType === 'dataflow' || filterForm.alertTargetType === 'rawdata'"
          class="inquire">
          <div class="alert-target-select">
            <bkdata-selector :filterable="true"
              :searchable="true"
              :displayKey="'name'"
              :allowClear="true"
              :list="filterForm.alertTargetList"
              :placeholder="$t('告警对象')"
              :selected.sync="filterForm.alertTarget"
              :settingKey="'key'" />
          </div>
        </div>
        <div class="inquire">
          <div class="active-select">
            <bkdata-selector :selected.sync="filterForm.active"
              :list="filterForm.activeList"
              :settingKey="'key'"
              :placeholder="$t('是否启用')"
              :displayKey="'name'"
              :allowClear="true" />
          </div>
        </div>
        <div class="inquire">
          <div class="notify-way-select">
            <bkdata-selector :selected.sync="filterForm.notifyWay"
              :list="filterForm.notifyWayList"
              :settingKey="'notify_way'"
              :placeholder="filterForm.notifyWayHoder"
              :displayKey="$i18n.locale === 'en' ? 'notify_way_name' : 'notify_way_alias'"
              :allowClear="true" />
          </div>
        </div>
        <div class="inquire">
          <div class="self-radio">
            <bkdata-checkbox v-model="filterForm.scope"
              :trueValue="'received'"
              :falseValue="'configured'">
              {{ $t('只查看我接收的') }}
            </bkdata-checkbox>
            <!-- <bkdata-radio-group v-model="filterForm.scope">
                            <bkdata-radio :value="'received'">我作为接受人的告警配置</bkdata-radio>
                            <bkdata-radio :value="'managed'">我管理的告警配置</bkdata-radio>
                        </bkdata-radio-group> -->
          </div>
        </div>
        <div class="alert-config-operation">
          <div class="project-right-sort">
            <bkdata-button theme="primary"
              class="mr5"
              @click="linkAlertConfigAdd">
              <i :title="$t('新建告警配置')"
                class="bk-icon icon-add_dataflow mr5" />
              {{ $t('新建告警配置') }}
            </bkdata-button>
            <!-- <div class="alert-target-input">
                            <bkdata-input type="text"
                                @enter="onParamsChange"
                                v-model="filterForm.searchAlertTargetText"
                                :placeholder="$t('告警对象ID或名称')"
                                :right-icon="'bk-icon icon-search'" />
                        </div> -->
            <!-- <bkdata-button type='primary'
                            @click="showCreateNewTaskWindow"
                            style="z-index: 2; height: 30px;">
                            <i :title="$t('批量启用')"
                                class="bk-icon icon-play-shape mr5">
                            </i>
                            {{$t('批量启用')}}
                        </bkdata-button>
                        <bkdata-button type='primary'
                            @click="showCreateNewTaskWindow"
                            style="z-index: 2; height: 30px;">
                            <i :title="$t('批量停用')"
                                class="bk-icon icon-stop-shape mr5">
                            </i>
                            {{$t('批量停用')}}
                        </bkdata-button>
                        <bkdata-button type='primary'
                            @click="showCreateNewTaskWindow"
                            style="z-index: 2; height: 30px;">
                            <i :title="$t('批量屏蔽')"
                                class="bk-icon icon-alert mr5">
                            </i>
                            {{$t('批量屏蔽')}}
                        </bkdata-button> -->
          </div>
        </div>
      </div>
      <div class="table">
        <div v-bkloading="{ isLoading: isLoading }"
          class="alert-config-list-table">
          <bkdata-table :data="alertConfigList"
            :pagination="pagination"
            :emptyText="$t('暂无数据')"
            @page-change="handlePageChange"
            @page-limit-change="handlePageLimitChange">
            <!-- <bkdata-table-column type="selection" width="60" align="center">
                        </bkdata-table-column> -->
            <bkdata-table-column :label="$t('告警配置对象')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div class="alert-table-config-option">
                  <a href="javascript:;"
                    :title="getAlertTargetTitle(props.row)"
                    class="operation-button text-overflow"
                    @click="linkAlertConfig(props.row)">
                    {{ getAlertTargetTitle(props.row) }}
                  </a>
                  <i class="bk-icon icon-link-to ml5 cursor-pointer"
                    :title="$t('跳转到告警对象详情页')"
                    @click="linkAlertTarget(props.row)" />
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('项目')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div v-if="props.row.project_id">
                  [{{ props.row.project_id }}]{{ props.row.project_alias }}
                </div>
                <div v-else>
                  -
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('业务')"
              :resizable="false"
              minWidth="150">
              <template slot-scope="props">
                <div v-if="props.row.bk_biz_id">
                  [{{ props.row.bk_biz_id }}]{{ props.row.bk_biz_name }}
                </div>
                <div v-else>
                  -
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('接收人')"
              :resizable="false"
              width="150">
              <template slot-scope="props">
                <div class="receiver-list"
                  :title="formatReceiver(props.row)">
                  {{ formatReceiver(props.row) }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('通知方式')"
              :resizable="false"
              width="150">
              <template slot-scope="props">
                <div class="notify-way-icon-list">
                  <span v-for="notifyWay in props.row.notify_config"
                    :key="notifyWay"
                    :title="notifyWayMappings[notifyWay]
                      ? notifyWayMappings[notifyWay].notify_way_alias : notifyWay">
                    <img v-if="notifyWayMappings[notifyWay]"
                      class="notify-way-icon"
                      :src="`data:image/png;base64, ${notifyWayMappings[notifyWay].icon}`">
                    <span v-else>{{ notifyWay }}</span>
                  </span>
                </div>
              </template>
            </bkdata-table-column>
            <!-- <bkdata-table-column :label="$t('告警数量(24小时内)')"
                            :resizable="false"
                            min-width="150">
                            <template slot-scope="props">
                                <div>
                                    {{props.row.alert_count || '(功能开发中)'}}
                                </div>
                            </template>
                        </bkdata-table-column> -->
            <bkdata-table-column :label="$t('更新者')"
              :resizable="false"
              width="150">
              <template slot-scope="props">
                <div>
                  {{ props.row.updated_by }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('更新时间')"
              :resizable="false"
              width="150">
              <template slot-scope="props">
                <div>
                  {{ props.row.updated_at }}
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('状态')"
              :resizable="false"
              width="200">
              <template slot-scope="props">
                <div class="alert-status-display">
                  <span><i :class="`bk-icon icon-${alertConfigStatusIcon(props.row)}`" /></span>
                  <span>{{ alertConfigStatusDisplay(props.row) }}</span>
                </div>
              </template>
            </bkdata-table-column>
            <bkdata-table-column :label="$t('操作')"
              :resizable="false"
              width="300">
              <template slot-scope="props">
                <div class="alert-table-config-option">
                  <div class="detail-button">
                    <a href="javascript:;"
                      @click="linkAlertConfig(props.row)">
                      {{ $t('详情') }}
                    </a>
                  </div>
                  <div v-if="props.row.active === true"
                    class="config-button">
                    <a href="javascript:;"
                      @click="disableAlertConfig(props.row)">
                      {{ $t('停用') }}
                    </a>
                  </div>
                  <div v-if="props.row.active === false"
                    class="config-button">
                    <a href="javascript:;"
                      @click="enableAlertConfig(props.row)">
                      {{ $t('启用') }}
                    </a>
                  </div>
                  <div class="shield-button">
                    <a href="javascript:;"
                      @click="openAlertShieldModal(props.row)">
                      {{ $t('屏蔽') }}
                    </a>
                  </div>
                </div>
              </template>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </div>
      <!-- 告警屏蔽 start -->
      <div>
        <alert-shield-modal v-if="isAlertShieldModalShow"
          ref="alertShieldModal"
          @closeAlertShieldModal="isAlertShieldModalShow = false" />
      </div>
      <!-- 告警屏蔽 end -->
    </div>
  </Layout>
</template>
<script>
import Cookies from 'js-cookie';
import Bus from '@/common/js/bus.js';
import Layout from '../../components/global/layout';
import { postMethodWarning, showMsg } from '@/common/js/util.js';
import { userPermScopes } from '@/common/api/auth';
import {
  alertTypeMappings,
  alertLevelMappings,
  alertCodeMappings,
  alertStatusMappings,
  alertSendStatusMappings
} from '@/common/js/dmonitorCenter.js';
import alertShieldModal from './components/alertShieldModal';

export default {
  components: {
    Layout,
    alertShieldModal,
  },
  data() {
    return {
      isLoading: false,
      alertConfigList: [],
      filteredAlertConfigList: [],
      currentPage: 1,
      pageCount: 1,
      pageSize: 10,

      isAlertShieldModalShow: false,
      alertShieldForm: {
        alertTargets: [],
      },
      notifyWayMappings: {},

      filterForm: {
        bkBizId: '',
        bizList: [],
        bizHoder: '',
        projectId: '',
        projectList: [],
        projectHoder: '',
        searchAlertTargetText: '',
        active: '',
        activeList: [
          {
            name: this.$t('已启用'),
            key: 'True',
          },
          {
            name: this.$t('未启用'),
            key: 'False',
          },
        ],
        targetTypeList: [
          {
            name: this.$t('数据开发任务'),
            key: 'dataflow',
          },
          {
            name: this.$t('数据源'),
            key: 'rawdata',
          },
        ],
        alertTargetType: '',
        notifyWay: '',
        notifyWayList: [],
        notifyWayHoder: '',
        alertTargetList: [],
        alertTarget: '',
        scope: 'received',
      },
    };
  },
  computed: {
    pagination() {
      return {
        current: Number(this.currentPage),
        count: this.pageCount,
        limit: this.pageSize,
      };
    },
  },
  watch: {
    'filterForm.scope': {
      handler(scope) {
        this.getAlertConfigList();
        this.$nextTick(() => {
          this.updateAlertTargetList();
        });
      },
    },
    'filterForm.bkBizId': {
      handler(bkBizId) {
        this.getAlertConfigList();
      },
    },
    'filterForm.projectId': {
      handler(projectId) {
        this.getAlertConfigList();
      },
    },
    'filterForm.notifyWay': {
      handler(notifyWay) {
        this.getAlertConfigList();
      },
    },
    'filterForm.active': {
      handler(active) {
        this.getAlertConfigList();
      },
    },
    'filterForm.alertTargetType': {
      handler(alertTargetType) {
        this.getAlertConfigList();
      },
    },
    'filterForm.alertTarget': {
      handler(alertTarget) {
        this.getAlertConfigList();
      },
    },
    'filterForm.bkBizId': {
      handler(bkBizId) {
        this.getAlertConfigList();
        this.updateAlertTargetList();
      },
    },
    'filterForm.projectId': {
      handler(projectId) {
        this.getAlertConfigList();
        this.updateAlertTargetList();
      },
    },
  },
  mounted() {
    this.eventListen();
    this.init();
  },
  methods: {
    init() {
      // 组件加载后默认显示告警
      this.getAlertConfigList();
      this.getProjectList();
      this.getBizList();
      this.getNotifyWayList();
      this.updateAlertTargetList();
    },
    eventListen() {
      // 监听message事件
      window.addEventListener(
        'message',
        event => {
          if (event.data.msg === 'closeModel') {
            this.handleCancel();
          }
        },
        false
      );
    },
    /**
     * 筛选参数改变触发事件
     */
    onParamsChange() {
      this.getAlertConfigList();
    },
    /**
     * 跳转到告警对象详情页
     */
    linkAlertTarget(item) {
      if (item.alert_target_type === 'rawdata') {
        this.$router.push(`/data-access/data-detail/${item.alert_target_id}/`);
      } else if (item.alert_target_type === 'dataflow') {
        this.$router.push(`/dataflow/ide/${item.alert_target_id}/`);
      }
    },
    /**
     * 跳转到告警详情
     */
    linkAlert(item) {
      this.$router.push(`/dmonitor-center/alert/${item.alert_id}/`);
    },
    /**
     * 跳转到产生改告警的告警配置
     */
    linkAlertConfig(item) {
      this.$router.push(`/dmonitor-center/alert-config/${item.id}/`);
    },
    /**
     * 新建告警配置
     */
    linkAlertConfigAdd() {
      this.$router.push('/dmonitor-center/alert-config/add');
    },
    /**
     * 启用告警配置
     */
    enableAlertConfig(item) {
      if (item.receivers.length === 0) {
        showMsg(this.$t('没有配置告警接收人，请点击详情进行配置'), 'warning');
        return;
      }
      if (item.notify_config.length === 0) {
        showMsg(this.$t('没有配置通知方式，请点击详情进行配置'), 'warning');
        return;
      }
      const options = {
        params: {
          alert_config_id: item.id,
          active: true,
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/updateAlertConfig', options).then(res => {
        if (res.result) {
          item.active = true;
          showMsg(this.$t('启用配置成功'), 'success');
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
    },
    /**
     * 停用告警配置
     */
    disableAlertConfig(item) {
      const options = {
        params: {
          alert_config_id: item.id,
          active: false,
        },
      };
      this.bkRequest.httpRequest('dmonitorCenter/updateAlertConfig', options).then(res => {
        if (res.result) {
          item.active = false;
          showMsg(this.$t('停用配置成功'), 'success');
        } else {
          postMethodWarning(res.message, 'error');
        }
      });
    },
    /**
     * 打开告警屏蔽模态框
     */
    openAlertShieldModal(item) {
      this.isAlertShieldModalShow = true;
      this.$nextTick(() => {
        this.$refs.alertShieldModal.openModalByConfig([item]);
      });
    },
    /**
     * 分页页码改变
     */
    handlePageChange(page) {
      this.currentPage = page;
      this.getAlertConfigList();
    },
    /**
     * 单页展示数量改变
     */
    handlePageLimitChange(pageSize, preLimit) {
      this.currentPage = 1;
      this.pageSize = pageSize;
      this.getAlertConfigList();
    },
    /**
     * 生成数据AlertTarget的title
     */
    getAlertTargetTitle(alertItem) {
      if (alertItem.alert_target_type === 'dataflow') {
        return `[${$t('数据开发任务')}] ${alertItem.alert_target_alias}`;
      } else if (alertItem.alert_target_type === 'rawdata') {
        return `[${$t('数据源')}] ${alertItem.alert_target_alias}`;
      } else {
        return alertItem.alert_target_type_alias;
      }
    },
    /**
     * 告警分类展示
     */
    getAlertTypeDisplay(alertItem) {
      if (alertItem.alert_type === 'task_monitor') {
        return this.$t('任务监控');
      } else if (alertItem.alert_type === 'data_monitor') {
        return this.$t('数据监控');
      } else {
        return this.$t('未知类型');
      }
    },
    /**
     * 获取告警列表
     */
    getAlertConfigList() {
      this.isLoading = true;
      const options = {
        query: {
          page: this.currentPage,
          page_size: this.pageSize,
          scope: this.filterForm.scope,
        },
      };
      if (this.filterForm.bkBizId) {
        options.query.bk_biz_id = this.filterForm.bkBizId;
      }
      if (this.filterForm.projectId) {
        options.query.project_id = this.filterForm.projectId;
      }
      if (this.filterForm.active) {
        options.query.active = this.filterForm.active;
      }
      if (this.filterForm.notifyWay) {
        options.query.notify_way = this.filterForm.notifyWay;
      }
      if (this.filterForm.alertTargetType) {
        options.query.alert_target_type = this.filterForm.alertTargetType;
      }
      if (this.filterForm.alertTarget) {
        options.query.alert_target = this.filterForm.alertTarget;
      }
      this.bkRequest.httpRequest('dmonitorCenter/getMineDmonitorAlertConfig', options).then(res => {
        if (res.result) {
          this.alertConfigList = res.data.results;
          this.alertConfigList.map(alertConfig => {
            alertConfig.notify_ways = [];
            for (let notify_way in alertConfig.notify_config) {
              if (alertConfig.notify_config[notify_way] === true) {
                alertConfig.notify_ways.push(notify_way);
              }
            }
            return alertConfig;
          });
          this.pageCount = res.data.count;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isLoading = false;
      });
    },
    getBizList() {
      this.filterForm.bizHoder = this.$t('数据加载中');
      const options = {
        params: {
          action_id: 'raw_data.update',
          dimension: 'bk_biz_id',
        },
      };
      this.bkRequest
        .httpRequest('dmonitorCenter/getMineAuthScopeDimension', options)
        .then(res => {
          if (res.result) {
            this.filterForm.bizList = res.data;
            this.filterForm.bizList.map(biz => {
              biz.bk_biz_id = Number(biz.bk_biz_id);
              biz.bk_biz_name = `[${biz.bk_biz_id}]${biz.bk_biz_name}`;
              return biz;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.filterForm.bizHoder = this.$t('请选择业务');
        });
    },
    getProjectList() {
      this.filterForm.projectHoder = this.$t('数据加载中');

      userPermScopes({
        show_display: true,
        action_id: 'project.manage_flow',
      })
        .then(res => {
          if (res.result) {
            this.filterForm.projectList = res.data.filter(project => {
              if (project.project_id === undefined) {
                console.log(project);
                return false;
              }
              return true;
            });
            this.filterForm.projectList.map(project => {
              project.project_id = Number(project.project_id);
              project.project_alias = project.project_name;
              return project;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.filterForm.projectHoder = this.$t('请选择项目');
        });
    },
    getNotifyWayList() {
      this.filterForm.notifyWayHoder = this.$t('数据加载中');
      this.bkRequest
        .httpRequest('dmonitorCenter/getNotifyWayList')
        .then(res => {
          if (res.result) {
            this.filterForm.notifyWayList = res.data;
            for (let notifyWay of this.filterForm.notifyWayList) {
              this.notifyWayMappings[notifyWay.notify_way] = notifyWay;
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.filterForm.notifyWayHoder = this.$t('通知方式');
        });
    },
    alertConfigStatusDisplay(alertItem) {
      if (!alertItem.active) {
        return this.$t('未启用');
      } else if (alertItem.is_shielded) {
        return this.$t('已屏蔽');
      } else {
        return this.$t('已启用');
      }
    },
    alertConfigStatusIcon(alertItem) {
      if (!alertItem.active) {
        return 'stop-shape';
      } else if (alertItem.is_shielded) {
        return 'exclamation-triangle';
      } else {
        return 'check-circle';
      }
    },
    updateAlertTargetList() {
      let options = {
        query: {
          base: 'alert_config',
        },
      };
      if (this.filterForm.bkBizId) {
        options.query.bk_biz_id = this.filterForm.bkBizId;
      }
      if (this.filterForm.projectId) {
        options.query.project_id = this.filterForm.projectId;
      }
      if (this.filterForm.alertTargetType) {
        options.query.alert_target_type = this.filterForm.alertTargetType;
      }
      this.bkRequest.httpRequest('dmonitorCenter/getMineAlertTargetList', options).then(res => {
        if (res.result) {
          this.filterForm.alertTargetList = res.data;
          this.filterForm.alertTargetList.map(alertTarget => {
            Object.assign(alertTarget, this.formatAlertTargetOption(alertTarget));
            return alertTarget;
          });
        } else {
          this.getMethodWarning(res.message, 'error');
        }
      });
    },
    /**
     * 格式化告警对象选项
     */
    formatAlertTargetOption(alertTarget) {
      if (alertTarget.alert_target_type === 'dataflow') {
        return {
          name: `[${alertTarget.alert_target_id}] ${alertTarget.alert_target_alias}`,
          key: alertTarget.alert_target_id,
        };
      } else {
        return {
          name: `[${alertTarget.alert_target_id}] ${alertTarget.alert_target_alias}`,
          key: `rawdata${alertTarget.alert_target_id}`,
        };
      }
    },
    /**
     * 格式化接收人
     */
    formatReceiver(alertConfig) {
      let users = [];
      for (let receiver of alertConfig.receivers) {
        if (receiver.receiver_type === 'user') {
          users.push(receiver.username);
        }
      }
      return users.join(',');
    },
  },
};
</script>
<style lang="scss">
.bk-option {
  min-width: 80px;
}
.alert-shield-dialog-button {
  margin: 10px 0 30px;
  display: flex;
  justify-content: space-evenly;
  text-align: center;
  .bk-button {
    margin-top: 20px;
    width: 120px;
    margin-right: 15px;
  }
}
.alert-config-list {
  min-width: 1600px;
  .cursor-pointer {
    cursor: pointer;
  }
  .detail {
    color: #3a84ff;
    cursor: pointer;
  }
  .is-disabled {
    color: #666;
    opacity: 0.6;
    cursor: not-allowed;
  }
  .bk-option {
    min-width: 80px;
  }
  .notify-way-icon-list {
    display: flex;
    align-items: center;
    span {
      display: flex;
      margin-right: 2px;
    }
    .notify-way-icon {
      width: 28px;
      height: 28px;
    }
  }
  /*样式覆盖*/
  .bk-dialog {
    .bk-dialog-style {
      width: 100%;
    }
    .content-from {
      margin: 0 auto;
      width: 590px;
    }
  }
  .bk-dialog-footer {
    height: 126px;
  }
  .bk-dialog-footer .bk-dialog-outer {
    text-align: center;
    background: #fff;
    padding: 18px 0 47px;
  }
  .bk-dialog-body {
    padding: 20px 20px 60px;
    overflow-y: auto;
    &::-webkit-scrollbar {
      width: 6px;
      height: 5px;
    }

    &::-webkit-scrollbar-thumb {
      border-radius: 20px;
      background-color: #a5a5a5;
    }
  }
  // min-width: 1200px;
  .header {
    background: #f2f4f9;
    padding: 19px 75px;
    color: #1a1b2d;
    font-size: 16px;
    p {
      line-height: 36px;
      padding-left: 22px;
      position: relative;
      &:before {
        content: '';
        width: 4px;
        height: 20px;
        position: absolute;
        left: 0px;
        top: 8px;
        background: #3a84ff;
      }
    }
  }
  .inquire-content {
    position: relative;
    line-height: 36px;
    padding: 5px 0 5px 0;
    display: flex;
    flex-direction: row;
    flex-wrap: wrap;
    .bk-option {
      min-width: 50px;
    }
    .new-button {
      position: absolute;
      right: 0;
      top: 4px;
      margin: 0;
      height: 36px;
    }
    .inquire {
      margin-right: 10px;
      margin-bottom: 10px;
      &:first-of-type {
        margin-left: 0;
      }
      .bk-select-list {
        z-index: 1005;
      }
      .bk-select-list .bk-select-list-item {
        height: auto;
      }

      .bkdata-selector-input {
        &.placeholder {
          color: inherit;
        }
      }
    }
    .inquire-button {
      float: left;
      margin-left: 22px;
    }
    .title {
      float: left;
      width: 80px;
      text-align: right;
    }
    .business-select,
    .project-select {
      float: left;
      width: 200px;

      .is-opened {
        border-color: #4f5399;
      }

      .el-input__inner {
        border: none;
        height: 36px;
        &::-webkit-input-placeholder {
          color: #666;
          line-height: 32px;
        }
      }
    }
    .alert-target-input {
      float: left;
      width: 150px;
      .bk-form-input {
        display: block;
      }
    }
    .active-select,
    .notify-way-select {
      float: left;
      width: 100px;
    }
    .target-type-select {
      float: left;
      width: 200px;
    }
    .alert-target-select {
      width: 200px;
      float: left;
    }
    .self-radio {
      margin-left: 20px;
    }
    .alert-config-operation {
      height: 30px;
      line-height: 30px;
      margin: 0 0 19px 0px;
      margin-left: auto;
      order: 2;

      .project-right-sort {
        align-items: center;
        display: flex;

        .bk-button {
          border-right-width: 0;
        }
        .bk-button:last-child {
          border-right-width: 1px;
        }
      }
    }
  }
  .bk-loading {
    margin-top: 0px;
  }
  .sideslider-wrapper {
    box-shadow: none !important;
  }
  .data-ip {
    .content-down {
      padding-top: 0px !important;
    }
    position: relative;
  }
  .alert-config-list-table {
    .danger {
      color: #ea3636;
    }
    .warning {
      color: #ff9c01;
    }
    .alert-status-display {
      display: flex;
      align-items: center;
      .bk-icon {
        margin-right: 3px;
      }
      .icon-exclamation-triangle {
        color: #ff9c01;
      }
      .icon-check-circle {
        color: #45e35f;
      }
      span {
        display: flex;
      }
    }
    .alert-table-config-option {
      position: absolute;
      left: 0;
      top: 0;
      bottom: 0;
      right: 0;
      display: flex;
      align-items: center;
      justify-content: flex-start;
      padding-left: 15px;
      .detail-button {
        margin-right: 25px;
      }
      .config-button {
        margin-right: 25px;
      }
      .shield-button {
        margin-right: 25px;
      }
      .del-button-wrap {
        flex: 0.5;
        display: flex;
        align-content: center;
      }
      .operation-button {
        display: flex;
        flex-direction: column;
        justify-content: center;
        width: 80%;
        white-space: nowrap;
        margin-right: 0;
        margin-left: 12.5px;
      }
      .text-overflow {
        display: block;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }
      .red-point {
        position: absolute;
        left: 10px;
        top: 50%;
        transform: translateY(-50%);
        width: 10px;
        height: 10px;
        background: #ea3636;
        border-radius: 50%;
      }
      .del-button {
        border: none;
        color: #ea3636;
      }
      .disabled {
        color: #babddd !important;
        cursor: not-allowed;
      }
      .copy-to {
        color: #3a84ff !important;
      }
      .table-select {
        display: flex;
        flex-direction: column;
        justify-content: center;
        flex: 0.5;
        .bk-selector {
          .bk-selector-list {
            > ul > .bk-selector-list-item > .bk-selector-node {
              .text {
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
              }
            }
          }
          .bk-selector-wrapper {
            .bk-selector-input {
              color: #666bb4;
              border: none;
            }
          }
        }
      }
    }
    .click-style {
      color: #666bb4;
      cursor: pointer;
    }
  }
}
</style>
