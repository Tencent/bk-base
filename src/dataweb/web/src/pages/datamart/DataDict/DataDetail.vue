

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
  <Layout class="data-detail-container"
    :crumbName="titleRoute"
    :withMargin="isSmallScreen">
    <template slot="defineTitle">
      <template v-for="(item, index) in titleRoute">
        {{ (index && '>') || '' }}
        <a
          :key="index"
          href="javascript:void(0);"
          :class="['crumb-item', (item.to && 'with-router') || '']"
          @click.stop="reflshPage(item.to)">
          {{ item.name }}
        </a>
      </template>
    </template>
    <div class="data-dict-detail">
      <div class="data-detail-content">
        <DictParams :rawInfo="rawInfo"
          :isLoading="isLoading"
          :data="paramsData">
          <template :slot="$t('中文名称')">
            <EditInput
              :contentType="'chineseName'"
              :isEdit="isModifyDes"
              :isCancelFieldLoading="isCancelFieldLoading"
              :toolTipsContent="modifyDesTips($t('中文名称'))"
              :inputValue="chineseName[$route.query.dataType]()"
              @change="modify" />
          </template>
          <template :slot="$t('标签')">
            <template v-if="resultTableInfo.tag_list && resultTableInfo.tag_list.length">
              <div class="name-wrap clearfix">
                <span v-for="(item, index) of resultTableInfo.tag_list"
                  :key="index"
                  class="name">
                  {{ $i18n.locale === 'en' ? item.tag_code : item.tag_alias }}
                </span>
              </div>
            </template>
            <template v-else>
              <span>{{ $t('无') }}</span>
            </template>
          </template>
          <template :slot="$t('数据管理员')">
            <template v-if="resultTableInfo.manager && resultTableInfo.manager.length">
              <div class="name-wrap clearfix">
                <span v-for="(item, index) of resultTableInfo.manager"
                  :key="index"
                  class="name">
                  {{ item }}
                </span>
              </div>
            </template>
            <template v-else>
              <span />
            </template>
          </template>
          <template :slot="description[$route.query.dataType]">
            <EditInput
              :contentType="'description'"
              :isEdit="isModifyDes"
              :isCancelFieldLoading="isCancelFieldLoading"
              :toolTipsContent="modifyDesTips(description[$route.query.dataType])"
              :inputValue="resultTableInfo.description"
              @change="modify" />
          </template>
          <!-- 业务运维 start -->
          <template v-if="$route.query.dataType !== 'tdw_table'">
            <template :slot="`${$t('业务运维')}-label`">
              <div class="biz-label-name">
                {{ dataSetSensitivityInfo.biz_role_name }}
                <span class="biz-label-des"> （{{ $t('默认具有管理权限') }}） </span>
              </div>
            </template>
            <template :slot="`${$t('业务运维')}`">
              <div class="name-wrap clearfix">
                <span v-for="(item, index) of dataSetSensitivityInfo.biz_role_memebers"
                  :key="index"
                  class="name">
                  {{ item }}
                </span>
              </div>
            </template>
          </template>
          <!-- 业务运维 end -->
          <template :slot="$t('数据观察员')">
            <template v-if="resultTableInfo.viewer && resultTableInfo.viewer.length">
              <div class="name-wrap clearfix">
                <span v-for="(item, index) of resultTableInfo.viewer"
                  :key="index"
                  class="name">
                  {{ item }}
                </span>
              </div>
            </template>
            <template v-else>
              <span>{{ $t('无') }}</span>
            </template>
          </template>
          <template :slot="$t('权限')"
            slot-scope="item">
            <div class="flex-normal">
              <!-- // 1-有 2-无，权限正在申请中 3-无，申请 -->
              <span v-if="item.slot.value === 1"
                style="color: #2dcb56">
                {{ $t('有') }}
              </span>
              <span v-else-if="item.slot.value === 2"
                class="disabled">
                {{ $t('无 权限正在申请中') }}
              </span>
              <span v-else>
                {{ $t('无') }}
                <a href="javascript:void(0);"
                  @click="applyPermission">
                  {{ $t('申请') }}
                </a>
              </span>
              <i
                v-if="resultTableInfo.no_pers_reason"
                v-bk-tooltips.top="resultTableInfo.no_pers_reason"
                class="bk-icon note icon-info-circle" />
            </div>
          </template>
          <template :slot="$t('所属业务')">
            <div
              class="text-overflow"
              :title="
                `[${resultTableInfo.bk_biz_id}] ${resultTableInfo.bk_biz_name
                  ? resultTableInfo.bk_biz_name : ''}`
              ">
              <span v-if="resultTableInfo.bk_biz_id"
                class="font-weight7">
                [{{ resultTableInfo.bk_biz_id ? resultTableInfo.bk_biz_id : '' }}]
              </span>
              {{ resultTableInfo.bk_biz_name ? resultTableInfo.bk_biz_name : '' }}
            </div>
          </template>
          <template :slot="$t('所属项目')">
            <div
              v-if="$route.query.dataType === 'result_table'"
              class="text-overflow"
              :title="
                `[${resultTableInfo.project_id}] ${resultTableInfo.project_name
                  ? resultTableInfo.project_name : ''}`
              ">
              <span v-if="resultTableInfo.project_id"
                class="font-weight7">
                [{{ resultTableInfo.project_id ? resultTableInfo.project_id : '' }}]
              </span>
              {{ resultTableInfo.project_name ? resultTableInfo.project_name : '' }}
            </div>
            <div
              v-if="$route.query.dataType === 'raw_data'"
              class="text-overflow"
              :title="`[4] ${resultTableInfo.project_name ? resultTableInfo.project_name : ''}`">
              <span class="font-weight7">[4] </span>
              {{ resultTableInfo.project_name ? resultTableInfo.project_name : '' }}
            </div>
          </template>
          <template :slot="$t('关联结果表')">
            <div
              v-if="resultTableInfo.result_table_id"
              class="can-click text-overflow"
              :title="``"
              @click="linkToResultDetail(resultTableInfo.result_table_id)">
              <span :title="resultTableInfo.result_table_id"
                class="text-overflow">
                {{ resultTableInfo.result_table_id }}
              </span>
              <i v-bk-tooltips.top="$t('已关联到IEG数据平台的结果表')"
                class="icon-info-circle" />
            </div>
            <span v-else
              style="color: #737987">
              {{ $t('无') }}
            </span>
          </template>
          <template :slot="$t('标准名称')">
            <div
              class="text-overflow bk-click-style"
              :title="resultTableInfo.standard_name"
              @click="linkToStandardDetail(false)">
              {{ resultTableInfo.standard_name }}
            </div>
          </template>
          <template :slot="$t('标准内容')">
            <div
              class="text-overflow"
              :class="{ 'bk-click-style': resultTableInfo.standard_content_name !== $t('无') }"
              :title="resultTableInfo.standard_content_name"
              @click="linkToStandardDetail(true)">
              {{ resultTableInfo.standard_content_name }}
            </div>
          </template>
          <template :slot="$t('数据敏感度')">
            <sensitive-block
              :isShowEdit="$route.query.dataType !== 'tdw_table'"
              :disabled="!isModifyDes"
              :warningClass="sensitivityId"
              :tagMethod="dataSetSensitivityInfo.tag_method"
              :text="sensitiveObj[sensitivityId]"
              @edit="editSensitivity" />
          </template>
          <template :slot="$t('元数据状态')">
            <span
              :style="{
                'line-height': '32px',
                display: 'block',
                color: getColor(resultTableInfo.meta_status),
              }">
              {{ safeValue(resultTableInfo.meta_status_message) }}
            </span>
          </template>
          <template :slot="'lifeTime-wraper'">
            <bkdata-container v-if="renderTraceData.length"
              v-bkloading="{ isLoading: isFootPointLoading }"
              :col="12">
              <bk-row>
                <bk-col :span="12">
                  <bk-timeline :class="{ 'trace-timeline': traceTimeline.hasMore }"
                    :list="renderTraceData">
                    <template v-for="(item, index) in renderTraceData"
                      :slot="`title${index}`"
                      slot-scope="data">
                      <div :key="index"
                        class="bk-timeline-title">
                        <template v-if="traceTimeline.hasMore
                          && index === renderTraceData.length - 1">
                          <bkdata-button
                            theme="primary"
                            text
                            @click.stop="traceTimeline.toggle = !traceTimeline.toggle">
                            {{ traceTimeline.toggle ? $t('收起') : $t('更多信息') }}
                            <i
                              :class="[
                                traceTimeline.toggle
                                  ? 'icon-angle-double-up-delete'
                                  : 'icon-angle-double-down-delete',
                              ]" />
                          </bkdata-button>
                        </template>
                        <template v-else>
                          <span v-if="data.hasJumpTarget">
                            {{ data.tag.split(data.tpl)[0] }}
                            <bkdata-button theme="primary"
                              text
                              @click="handleGoFlow(traceData[index].details.jump_to)">
                              [{{ traceData[index].details.jump_to.flow_id }}]
                            </bkdata-button>
                            {{ data.tag.split(data.tpl)[1] }}
                          </span>
                          <div v-else-if="data.showType === 'add_tips'"
                            v-bk-tooltips="data.tips">
                            <template v-if="data.typeAlias && data.subTypeAlias">
                              [{{ data.typeAlias }}]
                              <span style="border-bottom: 1px dashed #979ba5">
                                {{ data.subTypeAlias }}
                              </span>
                            </template>
                            <template v-else>
                              --
                            </template>
                          </div>
                          <span v-else>{{ data.tag }}</span>
                          <bkdata-button
                            v-if="data.hasDetails"
                            theme="primary"
                            text
                            @click.stop="handleToggleDetails(index)">
                            {{ $t('详情') }}
                            <i
                              :class="[
                                data.showDetails
                                  ? 'icon-angle-double-up-delete'
                                  : 'icon-angle-double-down-delete',
                              ]" />
                          </bkdata-button>
                        </template>
                      </div>
                    </template>
                  </bk-timeline>
                </bk-col>
              </bk-row>
            </bkdata-container>
            <span v-else />
          </template>
        </DictParams>
        <dictionContent-right
          v-if="isShowContentRight"
          :resultTableInfo="resultTableInfo"
          :isPermissionProcessing="isPermissionProcessing"
          :footData="footData"
          :hasPermission="isHasPermission"
          @changeFieldDes="changeFieldDes" />
      </div>
      <PermissionApplyWindow ref="apply" />
      <ModifySensitivity
        ref="modifySensitivity"
        :sensitivity="dataSetSensitivityInfo.sensitivity_id"
        :manager="resultTableInfo.manager"
        :data-set-id="dataSetId"
        :data-type="$route.query.dataType"
        :bkBizId="$route.query.bk_biz_id"
        @changeSensitivityStatus="changeSensitivityStatus" />
    </div>
  </Layout>
</template>
<script>
import { sensitiveObj } from '@/pages/datamart/common/config';
import dictionContentRight from './components/dictionContentRight';
import DictParams from './components/children/DictParams';
import SensitiveBlock from './components/children/SensitiveBlock';
import Layout from '@/components/global/layout';
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow';
import EditInput from './components/children/EditInput';
import Bus from '@/common/js/bus.js';
import { postMethodWarning } from '@/common/js/util';
import ModifySensitivity from './components/children/ModifySensitivity/index';
import { bkTimeline, bkCol, bkRow } from 'bk-magic-vue';
import { dataStatus } from '@/pages/datamart/common/config';

export default {
  components: {
    dictionContentRight,
    DictParams,
    SensitiveBlock,
    Layout,
    PermissionApplyWindow,
    EditInput,
    ModifySensitivity,
    bkTimeline,
    bkCol,
    bkRow,
  },
  data() {
    return {
      resultTableInfo: {
        tag_list: [],
        table_name: '',
        result_table_name: '',
        raw_data_name: '',
      },
      isLoading: false,
      rawInfo: {},
      isHasPermission: false,
      isPermissionProcessing: false,
      isShowContentRight: true,
      description: {
        result_table: this.$t('数据描述'),
        raw_data: this.$t('数据源描述'),
        tdw_table: this.$t('数据描述'),
      },
      chineseName: {
        tdw_table: () => this.resultTableInfo.description,
        result_table: () => this.resultTableInfo.result_table_name_alias,
        raw_data: () => this.resultTableInfo.raw_data_alias,
      },
      isCancelFieldLoading: false,
      dataStandard: {},
      dataSetSensitivityInfo: {
        sensitivity_id: '',
      },
      isFootPointLoading: false,
      traceData: [],
      traceTimeline: {
        limit: 4,
        hasMore: false,
        toggle: false,
      },
      footData: [],
      basicinfo: {
        title: this.$t('基本信息'),
        list: [],
      },
      dataFootPoint: {
        title: this.$t('数据足迹'),
        list: [
          {
            value: '',
            name: this.$t('元数据状态'),
          },
          {
            value: '',
            name: 'lifeTime',
          },
        ],
      },
      permissionInfo: {
        title: this.$t('数据权限'),
        list: [
          {
            value: '',
            name: this.$t('业务运维'),
          },
          {
            value: '',
            name: this.$t('数据管理员'),
          },
          {
            value: '',
            name: this.$t('数据观察员'),
          },
        ],
      },
    };
  },
  computed: {
    sensitiveObj() {
      return sensitiveObj;
    },
    dataSetId() {
      const ids = {
        raw_data: 'data_id',
        result_table: 'result_table_id',
        tdw_table: 'tdw_table_id',
      };
      return this.$route.query[ids[this.$route.query.dataType]];
    },
    isSmallScreen() {
      return document.body.clientWidth < 1441;
    },
    detailId() {
      if (this.$route.query.dataType === 'result_table') {
        return this.$route.query.result_table_id;
      } else if (this.$route.query.dataType === 'tdw_table') {
        return this.$route.query.tdw_table_id;
      } else {
        return this.$route.query.data_id;
      }
    },
    titleRoute() {
      return [
        { name: this.$t('数据字典'), to: '/data-mart/data-dictionary' },
        { name: `${this.$t('数据字典详情')}(${this.detailId})` },
      ];
    },
    isModifyDes() {
      return this.resultTableInfo.meta_status === 'success' && this.resultTableInfo.has_write_permission;
    },
    sensitivityId() {
      return this.dataSetSensitivityInfo.sensitivity_id || this.resultTableInfo.sensitivity;
    },
    paramsData() {
      return [this.rawInfo, this.basicinfo, this.dataStandard, this.dataFootPoint, this.permissionInfo];
    },
    traceDataFormatter() {
      const list = [];
      this.traceData.forEach(child => {
        const tag =          child.type_alias && child.sub_type_alias
          ? `[${child.type_alias}] ${child.sub_type_alias}`
          : child.type_alias || child.sub_type_alias || '--';
        const resItem = {
          tag,
          content: '',
          color: 'green',
          filled: true,
          typeAlias: child.type_alias,
          subTypeAlias: child.sub_type_alias,
          hasDetails: child.hasDetails,
          showDetails: child.showDetails,
          showType: child.show_type,
        };
        let content = '';
        const defaultContent = `<span class="timeline-update-time">${$t('时间')}：${child.datetime
          || '--'}</span> <span class="timeline-update-time">${$t('操作人')}：${child.created_by || '--'}</span>`;
        switch (child.show_type) {
          case 'add_tips':
            content = defaultContent;
            const hasJumpTarget = child.type === 'update_task_status';
            if (hasJumpTarget) {
              resItem.hasJumpTarget = true;
              resItem.tpl = '{change_content}';
              resItem.tag = child.details.desc_tpl || '';
            } else {
              let tips = [];
              const descList = (child.details && child.details.desc_params) || [];
              descList.forEach(desc => {
                let kvTpl = child.details.kv_tpl;
                Object.keys(desc).forEach(key => {
                  kvTpl = kvTpl.replace(`{${key}}`, desc[key]);
                });
                tips.push(kvTpl);
              });
              resItem.tips = {
                content: tips.join('<br />'),
                placement: 'right',
              };
            }
            break;
          case 'group':
            resItem.tag = `[${child.type_alias}] ${child.status_alias}`;
            const style = {
              migrate_failed: {
                color: 'red',
              },
              migrate_start: {
                color: 'blue',
              },
              migrate_finish: {
                color: 'green',
              },
            };
            resItem.color = style[child.status].color;
            if (child.hasDetails && child.showDetails) {
              const h = this.$createElement;
              content = h(bkTimeline, {
                props: {
                  list: child.sub_events.map(sub => {
                    return {
                      color: style[sub.sub_type].color,
                      filled: true,
                      tag: `<span class="timeline-update-time">${sub.sub_type_alias}</span>`,
                      content: `<span class="timeline-update-time">${$t('时间')}：${sub.datetime
                        || '--'}</span> <span class="timeline-update-time">${$t('操作人')}：${sub.created_by
                        || '--'}</span>`,
                    };
                  }),
                },
              });
            } else {
              content = defaultContent;
            }
            break;
          default:
            content = defaultContent;
            break;
        }
        resItem.content = content;
        list.push(resItem);
      });
      return list;
    },
    renderTraceData() {
      let renderList = [];
      const lastItem = {};
      if (this.traceTimeline.hasMore) {
        renderList = this.traceTimeline.toggle
          ? [...this.traceDataFormatter]
          : this.traceDataFormatter.slice(0, this.traceTimeline.limit);
        renderList.push(lastItem);
      } else {
        renderList = [...this.traceDataFormatter];
      }
      return renderList;
    },
  },
  mounted() {
    Bus.$on('reloadDataDetail', query => {
      this.isShowContentRight = false;
      this.getDetailInfo();
      this.$nextTick(() => {
        this.isShowContentRight = true;
      });
    });
    Bus.$on('changeFieldInfo', (value, index) => {
      this.resultTableInfo.fields.find(item => item.col_index === index).description = value;
      // this.resultTableInfo.fields[index].description = value
    });
    if (this.$route.query.dataType !== 'tdw_table') {
      this.getDataSetSensitivity();
    }
    this.getDetailInfo();
    this.getFootPointData();
  },
  methods: {
    getColor(status) {
      return status === 'success' ? '#2dcb56' : '#ea3636';
    },
    getFootPointData() {
      this.isFootPointLoading = true;
      this.bkRequest
        .httpRequest('dataDict/getDataTraces', {
          query: {
            dataset_type: this.$route.query.dataType,
            dataset_id: this.dataSetId,
          },
        })
        .then(res => {
          console.log(res);
          if (res.result && res.data.length) {
            this.footData = res.data;
            this.traceData = res.data.map(item => ({
              ...item,
              hasDetails: !!(item.sub_events && item.sub_events.length),
              showDetails: false,
            }));
            this.traceTimeline.hasMore = this.traceData.length > this.traceTimeline.limit;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isFootPointLoading = false;
        });
    },
    handleToggleDetails(index) {
      const cur = this.traceData[index];
      if (cur) {
        this.traceData.splice(index, 1, Object.assign({}, cur, { showDetails: !cur.showDetails }));
      }
    },
    handleGoFlow(data) {
      let res = confirm(window.gVue.$t('离开此页面_将前往对应任务详情'));
      if (!res) return;
      this.$router.push({
        name: 'dataflow_ide',
        params: {
          fid: data.flow_id,
        },
        query: {
          project_id: data.project_id,
        },
      });
    },
    changeSensitivityStatus(secretLevel, relueMode) {
      this.dataSetSensitivityInfo['tag_method'] = relueMode;
      this.dataSetSensitivityInfo['sensitivity_id'] = secretLevel;
    },
    editSensitivity() {
      this.$refs.modifySensitivity.open(this.dataSetSensitivityInfo.tag_method);
    },
    getDataSetSensitivity() {
      return this.bkRequest
        .httpRequest('dataDict/getDataSetSensitivity', {
          query: {
            data_set_type: this.$route.query.dataType,
            data_set_id: this.dataSetId,
          },
        })
        .then(res => {
          if (res.result) {
            this.dataSetSensitivityInfo = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    linkToStandardDetail(isShowContent) {
      if (isShowContent && this.resultTableInfo.standard_content_name === $t('无')) return;
      let contentId = null;
      if (isShowContent) {
        contentId = this.resultTableInfo.standard_content_id;
      }
      this.$router.push({
        name: 'DataStandardDetail',
        query: {
          id: this.resultTableInfo.standard_id,
          contentId,
        },
      });
    },
    modifyDesTips(keyWord = '') {
      return this.isModifyDes
        ? `${this.$t('点击修改')}${keyWord}`
        : this.resultTableInfo.no_write_pers_reason || this.$t('权限不足');
    },
    reflshPage(route) {
      this.$router.push(route);
      Bus.$emit('dataMarketReflshPage');
    },
    linkToDes() {
      if (!this.isModifyDes) return;
      let res = confirm(window.gVue.$t('即将离开此页面'));
      if (!res) return;
      if (this.$route.query.dataType === 'result_table') {
        this.$router.push({
          name: 'routeHub',
          query: {
            rtid: this.$route.query.result_table_id,
          },
        });
      } else if (this.$route.query.dataType === 'raw_data') {
        this.$router.push({
          name: 'data_detail',
          params: {
            did: this.$route.query.data_id,
          },
          hash: '#list',
          query: { page: 1 },
        });
      }
    },
    modify(value, rowData, contentType) {
      this.isCancelFieldLoading = false;
      const type = this.$route.query.dataType;
      const urls = {
        raw_data: 'dataDict/modifyRawTableDes',
        result_table: 'dataDict/modifyResultTableDes',
        tdw_table: 'dataDict/modifyTdwTableDes',
      };
      const options = {
        raw_data: {
          params: {
            raw_data_id: this.$route.query.data_id,
          },
        },
        result_table: {
          params: {
            result_table_id: this.$route.query.result_table_id,
            bk_username: this.$store.getters.getUserName,
          },
        },
        tdw_table: {
          params: {
            cluster_id: this.resultTableInfo.cluster_id,
            db_name: this.resultTableInfo.db_name,
            table_name: this.resultTableInfo.table_name,
            comment: value,
            bk_username: this.$store.getters.getUserName,
          },
        },
      };
      if (contentType === 'chineseName') {
        // 中文名
        if (type === 'result_table') {
          options[type].params.result_table_name_alias = value;
        } else if (type === 'raw_data') {
          options[type].params.raw_data_alias = value;
        }
      } else if (contentType === 'description') {
        options[type].params.description = value;
      }
      this.bkRequest
        .httpRequest(urls[type], options[type])
        .then(res => {
          if (res.result) {
            if (contentType === 'chineseName') {
              // 中文名
              if (type === 'result_table') {
                this.resultTableInfo.result_table_name_alias = value;
              } else if (type === 'raw_data') {
                this.resultTableInfo.raw_data_alias = value;
              }
            } else if (contentType === 'description') {
              this.resultTableInfo.description = value;
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isCancelFieldLoading = true;
        });
    },
    changeFieldDes(value, index) {
      this.resultTableInfo.fields[index].description = value;
    },
    linkToResultDetail(id) {
      this.$router.push({
        name: 'DataDetail',
        query: {
          dataType: 'result_table',
          result_table_id: id,
        },
      });
      this.getDetailInfo(id);
    },
    applyPermission() {
      let roleId, id;
      if (this.$route.query.dataType === 'raw_data') {
        roleId = 'raw_data.viewer';
        id = this.$route.query.data_id;
      } else if (this.$route.query.dataType === 'result_table') {
        roleId = 'result_table.viewer';
        id = this.$route.query.result_table_id;
      } else if (this.$route.query.dataType === 'tdw_table') {
        return window.open(window.TDWSiteUrl + 'db_table?s_menu=db');
      }
      this.$refs.apply.openDialog({
        data_set_type: this.$route.query.dataType,
        bk_biz_id: this.$route.query.bk_biz_id || this.resultTableInfo.bk_biz_id,
        data_set_id: id,
        roleId: roleId,
      });
    },
    safeValue(value) {
      return value || this.$t('无');
    },
    getDetailInfo(resultTableId) {
      this.rawInfo = {};
      this.dataStandard = {};
      let dataType = this.$route.query.dataType;
      let id = 'result_table_id';
      if (this.$route.query.dataType === 'raw_data') {
        id = 'data_id';
      } else if (this.$route.query.dataType === 'tdw_table' && this.$route.query.tdw_table_id) {
        id = 'tdw_table_id';
        dataType = 'TDW';
      }

      this.isLoading = true;
      let options = {
        query: {
          [id]: this.$route.query[id],
          data_set_type: dataType,
        },
      };
      if (this.$route.query.standard_id) {
        // 路由参数携带标准id
        options.query.standard_id = this.$route.query.standard_id;
      }
      if (resultTableId) {
        // 跳转到TDW关联的结果表详情
        options.query = {
          data_set_type: 'result_table',
          result_table_id: resultTableId,
        };
      }
      this.bkRequest.httpRequest('dataDict/getResultInfoDetail', options).then(res => {
        if (res.result) {
          if (!Object.keys(res.data).length) {
            postMethodWarning(this.$t('数据平台不存在该数据'), 'warning');
          }
          this.resultTableInfo = res.data;
          // 国际化
          this.resultTableInfo.processing_type_alias =            this.$i18n.locale === 'en'
            ? this.resultTableInfo.processing_type
            : this.resultTableInfo.processing_type_alias;
          this.isHasPermission = this.resultTableInfo.has_permission;
          this.isPermissionProcessing = this.resultTableInfo.ticket_status
            ? this.resultTableInfo.ticket_status === 'processing'
            : false;
          if (this.$route.query.dataType === 'raw_data') {
            this.rawInfo = {
              title: this.$t('来源信息'),
              list: [
                {
                  value: this.safeValue(this.resultTableInfo.data_scenario_alias),
                  name: this.$t('接入类型'),
                },
                {
                  value: this.safeValue(this.resultTableInfo.bk_app_code_alias),
                  name: this.$t('接入渠道'),
                },
                {
                  value: this.resultTableInfo.data_source_alias,
                  name: this.$t('数据来源'),
                },
                {
                  value: this.safeValue(this.resultTableInfo.data_encoding),
                  name: this.$t('字符集编码'),
                },
              ],
            };
          }
          this.basicinfo = {
            title: this.$t('基本信息'),
            list: [
              {
                value: this.$route.query.tdw_table_id
                  ? this.resultTableInfo.table_id
                  : this.$route.query.dataType === 'result_table'
                    ? this.safeValue(this.resultTableInfo.result_table_id)
                    : this.safeValue(this.resultTableInfo.id),
                name: this.$t('数据ID'),
              },
              {
                value: this.safeValue(
                  this.$route.query.tdw_table_id
                    ? this.resultTableInfo.table_name
                    : this.resultTableInfo.result_table_name || this.resultTableInfo.raw_data_name
                ),
                name: this.$t('数据名称'),
              },
              {
                value: this.safeValue(
                  this.$route.query.tdw_table_id
                    ? this.resultTableInfo.description
                    : this.resultTableInfo.result_table_name_alias
                                        || this.resultTableInfo.raw_data_alias
                ),
                name: this.$t('中文名称'),
              },
              {
                value: this.safeValue(this.resultTableInfo.description),
                name: this.description[this.$route.query.dataType],
              },
              {
                value:
                  this.$route.query.dataType !== 'raw_data'
                    ? this.resultTableInfo.processing_type_alias
                      ? `${this.resultTableInfo.processing_type_alias} ${this.$t('结果表')}`
                      : this.$t('结果表')
                    : this.$t('数据源'),
                name: this.$t('类型'),
              },
              {
                value: this.safeValue(this.resultTableInfo.platform),
                name: this.$t('数据所在系统'),
              },
              {
                value:
                  (this.resultTableInfo.bk_biz_id ? `【${this.resultTableInfo.bk_biz_id}】` : '')
                  + `${this.resultTableInfo.bk_biz_name ? this.resultTableInfo.bk_biz_name : ''}`,
                name: this.$t('所属业务'),
              },
              {
                value:
                  this.$route.query.dataType === 'result_table'
                    ? (this.resultTableInfo.project_id ? `【${this.resultTableInfo.project_id}】` : '')
                      + `${this.resultTableInfo.project_name ? this.resultTableInfo.project_name : ''}`
                    : `【4】${this.resultTableInfo.project_name}`,
                name: this.$t('所属项目'),
              },
              {
                value: this.safeValue(this.resultTableInfo.tag_list),
                name: this.$t('标签'),
              },
              {
                value: this.safeValue(this.resultTableInfo.cluster_alias),
                name: this.$t('集群'),
              },
              {
                value: this.safeValue(this.resultTableInfo.db_name),
                name: this.$t('数据库'),
              },
            ],
          };
          // TDW 增加的字段 渠道、集群、数据库 如果当前数据不是tdw，就过滤掉这几个字段
          if (
            (this.resultTableInfo.platform && this.resultTableInfo.platform !== 'TDW')
            || this.$route.query.dataType === 'raw_data'
          ) {
            this.basicinfo.list = this.basicinfo.list.filter(
              item => ![this.$t('数据所在系统'), this.$t('集群'), this.$t('数据库')].includes(item.name)
            );
          } else if (this.resultTableInfo.platform && this.resultTableInfo.platform === 'TDW') {
            this.basicinfo.list = this.basicinfo.list.filter(
              item => ![this.$t('所属项目'), this.$t('中文名称')].includes(item.name)
            );
          }

          if (this.resultTableInfo.standard_id && this.$modules.isActive('standard')) {
            this.resultTableInfo.standard_content_name = this.resultTableInfo.standard_content_name
                        || $t('无');
            this.dataStandard = {
              title: this.$t('数据标准'),
              list: [
                {
                  value: this.resultTableInfo.standard_name,
                  name: this.$t('标准名称'),
                },
                {
                  value: this.resultTableInfo.standard_description,
                  name: this.$t('标准描述'),
                },
                {
                  value: this.resultTableInfo.standard_content_name,
                  name: this.$t('标准内容'),
                },
                {
                  value: this.resultTableInfo.standard_version,
                  name: this.$t('版本'),
                },
              ],
            };
          }
          this.permissionInfo = {
            title: this.$t('数据权限'),
            list: [
              {
                value: '',
                name: this.$t('业务运维'),
              },
              {
                value: '',
                name: this.$t('数据管理员'),
              },
              {
                value: '',
                name: this.$t('数据观察员'),
              },
              {
                value: this.safeValue(this.resultTableInfo.sensitivity),
                name: this.$t('数据敏感度'),
              },
              {
                // 1-有 2-无，权限正在申请中 3-无，申请
                value: this.resultTableInfo.has_permission
                  ? 1
                  : this.resultTableInfo.ticket_status === 'processing'
                    ? 2
                    : 3,
                name: this.$t('权限'),
              },
              {
                value: this.resultTableInfo.result_table_id,
                name: this.$t('关联结果表'),
              },
            ],
          };
          // TDW 去掉数据管理员、数据观察员、业务运维，增加关联结果表  如果当前数据不是tdw，就过滤掉这几个字段
          if (
            (this.resultTableInfo.platform && this.resultTableInfo.platform !== 'TDW')
            || this.$route.query.dataType === 'raw_data'
          ) {
            this.permissionInfo.list = this.permissionInfo.list.filter(
              item => ![this.$t('关联结果表')].includes(item.name)
            );
          } else {
            this.permissionInfo.list = this.permissionInfo.list.filter(
              item => ![this.$t('数据管理员'), this.$t('数据观察员'), this.$t('业务运维')].includes(item.name)
            );
          }
          this.dataFootPoint.list.find(item => item.name === this.$t('元数据状态')).value = this.safeValue(
            this.resultTableInfo.meta_status_message
          );
        } else {
          this.getMethodWarning(res.message, res.code);
        }
        this.isLoading = false;
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.trace-timeline {
  ::v-deep {
    .bk-timeline-dot {
      padding-bottom: 14px;
    }
    > .bk-timeline-dot:last-child {
      margin-top: 0;
      &::before {
        display: none;
      }
      .bk-timeline-section {
        top: -16px;
        .bk-timeline-title {
          padding-bottom: 0;
        }
      }
    }
    .bk-timeline-section .bk-timeline-dot::before {
      width: 9px;
      height: 9px;
      left: -5px;
      top: -11px;
    }
  }
}
::v-deep .timeline-update-time {
  font-size: 12px;
  color: #979ba5;
}
::v-deep .timeline-content {
  p {
    margin-top: 0;
    margin-bottom: 8px;
  }
}
::v-deep .bk-grid-container {
  width: 100%;
  background-color: white;
  padding: 20px 0 0 0;
}
.ml-2 {
  margin-left: 2px;
}
.is-disabled {
  color: #666 !important;
  opacity: 0.6 !important;
  cursor: not-allowed !important;
}
.flex-normal {
  display: flex;
  justify-content: space-between;
  align-items: center;
  .icon-info-circle {
    margin-right: 10px;
  }
}
.name-wrap {
  padding-bottom: 4px;
}
.font-weight7 {
  font-weight: 700;
}
.data-detail-container {
  ::v-deep .layout-header {
    padding: 0 30px !important;
  }
  ::v-deep .layout-body {
    .layout-content {
      height: 100%;
    }
  }
}
.data-dict-detail {
  // padding: 0 55px;
  height: calc(100% - 60px);
  color: #63656e;
  width: 1600px;
  margin: 0 auto;
  .disabled {
    color: #babddd !important;
    cursor: not-allowed !important;
    pointer-events: none;
  }
  .data-detail-content {
    display: flex;
    flex-wrap: nowrap;
    margin: 0 auto;
    width: 1600px;
    justify-content: center;
    ::v-deep template {
      width: 100%;
      .text-overflow {
        display: block;
      }
    }
    .biz-label-name {
      display: flex;
      flex-direction: column;
      justify-content: center;
      align-items: center;
      padding: 5px 0;
      color: #63656e;
      font-weight: bold;
      .biz-label-des {
        font-size: 12px;
        font-weight: normal;
      }
    }
    .can-click {
      color: #3a84ff;
      display: flex !important;
      justify-content: space-between;
      align-items: center;
      > span {
        cursor: pointer;
      }
      .icon-info-circle {
        margin-right: 10px;
        color: #737987;
      }
    }
    .bk-form-textarea {
      margin: 5px 5px 5px 0px;
    }
    .dict-edit-des {
      display: flex;
      justify-content: space-between;
      align-items: center;
      .icon-edit {
        margin-right: 10px;
        color: #3a84ff;
        cursor: pointer;
      }
    }
  }
}
</style>
