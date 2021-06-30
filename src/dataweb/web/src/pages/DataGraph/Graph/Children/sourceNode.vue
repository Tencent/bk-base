

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
  <bkdata-tab :active="activeTab"
    :class="'node-editing'">
    <bkdata-tab-panel :label="$t('节点配置')"
      name="config">
      <div v-bkloading="{ isLoading: loading }"
        class="bk-form">
        <!--原始数据节点编辑 -->
        <div class="add-data-source node">
          <div class="content-up">
            <div class="bk-form-item sideslider-select">
              <label class="bk-label">{{ $t('所属业务') }}</label>
              <div class="bk-form-content">
                <bkdata-selector
                  :isLoading="bizLoading"
                  :placeholder="$t('请选择')"
                  :displayKey="'bk_biz_name'"
                  :list="bizList"
                  :searchKey="'bk_biz_name'"
                  :searchable="true"
                  :selected.sync="params.config.bk_biz_id"
                  :settingKey="'bk_biz_id'">
                  <template slot="bottom-option">
                    <div class="bkSelector-bottom-options">
                      <div class="bkdata-selector-create-item bkSelector-bottom-option"
                        @click="applyData">
                        <span class="text"
                          style="font-size: 12px">
                          {{ $t('申请业务数据') }}
                        </span>
                      </div>
                      <div
                        v-if="nodeType === 'tdw_source'"
                        class="bkdata-selector-create-item bkSelector-bottom-option"
                        @click="addDataTdw('add')">
                        <span class="text"
                          style="font-size: 12px">
                          {{ $t('新增TDW表') }}
                        </span>
                      </div>
                      <div v-else
                        class="bk-selector-create-item bkSelector-bottom-option"
                        @click="addDataId">
                        <span class="text"
                          style="font-size: 12px">
                          {{ $t('新接入数据源') }}
                        </span>
                      </div>
                    </div>
                  </template>
                </bkdata-selector>
              </div>
            </div>
            <div class="bk-form-item sideslider-select">
              <label class="bk-label">
                <bkdata-popover v-if="nodeType === 'tdw_source'"
                  class="item"
                  effect="dark"
                  placement="top">
                  <div slot="content"
                    class="tooltip time-tooltip">
                    {{ $t('目前仅支持周期类型为小时和日的TDW表') }}
                  </div>
                  <span class="note">
                    <i class="bk-icon icon-info-circle" />
                  </span>
                </bkdata-popover>
                <span>{{ $t('数据源表') }}</span>
              </label>
              <div class="bk-form-content">
                <bkdata-selector
                  :isLoading="resultData.disabled"
                  :displayKey="'show_name'"
                  :hasChildren="true"
                  :list="calcAuthRslList"
                  :placeholder="resultData.placeholder"
                  :searchKey="'show_name'"
                  :searchable="filterable"
                  :selected.sync="params.config.result_table_id"
                  :settingKey="'tb_id'"
                  @item-selected="getLatestMsg">
                  <template slot="bottom-option">
                    <div class="bkSelector-bottom-options">
                      <div class="bkdata-selector-create-item bkSelector-bottom-option"
                        @click="applyData">
                        <span class="text"
                          style="font-size: 12px">
                          {{ $t('申请业务数据') }}
                        </span>
                      </div>
                      <div
                        v-if="nodeType === 'tdw_source'"
                        class="bkdata-selector-create-item bkSelector-bottom-option"
                        @click="addDataTdw('add')">
                        <span class="text"
                          style="font-size: 12px">
                          {{ $t('新增TDW表') }}
                        </span>
                      </div>
                      <div v-else
                        class="bk-selector-create-item bkSelector-bottom-option"
                        @click="addDataId">
                        <span class="text"
                          style="font-size: 12px">
                          {{ $t('新接入数据源') }}
                        </span>
                      </div>
                    </div>
                  </template>
                </bkdata-selector>
              </div>
              <div class="iconWrapper">
                <goto-icon
                  v-if="params.config.result_table_id && isCleanNode"
                  :content="$t('跳转到清洗列表')"
                  @click="linkToCleanList" />
                <goto-icon
                  v-if="params.config.result_table_id && !isCleanNode"
                  :content="$t('跳转到任务详情')"
                  @click="linkToCleanList" />
                <i
                  v-if="params.config.result_table_id && nodeType === 'tdw_source'"
                  class="icons bk-icon icon-edit"
                  style="float: right; width: 30px"
                  @click="addDataTdw('edit')" />
              </div>
            </div>
            <div v-show="params.config.result_table_id"
              class="bk-form-item sideslider-select">
              <label class="bk-label">{{ $t('中文名称') }}</label>
              <div class="bk-form-content">
                <bkdata-input v-model="params.config.name"
                  :disabled="true"
                  class="is-disabled" />
              </div>
            </div>
          </div>
        </div>
        <div v-if="nodeType === 'tdw_source' && params.config.result_table_id"
          class="content-down">
          <StorageTypeFieldsConfig
            :clusterType="'tdw'"
            :forceAll="!storageConfig.map(item => `${item}_storage`).includes(nodeType)"
            :isNewform="!selfConfig.hasOwnProperty('node_id')"
            :rtId="params.config.result_table_id"
            :withConfiguration="false" />
        </div>
        <div
          v-else-if="params.config.result_table_id"
          v-bkloading="{ isLoading: buttonStatus.isDisabled }"
          class="content-down">
          <fieldset disabled="disabled">
            <legend>{{ $t('最近一条数据') }}</legend>
            <div class="lastest-data-content">
              <pre class="data">{{ latestMsg.msg }}</pre>
            </div>
          </fieldset>
        </div>
      </div>
    </bkdata-tab-panel>
  </bkdata-tab>
</template>

<script>
import { postMethodWarning } from '@/common/js/util';
import GotoIcon from './components/GotoIcon';
import StorageTypeFieldsConfig from './components/StorageTypeFieldsConfig';
let beautify = require('json-beautify');
import childMixin from './config/child.global.mixin.js';
import bkStorage from '@/common/js/bkStorage.js';
export default {
  components: {
    GotoIcon,
    StorageTypeFieldsConfig,
  },
  mixins: [childMixin],
  props: {
    project: {
      type: Object,
      default: () => ({}),
    },
    nodeDisname: String,
    nodeType: String,
  },
  data() {
    return {
      activeTab: 'config',
      tempRtId: '',
      resultData: {
        placeholder: this.$t('请选择'),
        disabled: false,
        dataSet: '',
      },
      buttonStatus: {
        isDisabled: false,
        Disabled: false,
      },
      latestMsg: {
        msg: this.$t('暂无数据'),
      },
      loading: false,
      bizLoading: false,
      filterable: true, // dataid是否支持过滤
      authRslList: [], // 新增数据源
      bizList: [],
      selfConfig: {}, // 传回的配置信息
      /* 新建节点的params */
      params: {
        node_type: '',
        config: {
          bk_biz_name: '',
          bk_biz_id: 0,
          result_table_id: 0,
          name: '',
        },
        from_links: [],
        frontend_info: {},
      },
      dataId: 0,
      bkStorageKey: 'dataflowSourceNodeBizId',
      setBkStorage: true,
    };
  },
  computed: {
    storageConfig() {
      return this.$store.state.ide.storageConfig;
    },
    calcAuthRslList() {
      return this.authRslList.map(item => {
        item.show_name = item.tb_id;
        return item;
      });
    },
    isTdw() {
      return /^tdw/i.test(this.nodeType);
    },
    isCleanNode() {
      return this.authRslList.some(
        item => item.tb_id === this.params.config.bk_biz_name
          && item.children.some(
            child => child.tb_id === this.params.config.result_table_id && /clean/i.test(child.processing_type)
          )
      );
    },
  },
  watch: {
    'project.project_id': {
      immediate: true,
      handler(val) {
        if (val) {
          this.getBizsByProjectId(val);
        }
      },
    },
    'params.config.bk_biz_id': {
      immediate: true,
      handler(val) {
        if (val) {
          this.setBkStorage && bkStorage.set(this.bkStorageKey, val);
          this.getResultList(val);
          // 数据回填不设置bkStorage,回填完成后开启设置
          !this.setBkStorage && (this.setBkStorage = true);
        }
      },
    },
    calcAuthRslList(val) {
      if (this.selfConfig.node_id) return;
      if (val.length && val.length === 1) {
        const group = val[0].children;
        this.params.config.result_table_id = group.length && group.length === 1 ? group[0].tb_id : '';
        this.getLatestMsg();
      }
    },
  },
  methods: {
    getResultList(bizId) {
      this.getAuthRslList(bizId);
      this.getLatestMsg();
    },
    getBizsByProjectId(id, config = {}) {
      this.bizLoading = true;
      this.bkRequest
        .httpRequest('dataFlow/getBizListByProjectId', { params: { pid: id } })
        .then(res => {
          if (res.result) {
            this.bizList = res.data;
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](_ => {
          this.bizLoading = false;
          if (config && config.biz_id) {
            this.params.config.bk_biz_id = config.biz_id;
            this.tempRtId = config.rtId;
          }
        });
    },
    getResultTableType() {
      let bkBiz = this.authRslList.find(item => item.tb_id === this.params.config.bk_biz_name);
      if (!bkBiz) {
        return null;
      }
      let resultTable = bkBiz.children.find(item => item.tb_id === this.params.config.result_table_id);
      if (resultTable) {
        return resultTable.processing_type;
      } else {
        return null;
      }
    },
    linkToCleanList() {
      let bkBiz = this.authRslList.find(item => +item.tb_id === +this.params.config.bk_biz_id);
      if (!bkBiz) {
        return;
      }
      let resultTable = bkBiz.children.find(item => item.tb_id === this.params.config.result_table_id);
      // 根据processing_type判断跳转清洗列表，否则跳转到对应的任务详情
      if (resultTable.processing_type === 'clean') {
        this.$store.dispatch('api/getDataIdEtl', { rid: this.params.config.result_table_id }).then(res => {
          if (res.result) {
            this.$emit('routerLeave', {
              name: 'data_detail',
              params: { did: res.data.raw_data_id, tabid: 3 },
              hash: '#clean',
            });
            // this.$router.push()
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
      } else {
        const options = {
          query: {
            result_table_id: this.params.config.result_table_id,
          },
        };
        this.bkRequest.httpRequest('dataFlow/getProcessingNodes', options).then(res => {
          if (res.result && res.data.length) {
            this.$emit('routerLeave', {
              name: 'dataflow_ide',
              params: {
                fid: res.data[0].flow_id,
              },
              query: {
                project_id: res.data[0].related_flow.project_id,
                NID: res.data[0].node_id,
              },
            });
          } else {
            postMethodWarning(res.message || this.$t('此结果表不支持任务跳转'), 'warning');
          }
        });
      }
    },
    getDataId(rid) {
      this.$store.dispatch('api/getDataIdEtl', { rid: rid }).then(res => {
        if (res.result) {
          this.dataId = res.data.raw_data_id;
        } else {
          this.dataId = 0;
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    /** @description
     * 申请数据
     */
    applyData() {
      this.$emit('applyData', this.project, this.params.config.bk_biz_id, this.nodeType);
    },
    /*
            addDataId
                */
    addDataId() {
      const url = this.$router.resolve({ name: 'createDataid' });
      window.open(url.href, '_blank');
    },
    /*
            addDataTdw
            */
    addDataTdw(type) {
      this.$emit('applyTdw', {
        data: this.project,
        bkBizId: this.params.config.bk_biz_id,
        resultTableId: type === 'edit' ? this.params.config.result_table_id : '',
        type,
      });
    },
    /*
     * 获取数据源列表
     * */
    async getAuthRslList(bkBizId) {
      this.resultData.placeholder = this.$t('数据加载中');
      this.resultData.disabled = true;
      await this.bkRequest
        .httpRequest('dataFlow/getBizResultList', {
          params: {
            pid: this.project.project_id,
            bk_biz_id: bkBizId,
            source_type: this.selfConfig.node_type,
          },
        })
        .then(res => {
          if (res.result) {
            this.loading = false;
            this.authRslList.splice(0);
            let temp = {};
            res.data.forEach(item => {
              let _rt = item;
              let _bizIndex = `${_rt.bk_biz_name}`;
              if (temp[_bizIndex] === undefined) {
                temp[_bizIndex] = [];
              }

              temp[_bizIndex].push({
                tb_id: item.result_table_id,
                name: item.result_table_name_alias || item.result_table_name,
                processing_type: item.processing_type,
                show_name: `${item.result_table_id}（${item.result_table_name_alias
                                || item.result_table_name}）`,
              });
            });
            let tempArr = [];
            for (let _bizIndex in temp) {
              this.authRslList.push({
                tb_id: _bizIndex,
                children: temp[_bizIndex],
              });
            }
            if (this.authRslList.length > 0) {
              this.resultData.placeholder = this.$t('请选择');
            } else {
              this.resultData.placeholder = this.$t('暂无数据');
            }
          } else {
            this.loading = false;
            this.getMethodWarning(res.message, res.code);
            this.tableLoading = false;
            this.resultData.placeholder = this.$t('加载失败');
          }
        })
        ['finally'](() => {
          this.resultData.disabled = false;
          if (this.tempRtId) {
            this.params.config.result_table_id = this.tempRtId;
            this.formatParams();
          }
        });
    },
    /*
                配置项目回填
            */
    setConfigBack(self, source, fl, option = {}) {
      // this.activeTab = option.elType === 'referTask' ? 'referTasks' : 'config'
      this.params.frontend_info = self.frontend_info;
      this.selfConfig = self;
      this.params.config.result_table_id = '';
      if (self.hasOwnProperty('node_id') || self.isCopy) {
        this.setBkStorage = false;
        this.params.config.bk_biz_name = this.getBizNameWithoutBizIdByBizId(self.node_config.bk_biz_id);
        this.params.config.name = self.node_config.name;
        this.params.config.bk_biz_id = self.node_config.bk_biz_id;
        this.params.config.result_table_id = self.node_config.result_table_id;
      } else {
        this.params.config.result_table_id = '';
        this.params.config.bk_biz_id = bkStorage.get(this.bkStorageKey) || 0;
      }
    },
    /**/
    closeSider() {
      this.buttonStatus.Disabled = false;
      this.$emit('closeSider');
    },

    formatParams() {
      this.authRslList.forEach(item => {
        const node = item.children.find(child => child.tb_id === this.params.config.result_table_id);
        node && Object.assign(this.params.config, { name: node.name, bk_biz_name: item.tb_id });
      });
    },

    /**
     * @description
     * 获取最近一条数据
     */
    getLatestMsg(obj, index) {
      this.formatParams();
      if (!this.params.config.result_table_id) {
        this.latestMsg.msg = this.$t('暂无数据');
        return;
      }
      this.buttonStatus.isDisabled = true;
      this.axios
        .get(
          'result_tables/'
            + this.params.config.result_table_id
            + `/get_latest_msg/?source_type=${this.selfConfig.node_type}`
        )
        .then(res => {
          if (res.result) {
            this.latestMsg.msg = beautify(this.extractObj(res.data), null, 4, 80);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.buttonStatus.isDisabled = false;
        });
    },
    /**
     * 提交 JSON 对象
     */
    extractObj(str) {
      try {
        return JSON.parse(str);
      } catch (e) {
        return str;
      }
    },
    /*
     * @改变按钮状态
     * */
    changeButtonStatus() {
      this.loading = false;
      this.buttonStatus.Disabled = false;
      this.addRelatedLocation();
    },
    addRelatedLocation() {
      this.$emit('add-location', this.latestMsg);
    },
    validateFormData() {
      this.params.from_links = [];
      this.formatParams();
      this.params.config.from_result_table_ids = [this.params.config.result_table_id];
      if (this.selfConfig.hasOwnProperty('node_id')) {
        this.params.flow_id = this.$route.params.fid;
      } else {
        this.params.node_type = this.selfConfig.node_type;
      }
      return true;
    },
  },
};
</script>

<style lang="scss" scoped>
.iconWrapper {
  position: absolute;
  top: 50%;
  right: 0;
  transform: translate(calc(100% + 10px), -50%);
  display: flex;
  left: 412px;
  i.icons {
    min-width: 30px;
    width: 30px;
    height: 30px;
    margin-left: 4px;
    text-align: center;
    line-height: 30px;
    border-radius: 2px;
    display: block;
    background: #3a84ff;
    position: static !important;
    float: none !important;
    color: #fff;
    cursor: pointer;
  }
}
.add-data-source {
  .content-up {
    .bk-form-item {
      .bk-label {
        ::v-deep .bk-tooltip {
          margin-right: 8px;
        }
      }
    }
  }
}
</style>
