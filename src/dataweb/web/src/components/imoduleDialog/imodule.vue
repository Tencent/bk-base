

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
  <div v-bkloading="{ isLoading: isLoading }"
    class="iModue-container">
    <div class="iModule-source">
      <bkdata-tab :active="activeTab[0]"
        type="border-card"
        @tab-change="handleTabChanged">
        <bkdata-tab-panel name="module"
          :label="$t('模块选择')">
          <div class="meta-container">
            <bkdata-input
              v-model="moduleSearchVal"
              :placeholder="$t('搜索')"
              :clearable="true"
              :rightIcon="'bk-icon icon-search'" />
            <template v-if="calcTreeData.length">
              <bkdata-big-tree
                ref="moduleTree"
                :defaultCheckedNodes="defaultCheckedNodes"
                :showCheckbox="true"
                :checkStrictly="true"
                :data="calcTreeData"
                :options="treeOption"
                @check-change="handleTreeEmitCheck" />
            </template>
            <template v-else>
              <div class="no-module">
                {{ $t('暂无模块') }}
              </div>
            </template>
          </div>
        </bkdata-tab-panel>
        <bkdata-tab-panel name="ip"
          :label="$t('IP填写')">
          <div class="meta-container">
            <bkdata-selector
              :defineEmptyText="$t('无数据')"
              :displayKey="'bk_cloud_name'"
              :list="sourceCloudArea"
              :multiSelect="false"
              :searchable="true"
              :selected.sync="sourceCloudAreaSelected"
              :settingKey="'bk_cloud_id'"
              :searchKey="'bk_cloud_name'"
              class="cloud-area-selector"
              title="云区域" />

            <bkdata-input
              v-model="sourceIpListValue"
              :placeholder="$t('云区域area_请输入IP_以回车分割再添加至右侧表格中')"
              :type="'textarea'"
              :rows="12"
              style="margin: 8px 0" />
            <bkdata-button :theme="'primary'"
              :title="$t('添加')"
              class="mr10 add-ip"
              @click="addToTarget">
              {{ $t('添加') }}
            </bkdata-button>
          </div>
        </bkdata-tab-panel>
      </bkdata-tab>
    </div>
    <div class="iModule-option">
      <span><i class="bk-icon icon-angle-double-right" /></span>
    </div>
    <div class="iModule-target">
      <bkdata-collapse v-model="activeTab"
        accordion
        class="iModule-target-items"
        @item-click="handleCollspan">
        <bkdata-collapse-item name="module"
          class="collapse-module">
          <div class="target-tools">
            <span class="target-tool tool-left">
              {{ $t('已选择') }} {{ targetModules.length }} {{ $t('IPModuleUnit') }}
            </span>
            <span class="target-tool tool-right" />
          </div>
          <div slot="content"
            class="bkdata-content-border">
            <bkdata-table
              :data="calcTragetModule"
              :outerBorder="false"
              :stripe="true"
              :showHeader="false"
              :emptyText="$t('暂无数据_请在左侧选择模块')"
              :height="tableHeight">
              <bkdata-table-column prop="bk_inst_name" />
              <bkdata-table-column width="50px">
                <div slot-scope="item"
                  class="bk-table-inlineblock">
                  <div class="operation-col">
                    <span class="border-btn"
                      @click.stop="removeItem(item.row, 'module')">
                      <i class="bk-icon icon-delete"
                        :title="$t('移除')" />
                    </span>
                  </div>
                </div>
              </bkdata-table-column>
            </bkdata-table>
            <div class="bkdata-footer-pagination">
              <bkdata-pagination v-bind="calcPagination['module']"
                @change="handlePageChange" />
            </div>
          </div>
        </bkdata-collapse-item>
        <bkdata-collapse-item name="ip"
          class="collapse-ip">
          <div class="target-tools">
            <span class="target-tool tool-left"> {{ $t('已填写') }} {{ targetIpList.length }} {{ $t('个IP') }} </span>
            <span class="target-tool tool-middle" />
            <span class="target-tool tool-right">
              <i class="bk-icon icon-delete"
                title="移除全部"
                @click.stop="removeItem(null, 'allIp')" />
            </span>
          </div>
          <div slot="content"
            class="bkdata-content-border">
            <!-- :pagination="pagination['ip']" -->
            <bkdata-table :data="calcIpList"
              :outerBorder="false"
              :height="tableHeight"
              :emptyText="$t('暂无数据')">
              <bkdata-table-column :label="$t('云区域名称')"
                prop="bk_cloud_name" />
              <bkdata-table-column :label="$t('服务器IP')"
                prop="ip" />
              <bkdata-table-column :label="$t('Agent状态')"
                prop="status_display" />
              <bkdata-table-column :label="$t('操作')"
                width="50px">
                <div slot-scope="props"
                  class="bk-table-inlineblock operation-col">
                  <span class="border-btn"
                    @click.stop="removeItem(props.row, 'ip')">
                    <i class="bk-icon icon-delete"
                      :title="$t('移除')" />
                  </span>
                </div>
              </bkdata-table-column>
            </bkdata-table>
            <div class="bkdata-footer-pagination">
              <bkdata-pagination v-bind="calcPagination['ip']"
                @change="handlePageChange" />
            </div>
          </div>
        </bkdata-collapse-item>
      </bkdata-collapse>
    </div>
  </div>
</template>
<script>
import { postMethodWarning } from '@/common/js/util.js';

export default {
  name: 'bkdata-ipmodule-selector',
  model: {
    prop: 'value',
    event: 'change',
  },
  props: {
    value: {
      type: Object,
      default: () => ({}),
    },
    bizid: {
      type: Number,
      default: 0,
    },
    /** 模块列表 */
    sourceModuleList: {
      type: Array,
      default: () => [],
      required: true,
    },
    /** 云区域列表 */
    sourceCloudArea: {
      type: Array,
      default: () => [],
      required: true,
    },
    /** 已选择云区域 */
    cloudAreaSelected: {
      type: [String, Number],
      default: '',
    },
  },
  data() {
    return {
      isLoading: false,
      treeOption: {
        idKey: 'uid',
        nameKey: 'bk_inst_name',
        childrenKey: 'child',
      },
      moduleSearchVal: '',
      tableHeight: 250,
      activeTab: ['module'],
      preActiveTab: ['module'],
      sourceCloudAreaSelected: this.cloudAreaSelected,
      sourceIpListValue: '',
      targetIpList: this.value.host_scope || [],
      targetModules: this.value.module_scope || [],
      cacheFunc: [],
      pagination: {
        base: {
          showLimit: false,
          type: 'compact',
          align: 'right',
          size: 'small',
        },
        module: {
          count: 0,
          limit: 10,
          current: 1,
        },
        ip: {
          count: 0,
          limit: 10,
          current: 1,
        },
      },
      collspanTabMap: {
        module: 'ip',
        ip: 'module',
      },
    };
  },
  computed: {
    cloudMap() {
      return this.sourceCloudArea.reduce(
        (pre, current) => Object.assign(pre, { [current.bk_cloud_id]: current.bk_cloud_name }),
        {}
      );
    },
    calcIpList() {
      return this.calcTragetIP.map(item => Object.assign({}, item, { bk_cloud_name: this.cloudMap[item.bk_cloud_id] }));
    },
    calcPagination() {
      return {
        module: Object.assign({}, this.pagination.base, this.pagination.module, {
          count: this.targetModules.length,
        }),
        ip: Object.assign({}, this.pagination.base, this.pagination.ip, { count: this.targetIpList.length }),
      };
    },
    calcTragetModule() {
      const { limit, current } = this.pagination.module;
      const start = (current - 1) * limit;
      const end = start + limit;
      return this.targetModules.slice(start, end);
    },
    calcTragetIP() {
      const { limit, current } = this.pagination.ip;
      const start = (current - 1) * limit;
      const end = start + limit;
      return this.targetIpList.slice(start, end);
    },
    defaultCheckedNodes() {
      return (this.value.module || this.value.module_scope || []).map(m => m.uid);
    },
    calcTreeData() {
      return this.sourceModuleList;
    },
    selectedCloudAreaItem() {
      return this.sourceCloudArea.find(item => item.bk_cloud_id === this.sourceCloudAreaSelected);
    },
    resultCollection() {
      return {
        module_scope: this.targetModules,
        host_scope: this.targetIpList,
      };
    },
  },
  watch: {
    cloudAreaSelected(val) {
      this.sourceCloudAreaSelected = val;
    },
    moduleSearchVal(val) {
      this.$refs.moduleTree && this.$refs.moduleTree.filter(val);
    },
  },
  mounted() {
    this.setData();
  },
  methods: {
    handlePageChange(page) {
      this.pagination[this.activeTab].current = page;
    },
    /** 树形节点勾选事件 */
    handleTreeEmitCheck(checkedList, node) {
      const checked = node.state.checked;
      // this.updateParentNodeState(node)
      this.updateAllChild(node.data, checked);
      this.addToTarget();
    },

    /** 更新父级节点状态，所有叶子节点全部选中时 */
    updateParentNodeState(node) {
      const parent = node.parent || {};
      const pchild = parent.children || [];

      if (!pchild.some(child => !child.state.checked)) {
        parent.data && this.updateAllChild(parent.data, true);
      }
    },

    /** 更新节点勾中状态、禁用状态 */
    updateAllChild(root, checked) {
      const tree = this.$refs.moduleTree;
      root.child.forEach(item => {
        tree.setChecked(item.uid, {
          checked: checked,
        });
        tree.setDisabled(item.uid, {
          disabled: checked,
        });
        this.updateAllChild(item, checked);
      });
    },

    /** Tab切换事件 */
    handleTabChanged(name) {
      this.activeTab.splice(0, 1, name);
    },

    handleCollspan(val) {
      if (val && val.length) {
        this.preActiveTab = val;
      } else {
        this.activeTab.splice(0, 1, this.collspanTabMap[this.preActiveTab[0]]);
        this.preActiveTab.splice(0, 1, this.activeTab[0]);
      }
    },

    /** 添加事件 */
    addToTarget(data, checked = true) {
      const source = {
        module: this.addModuleToTarget,
        ip: this.addIpToTarget,
      };

      source[this.activeTab[0]](data, checked);
      this.emitModel();
    },

    /** 添加模块到结果集 */
    addModuleToTarget(data, checked) {
      const checkedNodes = this.$refs.moduleTree.checkedNodes;
      const activeNodes = checkedNodes
        .filter(node => !node.state.disabled && (!node.children || node.children.every(child => child.state.disabled)))
        .map(node => node.data);
      this.targetModules.splice(0, this.targetModules.length, ...activeNodes);
      this.updatePagination('module', this.targetModules);
    },

    updatePagination(type = '', items = []) {
      this.pagination[type].count = items.length;
    },

    /** 更新数据，缓存处理，防止接口拉取返回结果太慢，导致设置数据默认勾选失败 */
    setData(val) {
      val = val || this.value;
      this.cacheFunc.push({
        params: val,
        execType: [],
        func: (val, type = '') => {
          if (/all|ip/g.test(type)) {
            val.host_scope && this.$set(this, 'targetIpList', val.host_scope);
            this.updatePagination('ip', this.targetIpList);
          }

          if (/all|module/g.test(type) && val.module_scope) {
            this.$set(this, 'targetModules', val.module_scope);
            this.updatePagination('module', this.targetModules);
            const treeMap = (this.$refs.moduleTree && (this.$refs.moduleTree.map || {})) || null;
            treeMap
              && val.module_scope.forEach(m => {
                const node = treeMap[m.uid];
                node && this.updateAllChild(node.data, true);
              });
          }
        },
      });

      this.execCacheFunc('all');
    },

    /** 执行缓存的操作
     * @param type: all(执行所有)，ip（执行IP选择设置），module（执行module赋值）
     */
    execCacheFunc(type = '') {
      const len = this.cacheFunc.length;
      for (let i = 0; i < len; i++) {
        const func = this.cacheFunc[0];
        func.func.call(this, func.params, type);
        func.func = null;
        this.cacheFunc[0] = null;
        this.cacheFunc.splice(0, 1);
      }
    },

    /** 添加IP到结果集 */
    async addIpToTarget() {
      const checkedResult = this.fontIPCheck();
      if (checkedResult.validate) {
        const validateIps = await this.checkIPStatus(checkedResult.ipList, this.sourceCloudAreaSelected);
        validateIps.forEach(ip => Object.assign(ip, this.selectedCloudAreaItem));
        this.targetIpList.splice(this.targetIpList.length, 0, ...validateIps);
        this.sourceIpListValue = '';
        this.updatePagination('ip', this.targetIpList);
      }
    },

    /** 移除结果集条目 */
    removeItem(item, type = 'module') {
      switch (type) {
        case 'module':
          this.removeModuleItem(item);
          break;
        case 'ip':
          this.removeIpItem(item);
          break;
        case 'allIp':
          this.removeIpItem(item, true);
          break;
      }

      this.emitModel();
    },

    /** 移除IP */
    removeIpItem(item, rmAll = false) {
      if (rmAll) {
        this.targetIpList.splice(0, this.targetIpList.length);
      } else {
        const index = this.targetIpList.findIndex(row => row.bk_cloud_id === item.bk_cloud_id && item.ip === row.ip);
        if (index >= 0) {
          this.targetIpList.splice(index, 1);
        }
      }

      this.updatePagination('ip', this.targetIpList);
    },

    /** 移除模块 */
    removeModuleItem(item) {
      const index = this.targetModules.findIndex(row => row.uid === item.uid);
      if (index >= 0) {
        this.targetModules.splice(index, 1);
        this.updateAllChild({ child: [item] }, false);
      }

      this.updatePagination('module', this.targetModules);
    },

    emitModel() {
      this.$emit('change', this.resultCollection);
    },

    /** 前端校验：IP格式、非空、去重 */
    fontIPCheck() {
      const validateReuslt = { validate: false, ipList: [] };
      // 校验IP所属云区域
      if (!(this.sourceCloudAreaSelected !== '' && this.sourceCloudAreaSelected >= 0)) {
        postMethodWarning(`${this.$t('请先选择云区域')}`, 'warning');
        return validateReuslt;
      }

      // IP非空校验
      if (!this.sourceIpListValue.trim()) {
        postMethodWarning(`${this.$t('IP输入不能为空')}`, 'warning');
        return validateReuslt;
      }

      const allInputedIpList = this.sourceIpListValue
        .split('\n')
        .filter(ip => ip)
        .map(item => item.trim());
      // eslint-disable-next-line max-len
      const ipReg = new RegExp(/^(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])\.(\d{1,2}|1\d\d|2[0-4]\d|25[0-5])$/);
      const invalidIp = allInputedIpList.filter(ip => !ipReg.test(ip));

      if (invalidIp.length) {
        postMethodWarning(`${this.$t('存在不合法的IP')}: ${invalidIp.join('，')}`, 'warning');
        return validateReuslt;
      }

      const repeateItems = this.targetIpList
        .filter(item => allInputedIpList
          .some(ip => item.ip === ip && item.bk_cloud_id === this.sourceCloudAreaSelected));
      if (repeateItems.length) {
        postMethodWarning(`${this.$t('重复添加')}: ${repeateItems.map(item => item.ip).join('，')}`, 'warning');
        return validateReuslt;
      }

      validateReuslt.validate = true;
      validateReuslt.ipList = allInputedIpList;
      return validateReuslt;
    },

    /**
     * 请求检查IP的状态
     */
    async checkIPStatus(ips, cloudId) {
      this.isLoading = true;
      const res = await this.bkRequest.httpRequest('meta/checkIpStatus', {
        params: {
          biz_id: this.bizid,
          bk_cloud_id: cloudId,
          ips: ips,
        },
      });

      if (!res.result) {
        postMethodWarning(`${res.msg || res.message}`, 'error');
        this.isLoading = false;
        return [];
      }

      // 提示不合法IP
      const invalidHosts = (res.data && res.data.invalid_hosts) || [];
      if (res.data && res.data.invalid_hosts && res.data.invalid_hosts.length) {
        let errMap = new Map();
        invalidHosts.forEach(item => {
          if (errMap.has(item.error)) {
            const errList = errMap.get(item.error).split(',');
            errList.push(item.ip);
            errMap.set(item.error, errList.join(','));
          } else {
            errMap.set(item.error, item.ip);
          }
        });
        let errorArray = [];
        errMap.forEach((val, key) => {
          errorArray.push(`${key}:${val}`);
        });
        postMethodWarning(errorArray.join(';'), 'warning');
      }

      this.isLoading = false;
      return (res.data && res.data.valid_hosts) || [];
    },
  },
};
</script>
<style lang="scss" scoped>
.iModue-container {
  display: flex;
  min-height: 200px;
  max-height: 400px;
  .iModule-source {
    width: 350px;
    display: flex;

    .meta-container {
      display: flex;
      flex-direction: column;
      height: 320px;
      overflow: auto;
      // margin: -10px;

      ::v-deep .bk-big-tree {
        width: 100%;
        .node-content {
          overflow: inherit;
          text-overflow: inherit;
          white-space: nowrap;
          font-size: 14px;
        }
      }
      .add-ip {
        width: 60px;
        margin-top: 7px;
        align-self: center;
      }
    }

    .no-module {
      text-align: center;
      padding: 30px 0;
    }

    ::v-deep .bk-tab {
      width: 100%;

      .bk-tab-content {
        min-height: 280px;
      }

      .bk-tab-section {
        padding: 10px;
      }
    }

    ::v-deep .bk-tab-label-list {
      width: 100%;
      li {
        width: 50%;

        &.is-last {
          border-right: none;
        }
      }
    }
  }

  .iModule-option {
    width: 30px;
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .iModule-target {
    width: 600px;
    display: flex;
    // border: solid 1px #ddd;
    .collapse-module {
      ::v-deep .bk-collapse-item-header {
        border-bottom: none;
      }
    }

    .bkdata-content-border {
      border: 1px solid #dfe0e5;
    }

    .collapse-ip {
      &:not(.bk-collapse-item-active) {
        ::v-deep .bk-collapse-item-header {
          border-top: none;
        }
      }
      &.bk-collapse-item-active {
        ::v-deep .bk-collapse-item-header {
          border-bottom: none;
        }
      }
    }

    .bkdata-footer-pagination {
      display: flex;
      height: 48px;
      align-items: center;
      justify-content: flex-end;
      border-top: solid 1px #dcdee6;
      padding: 5px 15px;
    }

    ::v-deep .bk-collapse-item-header {
      background: #fafbfd;
      border: solid 1px #dcdee6;
    }

    ::v-deep .bk-collapse-item-content {
      padding: 0;
    }

    .iModule-target-items {
      width: 100%;

      .target-tools {
        display: flex;
        .target-tool {
          height: 42px;
          display: flex;

          &.tool-left {
            width: 162px;
          }
          &.tool-middle {
            width: 100%;

            .link-cc {
              color: #3a84ff;
              cursor: pointer;

              &:hover {
                text-decoration: underline;
              }
            }
          }
          &.tool-right {
            // width: 100px;
            align-items: center;
            justify-content: flex-end;
            margin-right: 15px;
          }
        }
      }

      .border-btn {
        cursor: pointer;
      }

      ::v-deep .bk-table td {
        height: 25px;
      }

      ::v-deep .bk-table th {
        height: 28px;
        background-color: #dcdee6;
      }

      ::v-deep .bk-table th > .cell {
        height: 28px;
        line-height: 28px;
      }

      ::v-deep .bk-table-pagination-wrapper {
        padding: 8px;
      }
    }
  }
}
</style>
