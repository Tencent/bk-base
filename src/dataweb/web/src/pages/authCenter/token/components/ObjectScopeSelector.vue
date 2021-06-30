

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
  <div class="object-scope-selector">
    <bkdata-dialog
      v-model="selectorDialog.isShow"
      extCls="object-scope-selector"
      :hasHeader="false"
      :hasFooter="true"
      :closeIcon="true"
      :maskClose="false"
      :okText="$t('确认')"
      :cancelText="$t('取消')"
      :width="600"
      @cancel="cancelDialog"
      @confirm="confirm">
      <div v-bkloading="{ isLoading: isLoading }">
        <div class="title">
          <span>{{ $t('选择对象范围') }}</span>
          <span class="label-title">【{{ title }}】</span>
        </div>
        <bkdata-tab v-if="objectScopes.scope_object_classes.length"
          :active="activeName"
          @tab-change="handleTabchanged">
          <!-- @todo 目前仅支持同级的申请，即scope_object_class.scope_object_class === objectScopes.object_class -->
          <bkdata-tab-panel :name="scope.object_class"
            :label="scope.object_class_name">
            <div id="result-tables"
              class="content p20">
              <div v-if="!isProjectSelectorShow"
                class="bk-form-item">
                <label class="label-title">{{ $t('对象名称') }}：</label>
                <div class="bk-form-content">
                  <bkdata-selector
                    class="inner-selector"
                    :list="activeScopeItemList"
                    :selected.sync="selectedScope"
                    :settingKey="'scope_id_key'"
                    :displayKey="'scope_name_key'"
                    :isLoading="isLoading"
                    :searchable="true"
                    :allowClear="true"
                    multiSelect
                    :searchKey="'scope_name_key'" />
                </div>
              </div>
              <template v-else>
                <div class="bk-form-item">
                  <label class="label-title">{{ $t('业务名称') }}：</label>
                  <div class="bk-form-content">
                    <bkdata-selector
                      :selected.sync="biz"
                      :filterable="true"
                      :searchable="true"
                      :list="bizList"
                      :settingKey="'bk_biz_id'"
                      :displayKey="'bk_biz_name'"
                      :allowClear="false"
                      :searchKey="'bk_biz_name'"
                      @item-selected="loadObjects" />
                  </div>
                </div>
                <div class="bk-form-item">
                  <label class="label-title">{{ $t('对象名称') }}：</label>
                  <div class="bk-form-content">
                    <bkdata-selector
                      :selected.sync="selectedScope"
                      :filterable="true"
                      :isLoading="isObjectLoading"
                      :searchable="true"
                      :list="activeScopeItemList"
                      :settingKey="'scope_id_key'"
                      :displayKey="'scope_name_key'"
                      :allowClear="false"
                      multiSelect
                      :searchKey="'scope_name_key'" />
                  </div>
                </div>
              </template>
            </div>
          </bkdata-tab-panel>
        </bkdata-tab>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import { copyObj } from '@/common/js/util.js';
import { mapGetters, mapState } from 'vuex';
import { queryAuthScope } from '@/common/api/auth.js';

export default {
  props: {
    value: {
      type: Array,
    },
  },
  data() {
    return {
      isLoading: false,
      scope: {},
      title: '',
      activeName: '',
      activeIndex: 0,
      selectorDialog: {
        isShow: false,
      },
      // 原始的被选择的对象，用于取消时还原
      selectedObjectIdOrigin: {},
      // 实时同步的被选择的对象
      selectedObjectIdSync: {},
      objectScopes: {
        placeholder: this.$t('请选择'),
        Map: {
          data: [],
        },
        object_class: '',
        scope_object_classes: [],
      },
      isProjectSelectorShow: false,
      biz: '',
      isObjectLoading: false,
    };
  },
  computed: {
    selectedScope: {
      get: function () {
        const index = this.objectScopes.scope_object_classes.findIndex(
          obj => obj.scope_object_class === this.activeName
        );
        const val = this.selectedObjectIdSync[this.objectScopes.scope_object_classes[index].scope_object_class];
        return Array.isArray(val) ? val : val && val.length ? val.split(',') : [];
      },
      set: function (val) {
        const index = this.objectScopes.scope_object_classes.findIndex(
          obj => obj.scope_object_class === this.activeName
        );
        this.$set(this.selectedObjectIdSync, this.objectScopes.scope_object_classes[index].scope_object_class, val);
      },
    },
    activeScopeItemList() {
      return this.objectScopes.Map[this.activeName] ? this.objectScopes.Map[this.activeName].data : [];
    },
    activeScopeItem() {
      return (this.objectScopes && this.objectScopes.Map[this.activeName]) || { data: [] };
    },
    ...mapGetters({
      allBizList: 'global/getAllBizList',
    }),
    bizList() {
      return this.allBizList;
    },
  },
  watch: {},
  methods: {
    handleData(objectScopes) {
      this.objectScopes.object_class = objectScopes.object_class;
      this.objectScopes.key = objectScopes.key;
      this.objectScopes.value = objectScopes.value;
      this.objectScopes.Map = objectScopes.Map;
    },
    initData(scope) {
      this.objectScopes.scope_object_classes = scope.scope_object_classes;
      this.activeName = this.objectScopes.scope_object_classes[0].scope_object_class;
      for (let perm of this.permissions) {
        for (let scope_object_class of this.objectScopes.scope_object_classes) {
          let _scope_object_class = scope_object_class.scope_object_class;
          if (!(_scope_object_class in this.selectedObjectIdOrigin)) {
            this.selectedObjectIdOrigin[_scope_object_class] = [];
          }
          if (
            perm.object_class === this.scope.object_class
            && perm.action_id === this.scope.action_id
            && perm.scope_object_class === _scope_object_class
          ) {
            this.biz = perm.biz || '';
            this.selectedObjectIdOrigin[_scope_object_class].push(perm.scope[perm.scope_id_key]);
          }
        }
      }
      for (let scope_object_class of this.objectScopes.scope_object_classes) {
        let _scope_object_class = scope_object_class.scope_object_class;
        if (_scope_object_class in this.selectedObjectIdOrigin) {
          this.selectedObjectIdOrigin[_scope_object_class] = this.selectedObjectIdOrigin[_scope_object_class].join(',');
        } else {
          this.selectedObjectIdOrigin[_scope_object_class] = '';
        }
      }
      this.selectedObjectIdSync = copyObj(this.selectedObjectIdOrigin);
    },
    loadObjects() {
      // 加载列表
      this.isObjectLoading = true;
      queryAuthScope({
        object_class: this.activeName,
        bk_biz_id: this.biz,
      }).then(res => {
        if (res.result) {
          this.$store.dispatch('auth/actionSetObjectScopes', {
            objectScopes: res.data,
            currentScope: this.scope,
          });

          const objectScopes = this.$store.state.auth.objectScopes;

          this.isObjectLoading = false;
          this.handleData(objectScopes);
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    handleTabchanged(name, index) {
      this.activeName = name;
      this.activeIndex = index;
    },
    /**
     * 取消后清空数据
     */
    cancelDialog() {
      this.objectScopes.object_class = '';
      this.objectScopes.placeholder = this.$t('请选择');
      this.selectorDialog.isShow = false;
      this.selectedObjectIdSync = copyObj(this.selectedObjectIdOrigin);
      this.objectScopes.scope_object_classes = [];
    },
    confirm() {
      /* eslint-disable */
      this.selectorDialog.isShow = false;
      this.selectedObjectIdOrigin = copyObj(this.selectedObjectIdSync);
      let scope_permissions = [];
      for (let scope_object_class in this.selectedObjectIdOrigin) {
        let scope_ids = this.selectedObjectIdOrigin[scope_object_class];
        scope_ids = Array.isArray(scope_ids) ? scope_ids : scope_ids.split(',');
        for (let scope_id of scope_ids) {
          if (scope_id === '') {
            continue;
          }
          this.scope.scope_object_classes.map(_scope_object_class => {
            if (scope_object_class === _scope_object_class.scope_object_class) {
              let scope_id_key = _scope_object_class.scope_id_key;
              let scope_name_key = _scope_object_class.scope_name_key;
              let perm = {
                action_id: this.scope.action_id,
                object_class: this.scope.object_class,
                scope_id_key: scope_id_key,
                scope_name_key: scope_name_key,
                scope_object_class: scope_object_class,
                scope: {},
                scope_display: {},
                biz: this.isProjectSelectorShow ? this.biz : null,
              };
              perm.scope[scope_id_key] = scope_id;
              perm.scope_display[scope_name_key] = this.matchScopeName(
                scope_id,
                scope_object_class,
                scope_id_key,
                scope_name_key
              );
              scope_permissions.push(perm);
            }
          });
        }
      }
      this.$emit('setPermission', scope_permissions, this.scope);
      this.objectScopes.scope_object_classes = [];
    },
    matchScopeName(scope_id, scope_object_class, scope_id_key, scope_name_key) {
      // 匹配显示名称
      let scope_name = [scope_id_key, scope_id].join(': ');
      this.objectScopes.Map[scope_object_class].data.map(scope => {
        if (scope['scope_id_key'] === String(scope_id)) {
          scope_name = scope['scope_name_key'];
        }
      });
      return scope_name;
    },
    selectObject(scope, permissions) {
      // 初始化数据
      this.biz = '';
      this.permissions = permissions;
      this.selectorDialog.isShow = true;
      this.selectedObjectIdOrigin = {};
      this.selectedObjectIdSync = {};
      this.scope = scope;
      this.title = [scope.object_class_name, scope.action_name].join('-');
      this.isProjectSelectorShow =
        ['raw_data', 'result_table', 'biz'].includes(scope.object_class) && scope.object_class !== 'biz';

      this.initData(scope);
      if (this.biz) {
        // 回填数据
        this.loadObjects();
      }
      if (this.isProjectSelectorShow) {
        // 数据类型为项目时，取消下面的请求
        this.objectScopes.Map = {};
        return;
      }

      // 加载列表
      this.isLoading = true;
      queryAuthScope({
        object_class: scope.object_class,
        bk_biz_id: this.biz,
      }).then(res => {
        if (res.result) {
          this.$store.dispatch('auth/actionSetObjectScopes', {
            objectScopes: res.data,
            currentScope: scope,
          });

          const objectScopes = this.$store.state.auth.objectScopes;
          this.isLoading = false;
          this.handleData(objectScopes);
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
  },
};
</script>

<style lang="scss">
.object-scope-selector {
  .bk-tab2 .bk-tab2-head.is-fill {
    height: 40px;

    .bk-tab2-nav > li {
      height: 40px;
      line-height: 40px;

      &.active {
        border-color: #ddd;

        &:first-child {
          border-left-color: #fff;
        }
      }
    }
  }
}
</style>

<style lang="scss">
.object-scope-selector {
  .el-tabs__content {
    overflow: visible !important;
  }

  .title {
    padding: 10px;
    position: relative;
    font-weight: bold;
    margin-bottom: 5px;

    &:before {
      content: '';
      position: absolute;
      left: 0;
      top: 13px;
      width: 3px;
      height: 16px;
      background: #3a84ff;
    }
  }

  .button-group {
    text-align: center;
  }
}
</style>
<style lang="scss" scoped>
#result-tables {
  .bk-form-item {
    .label-title {
      float: left;
      margin-top: 5px;
      width: 80px;
    }
  }
}
</style>
