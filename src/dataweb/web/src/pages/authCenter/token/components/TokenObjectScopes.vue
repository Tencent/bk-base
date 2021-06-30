

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
  <div class="token-objects-scopes">
    <div class="all-list">
      <div v-if="selectedScopes.length !== 0"
        class="list-table">
        <bkdata-card
          v-for="(item, index) in selectedScopes"
          :key="index"
          :isCollapse="isCollapse"
          :collapseStatus="collapsesStatus"
          :collapseIcons="icons">
          <div slot="header">
            <span class="card-title">{{ item.action_name }}</span>
            <span class="card-title">
              （ {{ $t('数据范围：') }} <span>{{ getCount(item) }}</span> ）
            </span>
          </div>
          <div class="part-content">
            <table class="bkdata-info-table">
              <tr>
                <td>{{ $t('权限对象：') }}</td>
                <td>{{ item.object_class_name }}</td>
              </tr>
              <tr>
                <td>{{ $t('对象功能：') }}</td>
                <td>{{ item.action_name }}</td>
              </tr>
              <tr>
                <td>{{ $t('数据范围：') }}</td>
                <td>
                  <div
                    v-if="item.has_instance"
                    :class="{ 'object-selector': isEditable }"
                    @click="selectObject(item, selectedPermissions)">
                    <div v-if="hasChosenScope(item)">
                      <div
                        v-for="(scope_object_class, scope_index) of item.scope_object_classes"
                        :key="scope_index"
                        class="permissions-item">
                        <template v-if="item.object_class === scope_object_class.scope_object_class">
                          <div>
                            <template v-if="curSelectedPermissions(scope_object_class).length">
                              <div
                                v-for="(permission, permission_index) of curSelectedPermissions(scope_object_class)"
                                :key="permission_index"
                                class="selected-permissions">
                                <p v-if="isInScope(permission, item, scope_object_class)"
                                  class="itemstyle">
                                  {{ permission.scope_display[permission.scope_name_key] }}
                                </p>
                              </div>
                            </template>
                            <div v-else
                              class="selected-permissions">
                              ---
                            </div>
                          </div>
                        </template>
                      </div>
                    </div>
                    <div v-else>
                      <span :class="{ 'to-be-click': isEditable }">{{ $t('暂未选择数据范围') }}</span>
                    </div>
                  </div>
                  <div v-else>
                    <span class="not-click">{{ $t('无需关联具体对象') }}</span>
                  </div>
                </td>
              </tr>
            </table>
          </div>
        </bkdata-card>
      </div>
      <div v-else
        class="notChoose">
        {{ $t('暂未选择功能权限') }}
      </div>
    </div>

    <!-- 对象选择器 start -->
    <div v-if="isNeedToInitDialog"
      class="object-selector">
      <keep-alive>
        <ObjectScopeSelector ref="selector"
          @setPermission="setPermission" />
      </keep-alive>
    </div>
    <!-- 对象选择器 end -->
  </div>
</template>

<script>
import ObjectScopeSelector from './ObjectScopeSelector';

export default {
  components: {
    ObjectScopeSelector,
  },
  props: {
    isCollapse: {
      type: Boolean,
      default: true,
    },
    collapsesStatus: {
      type: Boolean,
      default: false,
    },
    isEditable: {
      type: Boolean,
    },
    selectedScopes: {
      type: Array,
      default: () => [],
    },
    selectedPermissions: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      icons: ['icon-right-shape', 'icon-down-shape'],
    };
  },
  computed: {
    /**
     * 是否需要初始化弹窗
     * @description 仅当已选权限对象 且 权限对象存在实例的情况下 才可以进行对象范围的设置
     */
    isNeedToInitDialog() {
      return this.selectedScopes.length && this.selectedScopes.find(item => item.has_instance);
    },
    tableWidth() {
      const name = this.$route.name;
      if (name === 'TokenNew') {
        return 180;
      } else {
        return 100;
      }
    },
  },
  methods: {
    getCount(item) {
      let count = 0;
      for (let scope_object_class of item.scope_object_classes) {
        if (item.object_class === scope_object_class.scope_object_class) {
          if (this.curSelectedPermissions(scope_object_class).length) {
            for (let permission of this.curSelectedPermissions(scope_object_class)) {
              if (this.isInScope(permission, item, scope_object_class)) {
                if (permission.scope_display[permission.scope_name_key] !== null) {
                  count++;
                }
              }
            }
          }
        }
      }
      return count;
    },
    hasChosenScope(scope) {
      // 判断是否已选择对象
      for (let permission of this.selectedPermissions) {
        if (permission.object_class === scope.object_class && permission.action_id === scope.action_id) {
          return true;
        }
      }
      return false;
    },
    isInScope(permission, scope, scope_object_class) {
      // 判断权限是否归属于此scope
      return (
        permission.object_class === scope.object_class
        && permission.action_id === scope.action_id
        && permission.scope_object_class === scope_object_class.scope_object_class
      );
    },
    selectObject(scope, permissions) {
      if (this.isEditable) {
        this.$refs.selector.selectObject(scope, permissions);
      }
    },
    setPermission(scopePermissions, scope) {
      // 1. 先过滤掉对应scope选择的权限
      let permissions = this.selectedPermissions;
      scope.scope_object_classes.map(scopeObjectClass => {
        permissions = permissions.filter(
          perm => perm.action_id !== scope.action_id
            || perm.object_class !== scope.object_class
            || perm.scope_object_class !== scope_object_class.scope_object_class
        );
      });

      // 2.再加入选择的权限
      scopePermissions.map(scopePerm => {
        permissions.push(scopePerm);
      });

      this.$emit('update:selectedPermissions', permissions);
      this.$emit('updatePermissions', permissions);
    },
    curSelectedPermissions(objClass) {
      return this.selectedPermissions.filter(item => item.scope_object_class === objClass.scope_object_class);
    },
  },
};
</script>

<style lang="scss" scoped>
.bk-card {
  margin-bottom: 10px;
  width: 500px;

  p {
    margin-top: 0;
    margin-bottom: 10px;
    &:last-child {
      margin-bottom: 0;
    }
  }
}
.bk-card .bk-card-head-left {
  padding-left: 0 !important;
}
.bkdata-parts .part-content {
  padding: 0;
  color: #979ba5;
  font-weight: 400;
  max-height: 320px;
  overflow: auto;
}
table.bkdata-info-table tr td:first-child {
  color: #979ba5;
  font-weight: 400;
  vertical-align: text-top;
}
table.bkdata-info-table tr td:last-child {
  color: #63656e;
  .itemstyle {
    margin-bottom: 10px;
  }
}
.card-title {
  height: 17px;
  font-size: 13px;
  font-family: MicrosoftYaHei;
  text-align: left;
  color: #313238;
  line-height: 17px;
  font-weight: 700;
}
.card-title:last-child {
  font-size: 12px;
  color: #63656e;
  font-weight: 400;
  span {
    color: #3a84ff;
    font-weight: bold;
  }
}
</style>
