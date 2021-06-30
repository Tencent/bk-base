

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
  <div v-bkloading="{ isLoading: loading }"
    class="auth-role-wrap-content"
    style="padding: 20px">
    <div slot="content"
      :class="{ 'has-left': showRoleName }">
      <div class="role-left">
        <h3>{{ $t(role.name) }}</h3>
      </div>
      <div class="role-right">
        <div v-show="accessObjects === 'biz'"
          class="info">
          暂不支持在该页面申请业务角色，申请方式请参考
          <a :href="$store.getters['docs/getPaths'].applyBizPermission"
            target="_blank">
            文档
          </a>
        </div>

        <div v-show="showInfo">
          <div class="info">
            {{ role.description }}
          </div>

          <div class="permissions">
            <div class="bk-form">
              <!-- @todo 这里有些特殊逻辑，把biz.access_raw_data 放到了 raw_data下展示，需讨论 -->
              <template v-for="(object, index) of objectTypeList">
                <div v-if="object.actions.length > 0"
                  :key="index"
                  class="bk-form-item">
                  <label class="bk-label pr15">{{ object.object_class_name }}</label>
                  <div class="bk-form-content">
                    <bkdata-checkbox-group v-model="role.checkedActionID">
                      <bkdata-checkbox
                        v-for="action of object.actions"
                        :key="action.action_id"
                        :value="action.action_id"
                        disabled="disabled">
                        {{ action.action_name }}
                      </bkdata-checkbox>
                    </bkdata-checkbox-group>
                  </div>
                </div>
              </template>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    showRoleName: {
      type: Boolean,
      default: true,
    },
    roleId: {
      type: String,
      required: true,
    },
    accessObjects: {
      type: String,
    },
  },
  data() {
    return {
      loading: false,
      role: {
        name: '',
        description: '',
        checkedActionID: [],
      },
      tempObjectList: [],
      objectTypeList: [],
      showInfo: false,
    };
  },
  watch: {
    /** 申请权限时，选择不同Role，需要调用接口渲染不同角色 */
    roleId(val, pre) {
      val !== pre && this.init();
    },
    accessObjects(newVal, oldVal) {
      if (newVal !== oldVal) {
        this.role.name = '';
        this.showInfo = false;
      }
    },
  },
  mounted() {
    this.init();
  },
  methods: {
    init() {
      this.loading = true;
      this.bkRequest
        .httpRequest('authV1/getObjectClass')
        .then(res => {
          this.tempObjectList = res.data;
        })
        ['finally'](_ => {
          this.renderRoleInfo();
        });
    },
    renderRoleInfo() {
      if (!this.roleId) {
        this.clear();
        this.loading = false;
        return;
      }

      this.bkRequest
        .httpRequest('authV1/getRole', { params: { roleId: this.roleId } })
        .then(res => {
          this.showInfo = true;
          this.role.name = res.data.role_name;
          this.role.description = res.data.description;
          this.role.checkedActionID = res.data.actions;

          this.objectTypeList = this.tempObjectList.filter(item => {
            const result = item.actions.some(actionItem => {
              return this.role.checkedActionID.includes(actionItem.action_id);
            });
            return result;
          });
        })
        ['finally'](_ => {
          this.loading = false;
        });
    },
    clear() {
      this.role.name = '';
    },
  },
};
</script>

<style lang="scss">
.auth-role-wrap-content {
  font-size: 12px;

  .role-left {
    display: none;
    float: left;
    width: 104px;

    h3 {
      font-weight: bold;
    }
  }

  .role-right {
    padding: 10px;
    border: 1px dashed #ccc;
    border-top-width: 0px;
    background-color: #efefef52;

    .info {
      color: #d8ad0f;
    }
    .permissions {
      margin-top: 10px;
    }
  }

  .has-left {
    .role-left {
      display: block;
    }

    .role-right {
      margin-left: 104px;
      border-width: 0px;
    }
  }

  .bk-form {
    .bk-label {
      width: 100px;
      font-size: 12px;
    }

    .bk-form-content {
      margin-left: 80px;
    }

    .bk-checkbox-text {
      font-size: 12px;
    }

    .bk-form-item {
      margin-top: 10px;
    }
  }
}
</style>
