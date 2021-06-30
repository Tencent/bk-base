

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
  <div class="new-resource-wrapper">
    <dialogWrapper :defaultFooter="true"
      :extCls="'resource-dialog'"
      :title="dialogTitle"
      :subtitle="$t('资源组可以包含多个离线计算资源_多个实时计算资源等')"
      :dialog="dialogConfig"
      :footerConfig="dialogFooter"
      @confirm="confirmClick"
      @cancle="closeDialog">
      <template #content>
        <div v-bkloading="{ isLoading: contentLoading }"
          class="resource-content">
          <bkdata-form ref="form"
            :labelWidth="180"
            :model="formData"
            :rules="validate">
            <bkdata-form-item :label="$t('资源组名称')"
              :required="true"
              :property="'group_name'">
              <bkdata-input v-model="formData.group_name" />
            </bkdata-form-item>
            <bkdata-form-item :label="$t('资源组英文标识')"
              :required="true"
              :property="'resource_group_id'">
              <bkdata-input v-model="formData.resource_group_id"
                :disabled="editMode" />
            </bkdata-form-item>
            <bkdata-form-item :label="$t('所属业务')"
              :required="true"
              :property="'bk_biz_id'">
              <bkdata-selector :selected.sync="formData.bk_biz_id"
                :loading="bizs.isLoading"
                :disabled="editMode"
                :placeholder="bizs.placeholder"
                :filterable="true"
                :searchable="true"
                :list="bizs.list"
                :settingKey="'bk_biz_id'"
                :displayKey="'bk_biz_name'"
                searchKey="bk_biz_name" />
            </bkdata-form-item>
            <bkdata-form-item :label="$t('资源组类型')">
              <bkdata-radio-group v-model="formData.group_type">
                <bkdata-radio :value="'private'"
                  :disabled="editMode">
                  {{ $t('私有') }}
                </bkdata-radio>
                <bkdata-radio :value="'public'"
                  :disabled="!isPublicOptionAvailable">
                  {{ $t('公开') }}
                </bkdata-radio>
              </bkdata-radio-group>
              <span v-bk-tooltips="tipContent"
                class="tips-icon icon-question-circle" />
            </bkdata-form-item>
            <bkdata-form-item :label="$t('资源组管理员')"
              :required="true"
              :property="'admin'">
              <bkdata-tag-input v-model="formData.admin"
                style="width: 100%"
                :hasDeleteIcon="true"
                :disabled="editMode"
                :list="userList" />
            </bkdata-form-item>
            <bkdata-form-item :label="$t('资源组描述')">
              <bkdata-input v-model="formData.description"
                :type="'textarea'"
                :placeholder="$t('请输入资源组描述')"
                :rows="3" />
            </bkdata-form-item>
          </bkdata-form>
        </div>
      </template>
    </dialogWrapper>
  </div>
</template>

<script>
import dialogWrapper from '@/components/dialogWrapper';
import { postMethodWarning } from '@/common/js/util';
import { mapGetters } from 'vuex';
import Cookies from 'js-cookie';
export default {
  components: {
    dialogWrapper,
  },
  data() {
    return {
      bkUser: '',
      contentLoading: false,
      editMode: false,
      dialogConfig: {
        isShow: false,
        width: 576,
        quickClose: false,
        loading: false,
      },
      dialogFooter: {
        loading: false,
        confirmText: window.$t('创建'),
        cancleText: window.$t('取消'),
      },
      bizs: {
        placeholder: '',
        isLoading: false,
        list: [],
      },
      formData: {
        group_name: '',
        resource_group_id: '',
        bk_biz_id: '',
        admin: [],
        description: '',
        group_type: 'private',
      },
      userList: [],
    };
  },

  computed: {
    ...mapGetters({
      adminList: 'auth/getAdminList',
    }),
    isPublicOptionAvailable() {
      return this.adminList.includes(this.bkUser);
    },
    tipContent() {
      const privateText = this.$t('私有_只有授权的应用才能该资源组资源');
      const publicText = this.$t('公开_无需授权就可以使用该资源组资源');
      return `
                <p>${privateText}</p>
                <p>${publicText}</p>
            `;
    },
    dialogTitle() {
      return this.editMode ? this.$t('编辑资源组') : this.$t('您即将创建一个资源组');
    },
    validate() {
      const keys = ['group_name', 'resource_group_id', 'bk_biz_id', 'admin'];
      const validator = {};
      keys.forEach(item => {
        validator[item] = [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ];
      });
      return validator;
    },
  },
  watch: {
    editMode(val) {
      this.dialogFooter.confirmText = val ? this.$t('修改') : this.$t('创建');
    },
    'dialogConfig.isShow'(val) {
      !val && this.$refs.form.clearError(); // 清空错误提示
    },
  },
  created() {
    this.getBizList();
    this.getProjectMember();
    this.bkUser = this.$store.getters.getUserName;
  },
  methods: {
    editResourceGroup(id) {
      this.openDialog();
      this.contentLoading = true;
      this.editMode = true;
      this.bkRequest
        .httpRequest('resourceManage/viewGroup', {
          params: {
            resource_group_id: id,
          },
        })
        .then(res => {
          if (res.result) {
            this.formData = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.contentLoading = false;
        });
    },
    /*
     * 获取业务列表
     */
    getBizList() {
      let actionId = 'result_table.query_data';
      let dimension = 'bk_biz_id';
      this.bizs.isLoading = true;
      this.bkRequest
        .httpRequest('meta/getMineBizs', { params: { action_id: actionId, dimension: dimension } })
        .then(res => {
          if (res.result) {
            this.bizs.placeholder = this.$t('请选择业务');
            let businesses = res.data;
            if (businesses.length === 0) {
              return;
            } else {
              businesses.map(v => {
                this.bizs.list.push({
                  bk_biz_id: parseInt(v.bk_biz_id),
                  bk_biz_name: v.bk_biz_name,
                });
              });
            }
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.bizs.isLoading = false;
        });
    },
    /*
                获取所有成员名单,获取成员
            */
    getProjectMember() {
      this.axios.get('projects/list_all_user/').then(res => {
        if (res.result) {
          this.userList = res.data.map(user => ({ id: user, name: user }));
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    resetConfig() {
      /** 清空配置 */
      this.formData = {
        group_name: '',
        resource_group_id: '',
        bk_biz_id: '',
        admin: [this.bkUser],
        description: '',
        group_type: 'private',
      };
    },
    newResourceGroup() {
      this.editMode = false;
      this.resetConfig();
      this.openDialog();
    },
    openDialog() {
      this.dialogConfig.isShow = true;
    },
    closeDialog() {
      this.dialogConfig.isShow = false;
      this.resetConfig();
    },
    confirmClick() {
      this.$refs.form.validate().then(validator => {
        this.dialogFooter.loading = true;
        this.editMode ? this.updateGroup() : this.createGroup();
      });
    },
    createGroup() {
      this.bkRequest
        .httpRequest('resourceManage/createGroup', {
          params: this.formData,
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('提交成功'), 'success');
            this.$emit('updateList');
            this.closeDialog();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.dialogFooter.loading = false;
        });
    },
    updateGroup() {
      this.bkRequest
        .httpRequest('resourceManage/updateGroup', {
          params: this.formData,
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('修改成功'), 'success');
            this.$emit('updateList');
            this.closeDialog();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.dialogFooter.loading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.resource-dialog {
  .resource-content {
    width: 480px;
    padding: 20px 0;
    .tips-icon {
      position: absolute;
      right: 5px;
      font-size: 12px;
      cursor: pointer;
    }
  }
}
</style>
