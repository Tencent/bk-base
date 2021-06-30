

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
  <DialogWrapper ref="dialog"
    :title="title"
    :subtitle="$t('申请使用资源组资源需要资源组管理人审批')"
    :icon="'icon-apply'"
    :dialog="dialogConfig"
    :defaultFooter="false"
    extCls="applied-recource-dialog up100px">
    <template #content>
      <div class="content-wrapper">
        <bkdata-form ref="bizForm"
          :labelWidth="126"
          :model="formData">
          <bkdata-form-item :label="$t('业务列表')"
            property="bk_biz_id"
            :rules="rules.change">
            <bkdata-selector :isLoading="biz.isSelectorLoading"
              :placeholder="$t('请选择')"
              :searchable="true"
              :list="biz.list"
              :displayKey="'bk_biz_name'"
              :selected.sync="formData.bk_biz_id"
              :settingKey="'bk_biz_id'"
              :searchKey="'bk_biz_name'"
              @item-selected="bizSelectedHandle" />
          </bkdata-form-item>
        </bkdata-form>
        <div class="main-content">
          <div class="left-content">
            <bkdata-form ref="mainForm"
              :labelWidth="126"
              :model="formData">
              <bkdata-form-item :label="$t('资源组类型')"
                :rules="rules.change"
                property="group_type">
                <bkdata-selector :list="resourceType"
                  :displayKey="'name'"
                  :selected.sync="formData.group_type"
                  :settingKey="'key'"
                  @item-selected="bizSelectedHandle" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('资源组')"
                :rules="rules.change"
                property="resource_group_id">
                <bkdata-selector :list="resourceGroup.list"
                  :isLoading="resourceGroup.loading"
                  :searchable="true"
                  :placeholder="$t('请先选择业务和资源组类型')"
                  :displayKey="'disName'"
                  :searchKey="'disName'"
                  :selected.sync="formData.resource_group_id"
                  :settingKey="'resource_group_id'" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('申请理由')"
                :rules="rules.blur"
                property="reason">
                <bkdata-input v-model="formData.reason"
                  :type="'textarea'"
                  :rows="3" />
              </bkdata-form-item>
              <bkdata-form-item>
                <bkdata-button :theme="'primary'"
                  style="width: 118px"
                  :loading="submitLoading"
                  @click="submitHandle">
                  {{ $t('提交') }}
                </bkdata-button>
              </bkdata-form-item>
            </bkdata-form>
          </div>
          <div class="right-content">
            <ApplyListView :listLoading="isApplyListLoading"
              :applyGroupList="applyGroupList" />
          </div>
        </div>
      </div>
    </template>
  </DialogWrapper>
</template>

<script>
import DialogWrapper from '@/components/dialogWrapper';
import ApplyListView from './ApplyListField';
import Cookies from 'js-cookie';
import { postMethodWarning } from '@/common/js/util.js';
export default {
  components: {
    DialogWrapper,
    ApplyListView,
  },
  data() {
    return {
      forms: ['bizForm', 'mainForm'],
      bkUserName: '',
      submitLoading: false,
      applyGroupList: [],
      isApplyListLoading: false,
      dialogConfig: {
        isShow: false,
        width: 802,
        quickClose: false,
        loading: false,
      },
      rules: {
        blur: [
          {
            required: true,
            message: window.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
        change: [
          {
            required: true,
            message: window.$t('必填项不可为空'),
            trigger: 'change',
          },
        ],
      },
      biz: {
        isSelectorLoading: false,
        list: [],
      },
      project: {},
      resourceGroup: {
        list: [],
        loading: false,
      },
      resourceType: [
        {
          name: window.$t('私有'),
          key: 'private',
        },
      ],
      formData: {
        bk_biz_id: 'all',
        group_type: 'private',
        resource_group_id: '',
        reason: '',
      },
    };
  },
  computed: {
    title() {
      return `${this.$t('项目申请资源组')}(${this.project.project_name})`;
    },
    bizId() {
      return this.formData.bk_biz_id === 'all' ? '' : this.formData.bk_biz_id;
    },
    groupType() {
      return this.formData.group_type === 'all' ? '' : this.formData.group_type;
    },
  },
  watch: {
    'dialogConfig.isShow': {
      immediate: true,
      handler(val, oldValue) {
        if (val) {
          this.getBizTicketsCount();
          this.getApplyViewList();
          this.bizSelectedHandle();
        } else if (!val && oldValue) {
          this.formData.bk_biz_id = '';
          this.formData.resource_group_id = '';
          this.resourceGroup.list = [];
          this.clearFormError();
        }
      },
    },
  },
  methods: {
    clearFormError() {
      this.forms.forEach(ref => {
        this.$refs[ref].clearError();
      });
    },
    submitHandle() {
      const self = this;
      Promise.all(this.forms.map(ref => self.$refs[ref].validate())).then(
        validator => {
          this.applyData();
        },
        validator => {
          console.log('验证不通过');
        }
      );
    },
    applyData() {
      if (!this.bkUserName) {
        this.bkUserName = this.$store.getters.getUserName;
      }
      this.submitLoading = true;
      this.bkRequest
        .httpRequest('resourceManage/projectApplyResource', {
          params: {
            project_id: this.project.project_id,
            resource_group_id: this.formData.resource_group_id,
            bk_username: this.bkUserName,
            reason: this.formData.reason,
          },
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('提交成功'), 'success');
            this.dialogConfig.isShow = false;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.submitLoading = false;
        });
    },
    getApplyViewList() {
      this.isApplyListLoading = true;
      this.bkRequest
        .httpRequest('resourceManage/resourceViewList', {
          params: {
            project_id: this.project.project_id,
          },
        })
        .then(res => {
          if (res.result) {
            const names = Object.keys(res.data);
            this.applyGroupList = Object.values(res.data).map((item, index) => {
              item.name = `${names[index]}(${item.group_name})`;
              return item;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isApplyListLoading = false;
        });
    },
    setProject(project) {
      this.$set(this, 'project', project);
      this.dialogConfig.isShow = true;
    },
    bizSelectedHandle() {
      this.formData.bk_biz_id && this.formData.group_type && this.getResourceGroupList();
    },
    getResourceGroupList() {
      const options = {
        query: {
          bk_biz_id: this.bizId,
          group_type: this.groupType,
          status: 'succeed',
        },
      };
      this.resourceGroup.loading = true;
      this.bkRequest
        .httpRequest('resourceManage/getGroupListWithoutOption', options)
        .then(res => {
          if (res.result) {
            this.resourceGroup.list = res.data.map(item => {
              item.disName = `${item.resource_group_id}(${item.group_name})`;
              return item;
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.resourceGroup.loading = false;
        });
    },
    getBizTicketsCount() {
      let options = {
        params: {
          project_id: this.project.project_id,
        },
      };
      this.biz.isSelectorLoading = true;
      this.bkRequest.httpRequest('auth/getBizTicketsCount', options).then(res => {
        if (res.result) {
          this.biz.list = res.data;
          if (this.biz.list.findIndex(item => item.bk_biz_id === 'all') === -1) {
            this.biz.list.unshift({
              bk_biz_id: 'all',
              bk_biz_name: this.$t('全部'),
            });
          }
          this.biz.isSelectorLoading = false;
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .applied-recource-dialog {
  .content-wrapper {
    padding: 20px 50px 50px 0;
    .main-content {
      padding: 20px 0px 50px 0;
      display: flex;
      justify-content: space-between;
      .left-content {
        width: 373px;
      }
      .right-content {
        position: relative;
        right: 0;
        width: 360px;
        height: 328px;
        padding: 20px 10px 20px 20px;
        background: #fafbfd;
      }
    }
  }
  .wrapper {
    .footer {
      display: none;
    }
  }
}
</style>
