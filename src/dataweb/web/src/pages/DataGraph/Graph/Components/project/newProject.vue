

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
  <div class="new-project-container">
    <bkdata-dialog
      v-model="newProjectDialog.isShow"
      :maskClose="newProjectDialog.isQuickClose"
      extCls="bkdata-dialog none-padding"
      :loading="loading.btnLoading"
      :okText="$t('创建')"
      :cancelText="$t('取消')"
      :hasHeader="newProjectDialog.hasHeader"
      :hasFooter="newProjectDialog.hasFooter"
      :width="newProjectDialog.width"
      :padding="0"
      @confirm="createNewProject"
      @cancel="close">
      <div class="new-project">
        <div class="hearder pl20">
          <div class="text fl">
            <p class="title">
              <i class="bk-icon icon-folder" />{{ $t('您即将创建一个项目') }}
            </p>
            <p>{{ $t('您的数据将以项目的形式进行组织') }}</p>
          </div>
        </div>
        <div class="content">
          <div class="bk-form">
            <div class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('项目名称') }}：</label>
              <div class="bk-form-content">
                <bkdata-input
                  v-model.trim="params.project_name"
                  :class="['project-name-form', { 'is-danger': error.nameError }]"
                  :maxlength="50"
                  autofocus="autofocus"
                  :placeholder="$t('请输入项目名称_50个字符限制')"
                  @input="nameTips()" />
                <div v-if="error.nameError"
                  class="bk-form-tip">
                  <span class="bk-tip-text">{{ error.nameTips }}</span>
                </div>
              </div>
            </div>
            <div v-if="false"
              class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('项目类型') }}：</label>
              <div class="bk-form-content">
                <bkdata-selector
                  :class="{ 'is-danger': error.typeError }"
                  :placeholder="projectSelect.placeholder"
                  :list="projectSelect.list"
                  :displayKey="'description'"
                  :selected.sync="params.tag_name"
                  :settingKey="'tag_name'"
                  :disabled="projectSelect.disabled"
                  @item-selected="typeSelected" />
                <i class="bk-icon icon-exclamation-circle">
                  <div class="tips">
                    {{
                      $t(
                        '项目类型决定任务的计算和存储集群_切换项目类型_会影响项目底下的任务的计算和存储集群_请谨慎选择'
                      )
                    }}
                  </div>
                </i>
                <div v-if="error.typeError"
                  class="bk-form-tip">
                  <p class="bk-tip-text">
                    {{ error.tyleTips }}
                  </p>
                </div>
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('项目管理员') }}：</label>
              <div :class="['bk-form-content', { 'is-danger': error.adminError }]">
                <!-- <bkdata-input

                                    :id='adminId'
                                    :placeholder="$t('添加项目管理员')"> -->
                <bkdata-tag-input
                  v-model="source_admin"
                  :placeholder="$t('添加项目管理员')"
                  :hasDeleteIcon="true"
                  :list="allUsers" />
                <i
                  v-bk-tooltips="$t('具有该项目的所有操作权限_包括项目信息管理_人员管理_资源管理_数据开发')"
                  class="bk-icon icon-exclamation-circle bk-icon-new">
                  <!-- <div class="tips">
                                        {{$t('具有该项目的所有操作权限_包括项目信息管理_人员管理_资源管理_数据开发')}}
                                    </div> -->
                </i>
                <div v-if="error.adminError"
                  class="bk-form-tip">
                  <p class="bk-tip-text">
                    {{ error.adminTips }}
                  </p>
                </div>
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('数据开发员') }}：</label>
              <div class="bk-form-content">
                <!-- <bkdata-input

                                    :id='otherMemberId'
                                    :placeholder="$t('添加项目成员')"> -->
                <bkdata-tag-input
                  v-model="source_member"
                  :placeholder="$t('添加项目成员')"
                  :hasDeleteIcon="true"
                  :list="allUsers" />
                <i
                  v-bk-tooltips="$t('具有该项目内的任务管理权限_包括实时计算_离线计算_数据视图_模型的开发和调试')"
                  class="bk-icon icon-exclamation-circle bk-icon-new">
                  <!-- <div class="tips">
                                        {{$t('具有该项目内的任务管理权限_包括实时计算_离线计算_数据视图_模型的开发和调试')}}
                                    </div> -->
                </i>
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label"><span class="required">*</span>{{ $t('项目描述') }}：</label>
              <div class="bk-form-content">
                <textarea
                  id=""
                  v-model.trim="params.description"
                  name=""
                  :class="['bk-form-input', { 'is-danger': error.desError }]"
                  :maxlength="50"
                  class="bk-form-textarea"
                  :placeholder="$t('请输入项目描述_50个字符限制')"
                  @input="descTips()" />
                <div v-if="error.desError"
                  class="bk-form-tip">
                  <p class="bk-tip-text">
                    {{ error.desTips }}
                  </p>
                </div>
              </div>
            </div>
            <div class="bk-form-item">
              <div class="bk-form-content">
                <span class="advanced-config"
                  @click="handleToggleAdvanced">
                  {{ $t('高级配置') }}
                  <i :class="['bk-icon', 'icon-angle-down', { show: showAdvanced }]" />
                </span>
              </div>
            </div>
            <template v-if="showAdvanced">
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('区域') }}：</label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :displayKey="'alias'"
                    :list="regionInfo"
                    :selected.sync="params.tags[0]"
                    :settingKey="'region'"
                    @item-selected="changeArea" />
                  <div v-if="error.areaError"
                    class="bk-form-tip">
                    <p class="bk-tip-text">
                      {{ error.areaTips }}
                    </p>
                  </div>
                </div>
              </div>
              <!-- TDW支持组件 -->
              <component :is="tdwFragments.componment"
                v-bind="tdwFragments.bindAttr"
                @changeCheckTdw="changeCheckTdw"
                @tipsClick="linkToUserAccount"
                @itemSelected="validateTdw" />
            </template>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
import { postMethodWarning } from '@/common/js/util.js';
import { mapGetters } from 'vuex';
import tdwPower from '@/pages/DataGraph/Common/tdwPower.js';
import extend from '@/extends/index';

export default {
  mixins: [tdwPower],
  props: {
    idNum: {
      type: Number,
    },
  },
  data() {
    return {
      allUsers: [],
      source_admin: [],
      source_member: [],
      newProjectDialog: {
        hasHeader: false,
        hasFooter: false,
        isQuickClose: false,
        width: 576,
        isShow: false,
      },
      projectSelect: {
        placeholder: this.$t('请选择项目类型'),
        bizplaceholder: this.$t('请选择业务'),
        disabled: true,
        list: [],
        multiple: true,
        filterable: true,
        bizDisabled: true,
      },
      error: {
        nameError: false,
        nameTips: '',
        typeError: false,
        tyleTips: '',
        adminError: false,
        adminTips: '',
        desError: '',
        areaError: '',
        areaTips: '',
        desTips: '',
        applicationError: false,
        applicationTips: '',
      },
      params: {
        name: '',
        tag_name: '',
        admin: '',
        member: '',
        description: '',
        biz_ids: '',
        enableTdw: false,
        tdw_app_groups: [],
        tags: [],
      },
      bizLists: [],
      loading: {
        btnLoading: true,
      },
      regionInfo: [],
      showAdvanced: false,
    };
  },
  computed: {
    /** 获取TDW支持组件 */
    tdwFragments() {
      const tdwFragments = extend.getVueFragment('tdwFragment', this) || [];
      return tdwFragments.filter(item => item.name === 'TdwSupport')[0];
    },
    adminId: function () {
      return 'admin' + this.idNum;
    },
    otherMemberId: function () {
      return 'otherMember' + this.idNum;
    },
    ...mapGetters(['getUserName']),
    /** 当前位置：海外还是国内 */
    currentLocation() {
      const versionMap = {
        tgdp: ['overseas'],
        ieod: ['mainland'],
        openpaas: ['mainland', 'overseas'],
      };
      return versionMap[window.BKBASE_Global.runVersion] || ['mainland'];
    },
  },
  created() {
    this.formatRegion();
  },
  methods: {
    formatRegion() {
      this.bkRequest.httpRequest('meta/getAvailableGeog').then(res => {
        this.regionInfo = res.data;
        if (this.regionInfo.length) {
          this.params.tags[0] = this.regionInfo[0].region;
        }
      });
    },
    linkToCC() {
      this.$linkToCC();
    },
    /*
                项目名称验证
            */
    nameTips() {
      if (this.params.project_name !== '') {
        this.error.nameError = false;
      } else {
        this.error.nameError = true;
        this.error.nameTips = this.validator.message.required;
      }
    },
    changeArea(val) {
      this.params.tags = [];
      if (!val) return;
      this.params.tags.push(val);
      this.areaTips();
    },
    areaTips() {
      if (this.params.tags[0] !== '') {
        this.error.areaError = false;
      } else {
        this.error.areaError = true;
        this.error.areaTips = this.validator.message.required;
      }
    },
    /*
                项目名称验证
            */
    descTips() {
      if (this.params.description) {
        this.error.desError = false;
      } else {
        this.error.desError = true;
        this.error.desTips = this.validator.message.required;
      }
    },
    /*
                项目类型验证
            */
    typeSelected() {
      // todo
      // if (this.params.tag_name !== '') {
      //     this.error.typeError = false
      // } else {
      //     this.error.typeError = true
      //     this.error.tyleTips = this.validator.message.required
      // }
    },
    /*
                项目管理员验证
            */
    adminVerify() {
      this.params.admin = Array.from(this.source_admin);
      this.params.member = Array.from(this.source_member);
      if (this.params.admin.length > 0 && this.params.admin[0] !== '') {
        this.error.adminError = false;
      } else {
        this.error.adminError = true;
        this.error.adminTips = this.validator.message.required;
      }
    },
    /**
     *  tdw相关验证
     */
    validateTdw(value, options) {
      if (this.params.tdw_app_groups.length > 0 && this.params.tdw_app_groups[0] !== '') {
        this.error.applicationError = false;
      } else {
        this.error.applicationError = true;
        this.error.applicationTips = this.validator.message.required;
      }
    },
    linkToUserAccount() {
      this.close();
      this.$router.push({ path: '/user-center', query: { tab: 'account' } });
    },
    close() {
      this.newProjectDialog.isShow = false;
      this.$emit('dialogChange', this.newProjectDialog.isShow);
    },
    open() {
      this.params = {
        project_name: '',
        tag_name: '',
        admin: [],
        member: [],
        description: '',
        enableTdw: false,
        tdw_app_groups: [],
        tags: ['inland'],
      };
      this.hasPower = '';
      this.init();
      this.source_admin = [this.getUserName];

      this.newProjectDialog.isShow = true;
      this.$emit('dialogChange', this.newProjectDialog.isShow);
      this.error.nameError = false;
      this.error.typeError = false;
      this.error.adminError = false;
      this.error.desError = false;
      this.error.applicationError = false;
      this.reset();
    },
    /*
                创建新项目
            */
    createNewProject() {
      this.nameTips();
      this.typeSelected();
      this.adminVerify();
      this.descTips();
      this.areaTips();
      if (this.enableTdw) {
        this.validateTdw();
      }
      let arr = [];
      if (this.params.biz_ids !== undefined && typeof this.params.biz_ids === 'string') {
        this.params.biz_ids = this.params.biz_ids.split(',');
        this.params.biz_ids.forEach((item, index) => {
          arr.push(Number(item));
        });
        this.params.biz_ids = arr;
      } else if (this.params.biz_ids instanceof Array) {
        this.params.biz_ids.forEach((item, index) => {
          arr.push(Number(item));
        });
        this.params.biz_ids = arr;
      } else {
        this.params.biz_ids = [];
      }

      if (this.loading.btnLoading) return;
      const { nameError, typeError, adminError, desError, applicationError, areaError } = this.error;
      if (!nameError && !typeError && !adminError && !desError && !areaError) {
        if (this.enableTdw && applicationError) {
          this.loading.btnLoading = true;
          postMethodWarning(this.$t('无TDW权限_无法创建支持TDW的项目'), 'error');
          setTimeout(() => {
            this.loading.btnLoading = false;
          }, 0);
          return false;
        }
        this.loading.btnLoading = true;
        this.$emit('createNewProject', this.params);
      } else {
        // 如果校验失败， loading置为ture， 下一个circyle，置位fales,防止对话框自己关闭
        this.loading.btnLoading = true;
        setTimeout(() => {
          this.loading.btnLoading = false;
        }, 0);
      }
    },
    /*
                获取项目类型下拉
            */
    getProjectType() {
      this.projectSelect.placeholder = this.$t('数据加载中');
      this.axios.get('projects/list_project_tag/').then(res => {
        if (res.result) {
          this.projectSelect.disabled = false;
          this.projectSelect.list = res.data;
          if (this.projectSelect.list.length === 0) {
            this.projectSelect.placeholder = this.$t('暂无数据');
            this.getMethodWarning(this.$t('后台未配置项目类型'), '0000');
          } else {
            this.params.tag_name = this.projectSelect.list[0].tag_name;
            this.projectSelect.placeholder = this.$t('请选择项目类型');
          }
        } else {
          this.getMethodWarning(res.message, res.code);
          this.projectSelect.placeholder = this.$t('加载失败_请重试');
        }
      });
    },
    /*
                获取所有成员名单,获取成员
            */
    getProjectMember() {
      this.axios.get('projects/list_all_user/').then(res => {
        if (res.result) {
          this.allUsers = res.data.map(user => ({ id: user, name: user }));
          // this.memberInit(res.data)
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    /** @description
     * 获取业务类型列表
     */
    getBizLists() {
      this.projectSelect.bizDisabled = true;
      this.projectSelect.bizplaceholder = this.$t('数据加载中');
      this.axios
        .get('bizs/list_mine_biz/')
        .then(res => {
          if (res.result) {
            this.bizLists.splice(0);
            for (let i = 0; i < res.data.length; i++) {
              this.bizLists.push({
                id: res.data[i].bk_biz_id,
                name: res.data[i].bk_biz_name,
              });
            }
            if (!this.bizLists.length) {
              this.projectSelect.bizplaceholder = this.$t('暂无数据');
            } else {
              this.projectSelect.bizplaceholder = this.$t('请选择');
            }
          } else {
            this.getMethodWarning(res.message, res.code);
            this.projectSelect.bizplaceholder = this.$t('加载失败_请重试');
          }
        })
        ['finally'](() => {
          this.projectSelect.bizDisabled = false;
        });
    },

    init() {
      this.getProjectMember();
      // this.getProjectType()
      this.getBizLists();
    },
    reset() {
      this.loading.btnLoading = false;
    },
    handleToggleAdvanced() {
      this.showAdvanced = !this.showAdvanced;
    },
  },
};
</script>

<style media="screen" lang="scss">
.new-project {
  .hearder {
    height: 70px;
    background: #23243b;
    position: relative;
    padding: 0 !important;
    .close {
      display: inline-block;
      position: absolute;
      right: 0;
      top: 0;
      width: 40px;
      height: 40px;
      line-height: 40px;
      text-align: center;
      cursor: pointer;
    }
    .icon {
      font-size: 32px;
      color: #abacb5;
      line-height: 60px;
      width: 142px;
      text-align: right;
      margin-right: 16px;
    }
    .text {
      margin-left: 28px;
      font-size: 12px;
    }
    .title {
      margin: 16px 0 3px 0;
      padding-left: 36px;
      position: relative;
      color: #fafafa;
      font-size: 18px;
      text-align: left;
      i {
        position: absolute;
        left: 10px;
        top: 2px;
      }
    }
  }
  .content {
    max-height: 500px;
    padding: 30px 0 45px 0;
    overflow-y: auto;
    .bk-form {
      width: 480px;
      .bk-label {
        padding-right: 16px;
        width: 165px;
      }
      .bk-label-checkbox {
        width: 79px !important;
        margin: 0;
        padding-top: 9px;
        padding-bottom: 9px;
      }
      .bk-form-checkbox {
        margin-right: 5px;
      }
      .bk-checkbox-text {
        font-style: normal;
        font-weight: normal;
        cursor: pointer;
        vertical-align: middle;
      }
      .bk-form-not-power {
        margin: 0 !important;
        line-height: 25px !important;
        a {
          vertical-align: sub;
          text-decoration: underline;
        }
      }
      .bkdata-selector-create-item {
        width: 100%;
        float: left;
        padding: 10px 10px;
        background-color: #fafbfd;
        cursor: pointer;
        .text {
          font-style: normal;
          color: #3a84ff;
        }
        &:hover {
          background: #cccccc;
        }
      }
      .bk-form-action {
        margin-top: 20px;
      }
      .bk-form-content {
        margin-left: 165px;
        display: block;
        .tdw-form-content {
          display: flex;
          align-items: end;
        }
        &.is-danger {
          .bk-data-wrapper {
            border-color: #ff5656;
          }
        }
        .is-danger {
          color: #000;
        }
        .no-data {
          padding: 5px;
        }
        .bk-icon.icon-exclamation-circle {
          position: absolute;
          left: 105%;
          top: 8px;
          font-size: 16px;
          cursor: pointer;
          .tips {
            display: none;
            margin-left: 0;
            position: absolute;
            width: 200px;
            border-radius: 4px;
            background: #000;
            color: #fff;
            font-size: 14px;
            right: 30px;
            top: 3px;
            transform: translate(0%, -50%);
            &::after {
              content: '';
              border: 5px solid #000;
              border-color: transparent transparent transparent #000;
              position: absolute;
              right: -10px;
              top: 50%;
            }
          }
        }
        .bk-icon.icon-exclamation-circle:hover .tips {
          display: block;
        }
        .advanced-config {
          display: flex;
          align-items: center;
          font-size: 12px;
          cursor: pointer;
          .bk-icon {
            font-size: 20px;
            transition: all 0.5s;
            &.show {
              transform: rotate(180deg);
            }
          }
        }
      }
      .bkdata-selector.is-danger {
        .bkdata-selector-input {
          border-color: #ff5656;
          &[placeholder],
          [placeholder],
          *[placeholder] {
            /* Chrome/Opera/Safari */
            color: #ff5656;
          }
        }
      }
      .bk-form-not-power {
        line-height: 36px;
        a {
          vertical-align: sub;
          text-decoration: underline;
        }
      }
      .bk-form-power-loading {
        height: 36px;
      }
      .bk-form-tip .bk-tip-text {
        color: #ff5656;
      }
      .bk-badge {
        line-height: 18px;
      }
    }
    .tips {
      background: #e1f3ff;
      min-height: 42px;
      line-height: 22px;
      color: #3c96ff;
      padding: 10px 10px;
      margin-left: 165px;
    }
    .required {
      color: #f64646;
      display: inline-block;
      vertical-align: -2px;
      margin-right: 5px;
    }
    .bk-button {
      min-width: 120px;
    }
  }
}
</style>
