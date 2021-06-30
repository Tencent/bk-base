

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
  <div class="bkdata-dialog">
    <bkdata-dialog
      v-model="isShow"
      :loading="isDialogLoading"
      :hasHeader="false"
      :hasFooter="true"
      :closeIcon="true"
      :maskClose="false"
      :okText="$t('保存')"
      :cancelText="$t('取消')"
      :width="800"
      @confirm="saveMember"
      @cancel="closeMemberManager">
      <div v-bkloading="{ isLoading: isLoading }"
        class="member-manager">
        <p class="title">
          {{ $t('更新成员列表') }}
        </p>

        <div class="info">
          {{ $t('您正在更新') }}
          【{{ objectClassName }}】 <span class="bold">{{ `[${bizId}]${objectName}` }}</span>
          {{ $t('的成员列表') }}
        </div>
        <table class="bkdata-table-bordered member-manager-table"
          style="border-collapse: collapse">
          <colgroup>
            <col style="width: 130px">
            <col>
          </colgroup>
          <tbody>
            <tr>
              <td class="role-name">
                {{ managerName }}
              </td>
              <td>
                <bkdata-tag-input
                  ref="userManager"
                  v-model="memberList"
                  :placeholder="inputPlaceholder"
                  :hasDeleteIcon="true"
                  :list="managerList"
                  @change="updateMemberList" />
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
export default {
  props: {
    objectClassName: {
      type: String,
    },
    objectName: {
      type: String,
      default: '',
    },
    managerName: {
      type: String,
    },
    bizId: {
      type: [Number, String],
      required: true,
      default: '',
    },
    tpl: {
      type: Function,
      default: () => {},
    },
    value: {
      type: Array,
      default: () => [],
    },
    managerList: {
      type: Array,
      default: () => [],
    },
    isDialogLoading: {
      type: Boolean,
      default: false,
    },
    isLoading: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      isShow: false,
      inputPlaceholder: window.$t('请输入并按Enter结束'),
      memberList: [],
    };
  },
  watch: {
    memberList(val) {
      this.$nextTick(() => {
        this.isShow = true;
        this.$refs.userManager.updateData && this.$refs.userManager.updateData(val);
      });
    },
    value: {
      immediate: true,
      handler(val) {
        if (val.length) {
          this.memberList = JSON.parse(JSON.stringify(val));
        }
      },
    },
  },
  methods: {
    updateMemberList(val) {
      this.$emit('input', val);
    },
    saveMember() {
      this.$emit('saveMember');
    },
    closeMemberManager() {
      this.$nextTick(() => {
        this.$emit('close', false);
      });
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .bk-dialog-body {
  padding: 3px 24px 26px !important;
}
</style>
<style lang="scss">
.member-manager {
  .title {
    height: 20px;
    line-height: 20px;
    border-left: 4px solid #3a84ff;
    padding-left: 15px;
    color: #212232;
    font-size: 16px;
  }
  .info {
    margin-top: 20px;
    margin-bottom: 10px;
  }
  span.bold {
    text-decoration: underline;
  }
  .bk-tag-input {
    min-height: 80px !important;
  }
  .role-name {
    font-weight: bold;
  }
  .member-manager-table {
    .bkdata-selector-node {
      text-align: left;
    }
  }
}
</style>
