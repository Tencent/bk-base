

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
      :extCls="!singleSelect.length ? 'disabled' : ''"
      :loading="isLoading"
      :draggable="false"
      :hasHeader="false"
      :hasFooter="true"
      :closeIcon="true"
      :maskClose="false"
      :okText="isShowNext ? next : confirm"
      :cancelText="$t('取消')"
      :width="800"
      :autoClose="false"
      @confirm="changeStatus"
      @cancel="closeDialog">
      <div>
        <div class="title app-title">
          {{ $t('权限移交') }}
        </div>
      </div>
      <div v-if="isShowNext"
        class="bkdata-form-horizontal apply-form">
        <div class="form-item">
          <label class="label-title">{{ $t('交接人') }}：</label>
          <div class="bk-form-content">
            {{ userName }}
          </div>
        </div>
        <div class="form-item">
          <label class="label-title">{{ $t('被交接人') }}：</label>
          <div class="bk-form-content">
            <bkdata-tag-input
              v-model="singleSelect"
              :placeholder="$t('请输入并按Enter结束')"
              :hasDeleteIcon="true"
              :extCls="'width100'"
              :list="allUsersForTag"
              :maxData="1"
              :tpl="tpl" />
          </div>
        </div>
      </div>
      <div v-else
        class="explain">
        <p>{{ $t('此操作将交接您拥有的全部权限，包括但不限于') }}：</p>
        <ul>
          <li class="explain-item">
            - {{ $t('全部项目的管理权限') }}
          </li>
          <li class="explain-item">
            - {{ $t('全部原始数据以及结果数据的管理权限') }}
          </li>
          <li class="explain-item">
            - {{ $t('调用DataAPI授权码的管理权限') }}
          </li>
          <li class="explain-item">
            - {{ $t('数据开发中自定义函数的管理权限') }}
          </li>
        </ul>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import { showMsg } from '@/common/js/util.js';
import { getAllUsers } from '@/common/api/base';
import { mapState } from 'vuex';
export default {
  data() {
    return {
      isShow: false,
      isLoading: false,
      singleSelect: [],
      allUsers: [],
      next: this.$t('下一步'),
      confirm: this.$t('确定'),
      isShowNext: true,
    };
  },
  computed: {
    allUsersForTag() {
      return (this.allUsers || []).map(user => {
        return {
          id: user,
          name: user,
        };
      });
    },
    ...mapState({
      userName: state => state.common.userName,
    }),
  },
  mounted() {
    this.initDataMember();
  },
  methods: {
    tpl(node, ctx) {
      let parentClass = 'bkdata-selector-node';
      let textClass = 'text';
      let imgClass = 'avatar';
      return (
        <div class={parentClass}>
          <span class={textClass}>{node.name}</span>
        </div>
      );
    },
    changeStatus() {
      if (this.isShowNext) {
        this.isShowNext = false;
      } else {
        this.saveData();
      }
    },
    initDataMember() {
      getAllUsers().then(res => {
        if (res.result) {
          this.allUsers = res.data;
        } else {
          showMsg(res.message, 'error');
        }
      });
    },
    saveData() {
      this.isLoading = true;
      this.bkRequest
        .httpRequest('auth/setAuthTransfer', {
          params: {
            receiver: this.singleSelect[0],
          },
        })
        .then(res => {
          if (res.result) {
            showMsg(this.$t('权限交接完成'), 'success', { delay: 2000 });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isLoading = false;
          this.closeDialog();
        });
    },
    closeDialog() {
      this.isShow = false;
      this.isShowNext = true;
      this.singleSelect = [];
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .bk-dialog-wrapper .bk-dialog-body {
  padding: 3px 24px 26px !important;
}
::v-deep .disabled {
  .bk-dialog-footer {
    button[name='confirm'] {
      display: none;
    }
  }
}
.title {
  height: 20px;
  line-height: 20px;
  border-left: 4px solid #3a84ff;
  padding-left: 15px;
  color: #212232;
  font-size: 16px;
}
.width100 {
  width: 100%;
  height: 32px;
}
.explain {
  padding: 20px 20px 20px 30px;
  color: #313238;
  p {
    font-size: 14px;
    margin-bottom: 5px;
  }
  ul {
    margin-left: 10px;
    .explain-item {
      padding: 5px 0;
    }
  }
}
.label-title {
  text-align: right;
}
</style>
