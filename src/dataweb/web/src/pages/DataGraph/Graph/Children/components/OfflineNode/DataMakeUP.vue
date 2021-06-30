

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
  <dialog-wrapper
    :dialog="dialog"
    extCls="data-make-up"
    :defaultFooter="true"
    :title="$t('数据补齐')"
    :footerConfig="dialogFooter"
    :subtitle="$t('使用当前节点历史执行成功产生的数据补齐选定执行时间的数据')"
    @confirm="confirmHandle"
    @cancle="cancleHandle">
    <template #content>
      <div class="bk-form">
        <div class="bk-form-item">
          <label class="bk-label">{{ $t('节点名称') }}：</label>
          <div class="bk-form-content">
            <!--eslint-disable-next-line vue/no-mutating-props-->
            <bkdata-input v-model="nodeName"
              :disabled="true" />
          </div>
        </div>
        <div class="bk-form-item">
          <label class="bk-label">{{ $t('需要补齐的任务时间') }}：</label>
          <div class="bk-form-content">
            <!--eslint-disable-next-line vue/no-mutating-props-->
            <bkdata-input v-model="executeTime"
              :disabled="true" />
          </div>
        </div>
        <div class="bk-form-item">
          <label class="bk-label">{{ $t('选择已成功执行任务') }}：</label>
          <div class="bk-form-content">
            <bkdata-selector
              :displayKey="'id'"
              :list="historyTimeList"
              :placeholder="$t('请选择')"
              :selected.sync="endTime"
              :settingKey="'id'" />
          </div>
        </div>
        <div class="bk-form-item clearfix">
          <bkdata-checkbox v-model="withChild">
            {{ $t('补齐依赖该节点的所有下游节点') }}
          </bkdata-checkbox>
          <bkdata-checkbox v-model="withStorage">
            {{ $t('下发到数据存储') }}
          </bkdata-checkbox>
          <i
            v-bk-tooltips="$t('下发到数据存储后可在数据查询页面和其他地方查询到补齐的数据')"
            class="bk-icon icon-question-circle" />
        </div>
      </div>
    </template>
  </dialog-wrapper>
</template>

<script>
import dialogWrapper from '@/components/dialogWrapper';
import { postMethodWarning } from '@/common/js/util.js';
export default {
  components: {
    dialogWrapper,
  },
  props: {
    fid: {
      type: [Number, String],
      default: '',
    },
    nid: {
      type: [Number, String],
      default: '',
    },
    nodeName: {
      type: String,
      default: '',
    },
    executeTime: {
      type: String,
      default: '',
    },
    historyTimeList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      dialogFooter: {
        loading: false,
        confirmText: '',
        cancleText: '',
      },
      endTime: '',
      withChild: false,
      withStorage: false,
      dialog: {
        isShow: false,
        width: 576,
        quickClose: false,
        loading: false,
      },
    };
  },
  created() {
    this.dialogFooter.confirmText = this.$t('提交');
    this.dialogFooter.cancleText = this.$t('取消');
  },
  methods: {
    showDataMakeUp() {
      this.dialog.isShow = true;
    },
    cancleHandle() {
      this.dialog.isShow = false;
    },
    confirmHandle() {
      this.dialogFooter.loading = true;
      this.bkRequest
        .httpRequest('dataFlow/submitDataMakeUp', {
          params: {
            fid: this.fid,
            nid: this.nid,
            target_schedule_time: this.executeTime,
            source_schedule_time: this.endTime,
            with_child: this.withChild,
            with_storage: this.withStorage,
          },
        })
        .then(res => {
          if (res.result) {
            postMethodWarning(this.$t('提交成功'), 'success');
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.dialogFooter.loading = false;
          this.dialog.isShow = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.data-make-up {
  .content {
    max-height: 500px;
    padding-left: 50px;
    .bk-form {
      width: 480px;
      padding: 20px 0;
      margin-bottom: 10px;
      margin-left: 20px;
      .bk-label {
        width: 180px;
        padding-right: 10px;
      }
      .bk-form-checkbox {
        margin-left: 182px;
      }
      .icon-question-circle {
        margin-left: -20px;
        cursor: pointer;
      }
    }
  }
}
</style>
