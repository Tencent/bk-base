

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
  <!-- 说明：
    数据开发统一弹框样式，传入title、subtitle与icon作为统一的表头
    slot:
        content 弹框的内容
        footer 自定义的页脚
    options:
        title: 标题
        subtitle: 副标题
        icon: 标题头icon
        dialog: 弹框的基础配置
        loading: 整体弹窗的loding，用于异步加载内容
        defaultFooter： 是否使用默认footer
        footerConfig: {  使用默认footer的配置
            loading: 提交按钮的loading状态
            confirmText: 提交按钮的文案
        }
    event:
        @confirm: 确定按钮点击的回调函数
        @cancle: 取消按钮点击的回调函数
 -->
  <bkdata-dialog
    v-model="isShow"
    :extCls="`${extCls} bkdata-dialog bkdata-dialog-w-c none-padding`"
    :showFooter="false"
    :width="dialog.width"
    :fullscreen="dialog.fullscreen"
    :showMask="dialog.showMask"
    :position="dialog.position"
    :maskClose="dialog.quickClose"
    :closeIcon="dialog.closeIcon">
    <div v-bkloading="{ isLoading: dialog.loading }"
      class="wrapper">
      <div class="hearder">
        <slot name="header">
          <div class="text fl">
            <p :class="['title', (icon && 'title-icon') || '']">
              <span v-if="!!icon"
                :class="icon" />{{ title }}
            </p>
            <div class="subtitle">
              <div class="subtitle-left">
                {{ subtitle }}
                <span v-if="subtitleTip"
                  v-bk-tooltips="subtitleTip.content"
                  :class="['bk-icon', subtitleTip.icon]" />
              </div>
              <slot name="subtitle-right" />
            </div>
          </div>
        </slot>
      </div>
      <div :class="['content', { 'bk-scroll-y': bkScroll }]">
        <slot name="content" />
      </div>
      <div class="footer">
        <template v-if="defaultFooter">
          <bkdata-button
            theme="primary"
            class="data-btn"
            :disabled="disabled"
            :loading="footerConfig.loading"
            @click="$emit('confirm')">
            {{ footerConfig.confirmText }}
          </bkdata-button>
          <bkdata-button class="data-btn"
            @click="$emit('cancle')">
            {{ footerConfig.cancleText }}
          </bkdata-button>
        </template>
        <slot name="footer" />
      </div>
    </div>
  </bkdata-dialog>
</template>
<script>
export default {
  props: {
    disabled: {
      type: Boolean,
      default: false,
    },
    extCls: {
      type: String,
      default: '',
    },
    bkScroll: {
      type: Boolean,
      default: true,
    },
    dialog: {
      type: Object,
      default: () => ({
        fullscreen: false,
        showMask: false,
        isShow: false,
        width: 576,
        quickClose: false,
        loading: false,
        position: {},
        renderDirective: 'show',
      }),
    },
    defaultFooter: {
      type: Boolean,
      default: false,
    },
    footerConfig: {
      type: Object,
      default: () => ({
        confirmText: '提交',
        cancleText: '取消',
        loading: false,
      }),
    },
    icon: {
      type: String,
      default: 'icon-recal-normtask',
    },
    title: {
      type: String,
      default: '',
    },
    subtitle: {
      type: String,
      default: '',
    },
    subtitleTip: {
      type: Object,
      default: null,
    },
  },
  computed: {
    isShow: {
      get(){
        return this.dialog.isShow;
      },
      set(val){
        this.dialog.isShow = val;
      }
    },
  },
};
</script>
<style lang="scss">
.bkdata-dialog-w-c {
  &.up100px {
    .bk-dialog {
      top: 100px;
    }
  }

  .wrapper {
    .hearder {
      height: 70px;
      background: #23243b;
      position: relative;
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
      .subtitle-left {
        display: flex;
        align-items: center;
        color: #979ba5;
        .bk-icon {
          margin-left: 5px;
          cursor: pointer;
        }
      }

      .text {
        padding: 0 24px;
        width: 100%;
        font-size: 12px;
        .title {
          margin: 16px 0 3px 0;
          padding-left: 20px;
          position: relative;
          color: #fafafa;
          font-size: 18px;
          text-align: left;
          border-left: 4px solid #3a84ff;
          height: 20px;
          display: flex;
          align-items: center;
          &.title-icon {
            padding-left: 6px;
          }
          span {
            width: 30px;
          }

          .icon {
            font-size: 32px;
            color: #abacb5;
            line-height: 60px;
            width: 142px;
            text-align: right;
            margin-right: 16px;
          }
        }
        .subtitle {
          display: flex;
          justify-content: space-between;
        }
      }
    }
    .footer {
      padding: 12px 24px;
      background-color: #fafbfd;
      border-top: 1px solid #dcdee5;
      border-radius: 2px;
      height: 60px;
      display: flex;
      justify-content: flex-end;
      .data-btn {
        margin-left: 8px;
      }
    }
  }
}
</style>
