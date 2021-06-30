

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
  <div v-bkloading="{ isLoading: loading.reportedLoading }"
    class="recent-wrapper">
    <ul :style="recentWrapperBg"
      class="clearfix">
      <li v-for="(item, index) in reportedData"
        :key="index"
        :class="{ active: item.show }"
        class="clearfix">
        <div class="symbol">
          &lt;/&gt;
        </div>
        <div class="content">
          <pre :class="{ active: !item.show, 'show-all': item.show }"
            class="data">{{ item.text }}</pre>
        </div>
        <div class="slide">
          <a class="slide-button"
            href="javascript:;"
            @click="slide(item, $event)">
            {{ $t('展开详情') }}
            <i />
          </a>
          <button v-clipboard="item.text"
            class="bk-button bk-primary bk-button-mini copy-button"
            @error="copyError"
            @success="copySuccess(item)">
            {{ $t('复制') }}
            <transition>
              <span v-show="item.copy_Sucess"
                class="copytips">
                {{ $t('复制成功') }}
              </span>
            </transition>
          </button>
        </div>
      </li>
    </ul>
    <div v-show="reportedData.length === 0"
      class="no-data">
      <slot name="content" />
    </div>
  </div>
</template>

<script>
let beautify = require('json-beautify');
import mixin from './mixin.js';
import Vue from 'vue';
import VueClipboards from 'vue-clipboards';

Vue.use(VueClipboards);
export default {
  mixins: [mixin],
  props: {
    loading: {
      type: Object,
      default: () => ({})
    },
    recentWrapperBg: {
      type: Object,
      default: () => ({})
    },
    reportedData: {
      type: Array,
      default: () => ([])
    }
  },
  methods: {
    /*
                获取最近上报数据点击下拉展开
            */
    slide(item, e) {
      item.show = !item.show;
      if (item.show) {
        // item.json = item.text
        item.text = beautify(JSON.parse(item.source), null, 4, 80);
      } else {
        item.text = this.formatTailData(item.source);
      }
    },
    formatTailData(data) {
      let res = '';
      try {
        res = JSON.stringify(JSON.parse(data));
      } catch (e) {
        res = data;
      }

      return res;
    },
    /**
     * @augments item 当前内容
     * 复制成功
     */
    copySuccess(item) {
      item.copy_Sucess = true;
      setTimeout(() => {
        item.copy_Sucess = false;
      }, 2000);
    },
    copyError(e) {
      console.log(e);
    },

  }
};
</script>

<style media="screen" lang="scss">
$bk-primary: #3a84ff;
$text-color: #212232;
$bg-color: #fff;
$data-success: #9dcb6b;
$data-doing: #fe771d;
$data-fail: #ff5656;
$data-nostart: #dedede;
.recent-wrapper {
      background: #fff;
      box-shadow: 0 0 10px 0 rgba(33, 34, 50, 0.05);
      min-height: 150px;
      .no-data {
        position: absolute;
        text-align: center;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        p {
          color: #cfd3dd;
        }
        .data-preview {
          font-size: 12px;
          font-weight: normal;
          span {
            color: #3a84ff;
            cursor: pointer;
          }
        }
      }
      .title {
        width: 250px;
        padding: 10px 0px;
      }
      ul {
        // position: relative;
        min-height: 70px;
        li {
          border-top: 1px solid $bg-color;
          display: flex;
          min-height: 60px;
          &:hover {
            background: #f4f6fb;
            .symbol {
              line-height: 46px;
              background: #a3c5fd;
              border: 1px solid #3a84ff;
            }
          }
          .symbol {
            box-sizing: border-box;
            line-height: 48px;
            width: 44px;
            text-align: center;
            border-right: 1px solid $bg-color;
            background: #fafafa;
          }
          .content {
            padding: 9px 0;
            width: calc(100% - 230px);
            padding-left: 15px;
            font-size: 14px;
            line-height: 30px;
            word-break: break-all;
            display: flex;
            justify-content: flex-start;
            .active {
              text-overflow: ellipsis;
              overflow: hidden;
              white-space: nowrap;
            }
            .show-all {
              display: inline !important;
            }
            .data {
              white-space: pre-wrap;
              word-break: break-all;
              display: -webkit-box;
              -webkit-box-orient: vertical;
              -webkit-line-clamp: 3;
              overflow: hidden;
            }
          }
          .slide {
            line-height: 48px;
            color: $bk-primary;
            text-align: right;
            flex: 1;
            position: relative;
            // overflow: hidden;
            .slide-button {
              padding: 14px 15px;
              cursor: pointer;
              color: $bk-primary;
              &:hover {
                opacity: 0.8;
              }
            }
            .copy-button {
              margin-right: 15px;
              position: relative;
              .copytips {
                position: absolute;
                bottom: 25px;
                font-size: 12px;
                line-height: 16px;
                font-style: normal;
                font-weight: 400;
                text-align: left;
                color: rgb(255, 255, 255);
                z-index: 1070;
                transform: translate(0px, -50%);
                padding: 10px;
                background: rgb(80, 80, 80);
                border-radius: 5px;
                left: -20px;
                pointer-events: none;
                &:before {
                  content: '';
                  position: absolute;
                  bottom: -20px;
                  width: 0;
                  height: 0;
                  border: 7px solid;
                  transform: translate(0, -50%);
                  left: 27px;
                  border-color: #505050 transparent transparent transparent;
                }
              }
            }
            i {
              position: relative;
              top: 4px;
              display: inline-block;
              width: 16px;
              height: 16px;
              background: url('../../../common/images/icon/icon-arrows-down-circle.png');
              margin-left: 10px;
              transition: 0.2s ease all;
            }
          }
          &.active {
            background: rgba(244, 246, 251, 0.5);
            .symbol {
              line-height: 46px;
              background: #a3c5fd;
              border: 1px solid #3a84ff;
              min-height: 100px;
            }
            .content {
              width: calc(100% - 230px);
              overflow: initial;
              white-space: inherit;
              overflow-x: auto;
            }
            .slide {
              i {
                transform: rotate(180deg);
              }
            }
          }
        }
      }
    }
</style>
