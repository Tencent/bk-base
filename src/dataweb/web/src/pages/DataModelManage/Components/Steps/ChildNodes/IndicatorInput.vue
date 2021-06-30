

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
  <div class="input-wrapper bk-scroll-y">
    <div class="header">
      {{ $t('数据输入') }}
    </div>
    <div class="content">
      <bkdata-collapse v-model="activeInput">
        <bkdata-collapse-item
          v-for="(input, index) in fieldInfo"
          :key="index"
          :name="input.key"
          :hideArrow="true"
          :customTriggerArea="true"
          extCls="data-input-collapse-item">
          <i v-if="activeInput.includes(input.key)"
            v-bk-tooltips="$t('收起')"
            class="icon-fold" />
          <i v-else
            v-bk-tooltips="$t('展开')"
            class="icon-unfold" />
          <span class="input-field"
            :title="mainTableName">
            {{ mainTableName }}
          </span>
          <div class="no-trigger-area">
            {{ typeName }}
          </div>
          <div slot="content">
            <ul class="filed-list">
              <li v-for="(field, key) in input.fields"
                :key="key">
                <div class="title">
                  <i :class="['icon', `icon-${iconNameMap[field.iconName]}`]" />
                  <span class="text">{{ field.name }}</span>
                </div>
                <div class="type">
                  {{ field.type }}
                </div>
              </li>
            </ul>
          </div>
        </bkdata-collapse-item>
      </bkdata-collapse>
    </div>
  </div>
</template>

<script lang="ts" src="./IndicatorInput.ts"></script>

<style lang="scss" scoped>
.no-trigger-area {
  font-size: 14px;
  color: #c4c6cc;
  white-space: nowrap;
}
.input-wrapper {
  width: 100%;
  height: 100%;
  background: #fff;
  padding: 17px 24px 24px 24px;
  overflow-y: auto;
  .header {
    width: 64px;
    height: 22px;
    font-size: 16px;
    font-family: PingFangSC, PingFangSC-Medium;
    font-weight: 500;
    text-align: left;
    color: #313238;
    line-height: 22px;
    margin-bottom: 9px;
  }
  ::v-deep .content {
    .input-select {
      width: 100%;
      margin-top: 30px;
      margin-bottom: 10px;
    }
    .data-input-collapse-item > .bk-collapse-item-header {
      padding: 0;
      display: flex;
      align-items: center;
      font-size: 0;
      .trigger-area {
        display: flex;
        align-items: center;
        overflow: hidden;
      }
      i,
      span {
        font-size: 14px;
      }
      .input-field {
        margin-left: 10px;
        display: inline-block;
        width: 100%;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      .no-trigger-area {
        i {
          cursor: pointer;
          &:hover {
            color: #3a84ff;
          }
        }
      }
    }
    .bk-collapse-item-content {
      padding: 0;
      .filed-list {
        margin-left: 27px;
        li {
          display: flex;
          justify-content: space-between;
          line-height: 32px;
          .title {
            font-size: 0;
            flex-grow: 1;
            width: 0;
            display: flex;
            align-items: center;
            cursor: pointer;
            span,
            i {
              font-size: 14px;
            }
            .icon {
              color: #979ba5;
              margin-right: 10px;
            }
            .text {
              color: #63656e;
              text-overflow: ellipsis;
              overflow: hidden;
              white-space: nowrap;
              width: calc(100% - 24px);
            }
          }
          .type {
            font-size: 14px;
            color: #c4c6cc;
            width: 48px;
            margin-left: 5px;
            text-align: right;
          }
        }
      }
    }
    fieldset {
      border-color: #dde4eb;
      border-radius: 2px;

      legend {
        padding: 10px;
      }
    }

    .lastest-data-content {
      word-wrap: break-word;
      padding: 0 10px;
      width: 100%;
      border-radius: 5px;
      min-height: 150px;
    }
  }
}
</style>
