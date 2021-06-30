

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
  <div class="diff-contents-layout">
    <div v-if="showDiffResult"
      class="diff-top">
      <bkdata-checkbox v-model="localOnlyShowDiff"
        extCls="mr0">
        {{ $t('仅显示有差异的项') }}
      </bkdata-checkbox>
      <div class="diff-result">
        <span class="result-text">{{ $t('模型对比结果') }}：</span>
        <span class="result-item">
          <i class="icon-block" />
          <span class="result-item-text">{{ $t('新增') }}</span>
          <span class="result-item-num">{{ diffResult.createNum || 0 }}</span>
        </span>
        <span class="result-item item-delete">
          <i class="icon-block" />
          <span class="result-item-text">{{ $t('删除') }}</span>
          <span class="result-item-num">{{ diffResult.deleteNum || 0 }}</span>
        </span>
        <span class="result-item item-update">
          <i class="icon-block" />
          <span class="result-item-text">{{ $t('更新') }}</span>
          <span class="result-item-num">{{ diffResult.updateNum || 0 }}</span>
        </span>
      </div>
    </div>
    <div class="diff-main">
      <div class="version-desc">
        <div class="before-verison-desc text-overflow">
          <slot name="before-verison-desc" />
        </div>
        <div class="after-verison-desc text-overflow">
          <slot name="after-verison-desc" />
        </div>
      </div>
      <div v-bkloading="{ isLoading: localIsLoading, opacity: 1 }"
        class="diff-list bk-scroll-y">
        <template v-for="(item, index) in list">
          <div :key="index"
            class="diff-item">
            <SingleCollapse class="diff-collapse"
              :collapsed.sync="item.collapsed"
              :theme="item.originContent.diffType">
              <template slot="title">
                <slot name="before-collapse-title"
                  :data="item">
                  <span>{{ item.originContent.collapseTitle }}</span>
                </slot>
              </template>
              <div
                class="diff-item-wrapper before-wrapper"
                :class="{
                  'is-empty': item.originContent.isEmpty,
                  'wrapper-padding': !item.originContent.isEmpty && item.type !== 'table',
                }">
                <template v-if="item.originContent.isEmpty">
                  <EmptyView width="102px"
                    height="64px"
                    :tips="$t('内容为空')" />
                </template>
                <template v-else-if="item.type === 'table'">
                  <bkdata-table
                    :data="item.originContent.fields"
                    :outerBorder="false"
                    v-bind="item.tableParams.bind || {}"
                    v-on="item.tableParams.on || {}">
                    <slot name="prepend-table-column"
                      :item-data="item" />
                    <template v-for="(config, configIndex) in item.configs.concat(item.originExtraConfigs || [])">
                      <bkdata-table-column
                        :key="`before_ ${item.originContent.objectId}_${configIndex}`"
                        :showOverflowTooltip="{ interactive: false }"
                        v-bind="config">
                        <template slot-scope="{ row }">
                          <slot :name="'diff-table-text-' + config.prop"
                            :data="row"
                            :item="item"
                            :isBefore="true">
                            <span :class="{ 'is-update': getItemStatus(row, config.prop) }">
                              {{
                                getDisplayText(row, config.prop)
                              }}
                            </span>
                          </slot>
                        </template>
                      </bkdata-table-column>
                    </template>
                  </bkdata-table>
                </template>
                <template v-else>
                  <template v-for="(config, configIndex) in item.configs.concat(item.originExtraConfigs || [])">
                    <div :key="`before_ ${item.originContent.objectId}_${configIndex}`"
                      class="diff-content-item">
                      <div class="diff-item-label text-overflow">
                        {{ config.label }}：
                      </div>
                      <slot :name="'diff-list-text-' + config.prop"
                        :data="item.originContent"
                        :item="item">
                        <div
                          class="diff-item-text"
                          :class="{
                            'is-update': getItemStatus(item.originContent, config.prop),
                          }">
                          {{ getDisplayText(item.originContent, config.prop) }}
                        </div>
                      </slot>
                    </div>
                  </template>
                </template>
              </div>
            </SingleCollapse>
            <SingleCollapse class="diff-collapse"
              :collapsed.sync="item.collapsed"
              :theme="item.newContent.diffType">
              <template slot="title">
                <slot name="after-collapse-title"
                  :data="item">
                  <span>{{ item.newContent.collapseTitle }}</span>
                </slot>
              </template>
              <div
                class="diff-item-wrapper after-wrapper"
                :class="{
                  'is-empty': item.newContent.isEmpty,
                  'wrapper-padding': !item.newContent.isEmpty && item.type !== 'table',
                }">
                <template v-if="item.newContent.isEmpty">
                  <EmptyView width="102px"
                    height="64px"
                    :tips="$t('内容为空')" />
                </template>
                <template v-else-if="item.type === 'table'">
                  <bkdata-table
                    :data="item.newContent.fields"
                    :outerBorder="false"
                    v-bind="item.tableParams.bind || {}"
                    v-on="item.tableParams.on || {}">
                    <slot name="prepend-table-column"
                      :item-data="item" />
                    <template v-for="(config, configIndex) in item.configs.concat(item.newExtraConfigs || [])">
                      <bkdata-table-column
                        :key="`after_ ${item.newContent.objectId}_${configIndex}`"
                        :showOverflowTooltip="{ interactive: false }"
                        v-bind="config">
                        <template slot-scope="{ row }">
                          <slot :name="'diff-table-text-' + config.prop"
                            :data="row"
                            :item="item"
                            :isBefore="false">
                            <span :class="{ 'is-update': getItemStatus(row, config.prop) }">
                              {{
                                getDisplayText(row, config.prop)
                              }}
                            </span>
                          </slot>
                        </template>
                      </bkdata-table-column>
                    </template>
                  </bkdata-table>
                </template>
                <template v-else>
                  <template v-for="(config, configIndex) in item.configs.concat(item.newExtraConfigs || [])">
                    <div :key="`after_ ${item.newContent.objectId}_${configIndex}`"
                      class="diff-content-item">
                      <div class="diff-item-label text-overflow">
                        {{ config.label }}：
                      </div>
                      <slot :name="'diff-list-text-' + config.prop"
                        :data="item.newContent"
                        :item="item">
                        <div
                          class="diff-item-text"
                          :class="{
                            'is-update': getItemStatus(item.originContent, config.prop),
                          }">
                          {{ getDisplayText(item.newContent, config.prop) }}
                        </div>
                      </slot>
                    </div>
                  </template>
                </template>
              </div>
            </SingleCollapse>
          </div>
        </template>
      </div>
    </div>
  </div>
</template>
<script lang="ts" src="./DiffContents.ts"></script>
<style lang="scss" scoped>
.diff-contents-layout {
  width: 100%;
  height: 100%;
  .diff-top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 14px;
    margin-bottom: 20px;
    .diff-result {
      display: flex;
      align-items: center;
    }
    .result-text {
      color: #313238;
      margin-right: 8px;
    }
    .result-item {
      display: flex;
      align-items: center;
      color: #63656e;
      margin-right: 16px;
      &.item-delete {
        .icon-block {
          border-color: #ffd2d2;
          background-color: #ffeded;
        }
        .result-item-num {
          color: #ea3636;
        }
      }
      &.item-update {
        .icon-block {
          border-color: #ffdfac;
          background-color: #fff4e2;
        }
        .result-item-num {
          color: #ff9c01;
        }
      }
      .icon-block {
        width: 12px;
        height: 12px;
        border: 1px solid #c6e2c3;
        background-color: #e8fae6;
      }
      .result-item-text {
        padding: 0 5px;
      }
      .result-item-num {
        color: #3fc06d;
        font-weight: bold;
      }
    }
  }
  .diff-main {
    height: calc(100% - 20px);
    .version-desc {
      display: flex;
      align-items: center;
      min-height: 44px;
      font-size: 14px;
      color: #63656e;
      background-color: #dcdee5;
      > div {
        flex: 0 0 50%;
        padding: 0 8px;
        &.before-verison-desc {
          position: relative;
          &::after {
            content: '';
            position: absolute;
            top: 50%;
            right: 0;
            transform: translateY(-50%);
            width: 1px;
            height: 20px;
            background-color: #c4c6cc;
          }
        }
      }
    }
    .diff-list {
      height: calc(100% - 62px);
      &.bk-scroll-y {
        overflow-y: auto;
      }
      .diff-item {
        display: flex;
        margin-top: 10px;
        .diff-collapse {
          width: calc(50% - 2px);
          &:first-child {
            margin-right: 4px;
          }
        }
        .diff-item-wrapper {
          height: 100%;
          &.is-empty {
            min-height: 200px;
          }
          &.wrapper-padding {
            padding: 14px;
          }
          .bk-table {
            &::before {
              display: none;
            }
          }
        }
        .diff-content-item {
          display: flex;
          font-size: 12px;
          line-height: 24px;
        }
        .diff-item-label {
          flex: 0 0 120px;
          color: #979ba5;
          text-align: right;
        }
        .diff-item-text {
          color: #63656e;
        }
        .is-update {
          color: #ff9c01;
        }
      }
    }
  }
}
</style>
