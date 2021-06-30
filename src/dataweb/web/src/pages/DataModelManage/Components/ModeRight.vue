

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
  <section class="mode-right-view">
    <div class="mode-right-header">
      <div class="header-top">
        <div class="header-tabs">
          <template v-for="tab in tabList">
            <span
              :key="tab.id"
              v-bk-tooltips.bottom="`${tab.name} (${tab.displayName})`"
              :class="[{ 'is-active': tab.isActive }, 'tab-item']"
              @click.stop="handleTabItemClick(tab)">
              <i :class="['pre-icon icon', `icon-${tab.icon}`]" />
              <span class="text text-overflow">{{ tab.displayName }}</span>
              <i class="after-icon icon icon-close"
                @click.stop="handleTabClose(tab)" />
            </span>
          </template>
          <i
            v-show="!!tabList.length"
            v-bk-tooltips.bottom="$t('清空所有页签')"
            class="icon-close close-all-btn"
            @click.stop="handleCloseAllTabs" />
        </div>
        <span class="help-section">
          <a
            :href="$store.getters['docs/getPaths'].dataMarket"
            target="_blank"
            rel="noopener noreferrer">
            <i class="icon icon-help-fill" />
            <span>帮助文档</span>
          </a>
        </span>
      </div>
      <div v-if="!isEmptyTab"
        class="header-actions active-tab">
        <span class="active-tab-left">
          <i
            v-bk-tooltips="status[publishStatus]"
            :class="[
              'icon-status',
              published ? 'icon-publish-fill' : 'icon-unpublished-line',
              { 're-developing': publishStatus === 're-developing' },
            ]" />
          <i v-bk-tooltips="activeModelTypeTips"
            :class="['icon-status ml10', `icon-${activeTabitem.icon}`]" />
          <template v-if="isOperationModel">
            <span class="bk-mode-edit">
              <bkdata-button text
                @click.stop="handleGoBack">
                {{ activeModeDisplayName }}
              </bkdata-button>
              /
              <span class="ml5"
                style="color: #313238">
                {{ $t('操作记录') }}
              </span>
            </span>
          </template>
          <template v-else-if="isModeNameEdit">
            <div class="bk-mode-edit">
              <bkdata-input
                ref="modelRenameInput"
                v-model="activeModeDisplayName"
                style="width: 240px"
                :maxlength="50"
                :disabled="isLoading"
                @blur="handleEditNameBlur" />
              <img v-if="isLoading"
                class="input-loading"
                src="../../../common/images/spinner.svg">
            </div>
          </template>
          <template v-else>
            <span class="bk-mode-edit">
              <span>{{ activeModeDisplayName }}</span>
              <i v-if="!activeTabitem.isNew"
                class="icon icon-edit-big"
                @click="handleEditModeName" />
            </span>
          </template>
        </span>
        <template v-if="isEditModel">
          <span class="active-tab-right">
            <template v-for="item in rightActions">
              <span
                v-if="item.disabled"
                :key="item.icon"
                v-bk-tooltips="$t('功能暂未开放')"
                style="cursor: not-allowed"
                class="action-item">
                <i :class="['icon', item.icon]" />
              </span>
              <span
                v-else
                :key="item.icon"
                v-bk-tooltips="item.name"
                class="action-item"
                @click="() => item.callFn(item)">
                <i :class="['icon', item.icon]" />
              </span>
            </template>
          </span>
        </template>
        <template v-else-if="isOperationModel">
          <span class="btns">
            <bkdata-button theme="default"
              @click="handleGoBack">
              {{ $t('返回') }}
            </bkdata-button>
          </span>
        </template>
        <template v-else>
          <span class="btns">
            <div v-if="publishStatus !== 'developing'"
              class="bk-button-group">
              <bkdata-button
                :class="{ 'is-selected': activeViewModel === 'preview' }"
                @click="handleChangeView('preview')">
                <i class="icon-model-info btn-icon" />
                {{ $t('预览') }}
              </bkdata-button>
              <bkdata-button :class="{ 'is-selected': activeViewModel === 'list' }"
                @click="handleChangeView('list')">
                <i class="icon-model-list btn-icon" />
                {{ $t('详情') }}
              </bkdata-button>
            </div>
            <bkdata-button class="mr10"
              theme="default"
              @click="handleChangeModel('dataModelOperation')">
              {{ $t('操作记录') }}
            </bkdata-button>
            <bkdata-button theme="primary"
              @click="handleChangeModel('dataModelEdit')">
              {{ $t('编辑') }}
            </bkdata-button>
          </span>
        </template>
      </div>
    </div>
    <router-view ref="modeRightBody"
      :key="'view_route_' + modelId"
      :activeView="activeViewModel" />
    <!-- <ModeRightBody ref="modeRightBody" class="mode-right-body" /> -->
  </section>
</template>
<script lang="ts" src="./ModeRight.ts"></script>
<style lang="scss" scoped>
.mode-right-view {
  .mode-right-header {
    background: #ffffff;
    .header-top {
      position: relative;
      display: flex;
      align-items: center;
      background: #fafbfd;
      &::after {
        content: '';
        position: absolute;
        right: 0;
        left: 0;
        bottom: 0;
        height: 1px;
        background-color: #dcdee5;
      }
      .header-tabs {
        position: relative;
        flex: 1;
        height: 45px;
        display: flex;
        overflow: hidden;
        z-index: 1;
        .tab-item {
          flex: 0 1 200px;
          position: relative;
          display: flex;
          align-items: center;
          line-height: 45px;
          height: 45px;
          max-width: 200px;
          color: #63656e;
          padding: 0 30px 0 10px;
          border-bottom: solid 1px #dcdee5;

          cursor: pointer;
          i {
            font-size: 16px;
            &.after-icon {
              display: none;
              position: absolute;
              right: 10px;
              top: 50%;
              transform: translateY(-50%);
              width: 18px;
              height: 18px;
              font-size: 18px;
              color: #979ba5;
              border-radius: 50%;
              &:hover {
                color: #fff;
                background: #c4c6cc;
              }
            }

            &.pre-icon {
              color: #979ba5;
              margin-right: 5px;
            }
          }

          .text {
            display: inline-block;
            flex: 1;
            width: 0;
          }

          // &:last-child {
          //     border-right: solid 1px #dcdee5;
          // }

          &:first-child {
            &.is-active {
              border-left: none;
            }
          }
          &.is-active {
            border-bottom: #ffffff;
            border-right: solid 1px #dcdee5;
            border-left: solid 1px #dcdee5;
            // border-top: solid 4px #3a84ff;
            color: #3a84ff;
            background-color: #ffffff;

            i.after-icon {
              display: inline-block;
            }

            i.pre-icon {
              color: #3a84ff;
            }

            &::before {
              content: '';
              position: absolute;
              top: 0;
              left: 0;
              right: 0;
              height: 4px;
              background: #3a84ff;
            }
          }

          &:not(.is-active) {
            &::after {
              content: '';
              width: 1px;
              height: 20px;
              border-right: 1px solid #dcdee5;
              position: absolute;
              right: 0;
              top: 50%;
              transform: translate(50%, -50%);
            }
          }

          &:hover {
            i.after-icon {
              display: inline-block;
            }
          }
        }
        .close-all-btn {
          flex: 0 0 20px;
          align-self: center;
          font-size: 20px;
          color: #979ba5;
          border-radius: 50%;
          margin-left: 6px;
          cursor: pointer;
          &:hover {
            color: #ffffff;
            background-color: #ea3636;
          }
        }
      }

      .help-section {
        display: flex;
        align-items: center;
        margin: 0 20px;

        a {
          position: relative;
          padding-left: 20px;
          color: #979ba5;

          i {
            position: absolute;
            left: 0;
            top: 2px;
            font-size: 16px;
            color: #c4c6cc;
          }
        }
      }
    }

    .header-actions {
      height: 50px;
      padding-left: 10px;
      font-size: 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: solid 1px #dcdee5;

      .active-tab-left {
        display: flex;
        align-items: center;

        .bk-mode-edit {
          position: relative;
          margin-left: 5px;
          ::v-deep .bk-form-input:disabled {
            padding-right: 30px;
          }
          .input-loading {
            position: absolute;
            top: 50%;
            right: 10px;
            transform: translateY(-50%);
            width: 18px;
            height: 18px;
          }
          .bk-button-text {
            color: #63656e;
            font-size: 16px;
            margin-right: 5px;
            &:hover {
              color: #3a84ff;
            }
          }
        }

        i.icon-status {
          font-size: 16px;
          padding: 1px;
          color: #979ba5;
          background: #f0f1f5;
          border-radius: 2px;
          &.re-developing {
            color: #ffb848;
            background-color: #ffefd6;
          }
        }
        .icon-edit-big {
          cursor: pointer;
        }
      }

      .active-tab-right {
        display: flex;
        justify-content: end;
        align-items: center;

        .action-item {
          width: 32px;
          height: 33px;
          border-radius: 2px;
          margin-right: 13px;
          margin-left: 3px;
          display: flex;
          justify-content: center;
          align-items: center;
          cursor: pointer;
          &:hover {
            background: #f0f1f5;
          }
        }
      }
      .btns {
        display: flex;
        align-items: center;
        padding-right: 20px;
        .bk-button-group {
          margin-right: 12px;
          .bk-button {
            width: 78px;
            .btn-icon {
              font-size: 18px;
              margin-left: -4px;
              margin-right: 2px;
            }
          }
        }
        .bk-button {
          width: 86px;
        }
      }
    }
  }
}
</style>
