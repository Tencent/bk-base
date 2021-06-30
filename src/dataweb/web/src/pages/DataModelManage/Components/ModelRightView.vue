

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
  <div v-bkloading="{ isLoading, opacity: 1 }"
    class="model-right-view bk-scroll-y">
    <template v-if="isEmptyModel">
      <EmptyView class="empty-data"
        tips="请选择模型" />
    </template>
    <template v-else-if="isUnpublished">
      <EmptyView class="empty-data"
        :tips="$t('暂无发布版本')">
        <p style="padding-top: 6px;">
          <bkdata-button style="font-size: 12px;"
            text
            @click.stop="handleGoEditStep(1)">
            {{ $t('去编辑') }}
          </bkdata-button>
        </p>
      </EmptyView>
    </template>
    <template v-else-if="activeView === 'preview'">
      <DataModelPreviewView />
    </template>
    <div v-else
      class="list-info">
      <template v-for="view in viewList">
        <single-collapse :key="view.id"
          class="collapse-cls mb20"
          :collapsed.sync="collapsedMap[view.id]">
          <div slot="title"
            class="view-collapse-title">
            <div class="title-left">
              <span class="name">{{ view.name }}</span>
              <div
                v-if="!!view.updateCount"
                v-bk-tooltips="`有 ${view.updateCount} 处未发布的更新，点击查看对比`"
                class="update-tips ml10"
                @click.stop="handleShowDiffSlider(view.id)">
                <span>{{ view.updateCount }} {{ $t('更新') }}</span>
              </div>
            </div>
            <bkdata-button
              v-if="view.showEditBtn"
              theme="default"
              :outline="true"
              @click.stop="handleGoEditStep(view.step)">
              {{ $t('去编辑') }}
            </bkdata-button>
          </div>
          <component :is="view.component"
            :data="view.data" />
        </single-collapse>
      </template>
    </div>

    <!-- diff slider -->
    <bkdata-sideslider :isShow.sync="diffSlider.isShow"
      :title="$t('变更详情')"
      :quickClose="true"
      :width="1200">
      <template slot="content">
        <BizDiffContents
          :diff="diffData"
          :newContents="diffSlider.newContents"
          :origContents="diffSlider.originContents"
          :fieldContraintConfigList="fieldContraintConfigList"
          :onlyShowDiff="true"
          :showResult="false">
          <span slot="before-verison-desc">【已发布】{{ versionInfo }}</span>
          <span slot="after-verison-desc">【未发布】</span>
        </BizDiffContents>
      </template>
    </bkdata-sideslider>

    <!-- 发布成功反馈 -->
    <bkdata-dialog v-model="isShowReleaseDialog"
      :showFooter="false"
      :maskClose="false">
      <div class="release-guide">
        <i class="icon-corret-fill" />
        <h2>{{ $t('发布成功') }}</h2>
        <div class="can-do">
          <p>接下来，你可以：</p>
          <p>1. 在 数据开发 中应用数据模型，<span @click.stop="handleGoApplication">前往应用</span></p>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script lang="ts" src="./ModelRightView.ts"></script>
<style lang="scss" scoped>
.model-right-view {
  min-height: 300px;
  height: calc(100% - 91px);
  .empty-data {
    min-height: 300px;
    ::v-deep .empty-img {
      margin-bottom: 0;
    }
    ::v-deep .empty-text {
      font-size: 14px;
      color: #63656e;
    }
  }
  .list-info {
    padding: 24px 30px;
  }
  .collapse-cls {
    ::v-deep .collapse-text {
      flex: 1;
    }
    ::v-deep .single-collapse-trigger {
      background-color: #e1e3eb;
    }
    .view-collapse-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      font-size: 14px;
      .title-left {
        display: flex;
        align-items: center;
      }
      .name {
        padding-left: 6px;
      }
      .update-tips {
        position: relative;
        width: 50px;
        line-height: 16px;
        font-size: 12px;
        font-weight: normal;
        text-align: center;
        color: #ffffff;
        padding-left: 8px;
        cursor: pointer;
        &::before {
          content: '';
          position: absolute;
          top: 0;
          left: -8px;
          border: 8px solid transparent;
          border-right-color: #ff9c01;
        }
        > span {
          position: relative;
          display: inline-block;
          width: 100%;
          background-color: #ff9c01;
          &::before {
            content: '';
            position: absolute;
            top: 6px;
            left: -3px;
            width: 4px;
            height: 4px;
            border-radius: 50%;
            background-color: #ffffff;
          }
        }
      }
      .bk-button {
        height: 24px;
        line-height: 22px;
        font-size: 12px;
        font-weight: normal;
      }
    }
  }
}
.release-guide {
  padding: 0 23px;
  text-align: center;
  .icon-corret-fill {
    font-size: 68px;
    color: #2dcb56;
  }
  h2 {
    color: #313238;
    font-size: 24px;
    font-weight: normal;
    margin: 10px 0 20px;
  }
  p {
    font-size: 12px;
    margin-bottom: 10px;
    text-align: left;
  }
  span {
    color: #3a84ff;
    cursor: pointer;
  }
}
::v-deep .diff-wrapper {
  margin-top: 10px !important;
}
</style>
