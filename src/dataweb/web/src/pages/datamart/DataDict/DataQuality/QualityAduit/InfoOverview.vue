

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
  <div>
    <HeaderType :title="$t('信息总览')">
      <div class="wrapper">
        <div v-bkloading="{ isLoading: isOverviewDataLoading }"
          class="quality-event info-bar">
          <div class="info-title">
            {{ $t('质量事件') }}
            <span class="info-explain">{{ $t('最近24小时统计量') }}</span>
          </div>
          <div class="info-content">
            <ImgCombineExplain :iconClass="'event'"
              :explain="$t('事件总数')"
              :count="overviewData.eventCount" />
            <ImgCombineExplain :iconClass="'exclamation-triangle-shape'"
              :explain="$t('告警总数')"
              :count="overviewData.eventNotifiedCount" />
          </div>
        </div>
        <div v-bkloading="{ isLoading: isAuditRuleDataLoading }"
          class="aduit-rule info-bar">
          <div class="info-title">
            {{ $t('审核规则') }}
          </div>
          <div class="info-content">
            <ImgCombineExplain :iconClass="'project-custom'"
              :explain="$t('规则总数')"
              :count="auditRuleData.ruleCount" />
            <ImgCombineExplain :iconClass="'running'"
              :explain="$t('运行中')"
              :count="auditRuleData.runningRuleCount" />
            <ImgCombineExplain :iconClass="'ellipsis'"
              :explain="$t('待启动')"
              :count="auditRuleData.waitingRuleCount" />
            <ImgCombineExplain :iconClass="'fail'"
              :explain="$t('失败')"
              :count="auditRuleData.failedRuleCount" />
          </div>
        </div>
      </div>
    </HeaderType>
  </div>
</template>

<script lang="ts" src="./InfoOverview.ts"></script>

<style lang="scss" scoped>
.wrapper {
  display: flex;
  flex-wrap: nowrap;
  .quality-event {
    flex: 0.4;
    margin-right: 15px;
  }
  .aduit-rule {
    flex: 0.6;
  }
  .info-bar {
    background-color: rgb(245, 245, 245);
    border-radius: 10px;
    padding: 8px 20px;
    .info-title {
      color: #63656e;
      padding-bottom: 10px;
      font-size: 14px;
      font-weight: 550;
      .info-explain {
        font-size: 12px;
        color: #5e6d82;
        vertical-align: bottom;
      }
    }
    .info-content {
      display: flex;
      flex-wrap: nowrap;
      justify-content: space-around;
      padding: 8px;
      ::v-deep .icon-ellipsis {
        transform: scale(1.2);
      }
    }
  }
}
</style>
