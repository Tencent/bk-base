

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
    <ul class="label-explain">
      <li v-for="(item, index) in ruleTemplateList"
        :key="index"
        @click="handlerSelectedRuleTemplate(item)">
        <div class="label-detail">
          <span>{{ item.templateAlias }}</span>
          <p class="text-overflow"
            :title="item.description">
            {{ item.description }}
          </p>
        </div>
        <div class="label-des bk-text-primary">
          <!-- [数据完整性] -->
        </div>
      </li>
    </ul>
  </div>
</template>

<script lang="ts" src="./RuleModel.ts"></script>

<style lang="scss" scoped>
.label-container {
  display: flex;
  flex-wrap: nowrap;
  align-items: center;
  padding: 10px;
  .icon-tag {
    margin-right: 10px;
    font-size: 18px;
  }
  ul {
    li {
      float: left;
      border: 1px solid #dddddd;
      background-color: #f5f6fa;
      padding: 5px 8px;
      border-radius: 15px;
      line-height: normal;
      margin-right: 10px;
    }
  }
}
.label-explain {
  li:nth-child(odd) {
    background-color: #f5f6fa;
  }
  li {
    padding: 10px 20px;
    .label-detail {
      display: flex;
      align-items: center;
      flex-wrap: nowrap;
      span {
        font-size: 18px;
        font-weight: bold;
        white-space: nowrap;
        margin-right: 10px;
      }
      p {
        width: 100%;
      }
    }
  }
}
</style>
