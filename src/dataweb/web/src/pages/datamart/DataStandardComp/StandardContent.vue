

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
  <div class="basic-container">
    <div class="content-left">
      <BasicConstruct :title="$t('标准内容')">
        <TreeContent :treeId="treeId"
          :standardContent="standardContent"
          class="pl15" />
      </BasicConstruct>
      <BasicConstruct>
        <div slot="title"
          class="title">
          <span>{{ $t('用户反馈') }}</span>
          <a v-bk-tooltips="$t('前往CE平台进行反馈')"
            target="_blank"
            :href="CESiteUrl">
            <i class="icon-edit2" />
          </a>
        </div>
        <p class="feed-explain">
          {{ $t('请描述您需要反馈的内容') }}
          <a href="wxwork://message/?username=TGDP">{{ $t('TGDP助手') }}</a>
          {{ $t('协助') }}。
        </p>
      </BasicConstruct>
    </div>
    <div class="content-right">
      <StandardDataTable :standardContent="standardContent.standard_content" />
    </div>
  </div>
</template>

<script>
import BasicConstruct from './BasicConstruct';
import TreeContent from './ContentComp/TreeContent';
import StandardDataTable from './ContentComp/StandardDataTable';

export default {
  components: {
    BasicConstruct,
    StandardDataTable,
    TreeContent,
  },
  props: {
    standardContent: {
      type: Object,
      default: () => ({}),
    },
    treeId: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      CESiteUrl: window.BKBASE_Global.CESiteUrl + 'products/view/2282?&tab_id=all_issues_btn#create-feedback',
    };
  },
};
</script>

<style lang="scss" scoped>
.basic-container {
  display: flex;
  flex-wrap: nowrap;
  .content-left {
    width: 450px;
    box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
    border-radius: 2px;
    border: 1px solid rgba(195, 205, 215, 0.6);
    padding: 0 20px 20px 5px;
    margin: 0 20px 0px 0;
    min-height: 450px;
  }
  .content-right {
    width: calc(100% - 450px);
    // min-height: 430px;
    border: 1px solid rgba(195, 205, 215, 0.6);
    box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
  }
}
.title {
  height: 55px;
  line-height: 55px;
  padding: 0 15px;
  font-weight: 700;
  &::before {
    content: '';
    width: 2px;
    height: 19px;
    background: #3a84ff;
    display: inline-block;
    margin-right: 15px;
    position: relative;
    top: 4px;
  }
  i {
    font-size: 18px;
    margin: 0 5px;
  }
  .icon-question-circle {
    color: #ff9c01;
  }
}
.feed-explain {
  font-size: 14px;
  color: #979ba5;
  font-weight: normal;
  padding: 0 15px;
  text-indent: 1em;
}
</style>
