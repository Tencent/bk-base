

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
  <div class="applyPop">
    <a href="javascript:;"
      class="close-pop"
      @click="closePop">
      <i class="bk-icon icon-close" />
    </a>
    <p class="title">
      [268] CODOL使命召唤Online-内部版业务
    </p>
    <div class="content">
      <div class="conten-up clearfix">
        <p class="content-titpe fl">
          已申请的敏感数据
        </p>
        <ul class="applied-list fl">
          <li>[6373] 游戏充值</li>
          <li>[5140] CSR接口赠送日志</li>
          <li>[5405] 完整包下载结果</li>
          <li>[5406] 完整包下载开始</li>
          <li>[5407] TGP启动游戏</li>
        </ul>
      </div>
      <div class="conten-down clearfix">
        <p class="content-titpe fl">
          选择敏感数据
        </p>
        <div class="content-select">
          <bk-select :selected="defaultModel"
            :multiple="multiple">
            <bk-select-option v-for="option of list"
              :key="option.value"
              :value="option.value"
              :label="option.label" />
          </bk-select>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  props: {
    applyData: {
      type: Array,
    },
  },
  data() {
    return {
      defaultModel: '',
      list: [
        {
          value: 'climbing',
          label: '爬山',
          alias: 'pashan',
        },
        {
          value: 'running',
          label: '跑步',
          alias: 'paobu',
        },
        {
          value: 'dancing',
          label: '跳舞',
          alias: 'tiaowu',
        },
        {
          value: 'gym',
          label: '健身',
          alias: 'jianshen',
        },
        {
          value: 'play-ball',
          label: '打球',
          alias: 'daqiu',
        },
      ],
      multiple: true,
      data: [],
    };
  },
  mounted() {
    this.data = this.applyData;
  },
  methods: {
    closePop() {
      this.$emit('close-pop');
    },
  },
};
</script>

<style media="screen" lang="scss" scoped>
.applyPop {
  .content {
    padding: 50px 50px 0;
    .conten-up {
      padding: 0 0 22px 0;
      border-bottom: 1px solid #eaeaea;
    }
    .content-titpe {
      width: 131px;
      text-align: right;
      line-height: 36px;
      margin-top: 10px;
    }
    .applied-list {
      width: 599px;
      margin-left: 20px;
      li {
        float: left;
        padding: 6px 15px;
        border: 1px solid #dde4eb;
        color: #737987;
        background: #fafbfd;
        border-radius: 2px;
        margin-right: 10px;
        margin: 10px 10px 0 0;
      }
    }
    .conten-down {
      margin-top: 25px;
    }
    .content-select {
      margin: 10px 0 0 20px;
      float: left;
      width: 500px;
    }
  }
}
</style>
