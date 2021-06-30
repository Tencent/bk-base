

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
  <div class="wrapper">
    <div class="publish-tip">
      <span class="icon icon-info" />
      <span class="desp">{{ $t('当函数有改动时') }}</span>
    </div>
    <block-content :header="$t('函数配置')">
      <div class="block-item">
        <span class="pub-desp">{{ $t('开发语言') }}：</span>
        <span class="content">{{ devlopParams.func_language }}</span>
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('函数类型') }}：</span>
        <span class="content">{{ devlopParams.func_udf_type }}</span>
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('计算类型') }}：</span>
        <text-item
          v-for="(item, index) in devlopParams.calculateType"
          :key="index"
          style="margin-right: 9px"
          :text="calTypeMap[item]"
          :shape="'radius'" />
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('函数名称') }}：</span>
        <span class="content">{{ devlopParams.func_name }}</span>
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('函数中文名称') }}：</span>
        <span class="content">{{ devlopParams.func_alias }}</span>
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('输入参数类型') }}：</span>
        <text-item
          v-for="(item, index) in devlopParams.input_type"
          :key="index"
          style="margin-right: 9px"
          :text="item"
          :shape="'rect'" />
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('函数返回类型') }}：</span>
        <text-item
          v-for="(item, index) in devlopParams.return_type"
          :key="index"
          style="margin-right: 9px"
          :text="item"
          :shape="'rect'" />
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('函数说明') }}：</span>
        <span class="content">{{ devlopParams.explain }}</span>
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('使用样例') }}：</span>
        <span class="content">{{ devlopParams.example }}</span>
      </div>
      <div class="block-item">
        <span class="pub-desp">{{ $t('样例返回') }}：</span>
        <span class="content">{{ devlopParams.example_return_value }}</span>
      </div>
    </block-content>
    <block-content :header="$t('发布日志')"
      :noPadding="true"
      :headRequired="true">
      <div class="text-area">
        <bkdata-input v-model="publishLog"
          :placeholder="$t('请填写本次发布日志')"
          :type="'textarea'"
          :rows="2" />
      </div>
    </block-content>
  </div>
</template>

<script>
import blockContent from '../components/contentBlock';
import { mapGetters } from 'vuex';
import textItem from '../components/textItem';
export default {
  components: { blockContent, textItem },
  data() {
    return {
      publishLog: '',
      calTypeMap: {
        stream: '实时',
        batch: '离线',
      },
    };
  },
  computed: {
    ...mapGetters({
      devlopParams: 'udf/devlopParams',
    }),
  },
};
</script>

<style lang="scss" scoped>
.wrapper {
  .text-area {
    width: 740px;
    margin: 12px;
    background: rgba(255, 255, 255, 1);
    border-radius: 2px;
  }
  .publish-tip {
    width: 883px;
    height: 44px;
    background: rgba(225, 236, 255, 1);
    border-radius: 2px;
    display: flex;
    align-items: center;
    margin-bottom: 15px;
    .icon {
      font-size: 20px;
      color: rgba(58, 132, 255, 1);
      margin: 0 10px 0 20px;
    }
    .content {
      font-size: 14px;
      font-family: MicrosoftYaHeiUI;
      color: rgba(99, 101, 110, 1);
      line-height: 18px;
    }
  }
}
</style>
