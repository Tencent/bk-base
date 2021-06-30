

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
  <div class="depent-wrapper">
    <div class="left">
      <p class="header">
        {{ index }}
      </p>
    </div>
    <div class="content">
      <div class="content-item">
        <span class="desp">groupId</span>
        <bkdata-input v-model="localParam.group_id"
          :placeholder="$t('请输入')"
          style="width: 240px; flex: 0.9" />
      </div>
      <div class="content-item">
        <span class="desp">artifactId</span>
        <bkdata-input v-model="localParam.artifact_id"
          :placeholder="$t('请输入')"
          style="width: 240px; flex: 0.9" />
      </div>
      <div class="content-item">
        <span class="desp">version</span>
        <bkdata-input v-model="localParam.version"
          :placeholder="$t('请输入')"
          style="width: 240px; flex: 0.9" />
      </div>
      <Delete-confirm ref="deletePop"
        @confirmHandle="remove"
        @cancleHandle="cancle">
        <span class="delete-btn icon-delete" />
      </Delete-confirm>
    </div>
  </div>
</template>

<script>
import DeleteConfirm from '@/components/tooltips/DeleteConfirm.vue';
export default {
  components: {
    DeleteConfirm,
  },
  props: {
    index: {
      type: Number,
      default: 0,
    },
    params: {
      type: Object,
      default: () => ({
        group_id: '',
        artifact_id: '',
        version: '',
      }),
    },
  },
  data() {
    return {
      htmlConfig: {
        allowHtml: true,
        width: 85,
        trigger: 'click',
        theme: 'light',
        content: '#dev-config-tootip',
        placement: 'top',
      },
    };
  },
  computed: {
    localParam: {
      get() {
        return this.params;
      },
      set(val) {
        Object.assign(this.params, val || {});
      },
    },
  },
  methods: {
    cancle() {
      this.$refs.deletePop.closeTip();
    },
    remove() {
      this.$emit('remove', this.index);
      this.$refs.deletePop.closeTip();
    },
  },
};
</script>

<style lang="scss">
.depent-wrapper {
  width: 600px;
  height: 146px;
  background: rgba(250, 251, 253, 1);
  display: flex;
  margin-bottom: 10px;
  .left {
    width: 44px;
    height: 100%;
    background: rgba(240, 241, 245, 1);
    display: flex;
    justify-content: center;
    align-items: center;
    .header {
      width: 15px;
      height: 30px;
      font-size: 24px;
      font-family: MicrosoftYaHeiUI-Bold;
      font-weight: bold;
      color: rgba(196, 198, 204, 1);
      line-height: 30px;
    }
  }
  .content {
    position: relative;
    padding-top: 15px;
    width: 100%;
    .content-item {
      display: flex;
      align-items: center;
      margin-bottom: 10px;
      .desp {
        width: 85px;
        text-align: right;
        margin-right: 13px;
        font-size: 14px;
        font-family: MicrosoftYaHeiUI;
        color: rgba(99, 101, 110, 1);
        line-height: 18px;
      }
    }
    .bk-tooltip {
      position: absolute;
      right: 11px;
      bottom: 115px;
      cursor: pointer;
    }
  }
}
.dev-config-tootip {
  display: flex;
  span {
    font-size: 12px;
    font-family: MicrosoftYaHei;
    color: rgba(58, 132, 255, 1);
    line-height: 16px;
    cursor: pointer;
  }
  span:nth-child(1) {
    margin-right: 9px;
  }
  &::after {
    content: '';
    position: absolute;
    height: 12px;
    border-right: 1px solid rgba(240, 241, 245, 1);
    left: 42px;
    top: 10px;
  }
}
</style>
