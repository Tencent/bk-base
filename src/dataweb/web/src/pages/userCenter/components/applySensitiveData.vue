

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
  <div class="sensitive">
    <bkdata-dialog v-model="sensitive.isShow"
      extCls="bkdata-dialog"
      width="1000"
      :confirm="'提交'"
      :hasHeader="false"
      @confirm="confirmApply"
      @cancel="closeDialog">
      <div slot="content">
        <div class="sensitive-header">
          {{ $t('申请敏感数据') }}({{ projectName }})
          <i :title="$t('关闭')"
            class="bk-icon icon-close"
            @click="closeDialog" />
        </div>
        <div class="sensitive-content">
          <div class="sensitive-select">
            {{ $t('请选择敏感数据') }}
          </div>
          <div class="sensitive-list">
            <div v-for="(item, index) of sensitiveList"
              :key="index"
              class="sensitive-list-item">
              <div class="bk-form-item">
                <div class="bk-form-content">
                  <label class="bk-form-radio">
                    <input type="radio"
                      name="radio1"
                      checked="checked">
                    <i class="bk-radio-text">{{ item }}</i>
                  </label>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
export default {
  props: {
    projectName: String,
  },
  data() {
    return {
      sensitive: {
        isShow: false,
      },
      sensitiveList: [],
    };
  },
  methods: {
    show() {
      this.sensitive.isShow = true;
    },
    confirmApply() {},
    closeDialog() {
      this.sensitive.isShow = false;
    },
  },
};
</script>
<style lang="scss">
.sensitive {
  &-header {
    padding-left: 10px;
    position: relative;
    color: #212232;
    font-size: 16px;
    font-weight: bold;
    &::after {
      position: absolute;
      height: 100%;
      left: 0px;
      content: '';
      border-left: 4px solid #3a84ff;
    }
    .bk-icon {
      float: right;
      cursor: pointer;
    }
  }

  &-content {
    margin: 40px 40px 0 40px;
    display: flex;
  }
  &-select {
    min-width: 120px;
  }
  &-list {
    display: flex;
    flex-wrap: wrap;
    overflow: auto;
    height: 310px;
    /* 设置滚动条的样式 */
    &::-webkit-scrollbar {
      width: 4px;
    }

    /* 滚动槽 */
    &::-webkit-scrollbar-track {
      -webkit-box-shadow: inset 0 0 6px rgba(0, 0, 0, 0.3);
      border-radius: 10px;
    }

    /* 滚动条滑块 */
    &::-webkit-scrollbar-thumb {
      width: 4px;
      border-radius: 10px;
      background: #ccc;
      -webkit-box-shadow: inset 0 0 6px rgba(0, 0, 0, 0.5);
    }
    &-item {
      height: 35px;
      line-height: 35px;
      padding: 0px 20px 0px 10px;
      border: 1px solid #dde4eb;
      background: #fafbfd;
      border-radius: 4px;
      margin-right: 10px;
      margin-bottom: 10px;
      cursor: pointer;
    }
  }
  .bk-dialog-footer {
    padding-top: 20px;
    padding-bottom: 100px;
  }
  .bk-form-radio input[type='radio'] {
    vertical-align: top;
  }
  .bk-dialog-footer .bk-dialog-outer {
    text-align: left;
    padding-left: 180px;
    padding-bottom: 40px;
    button {
      width: 120px;
    }
  }
}
</style>
