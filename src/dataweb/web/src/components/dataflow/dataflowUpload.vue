

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
  <bkdata-dialog
    v-model="dialogConfig.isShow"
    extCls="bkdata-dialog"
    :loading="isLoading"
    :width="dialogConfig.width"
    :maskClose="dialogConfig.quickClose"
    :okText="$t('确定')"
    :cancelText="$t('关闭')"
    @confirm="uploadData">
    <div class="dataflow-upload">
      <div class="hearder">
        <div class="text fl">
          <p class="title">
            <i class="bk-icon icon-startup-config" />
            {{ $t('导入配置') }}
          </p>
          <p>{{ $t('导入或导出画布配置') }}</p>
        </div>
      </div>
      <div class="content">
        <div class="content-item">
          <label class="item-label">{{ $t('上传文件') }}：</label>
          <bkdata-button width="95px"
            style="width: 95px"
            size="small"
            @click="onUploadClick">
            {{ $t('选择文件') }}
          </bkdata-button>
          <input ref="file"
            class="button__file"
            type="file"
            @change="onFileChange">
          <div :class="[{ error: errorInfo }, { success: selectFile }, 'fileInfo']">
            {{ textInfo }}
            <span v-if="selectFile === true"
              class="icon-close-circle-shape"
              :title="$t('清空')"
              @click="clearFile" />
          </div>
        </div>
        <div v-if="uploadLoading"
          class="uploadStatus">
          <div class="bk-spin-loading bk-spin-loading-mini">
            <div class="rotate rotate1" />
            <div class="rotate rotate2" />
            <div class="rotate rotate3" />
            <div class="rotate rotate4" />
            <div class="rotate rotate5" />
            <div class="rotate rotate6" />
            <div class="rotate rotate7" />
            <div class="rotate rotate8" />
          </div>
          <span class="text">{{ $t('正在生成dataflow_此过程可能耗时较长') }}</span>
        </div>
        <div class="content-tip">
          <span class="icon-info" />
          <span class="text">{{ $t('导入将清空当前画布') }}</span>
        </div>
      </div>
    </div>
  </bkdata-dialog>
</template>
<script>
import Bus from '@/common/js/bus.js';
import { showMsg } from '@/common/js/util.js';
export default {
  data() {
    return {
      dialogConfig: {
        hasHeader: false,
        hasFooter: false,
        quickClose: false,
        width: 541,
        isShow: false,
      },
      isLoading: false,
      uploadLoading: false,
      selectFile: false,
      errorInfo: false,
      fileName: '',
      file: null,
    };
  },
  computed: {
    textInfo() {
      return this.fileName === '' ? this.$t('未选择任何文件') : this.fileName;
    },
    flowId() {
      return this.$route.params.fid;
    },
  },
  methods: {
    resetStatus() {
      this.fileName = '';
      this.file = '';
      this.dialogConfig.isShow = false;
      this.errorInfo = false;
      this.selectFile = false;
    },
    /**
     * 文件上传相关
     */
    clearFile() {
      this.fileName = '';
      this.file = '';
      this.selectFile = false;
      this.errorInfo = false;
      this.$refs.file.value = '';
    },
    onFileChange(event) {
      this.errorInfo = false;
      this.selectFile = true;
      this.fileName = event.target.files[0].name;
      let validate = this.validateFile();
      if (validate) {
        this.errorInfo = true;
        this.selectFile = false;
        this.fileName = validate;
        return;
      }
      this.file = event.target.files[0];
    },
    resolveImportJson(jsonStr) {
      const params = { nodes: {} };
      try {
        const jsonObj = JSON.parse(jsonStr);
        params.nodes = jsonObj.nodes;
      } catch (e) {
        params.result = false;
      }
      return params;
    },
    onUploadClick() {
      this.$refs.file.click();
      this.$refs.file.value = '';
    },
    validateFile() {
      let isValidate = '';
      if (!this.fileName) {
        isValidate = this.validator.message.required;
      }
      if (!isValidate && !/.*\.json$/gi.test(this.fileName)) {
        isValidate = this.$t('只支持json文件');
      }
      return isValidate;
    },
    uploadData() {
      this.isLoading = true;
      this.uploadLoading = true;
      const reader = new FileReader();
      reader.onload = fe => {
        let params = this.resolveImportJson(fe.target.result);
        this.$store
          .dispatch('api/flows/uploadDataFlow', {
            params: params,
            flowId: this.flowId,
          })
          .then(res => {
            if (res.result) {
              Bus.$emit('updateGraph');
              this.resetStatus();
              showMsg(this.$t('导入成功'), 'success');
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          })
          ['finally'](() => {
            this.uploadLoading = false;
            this.isLoading = false;
            this.$refs.file.value = '';
          });
      };
      reader.readAsText(this.file);
    },
  },
};
</script>
<style media="screen" lang="scss">
.dataflow-upload {
  .hearder {
    height: 70px;
    background: #23243b;
    position: relative;
    .close {
      display: inline-block;
      position: absolute;
      right: 0;
      top: 0;
      width: 40px;
      height: 40px;
      line-height: 40px;
      text-align: center;
      cursor: pointer;
    }
    .icon {
      font-size: 32px;
      color: #abacb5;
      line-height: 60px;
      width: 142px;
      text-align: right;
      margin-right: 16px;
    }
    .text {
      margin-left: 28px;
      font-size: 12px;
    }
    .title {
      margin: 16px 0 3px 0;
      padding-left: 36px;
      position: relative;
      color: #fafafa;
      font-size: 18px;
      text-align: left;
      i {
        position: absolute;
        left: 10px;
        top: 2px;
      }
    }
  }
  .content {
    max-height: 500px;
    padding: 30px 0 15px 0;
    overflow-y: auto;
    .content-item {
      display: flex;
      padding-left: 110px;
      align-items: center;
      justify-content: left;
      input {
        display: none;
      }
      .item-label {
        width: 85px;
      }
      .fileInfo {
        margin-left: 20px;
        width: 130px;
        color: rgb(99, 101, 110);
        position: relative;
        .icon-close-circle-shape {
          color: #727985;
          cursor: pointer;
          position: absolute;
          right: -15px;
          top: 50%;
          transform: translateY(-50%);
        }
      }
      .error {
        color: red;
      }
      .success {
        color: green;
      }
    }
    .content-tip {
      width: 300px;
      height: 30px;
      margin-left: 110px;
      margin-top: 20px;
      background: rgba(250, 251, 253, 1);
      color: rgba(4, 132, 255, 1);
      border-radius: 2px;
      opacity: 1;
      display: flex;
      align-items: center;
      .icon-info {
        font-size: 18px;
        height: 18px;
        line-height: 18px;
        margin: 0 7px 0 18px;
      }
      .text {
        width: 196px;
        height: 19px;
        font-size: 12px;
        font-family: Microsoft YaHei;
        font-weight: 400;
        line-height: 20px;
      }
    }
    .uploadStatus {
      margin-left: 110px;
      margin-top: 10px;
      .text {
        position: relative;
        top: 2px;
      }
    }
  }
}
</style>
