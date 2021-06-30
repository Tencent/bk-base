

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
  <div class="access-file">
    <Tips v-if="!display"
      style="width: 788px" />
    <Container>
      <FieldX2 :label="$t('上传文件')">
        <template v-if="display">
          {{ params.file_name }}
        </template>
        <template v-else>
          <Upload
            :url="uploadConfig.url"
            :mergeUrl="uploadConfig.mergeUrl"
            :name="uploadConfig.name"
            :validateName="uploadConfig.validateName"
            :accept="uploadConfig.accept"
            :formDataAttribute="uploadConfig.formDataAttribute"
            style="width: 100%"
            @uploadFileName="setUploadFileName"
            @fileDelete="params.file_name = ''" />
        </template>
      </FieldX2>
    </Container>
  </div>
</template>
<script>
import Container from '../ItemContainer';
import FieldX2 from '../FieldComponents/FieldX2';
import mixin from '@/pages/DataAccess/Config/mixins.js';
import Tips from '@/pages/DataExplore/components/UploadFilesTips.vue';
import Upload from '@/components/bigFileUploader/index.vue';
export default {
  components: {
    Container,
    FieldX2,
    Tips,
    Upload,
  },
  mixins: [mixin],
  props: {
    bizid: {
      type: Number,
      default: 0,
    },
    display: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      uploadConfig: {
        url: window.BKBASE_Global.siteUrl + 'v3/access/collector/upload/part/',
        mergeUrl: window.BKBASE_Global.siteUrl + 'v3/access/collector/upload/merge/',
        name: 'file',
        validateName: /^[_a-zA-Z0-9\-]*\.(csv|xlsx)$/,
        accept: '.csv, .xlsx',
        formDataAttribute: [
          {
            name: 'bk_biz_id',
            value: this.bizid,
          },
          {
            name: 'raw-data-id',
            value: +this.$route.params.did || -1,
          },
        ],
      },
      isFirstValidate: true,
      size: 0,
      file_name: '',
      params: {
        file_name: '',
        type: 'hdfs',
      },
      validate: {
        file_name: {
          regs: [{ required: true, error: window.$t('不能为空') }],
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
    };
  },
  computed: {
    fileTemplateUrl() {
      let lang = '';
      if (this.$i18n.locale === 'zh-cn') {
        lang = '-cn';
      } else {
        lang = '-us';
      }
      return {
        excel: window.staticUrl + `assets/templatefile/example${lang}.xlsx`,
        csv: window.staticUrl + `assets/templatefile/example${lang}.csv`,
      };
    },
  },
  watch: {
    bizid(val) {
      this.setFileValidate('请选择业务', !val);
      this.uploadConfig.formDataAttribute[0].value = val;
      this.$forceUpdate();
    },
  },
  methods: {
    setUploadFileName(fileName) {
      this.params.file_name = fileName;
    },
    handleFileUpload() {
      if (!this.validateBizId()) {
        this.$forceUpdate();
        return;
      }
      this.$refs.fileUpload.click();
    },
    validateForm(validateFunc, isSubmit = true) {
      if (isSubmit) {
        this.isFirstValidate = false;
      }
      const isValidate = this.validateFile();
      this.$forceUpdate();
      return isValidate;
    },
    validateBizId() {
      let isValidate = !this.setFileValidate('', false);
      if (!this.bizid) {
        isValidate = !this.setFileValidate(window.$t('请选择所属业务'), true);
      }

      return isValidate;
    },
    validateFile(ignoreFileName = true) {
      return this.params.file_name !== '';

      // let isValidate = !this.setFileValidate('', false)
      // // if (!this.bizid) {
      // //     isValidate = !this.setFileValidate('请选择业务', true)
      // // }

      // if (!ignoreFileName && isValidate && !this.params.file_name) {
      //     isValidate = !this.setFileValidate(window.$t('不能为空'), true)
      // }
      // if (isValidate && !/.*\.csv|.*\.xlsx?$/gi.test(this.file_name)) {
      //     isValidate = !this.setFileValidate(window.$t('只支持Csv_Excel文件'), true)
      // }

      // if (isValidate && this.size > 10) {
      //     isValidate = !this.setFileValidate(window.$t('文件大小小于10M'), true)
      // }
      // return isValidate
    },

    setFileValidate(content, visible = true) {
      this.$set(this.validate.file_name, 'content', content);
      this.$set(this.validate.file_name, 'visible', visible);
      return visible;
    },
    formatFormData() {
      return {
        group: 'access_conf_info',
        identifier: 'resource',
        data: {
          scope: [this.params],
        },
      };
    },
    renderData(data) {
      this.params.file_name = data['access_conf_info'].resource.scope[0]['file_name'];
      this.file_name = this.params.file_name;
    },
  },
};
</script>
<style lang="scss" scoped>
.access-file {
  .file-tips {
    position: relative;
    line-height: 30px;
    height: 30px;
    display: flex;
    ::v-deep .bk-form-content {
      height: 30px;
    }
    .bk-form-input {
      background: #f5f5f5;
      width: 1px;
      height: 1px;
    }
  }
}

.file-input {
  width: 120px;
  overflow: hidden;
  opacity: 0;
  position: relative;
  z-index: 9;
}

.file-input-style {
  position: absolute;
  left: 0;
  top: 0;

  .file-input-btn {
    width: 267px;
    height: 30px;
    overflow: hidden;
    text-overflow: ellipsis;
    padding: 0 5px;
    cursor: pointer;
    text-align: left;

    .bk-icon {
      font-size: 12px;
      &.icon-upload {
        width: 100%;
        font-size: 12px;
      }
    }
  }

  .file-upload-text {
    padding-left: 5px;
  }

  .file-template {
    a {
      display: inline-block;
      margin-right: 10px;
      color: #3a84ff;
    }
  }
}
</style>
