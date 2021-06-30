

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
    <span v-if="!display"
      class="bk-item-des">
      {{ $t('目前只支持Excel_CSV_支持') }}
      <a href="https://tools.ietf.org/html/rfc4180">RFC 4180</a>){{ $t('格式的文件_文件需小于等于10MB') }}
    </span>
    <Container>
      <FieldX2
        :tips="{
          visible: !display,
          content: $t('上传本地文件支持csv_excel格式到数据平台_请注意_一次上传文件大小需小于等于10M'),
        }"
        class="file-tips"
        :label="$t('上传文件')">
        <template v-if="display">
          {{ params.file_name }}
        </template>
        <template v-else>
          <input
            ref="fileUpload"
            :disabled="display"
            accept=".csv, .xls, .xlsx"
            class="bk-form-input file-input file-hidden"
            type="file"
            @change="getFile">
          <div class="file-input-style">
            <bkdata-button
              v-bk-tooltips="params.file_name || $t('点击选择上传文件')"
              v-tooltip.notrigger.left="validate['file_name']"
              class="file-input-btn"
              size="small"
              theme="default"
              @click="handleFileUpload">
              <span v-if="params.file_name">{{ params.file_name }}</span>
              <span v-else
                class="bk-icon icon-upload">
                <span class="file-upload-text">{{ $t('选择文件') }}</span>
              </span>
            </bkdata-button>
            <span class="file-template ml10">
              <a
                :href="fileTemplateUrl.excel"
                :download="$i18n.locale === 'zh-cn' ? 'example-cn.xlsx' : 'example-us.xlsx'">
                {{ $t('excel模板') }}
              </a>
              <a :href="fileTemplateUrl.csv"
                :download="$i18n.locale === 'zh-cn' ? 'example-cn.csv' : 'example-us.csv'">
                {{ $t('csv模板') }}
              </a>
            </span>
          </div>
        </template>
      </FieldX2>
    </Container>
  </div>
</template>
<script>
import Container from '../ItemContainer';
import FieldX2 from '../FieldComponents/FieldX2';
import mixin from '@/pages/DataAccess/Config/mixins.js';
export default {
  components: {
    Container,
    FieldX2,
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
      isFirstValidate: true,
      size: 0,
      file_name: '',
      params: {
        file_name: '',
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
      this.$forceUpdate();
    },
  },
  methods: {
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
    getFile(e) {
      let size = '';
      size = e.target.files[0].size; // byte
      size = size / 1024; // kb
      this.size = (size / 1024).toFixed(3); // mb
      this.file_name = e.target.files[0].name;

      if (!this.validateFile(true)) {
        e.target.value = '';
        this.file_name = '';
        this.$forceUpdate();
        return;
      }
      this.file_name = e.target.files[0].name;
      this.fileUpload(e.target.files[0]);
    },
    validateBizId() {
      let isValidate = !this.setFileValidate('', false);
      if (!this.bizid) {
        isValidate = !this.setFileValidate(window.$t('请选择所属业务'), true);
      }

      return isValidate;
    },
    validateFile(ignoreFileName = true) {
      let isValidate = !this.setFileValidate('', false);
      // if (!this.bizid) {
      //     isValidate = !this.setFileValidate('请选择业务', true)
      // }

      if (!ignoreFileName && isValidate && !this.params.file_name) {
        isValidate = !this.setFileValidate(window.$t('不能为空'), true);
      }
      if (isValidate && !/.*\.csv|.*\.xlsx?$/gi.test(this.file_name)) {
        isValidate = !this.setFileValidate(window.$t('只支持Csv_Excel文件'), true);
      }

      if (isValidate && this.size > 10) {
        isValidate = !this.setFileValidate(window.$t('文件大小小于10M'), true);
      }
      return isValidate;
    },

    setFileValidate(content, visible = true) {
      this.$set(this.validate.file_name, 'content', content);
      this.$set(this.validate.file_name, 'visible', visible);
      return visible;
    },

    fileUpload(file) {
      let formData = new FormData();
      formData.append('file_data', file);
      formData.append('bk_biz_id', this.bizid);
      this.bkRequest
        .request('v3/access/collector/upload/', {
          params: formData,
          method: 'POST',
          useSchema: false,
        })
        .then(res => {
          if (res.result) {
            this.params.file_name = res.data.file_name;
          }
        });
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
