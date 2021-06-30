

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
  <Container id="notebook-side-upload">
    <FieldX2 :label="$t('上传文件')"
      :requried="true">
      <div :class="['bk-form-content']">
        <large-file-upload
          v-tooltip.notrigger="validater"
          v-bind="uploadConfig"
          @uploadSuccess="setUploadFile"
          @uploadFileName="setUploadFileName" />
      </div>
    </FieldX2>
  </Container>
</template>

<script>
import Container from '@/pages/DataAccess/NewForm/FormItems/ItemContainer.vue';
import LargeFileUpload from '@/components/bigFileUploader/index.vue';
import FieldX2 from '@/pages/DataAccess/NewForm/FormItems/FieldComponents/FieldX2.vue';
export default {
  components: {
    Container,
    FieldX2,
    LargeFileUpload,
  },
  props: {
    uploadConfig: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      file: null,
      fileName: '',
      validater: {
        content: $t('不能为空'),
        visible: false,
        class: 'error-red',
      },
    };
  },
  methods: {
    validateForm() {
      const result = this.file === null;
      this.validater.visible = result;
      this.$forceUpdate();
      return !result;
    },
    setUploadFile(fileList) {
      if (fileList.length) {
        this.file = fileList[0];
      }
    },
    setUploadFileName(fileName) {
      this.fileName = fileName;
    },
  },
};
</script>

<style lang="scss" scoped>
#notebook-side-upload {
  .bk-upload {
    width: 100%;
  }
  ::v-deep .access-item {
    align-items: flex-start;
    .bk-label {
      line-height: 80px;
    }
  }
}
</style>
