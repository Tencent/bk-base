

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
  <div class="bk-form-item">
    <label class="bk-label">
      {{ $t('上传文件') }}
      <span class="required">*</span>
    </label>
    <div class="upload-file">
      <bkdata-upload
        ref="upload"
        :theme="'button'"
        :size="200"
        :multiple="false"
        :name="'file'"
        :validateName="validate"
        :delayTime="1"
        :tip="tip"
        :accept="accept"
        :form-data-attributes="formDataAttributes"
        :handleResCode="handleResCode"
        :withCredentials="true"
        :url="uploadUrl"
        @on-success="successHandle" />
      <bkdata-table v-if="loadedList.length"
        :data="loadedList"
        :border="true"
        :emptyText="$t('未上传文件')">
        <bkdata-table-column :label="$t('名称')"
          align="center">
          <template slot-scope="props">
            <div v-bk-tooltips="props.row.name">
              {{ props.row.name }}
            </div>
          </template>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('上传时间')"
          align="center">
          <template slot-scope="props">
            <div v-bk-tooltips="props.row.created_at">
              {{ props.row.created_at }}
            </div>
          </template>
        </bkdata-table-column>
        <bkdata-table-column :label="$t('上传者')"
          align="center">
          <template slot-scope="props">
            <div v-bk-tooltips="props.row.created_by">
              {{ props.row.created_by }}
            </div>
          </template>
        </bkdata-table-column>
      </bkdata-table>
    </div>
    <div v-show="error.status"
      class="help-block"
      style="margin-left: 125px"
      v-text="error.errorMsg" />
  </div>
</template>

<script>
export default {
  name: 'tdw-jar',
  props: {
    validate: {
      type: RegExp,
    },
    successHandle: {
      type: Function,
      default: () => {},
    },
    loadedList: {
      type: Array,
      default: () => [],
    },
    error: {
      type: Object,
      default: () => ({
        status: false,
        errorMsg: '必填项不可为空',
      }),
    },
    handleResCode: {
      type: Function,
    },
    url: {
      type: String,
      default: '',
    },
    accept: {
      type: String,
      default: '.jar',
    },
    tip: {
      type: String,
      default: '只允许上传jar包文件',
    },
    formDataAttributes: {
      type: Array,
      default: () => [
        {
          name: 'type',
          value: 'sdk',
        },
      ],
    },
  },
  computed: {
    flowId() {
      return this.$route.params.fid;
    },
    uploadUrl() {
      return this.url ? this.url : `${window.BKBASE_Global.siteUrl}v3/dataflow/flow/flows/${this.flowId}/upload/`;
    },
    fileList() {
      return this.$refs.upload.fileList;
    },
  },
  methods: {
    /*
     * 列表更新，用于初始化回填
     */
    updateList(list) {
      list.forEach(item => {
        this.$refs.upload.fileIndex++;
        const fileObj = {
          name: item.name,
          type: 'zip',
          done: true,
          progress: '100%',
        };
        this.$refs.upload.fileList.push(fileObj);
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.upload-file {
  margin-left: 125px;
  ::v-deep .file-item .file-info .file-name span {
    display: inline-block;
    width: 65px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  ::v-deep .bk-table-empty-block {
    max-height: 100px;
  }
}
</style>
