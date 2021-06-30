

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
  <bkdata-sideslider
    :isShow.sync="show"
    :title="$t('接入数据按原文件上传')"
    :width="874"
    :transfer="true"
    extCls="upload-file-access-slider">
    <div slot="content"
      class="upload-file-slider-wrapper">
      <div v-if="!successPage.isSuccess"
        class="first-step-wrapper">
        <div class="tips-into">
          <i class="icon-info" />
          <p class="first-line-text text">
            {{ $t('接入类型为文件数据源时') }}
          </p>
          <p class="second-line-text text">
            {{ $t('提交成功后的上传提示') }}
          </p>
        </div>

        <div v-for="(item, index) in formConfig"
          :key="index"
          class="form-content-block">
          <div class="header">
            <span class="label">{{ item.label }}</span>
            <component :is="item.description"
              v-if="item.isDyncComp" />
            <span v-else
              class="description">
              {{ item.description }}
            </span>
          </div>
          <div class="content">
            <component
              :is="item.component"
              v-bind="item.props"
              :ref="item.ref"
              :encodeLists="encodeLists"
              @bizIdSekected="setCurBiz" />
          </div>
        </div>

        <div class="button-block"
          style="margin-left: 120px">
          <bkdata-button
            :theme="'primary'"
            :title="'确定'"
            class="mr10"
            style="width: 86px"
            :loading="isLoading"
            @click="submitForm">
            {{ $t('确定') }}
          </bkdata-button>
          <bkdata-button :title="'取消'"
            style="width: 86px"
            @click="show = false">
            {{ $t('取消') }}
          </bkdata-button>
        </div>
      </div>

      <div v-else
        class="second-step-wrapper">
        <div class="title">
          <p class="icon-check-circle-fill icon" />
          <p class="text">
            {{ $t('数据源接入成功') }}
          </p>
        </div>
        <div class="content">
          <p class="text-tip">
            {{ '1. ' + $t('您可在笔记中使用_Python_等语言读取该文件') }}
          </p>
          <pre v-highlightjs="sourcecode"
            class="code-block"><code class="python" /></pre>
          <p class="text-tip">
            {{ '2. ' + $t('此外也可以在笔记右侧的') }}<strong>{{ $t('原始数据源') }}</strong>{{ $t('栏找到相关文件') }}
          </p>
          <p class="text-tip">
            {{ '3. ' + $t('如果需要文件清洗文案')
            }}<span class="link"
              @click="jumpToDataDetail">
              {{ $t('数据源详情') }}
            </span>{{ $t('页面') }}
          </p>
        </div>
      </div>
    </div>
  </bkdata-sideslider>
</template>

<script>
import DataDefine from '@/pages/DataAccess/NewForm/FormItems/DataDefine.vue';
import UploadForm from '../components/FileUploadForm.vue';
import Tips from '../components/UploadFilesTips.vue';
import { validateComponentForm } from '@/pages/DataAccess/NewForm/SubformConfig/validate.js';
import Vue from 'vue';
import VueHighlightJS from 'vue-highlightjs';

Vue.use(VueHighlightJS);

export default {
  components: {
    DataDefine,
    UploadForm,
    Tips,
  },
  props: {
    notebook: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      show: false,
      isLoading: false,
      tipsText: $t(''),
      encodeLists: [],
      sourcecode: `# 文件加载进笔记：
DataSet.download_file(raw_data_id, file_name)

# 查看笔记内已加载文件：
DataSet.show_notebook_file()

# 读取文件内容生成dataframe
import pandas as pd
df = pd.read_csv('文件路径', encoding='utf-8-sig')

# 基于dataframe生成平台结果表
ResultTable.create(dataframe, result_table_id)
`,
      successPage: {
        fileName: '',
        fileType: '',
        isSuccess: false,
        sourceId: '',
      },
      params: {
        bk_app_code: 'bk_dataweb',
        bk_username: this.$store.getters.getUserName,
        data_scenario: 'offlinefile',
        bk_biz_id: '',
        description: '',
        access_raw_data: {
          data_source: 'svr',
          maintainer: this.$store.getters.getUserName,
          sensitivity: 'private',
        },
        access_conf_info: {
          collection_model: {
            collection_type: 'all',
            period: '-1',
          },
          filters: {
            delimiter: '|',
            fields: [],
          },
          resource: {
            scope: [
              {
                file_name: '',
                type: 'hdfs',
              },
            ],
          },
        },
        data_scenario_id: 50,
        permission: 'all',
      },
      formConfig: [
        {
          label: $t('数据定义'),
          description: $t('对数据定义的描述或者说明信息'),
          component: DataDefine,
          ref: 'dataDefine',
          isDyncComp: false,
          props: {
            scenarioType: 'file',
            scenarioId: 39,
          },
        },
        {
          label: $t('接入对象'),
          description: Tips,
          component: UploadForm,
          isDyncComp: true,
          ref: 'upload',
          props: {
            uploadConfig: {
              url: window.BKBASE_Global.siteUrl + 'v3/access/collector/upload/part/',
              mergeUrl: window.BKBASE_Global.siteUrl + 'v3/access/collector/upload/merge/',
              name: 'file',
              validateName: /^[_a-zA-Z0-9\-]*\.(csv|xlsx)$/,
              accept: '.csv, .xlsx',
              formDataAttribute: [
                {
                  name: 'bk_biz_id',
                  value: '',
                },
              ],
            },
          },
        },
      ],
    };
  },
  computed: {
    codeByFileType() {
      return this.successPage.fileType ? `pd.read_${this.successPage.fileType}` : '';
    },
  },
  watch: {
    show(val) {
      if (!val) {
        this.successPage = {
          fileName: '',
          fileType: '',
          isSuccess: false,
          sourceId: '',
        };
      }
    },
  },
  created() {
    this.getEncodeLists();
  },
  methods: {
    setCurBiz(id) {
      this.$set(this.formConfig[1].props.uploadConfig, 'formDataAttribute', [
        {
          name: 'bk_biz_id',
          value: id,
        },
      ]);
    },
    jumpToDataDetail() {
      const { href } = this.$router.resolve({
        name: 'data_detail',
        params: {
          did: this.successPage.sourceId,
          tabid: '2',
        },
      });
      window.open(href, '_blank');
    },
    submitForm() {
      const result = [
        this.$refs.dataDefine[0].validateForm(validateComponentForm),
        this.$refs.upload[0].validateForm(),
      ].every(item => item === true);
      if (result) {
        const formParams = this.$refs.dataDefine[0].params;
        this.params.bk_biz_id = formParams.bk_biz_id;
        this.$set(this.params, 'access_raw_data', Object.assign(this.params.access_raw_data, formParams));
        this.params.access_conf_info.resource.scope[0].file_name = this.$refs.upload[0].fileName; // 上传文件名
        this.newform();
      }
    },
    newform() {
      this.isLoading = true;
      const config = this.params;
      this.bkRequest
        .httpRequest('dataAccess/deployPlan', {
          params: {
            ...config,
          },
        })
        .then(res => {
          // 项目下的文件上传，需要再次发送一个请求，绑定project_id
          if (res.result) {
            this.saveSuccessCallback(res);
          } else {
            this.getMethodWarning(res.message, res.code);
            this.isLoading = false;
          }
        });
    },
    saveSuccessCallback(params) {
      this.successPage.sourceId = params.data.raw_data_id;
      const fileName = this.$refs.upload[0].file.name;
      const fileType = fileName && fileName.split('.').length && fileName.split('.')[1];
      if (this.notebook.project_type === 'common') {
        this.bkRequest
          .httpRequest('dataExplore/relateFile', {
            params: {
              bk_biz_id: this.$refs.dataDefine[0].params.bk_biz_id,
              project_id: this.notebook.project_id,
              object_id: params.data.raw_data_id,
              action_id: 'raw_data.query_data',
            },
          })
          .then(res => {
            if (res.result) {
              this.successPage.fileName = fileName;
              this.successPage.fileType = fileType;
              this.successPage.isSuccess = true;
            } else {
              this.getMethodWarning(res.message, 'error');
            }
          })
          ['finally'](() => {
            this.isLoading = false;
          });
      } else {
        this.isLoading = false;
        this.successPage.fileName = fileName;
        this.successPage.fileType = fileType;
        this.successPage.isSuccess = true;
      }
    },
    getEncodeLists() {
      this.bkRequest.httpRequest('/dataAccess/getEncodeList').then(res => {
        if (res.result) {
          this.$set(this, 'encodeLists', res.data);
        } else {
          this.getMethodWarning(res.message, 'error');
        }
      });
    },
  },
};
</script>

.
<style lang="scss" scoped>
.upload-file-access-slider {
  .upload-file-slider-wrapper {
    padding: 30px;
    .first-step-wrapper {
      .tips-into {
        padding: 8px 34px 10px 34px;
        background-color: #f0f8ff;
        border: 1px solid #c5daff;
        border-radius: 2px;
        position: relative;
        margin-bottom: 20px;
        .icon-info {
          position: absolute;
          top: 11px;
          left: 10px;
          font-size: 16px;
          color: #3a84ff;
        }
      }
      ::v-deep .form-content-block {
        margin-bottom: 30px;
        .header {
          font-size: 0;
          display: flex;
          padding-right: 15px;
          .label {
            font-size: 15px;
            color: #313238;
            line-height: 20px;
            margin-right: 20px;
          }
          .description {
            font-size: 14px;
            color: #979ba5;
            line-height: 19px;
          }
        }
        ::v-deep .content {
          .bk-label {
            line-height: 32px;
          }
        }
      }
    }
    .second-step-wrapper {
      padding: 50px 107px;
      .title {
        margin: 0 auto;
        text-align: center;
        margin-bottom: 60px;
        .icon {
          font-size: 60px;
          color: #2dcb56;
        }
        .text {
          font-size: 24px;
          color: #313238;
          line-height: 31px;
          margin-top: 5px;
        }
      }
      .content {
        .text-tip {
          font-size: 12px;
          text-align: left;
          color: #63656e;
          line-height: 24px;
          .link {
            cursor: pointer;
            color: #3ab4ff;
          }
        }
        .code-block {
          code {
            margin: 10px 0 20px 0;
            background: #f0f1f5;
            border-radius: 2px;
            padding: 10px 14px;
            line-height: 20px;
            .code {
              font-size: 0;
              span {
                font-size: 12px;
              }
              .green {
                color: #4d9c4e;
              }
              .red {
                color: #bf5f62;
              }
            }
          }
        }
      }
    }
  }
}
</style>
