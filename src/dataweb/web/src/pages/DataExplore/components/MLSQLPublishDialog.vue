

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
  <dialog-wrapper
    :dialog="dialog"
    extCls="mlsql-publish-dialog"
    :title="$t('发布')"
    :bkScroll="false"
    :subtitle="$t('发布后可以作为数据开发任务的算法模型节点被周期性调度')">
    <template #content>
      <div v-bkloading="{ isLoading: loading }"
        class="form-wrapper">
        <bkdata-alert type="info"
          :title="$t('MLSQL发布提示文案')" />
        <bkdata-form ref="publishForm"
          :labelWidth="80"
          :model="updateForm"
          style="margin: 20px 0">
          <bkdata-form-item :label="$t('发布到')"
            :rules="rules.required"
            property="model_name"
            required>
            <bkdata-select
              v-model="updateForm.model_name"
              style="width: 260px; margin-right: 10px"
              :loading="releaseMethod.loading"
              :placeholder="$t('请选择模型')"
              @change="getModelId">
              <bkdata-option
                v-for="item in releaseModel.list"
                :id="item.model_name"
                :key="item.model_id"
                :name="`${item.model_name}(${item.model_alias})`" />
              <bkdata-option id="newCreatedModel"
                slot="extension"
                :name="$t('新建模型')"
                style="cursor: pointer">
                <i class="bk-icon icon-plus-circle" /> {{ $t('新建模型') }}
              </bkdata-option>
            </bkdata-select>
            <bkdata-select
              v-model="updateForm.experiment_name"
              style="width: 260px"
              :disabled="isExperimentDisabled"
              :loading="releaseExperiment.loading"
              :placeholder="$t('请选择实验')"
              @change="getExperimentId">
              <bkdata-option
                v-for="(item, index) in releaseExperiment.list"
                :id="item.experiment_name"
                :key="index"
                :name="`${item.experiment_name}(${item.experiment_alias})`" />
              <bkdata-option id="createdExperiment"
                slot="extension"
                :name="$t('新建实验')"
                style="cursor: pointer">
                <i class="bk-icon icon-plus-circle" /> {{ $t('新建实验') }}
              </bkdata-option>
            </bkdata-select>
          </bkdata-form-item>
        </bkdata-form>
        <bkdata-form ref="contentForm"
          :labelWidth="80"
          :model="formData">
          <bkdata-form-item
            v-if="isShowData"
            :label="$t('英文名称')"
            :rules="rules.modelName"
            property="model_name"
            required>
            <bkdata-input v-model="formData.model_name" />
          </bkdata-form-item>
          <bkdata-form-item
            v-if="isShowData"
            :label="$t('中文名称')"
            :rules="rules.required"
            property="model_alias"
            required>
            <bkdata-input v-model="formData.model_alias" />
          </bkdata-form-item>
          <bkdata-form-item v-if="isShowData"
            :label="$t('是否公开')"
            property="is_public">
            <bkdata-switcher v-model="formData.is_public" />
          </bkdata-form-item>
          <bkdata-form-item v-if="isShowData"
            :label="$t('备注')"
            property="description">
            <bkdata-input v-model="formData.description"
              type="textarea" />
          </bkdata-form-item>
          <bkdata-form-item
            v-if="isShowExperiment"
            :label="$t('实验名称')"
            :rules="rules.experimentName"
            property="experiment_name"
            required>
            <bkdata-input v-model="formData.experiment_name" />
          </bkdata-form-item>
        </bkdata-form>
      </div>
    </template>
    <template #footer>
      <bkdata-popover ref="popover"
        :theme="'light'"
        trigger="manual"
        zIndex="1">
        <bkdata-button
          :theme="'primary'"
          extCls="publish-mlsql-button mr10"
          style="width: 93px"
          :disabled="loading"
          :loading="confirmLoading"
          @click="confirmHandle">
          {{ $t('确定') }}
        </bkdata-button>
        <div slot="content"
          style="width: 382px">
          <bkdata-table :data="releaseResult"
            :showOverflowTooltip="true">
            <bkdata-table-column :label="$t('时间')"
              prop="time" />
            <bkdata-table-column :label="$t('状态')">
              <template slot-scope="props">
                <span
                  class="result-table-msg"
                  :style="`color: ${props.row.status === 'failure' ? 'red' : 'inherit'}`"
                  :title="props.row.message">
                  {{ props.row.message }}
                </span>
              </template>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </bkdata-popover>
      <bkdata-button
        extCls="publish-mlsql-button"
        style="margin-right: 36px; width: 93px"
        :disabled="loading"
        @click="cancleHandle">
        {{ $t('取消') }}
      </bkdata-button>
    </template>
  </dialog-wrapper>
</template>

<script>
import dialogWrapper from '@/components/dialogWrapper';
export default {
  components: {
    dialogWrapper,
  },
  data() {
    return {
      box: null,
      loading: false,
      confirmLoading: false,
      releaseModel: {
        list: [],
        loading: false,
      },
      releaseExperiment: {
        list: [],
        loading: false,
      },
      sql: '',
      releaseMethod: false,
      releaseResult: [],
      pollingTimer: null,
      formData: {
        model_name: '',
        model_alias: '',
        is_public: false,
        description: '',
        project_id: this.$route.query.projectId,
        last_sql: '',
        experiment_name: '', //实验名称
        experiment_alias: '',
        experiment_id: null,
        result_table_ids: [],
        evaluation_result: [],
        notebook_id: this.$route.query.notebook,
      },
      updateForm: {
        model_name: '',
        description: '',
      },
      rules: {
        required: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
        ],
        modelName: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
          {
            regex: /^[a-zA-Z0-9_\u4e00-\u9fa5]+$/,
            message: this.$t('只能包含汉字，英文字母，数字和下划线'),
            trigger: 'blur',
          },
          {
            validator: this.checkName,
            message: this.$t('英文名已存在'),
            trigger: 'blur',
          },
        ],
        experimentName: [
          {
            required: true,
            message: this.$t('必填项不可为空'),
            trigger: 'blur',
          },
          {
            regex: /^[a-zA-Z0-9_\u4e00-\u9fa5]+$/,
            message: this.$t('只能包含汉字，英文字母，数字和下划线'),
            trigger: 'blur',
          },
        ],
      },
      dialog: {
        isShow: false,
        width: 730,
        quickClose: false,
        loading: false,
      },
    };
  },
  computed: {
    isExperimentDisabled() {
      return (
        this.updateForm.model_name === ''
        || this.updateForm.model_name === undefined
        || this.updateForm.model_name === 'newCreatedModel'
      );
    },
    isShowData() {
      return this.updateForm.model_name === 'newCreatedModel';
    },
    isShowExperiment() {
      return this.isShowData || this.updateForm.experiment_name === 'createdExperiment';
    },
  },
  methods: {
    async checkName() {
      const result = await this.bkRequest.httpRequest('dataFlow/checkModelingName', {
        params: {
          modelName: this.formData.model_name,
        },
      });
      return result;
    },
    initDialog(sql, outputs) {
      this.$refs.publishForm.clearError();
      this.evaluation_result = undefined;
      const outputsJsonList = outputs.map(output => (output.text ? JSON.parse(output.text) : {}));
      outputsJsonList.map(value => {
        if (value.evaluate) {
          this.evaluation_result = value.evaluate;
        } else {
          return;
        }
      });
      this.updateForm = {
        model_name: '',
        experiment_name: '',
        experiment_id: null,
        description: '',
      };
      this.formData = {
        model_name: '',
        model_alias: '',
        is_public: false,
        description: '',
        project_id: this.$route.query.projectId,
        last_sql: '',
        experiment_name: '', //实验名称
        experiment_alias: '',
        experiment_id: null,
        result_table_ids: [],
        notebook_id: this.$route.query.notebook,
      };
      this.sql = sql;
      this.getInitFormData();
      this.getResultTables();
      this.getReleasedModel();
    },
    getInitFormData() {
      this.setDialogStatus(true);
      this.loading = true;
      this.bkRequest
        .httpRequest('dataFlow/releaseInspection', {
          params: {
            sql: this.sql,
            result_table_ids: this.formData.result_table_ids,
          },
        })
        .then(res => {
          if (res.result) {
            this.setLastSql(res.data.last_sql);
            // this.formData.model_name = res.data.model_name  新建模型不再获取当前的模型英文名称
          } else {
            this.setDialogStatus(false);
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
    setDialogStatus(status) {
      this.dialog.isShow = status;
    },
    setLastSql(sql) {
      this.formData.last_sql = sql;
    },
    getReleasedModel() {
      this.releaseModel.loading = true;
      this.bkRequest
        .httpRequest('dataFlow/getMyModelList', {
          query: {
            project_id: this.$route.query.projectId,
          },
        })
        .then(res => {
          if (res.result) {
            this.releaseModel.list = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['catch'](res => {
          this.getMethodWarning(res.message, res.code);
        })
        ['finally'](() => {
          this.releaseModel.loading = false;
        });
    },
    getModelId(modelId) {
      if (modelId === 'newCreatedModel') {
        this.updateForm.experiment_name = 'createdExperiment';
        this.formData = {
          model_name: '',
          model_alias: '',
          is_public: false,
          description: '',
          project_id: this.$route.query.projectId,
          last_sql: this.formData.last_sql,
          experiment_name: '', //实验名称
          experiment_alias: '',
          result_table_ids: this.formData.result_table_ids,
          evaluation_result: this.evaluation_result,
          notebook_id: this.$route.query.notebook,
        };
      } else if (modelId === '' || undefined) {
        this.updateForm.experiment_name = '';
        this.releaseExperiment.list = [];
      } else {
        this.updateForm.experiment_name = '';
        this.formData.experiment_name = '';
        this.releaseExperiment.loading = true;
        this.bkRequest
          .httpRequest('dataFlow/getExperimentalList', {
            params: {
              modelId: modelId,
            },
          })
          .then(res => {
            if (res.result) {
              this.releaseExperiment.list = res.data;
              this.formData.experiment_id = res.data[0].experiment_id;
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          })
          ['catch'](res => {
            this.getMethodWarning(res.message, res.code);
          })
          ['finally'](() => {
            this.releaseExperiment.loading = false;
          });
      }
    },
    getExperimentId(experimentId) {
      if (experimentId === 'createdExperiment') {
        this.formData.experiment_name = '';
      }
    },
    getResultTables() {
      this.loading = true;
      this.bkRequest
        .httpRequest('dataExplore/getNotebookOutputs', {
          params: {
            notebookId: this.$route.query.notebook,
          },
        })
        .then(res => {
          if (res.result) {
            this.formData.result_table_ids = res.data.result_tables;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['catch'](res => {
          this.getMethodWarning(res.message, res.code);
        })
        ['finally'](() => {
          this.loading = false;
        });
    },
    getSubmitRequest() {
      if (this.updateForm.model_name === 'newCreatedModel') {
        return this.bkRequest.httpRequest('dataFlow/createModel', {
          params: this.formData,
        });
      } else {
        if (this.updateForm.experiment_name === 'createdExperiment') {
          return this.bkRequest.httpRequest('dataFlow/updateModel', {
            params: {
              ...this.updateForm,
              last_sql: this.formData.last_sql,
              project_id: this.formData.project_id,
              result_table_ids: this.formData.result_table_ids,
              experiment_name: this.formData.experiment_name,
              evaluation_result: this.evaluation_result,
            },
          });
        } else {
          return this.bkRequest.httpRequest('dataFlow/updateModel', {
            params: {
              ...this.updateForm,
              last_sql: this.formData.last_sql,
              project_id: this.formData.project_id,
              result_table_ids: this.formData.result_table_ids,
              experiment_id: this.formData.experiment_id,
              evaluation_result: this.evaluation_result,
            },
          });
        }
      }
    },
    startQueryPolling(id, timeout) {
      this.pollingTimer = setTimeout(() => {
        console.log(id);
        this.bkRequest
          .httpRequest('dataFlow/getModelReleaseResult', {
            query: {
              task_id: id,
            },
          })
          .then(res => {
            if (res.result) {
              const popover = this.$refs.popover.instance;
              !popover.state.isShow && popover.show();
              this.releaseResult = res.data.logs;
              if (res.data.status === 'success') {
                popover.hide();
                clearTimeout(this.pollingTimer);
                this.pollingTimer = null;
                this.confirmLoading = false;
                this.dialog.isShow = false;
                this.showSuccessInfoBox();
              } else if (res.data.status === 'failure') {
                clearTimeout(this.pollingTimer);
                this.pollingTimer = null;
                this.confirmLoading = false;
                this.getMethodWarning(res.data.message, res.data.status);
              } else {
                this.startQueryPolling(id, timeout);
              }
            } else {
              this.confirmLoading = false;
              this.getMethodWarning(res.message, res.code);
            }
          })
          ['catch'](res => {
            this.getMethodWarning(res.message, res.code);
            this.confirmLoading = false;
          });
      }, timeout);
    },
    showSuccessInfoBox() {
      const h = this.$createElement;
      this.box = this.$bkInfo({
        type: 'success',
        title: this.$t('发布成功'),
        showFooter: false,
        subHeader: h(
          'div',
          {
            style: {
              width: '225px',
              fontSize: '12px',
              margin: '0 auto',
              textAlign: 'left',
              lineHeight: '24px',
            },
          },
          [
            '接下来，你可以:',
            h('p', {}, [
              '1.在数据开发中应用数据模型',
              h(
                'a',
                {
                  style: {
                    color: '#3a84ff',
                    marginLeft: '5px',
                    cursor: 'pointer',
                  },
                  on: {
                    click: () => this.jumpToLink({ name: 'dataflow_ide' }),
                  },
                },
                '前往应用'
              ),
            ]),
            h('p', {}, [
              '2.进入',
              h(
                'a',
                {
                  style: {
                    color: '#3a84ff',
                    marginLeft: '5px',
                    cursor: 'pointer',
                  },
                  on: {
                    click: () => this.jumpToLink({
                      name: 'user_center',
                      query: { tab: 'model' },
                    }),
                  },
                },
                '模型详情页'
              ),
              '，查看模型详情',
            ]),
          ]
        ),
      });
    },
    jumpToLink(route) {
      /** 清除弹框元素 */
      this.box.value = false;
      this.box.$el.parentNode.removeChild(this.box.$el);
      this.vox = null;

      let routeData = this.$router.resolve(route);
      window.open(routeData.href, '_blank');
    },
    confirmHandle() {
      if (this.isShowData === false && this.isShowExperiment === false) {
        this.$refs.publishForm.validate().then(
          () => {
            this.confirmLoading = true;
            this.sendRequest();
          },
          err => {
            console.log(err.content);
            this.confirmLoading = false;
          }
        );
      } else {
        this.$refs.contentForm.validate().then(
          () => {
            this.confirmLoading = true;
            this.sendRequest();
          },
          err => {
            console.log(err.content);
            this.confirmLoading = false;
          }
        );
      }
    },
    cancleHandle() {
      this.dialog.isShow = false;
      clearTimeout(this.pollingTimer);
    },
    sendRequest() {
      this.getSubmitRequest()
        .then(res => {
          if (res.result) {
            const taskId = res.data;
            this.startQueryPolling(taskId, 1000);
          } else {
            this.getMethodWarning(res.message, res.code);
            this.confirmLoading = false;
          }
        })
        ['catch'](res => {
          this.getMethodWarning(res.message, res.code);
          this.confirmLoading = false;
        });
    },
  },
};
</script>

<style lang="scss" scoped>
.mlsql-publish-dialog {
  .form-wrapper {
    padding: 30px 60px;
    font-size: 0;
    .publish-way {
      margin-bottom: 20px;
      line-height: 64px;
      .label {
        font-size: 14px;
        width: 80px;
        padding-right: 24px;
        font-weight: bold;
        position: relative;
        &::after {
          height: 8px;
          line-height: 1;
          content: '*';
          color: #ea3636;
          font-size: 12px;
          position: absolute;
          display: inline-block;
          vertical-align: middle;
          top: 50%;
          transform: translate(3px, -50%);
        }
      }
      .publish-card {
        position: relative;
        cursor: pointer;
        font-size: 14px;
        display: inline-flex;
        width: 260px;
        height: 64px;
        background-color: #fff;
        border: 1px solid #c4c6cc;
        border-radius: 2px;
        &.active {
          border: 1px solid #3a84ff;
          .left-area {
            background-color: #e1ecff;
            border-right: 1px solid #3a84ff;
            color: #3a84ff;
          }
          .header-triangle {
            border-right: 36px solid #3a84ff;
          }
        }
        &:first-of-type {
          margin-right: 10px;
        }
        .left-area {
          width: 40px;
          background: #fafbfd;
          border-right: 1px solid #c4c6cc;
          display: flex;
          align-items: center;
          justify-content: center;
        }
        .content {
          padding-left: 14px;
          display: flex;
          flex-direction: column;
          justify-content: center;
          .card-title {
            color: #313238;
            line-height: 19px;
          }
          .description {
            font-size: 12px;
            color: #979ba5;
            line-height: 20px;
          }
        }
        .header-triangle {
          position: absolute;
          right: -1px;
          top: -1px;
          width: 0;
          height: 0;
          border-bottom: 33px solid transparent;
          .icon-check-1 {
            position: absolute;
            right: -38px;
            top: -2px;
            font-size: 25px;
            color: #fff;
          }
        }
      }
    }
  }
  .publish-mlsql-button {
    width: 85px;
    margin-right: 10px;
  }
}
</style>
