

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
  <div>
    <div>
      <bkdata-dialog
        v-model="newTaskDialog.isShow"
        extCls="bkdata-dialog none-padding"
        :hasHeader="newTaskDialog.hasHeader"
        :loading="isDiagLoading"
        :okText="$t('创建')"
        :cancelText="$t('取消')"
        :hasFooter="newTaskDialog.hasFooter"
        :width="newTaskDialog.width"
        :padding="0"
        @confirm="createNewSample"
        @cancel="close">
        <div class="new-sample">
          <div class="hearder pl20">
            <div class="text fl">
              <p class="title">
                <i class="bk-icon icon-dataflow" />{{ $t('创建样本集') }}
              </p>
              <p>{{ $t('请确认已有原始数据任务使用') }}</p>
            </div>
          </div>
          <div class="content">
            <div class="bk-form">
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('所属项目') }}：</label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :selected.sync="params.project_id"
                    :isLoading="projectLoading"
                    :placeholder="project.placeholder"
                    :disabled="project.isDisabled"
                    :searchable="true"
                    :list="project.projectLists"
                    :settingKey="'project_id'"
                    :displayKey="'project_name'"
                    @item-selected="tips('project_id')" />
                  <div v-if="error.projectError"
                    class="bk-form-tip">
                    <p class="bk-tip-text">
                      {{ error.tips }}
                    </p>
                  </div>
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('样本集名称') }}：</label>
                <div class="bk-form-content">
                  <bkdata-input
                    v-model.trim="params.sample_set_name"
                    type="text"
                    autofocus="autofocus"
                    :class="[{ 'is-danger': error.nameError }]"
                    :maxlength="15"
                    :placeholder="$t('请输入样本集名称_15个字符限制')"
                    @input="tips('sample_set_name')" />
                  <div v-if="error.nameError"
                    class="bk-form-tip">
                    <p class="bk-tip-text">
                      {{ error.tips }}
                    </p>
                  </div>
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label">{{ $t('样本集描述') }}：</label>
                <div class="bk-form-content">
                  <textarea
                    id=""
                    v-model.trim="params.description"
                    name=""
                    :maxlength="50"
                    class="bk-form-textarea"
                    :placeholder="$t('请输入任务描述_50个字符限制')" />
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('所属场景') }}：</label>
                <div class="bk-form-content">
                  <bkdata-selector
                    :selected.sync="params.scene_name"
                    :disabled="!!copySceneName.length"
                    :isLoading="sceneLoading"
                    :placeholder="$t('请选择场景')"
                    :list="sampleSceneList"
                    :settingKey="'scene_name'"
                    :displayKey="'scene_zh_name'"
                    @item-selected="tips('scene_name')" />
                  <div v-if="error.sceneNameError"
                    class="bk-form-tip">
                    <p class="bk-tip-text">
                      {{ error.tips }}
                    </p>
                  </div>
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label"><span class="required">*</span>{{ $t('是否公开') }}：</label>
                <div class="bk-form-content">
                  <bkdata-switcher v-model="params.is_public"
                    :onText="$t('是')"
                    :showText="true"
                    :offText="$t('否')" />
                  <span v-if="!params.is_public">{{ $t('仅可在本项目中使用') }}</span>
                  <span v-else>{{ $t('可在所有项目中使用') }}</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </bkdata-dialog>
    </div>
  </div>
</template>
<script>
import { showMsg } from '@/common/js/util.js';
export default {
  props: {
    isCreated: {
      type: Boolean,
    },
    newTask: {
      type: Boolean,
    },
    copySceneName: {
      type: String,
      default: '',
    },
    sampleSetId: {
      type: Number,
      default: 0,
    },
  },
  data() {
    return {
      isLoading: false,
      isDiagLoading: false,
      sceneLoading: true,
      projectLoading: false,
      project: {
        projectLists: [],
        placeholder: this.$t('请选择项目'),
        isDisabled: false,
      },
      newTaskDialog: {
        hasHeader: false,
        hasFooter: false,
        width: 576,
        isShow: false,
      },
      params: {
        sample_set_name: '', // 样本名称
        project_id: 0, // 项目ID
        scene_name: '', // 场景标识
        is_public: false, // 是否公开
        properties: '', // 属性
        description: '', // 描述
        submitted_by: '', // 提交人
      },
      error: {
        nameError: false,
        tips: '',
        sceneNameError: false,
        projectError: false,
      },
      sampleSceneList: [],
    };
  },
  watch: {
    isCreated(val) {
      this.isLoading = !val;
    },
  },
  mounted() {
    // this.getSampleSceneList()
  },
  methods: {
    getProjectList() {
      this.projectLoading = true;
      this.project.placeholder = this.$t('数据加载中');
      this.project.isDisabled = true;
      let param = {
        add_del_info: 1,
        add_flow_info: 1,
        ordering: 'active',
      };
      this.project.projectLists = [];
      this.axios
        .get('projects/list_my_projects/?' + this.qs.stringify(param))
        .then(res => {
          if (res.result) {
            res.data.projects.forEach(item => {
              this.project.projectLists.push(item);
            });
            this.project.placeholder = this.$t('请选择项目');
          } else {
            this.getMethodWarning(res.message, res.code);
            this.project.placeholder = this.$t('数据加载失败');
          }
        })
        ['finally'](() => {
          this.project.isDisabled = false;
          this.projectLoading = false;
        });
    },
    open(item) {
      this.getProjectList();
      if (item) {
        this.params = {
          project_id: item.project_id,
          flow_name: '',
          description: '',
          scene_name: '',
        };
      }
      this.error.nameError = false;
      this.newTaskDialog.isShow = true;
      this.$emit('dialogChange', true);
    },
    close() {
      this.newTaskDialog.isShow = false;
      this.$emit('closeNewSample', false);
    },
    tips(name) {
      if (name === 'sample_set_name' || name === 'all') {
        if (this.params.sample_set_name) {
          this.error.nameError = false;
        } else {
          this.error.nameError = true;
          this.error.tips = this.validator.message.required;
        }
      }
      if (name === 'scene_name' || name === 'all') {
        if (this.params.scene_name) {
          this.error.sceneNameError = false;
        } else {
          this.error.sceneNameError = true;
          this.error.tips = this.validator.message.required;
        }
      }
      if (name === 'project_id' || name === 'all') {
        if (this.params.project_id) {
          this.error.projectError = false;
        } else {
          this.error.projectError = true;
          this.error.tips = this.validator.message.required;
        }
      }
      if (name !== 'all') {
        this.isDiagLoading = false;
      }
    },
    createNewSample() {
      this.isDiagLoading = true;
      this.tips('all');
      if (!this.error.nameError && !this.error.sceneNameError && !this.error.projectError) {
        this.isLoading = true;
        let options = {
          params: this.params,
        };
        let url = '/sample/creatSampleSet';
        if (this.copySceneName) {
          url = 'sample/copySampleSet';
          this.params.sample_set_id = this.sampleSetId;
        }
        this.bkRequest.httpRequest(url, options).then(res => {
          if (res.result) {
            let tipText = this.$t('创建样本集成功');
            if (this.copySceneName) tipText = this.$t('复制样本集成功');
            showMsg(tipText, 'success');
            this.close();
            this.$router.push({
              path: '/sample-set/detail',
              query: {
                sample_set_id: res.data.sample_set_id,
                project_id: this.params.project_id,
              },
            });
            this.$emit('closeNewSample', false);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isDiagLoading = false;
        });
      }
    },
    // getSampleSceneList() {
    //     this.sceneLoading = true
    //     this.bkRequest.httpRequest('sample/getSampleScene').then(res => {
    //         if (res.result) {
    //             if (this.copySceneName) {
    //                 this.params.scene_name = this.copySceneName
    //             }
    //             this.sampleSceneList = res.data
    //             this.sampleSceneList = this.sampleSceneList.map(item => {
    //                 item.disabled = item.status !== 'finished'
    //                 return item
    //             })
    //         } else {
    //             this.getMethodWarning(res.message, res.code)
    //         }
    //         this.sceneLoading = false
    //     })
    // }
  },
};
</script>
<style media="screen" lang="scss">
.new-sample {
  .hearder {
    height: 70px;
    background: #23243b;
    position: relative;
    padding: 0 !important;
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
    padding: 30px 0 45px 0;
    .bk-form {
      width: 480px;
      .bk-label {
        padding: 11px 16px 11px 10px;
        width: 180px;
      }
      .bk-form-content {
        display: block;
        margin-left: 180px;
        .bk-tip-text {
          color: #ff5656;
        }
        .sample-data-width {
          width: 100px;
        }
        & > span {
          height: 36px;
          line-height: 36px;
          margin-left: 8px;
        }
      }
    }
    .required {
      color: #f64646;
      display: inline-block;
      vertical-align: -2px;
      margin-right: 5px;
    }
    .bk-button {
      min-width: 120px;
    }
  }
}
</style>
