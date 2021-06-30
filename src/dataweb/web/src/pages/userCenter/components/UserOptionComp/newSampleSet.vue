

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
    <bkdata-dialog
      v-model="newTaskDialog.isShow"
      extCls="bkdata-dialog"
      :hasHeader="newTaskDialog.hasHeader"
      :loading="isDiagLoading"
      :okText="getContent(mode, 'okText')"
      :cancelText="$t('取消')"
      :position="{ top: 100 }"
      :hasFooter="newTaskDialog.hasFooter"
      :width="newTaskDialog.width"
      :padding="0"
      @confirm="createNewSample"
      @cancel="close">
      <div :class="[ 'new-sample', { 'en-new-sample' : curLang === 'en'}]">
        <div class="hearder pl20">
          <div class="text fl">
            <p class="title">
              <i class="bkdata-icon icon-dataflow" />{{ getContent(mode, 'title') }}
            </p>
            <p>{{ $t('样本集可以在模型中使用') }}</p>
          </div>
        </div>
        <div class="content">
          <div v-if="mode === 'copy'"
            class="bk-form mb20">
            <div class="bk-form-item">
              <label class="bk-label copy-label-title"
                style="width: 200px">
                {{ $t('将')
                }}<span class="sample-name text-overflow"
                  :title="copyedSampleSetName">
                  {{ copyedSampleSetName }}
                </span>{{ $t('复制为') }}
              </label>
            </div>
          </div>
          <div class="form-container">
            <bkdata-form ref="validateForm1"
              :labelWidth="curLang === 'en' ? 135 : 130"
              :model="formData"
              :rules="rules">
              <div class="desc-title">
                {{ $t('场景信息') }}
              </div>
              <bkdata-form-item :label="$t('数据类型')"
                :required="true"
                :property="'sample_type'">
                <bkdata-selector
                  :selected.sync="formData.sample_type"
                  :disabled="mode === 'copy' && copySensitivity !== 'public'"
                  :placeholder="$t('请选择数据类型')"
                  :isLoading="$attrs.isProjectListLoading"
                  :searchable="true"
                  :list="typeList"
                  :settingKey="'data_type_name'"
                  :displayKey="'data_type_alias'" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('场景')"
                :required="true"
                :property="'scene_name'">
                <bkdata-select v-model="formData.scene_name"
                  :isLoading="isSceneLoading"
                  :disabled="mode === 'copy'"
                  :placeholder="$t('请选择场景')">
                  <bkdata-option v-for="option in sceneList"
                    :id="option.scene_name"
                    :key="option.scene_name"
                    :name="option.scene_alias" />
                </bkdata-select>
                <!-- <bkdata-selector :selected.sync="formData.scene_name"
                  :placeholder="$t('请选择场景')"
                  :isLoading="isSceneLoading"
                  :disabled="mode === 'copy'"
                  :list="sceneList"
                  :settingKey="'scene_name'"
                  :displayKey="'scene_alias'" /> -->
              </bkdata-form-item>

              <div class="desc-title">
                {{ $t('样本集属性') }}
              </div>

              <bkdata-form-item :label="$t('所属项目')"
                :required="true"
                :property="'project_id'">
                <bkdata-selector
                  :selected.sync="formData.project_id"
                  :disabled="mode === 'copy' && copySensitivity !== 'public'"
                  :placeholder="$t('请选择项目')"
                  :isLoading="$attrs.isProjectListLoading"
                  :searchable="true"
                  :list="projectList"
                  :settingKey="'project_id'"
                  :displayKey="'project_name'"
                  @change="handleProjectChange" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('样本集名称')"
                :required="true"
                :property="'sample_set_name'">
                <bkdata-input v-model.trim="formData.sample_set_name"
                  type="text"
                  autofocus="autofocus"
                  :placeholder="$t('请输入样本集名称3~50个字符限制')" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('样本集描述')"
                :required="true"
                :property="'description'">
                <bkdata-input v-model="formData.description"
                  :placeholder="$t('请输入样本集描述_50个字符限制')"
                  :type="'textarea'"
                  :rows="3" />
              </bkdata-form-item>
              <bkdata-form-item :label="$t('依赖历史数据长度')"
                :required="true"
                :property="'tsDepend'"
                class="tsDepend">
                <bkdata-input v-model="formData.tsDepend"
                  :disabled="mode === 'copy' && copySensitivity !== 'public'"
                  placeholder="int (>=0)">
                  <template slot="append">
                    <bkdata-select v-model="formData.tsUnit"
                      style="width: 60px"
                      extCls="select-custom"
                      extPopoverCls="select-popover-custom"
                      :searchable="false"
                      :clearable="false">
                      <bkdata-option v-for="(key, value) in tsDependConfig.tsDependUnitOptions"
                        :id="value"
                        :key="value"
                        :name="key" />
                    </bkdata-select>
                  </template>
                </bkdata-input>
              </bkdata-form-item>
              <bkdata-form-item :label="$t('数据频率')"
                :required="true"
                :property="'ts_freq'"
                class="ts_freq">
                <bkdata-input v-model="formData.ts_freq"
                  :disabled="mode === 'copy' && copySensitivity !== 'public'"
                  placeholder="int (>=0)">
                  <template slot="append">
                    <div class="group-text">
                      {{ $t('秒') }}
                    </div>
                  </template>
                </bkdata-input>
              </bkdata-form-item>
              <bkdata-form-item :label="$t('是否公开')"
                :required="true"
                :property="'sensitivity'">
                <bkdata-switcher v-model="formData.sensitivity"
                  :onText="$t('是')"
                  :showText="true"
                  :offText="$t('否')"
                  :disabled="true" />
                <span class="ml5">{{ formData.sensitivity ? $t('可在所有项目中使用') : $t('仅可在本项目中使用') }}</span>
              </bkdata-form-item>
              <div class="desc-title">
                {{ $t('运行配置') }}
              </div>
              <bkdata-form-item :label="$t('计算集群')"
                :required="true"
                :property="'processing_cluster_id'">
                <bkdata-select v-model="formData.processing_cluster_id"
                  :disabled="!formData.project_id"
                  :placeholder="$t('请选择')"
                  searchable>
                  <bkdata-option-group v-for="(group, index) in processingClusters"
                    :key="index"
                    :name="group.cluster_group_alias">
                    <bkdata-option v-for="option in group.clusters"
                      :id="option.cluster_id"
                      :key="option.cluster_id"
                      :name="option.description" />
                  </bkdata-option-group>
                </bkdata-select>
              </bkdata-form-item>
              <bkdata-form-item :label="$t('存储集群')"
                :required="true"
                :property="'storage_cluster_id'">
                <bkdata-select v-model="formData.storage_cluster_id"
                  :disabled="!formData.project_id"
                  :placeholder="$t('请选择')"
                  searchable>
                  <bkdata-option-group v-for="(group, index) in storageClusters"
                    :key="index"
                    :name="group.cluster_group_alias">
                    <bkdata-option v-for="option in group.clusters"
                      :id="option.cluster_id"
                      :key="option.cluster_id"
                      :name="option.description" />
                  </bkdata-option-group>
                </bkdata-select>
              </bkdata-form-item>
            </bkdata-form>
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>
<script>
import Cookies from 'js-cookie';
const curLang = Cookies.get('blueking_language') || 'zh-cn';

export default {
  props: {
    mode: {
      type: String,
      default: 'new',
    },
  },
  data() {
    return {
      curLang: curLang,
      typeList: [],
      copySensitivity: '',
      /** 存储集群组 */
      storageClusters: [],
      tsFreqOptions: [30, 60, 180, 300, 600],
      /** 计算集群组 */
      processingClusters: [],
      formData: {
        project_id: '',
        sample_set_name: '',
        description: '',
        scene_name: '',
        sample_type: '',
        ts_freq: '',
        processing_cluster_id: '',
        storage_cluster_id: '',
        sensitivity: false,
        tsDepend: 0,
        tsUnit: 'd'
      },
      rules: {
        storage_cluster_id: [
          {
            required: true,
            message: $t('请选择存储集群'),
            trigger: 'blur',
          },
        ],
        processing_cluster_id: [
          {
            required: true,
            message: $t('请选择计算集群'),
            trigger: 'blur',
          },
        ],
        ts_freq: [
          {
            required: true,
            message: this.$t('必填项'),
            trigger: 'blur',
          },
          {
            validator: this.inputValidator,
            message: $t('仅需添加实时数据作为样本数据，请设为 0、30、60、180、300、600秒；需添加离线数据作为样本数据，请设为3600秒的整数倍'),
            trigger: 'blur',
          },
        ],
        tsDepend: [
          {
            required: true,
            message: this.$t('必填项'),
            trigger: 'blur',
          },
          {
            regex: /^[0-9]\d*$/,
            message: $t('请输入正整数'),
            trigger: 'blur',
          },
        ],
        sample_type: [
          {
            required: true,
            message: this.$t('必填项'),
            trigger: 'blur',
          },
        ],
        project_id: [
          {
            required: true,
            message: this.$t('必填项'),
            trigger: 'blur',
          },
        ],
        sample_set_name: [
          {
            required: true,
            message: this.$t('必填项'),
            trigger: 'blur',
          },
          {
            validator: function (val) {
              return val.length > 2 && val.length < 51;
            },
            message: $t('字符长度只能为3-50'),
            trigger: 'blur',
          },
        ],
        description: [
          {
            required: true,
            message: this.$t('必填项'),
            trigger: 'blur',
          },
          {
            max: 50,
            message: $t('不能多于50个字符'),
            trigger: 'blur',
          },
        ],
        scene_name: [
          {
            required: true,
            message: this.$t('必填项'),
            trigger: 'blur',
          },
        ],
      },
      isLoading: false,
      isDiagLoading: false,
      newTaskDialog: {
        hasHeader: false,
        hasFooter: false,
        width: 616,
        isShow: false,
      },
      copyedSampleSetName: '',
      isSceneLoading: false,
      sceneList: [],
      tsDependConfig: {
        tsDepend: '',
        tsDependUnit: 'd',
        tsDependUnitOptions: {
          d: $t('天'),
          h: $t('时'),
          m: $t('分'),
          s: $t('秒'),
        },
      }
    };
  },
  computed: {
    projectList() {
      return this.$attrs.projectList.map(item => {
        item = JSON.parse(JSON.stringify(item));
        item.project_name = `[${item.project_id}]${item.project_name}`;
        return item;
      });
    },
  },
  mounted() {
    this.formData.project_id = this.$route.query.project_id || '';
    this.getType();
    this.formData.storage_cluster_id = this.formData.processing_cluster_id = '';
    this.getSceneList();
  },
  methods: {
    inputValidator(val) {
      if (val !== '' && val !== null && Number(val) === 0) {
        return true;
      }
      const reg = /^[1-9]\d*$/;
      return reg.test(val) && (this.tsFreqOptions.includes(Number(val)) || val % 3600 === 0);
    },
    handleProjectChange() {
      setTimeout(() => {
        this.getClusters();
      });
    },
    async getType() {
      const result = await this.axios.get('/v3/aiops/data/type/');
      this.typeList = result.data;
    },
    /** 获取计算与存储集群列表 */
    async getClusters() {
      const { project_id } = this.formData;
      const result = await Promise.all([
        this.bkRequest.httpRequest('customModel/getStorageClusters', { query: { project_id } }),
        this.bkRequest.httpRequest('customModel/getProcessingClusters', { query: { project_id } })
      ]);
      ['storageClusters', 'processingClusters'].forEach((key, index) => {
        this[key] = result[index].data || [];
        this[key] = this[key].filter(item => item.clusters && item.clusters.length > 0);
        console.log(this[key]);
      });
    },
    getSceneList() {
      this.isSceneLoading = true;
      this.bkRequest
        .httpRequest('sampleManage/getSceneList', {
          query: {
            project_id: this.formData.project_id,
            scene_type: 'normal',
            active: true,
          },
        })
        .then(res => {
          if (res.result) {
            this.sceneList = res.data.scene_list;
            this.sceneList.forEach(scene => {
              scene.disabled = scene.status === 'developing';
              scene.scene_alias += scene.disabled ? `（${$t('暂不支持')}）` : '';
            });
            this.sceneList = this.sceneList.sort((a, b) => Number(a.disabled) - Number(b.disabled));
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.isSceneLoading = false;
        });
    },
    getContent(mode, field) {
      const sampleTitle = {
        new: {
          title: this.$t('创建样本集'),
          okText: this.$t('创建'),
        },
        copy: {
          title: this.$t('复制样本集'),
          okText: this.$t('复制'),
        },
      };
      return sampleTitle[mode][field];
    },
    validate1() {
      this.isDiagLoading = true;
      return this.$refs.validateForm1.validate().then(
        validator => {
          return validator;
        },
        validator => {
          this.isDiagLoading = false;
          return validator;
        }
      );
    },
    open(data) {
      // 复制样本集时，需要回填数据
      if (data.id) {
        const { project_id,
          sample_set_name,
          description, scene_name,
          sensitivity,
          id,
          ts_freq,
          sample_type,
          processing_cluster_id = '',
          storage_cluster_id = ''
        } = data;
        // console.log(data, '....');
        this.copyedSampleSetName = sample_set_name;
        this.copySensitivity = sensitivity;
        this.formData = {
          sample_set_id: id,
          project_id,
          ts_freq,
          sample_type,
          sample_set_name: `${sample_set_name}_副本`,
          processing_cluster_id,
          storage_cluster_id,
          description,
          scene_name,
          sensitivity: false, // sensitivity === 'public'
        };
      }
      // this.formData.project_id = Number(this.$route.query.project_id || null)
      this.newTaskDialog.isShow = true;
    },
    close() {
      this.resetState();
      this.newTaskDialog.isShow = false;
    },
    resetState() {
      this.$refs.validateForm1.clearError();
      this.formData = {
        project_id: '',
        sample_set_name: '',
        description: '',
        scene_name: '',
        sensitivity: false,
      };
    },
    async createNewSample() {
      const result = await this.validate1();
      // 表单校验通过，会返回布尔值true，否则，返回校验出错的对象
      if (typeof result !== 'boolean') return;
      const sampleUrl = {
        new: 'sampleManage/creatSampleSet',
        copy: 'sampleManage/copySampleSet',
      };
      this.isDiagLoading = true;
      this.isLoading = true;
      const {
        project_id,
        sample_set_name,
        description,
        scene_name,
        sensitivity,
        sample_set_id,
        sample_type,
        ts_freq,
        tsDepend,
        tsUnit,
        processing_cluster_id,
        storage_cluster_id
      } = this.formData;
      let params = {
        project_id,
        sample_set_name,
        processing_cluster_id: processing_cluster_id || null,
        storage_cluster_id: storage_cluster_id || null,
        sample_type: sample_type || null,
        ts_freq,
        description,
        scene_name,
        ts_depend: tsDepend + tsUnit,
        sensitivity: 'private', // sensitivity ? 'public' : 'private'
      };
      if (this.mode === 'copy') {
        params.sample_set_id = sample_set_id;
      }
      this.bkRequest
        .httpRequest(sampleUrl[this.mode], {
          params,
        })
        .then(res => {
          if (res.result) {
            this.$bkMessage({
              message: this.mode === 'new' ? $t('创建样本集成功！') : $t('复制样本集成功！'),
              theme: 'success',
            });
            this.$emit('refresh');
            this.close();
            this.$router.push({
              name: 'sampleData',
              params: {
                sampleSetId: res.data.id,
              },
              query: {
                project_id: res.data.project_id,
              },
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.isDiagLoading = false;
        });
    },
  },
};
</script>
<style lang="scss" scoped>
.select-custom {
    border: none;
}
::v-deep .new-sample {
  .bk-form-control .group-box {
    top: 1px;
  }
  ::v-deep .ts_freq {
    ::v-deep .bk-icon {
      right: -20px !important;
    }
  }
  ::v-deep .group-box {
    top: 1px;
  }
  .desc-title {
    position: relative;
    width: 520px;
    height: 28px;
    background: #f5f6fa;
    padding-left: 10px;
    font-size: 14px;
    margin: 20px 0px;
    font-weight: 700;
    text-align: left;
    color: #63656e;
    line-height: 28px;
    &:first-child {
      margin-top: 0px;
    }
    &::after {
      content: '';
      width: 3px;
      height: 14px;
      background: #3a84ff;
      position: absolute;
      left: 0;
      top: 7px;
    }
  }
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
    padding: 30px 48px 45px;
    > .bk-form {
      // margin-left: 30px;
      .copy-label-title {
        font-size: 16px;
        font-weight: normal;
        display: flex;
        align-items: center;
        .sample-name {
          display: inline-block;
          font-weight: bold;
          margin: 0 5px;
          max-width: 100px;
        }
      }
    }
    .form-container {
      width: 600px;
      // margin-left: 30px;
    }
  }
}
::v-deep .en-new-sample {
  .bk-label-text {
    font-weight: 400;
    font-size: 12px;
    line-height: 16px;
  }
  .tooltips-icon {
    right: -20px !important;
  }
  .form-container {
    // width: 600px !important;
  }
}

</style>
