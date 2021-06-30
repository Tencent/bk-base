

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
  <div class="sliderWrapper">
    <bkdata-sideslider :isShow.sync="isShow"
      class="udfSlider"
      :title="'自定义函数开发'"
      :width="946">
      <div slot="content"
        class="wrapper"
        style="margin-left: 31px">
        <div class="processBar">
          <bkdata-process
            :list="processList"
            style="width: 883px"
            :curProcess.sync="curProcess"
            :displayKey="'content'"
            :controllable="false" />
        </div>
        <keep-alive>
          <component
            :is="currentView"
            ref="udfComponent"
            :sliderShow="isShow"
            @dubugFinish="changeDebugStatus"
            @sqlChecked="changeSqlCheckingStatus" />
        </keep-alive>
        <div class="btnGroup">
          <bkdata-button
            v-if="curProcess !== 1"
            :theme="'default'"
            type="submit"
            style="width: 110px"
            class="mr10"
            @click="lastStep">
            {{ $t('上一步') }}
          </bkdata-button>
          <bkdata-button
            v-if="curProcess !== 3"
            :theme="'primary'"
            :disabled="nextDisable"
            :style="nextBtnWidth"
            :loading="btnLoading"
            type="submit"
            class="mr10"
            @click="nextStep">
            {{ $t('下一步') }}
          </bkdata-button>
          <bkdata-button
            v-if="curProcess === 3"
            :theme="'primary'"
            type="submit"
            style="width: 120px"
            class="mr10"
            :loading="isPublishLoading"
            @click="publishHandle">
            {{ $t('函数发布') }}
          </bkdata-button>
          <bkdata-button
            v-if="curProcess === 1"
            :theme="'default'"
            type="submit"
            style="width: 110px"
            class="mr10"
            :loading="isLoading"
            @click="handScraftHandle">
            {{ $t('草稿') }}
          </bkdata-button>
          <bkdata-button
            v-if="curProcess === 1"
            :theme="'default'"
            style="width: 86px"
            type="submit"
            class="mr10"
            @click="isShow = false">
            {{ $t('取消') }}
          </bkdata-button>
          <div class="publishStatus">
            <debuger-status v-if="curProcess === 3"
              :isDebugStatus="false" />
          </div>
        </div>
      </div>
    </bkdata-sideslider>
  </div>
</template>

<script>
import debuger from './pages/debuger';
import debugerStatus from './components/debugStatusBlock';
import development from './pages/development';
import fnPublish from './pages/fnPublish';
import Bus from '@/common/js/bus.js';
import { mapGetters } from 'vuex';
import { showMsg, postMethodWarning } from '@/common/js/util.js';
export default {
  components: { debuger, development, debugerStatus },
  data() {
    return {
      btnLoading: false,
      isPublishLoading: false,
      debugSuccess: false,
      isSqlchecking: false,
      isLoading: false,
      isShow: false,
      processList: [{ content: this.$t('开发') }, { content: this.$t('调试') }, { content: this.$t('发布') }],
      curProcess: 1,
      stepStore: [development, debuger, fnPublish],
    };
  },
  computed: {
    ...mapGetters({
      initStatus: 'udf/initStatus',
      devlopParams: 'udf/devlopParams',
    }),
    nextDisable() {
      return (this.curProcess === 2 && !this.debugSuccess) || (this.curProcess === 1 && this.isSqlchecking);
    },
    currentView() {
      let step = this.curProcess - 1;
      return this.stepStore[step];
    },
    nextBtnWidth() {
      return this.curProcess === 1 ? 'width: 130px;' : 'width: 120px';
    },
  },
  watch: {
    isShow(value) {
      if (!value) {
        this.$nextTick(() => {
          this.$emit('udfSliderClose');
        });
        // this.$emit('udfSliderClose')
        this.unlock();
      }
    },
  },

  methods: {
    paramsToSend() {
      let self = this.$refs.udfComponent;
      let inputs = self.params.input_type.map(item => item.content);
      let returns = self.params.return_type.map(item => item.content);
      return Object.assign(JSON.parse(JSON.stringify(self.params)), { input_type: inputs }, { return_type: returns });
    },
    changeSqlCheckingStatus(status) {
      this.isSqlchecking = status;
    },
    changeDebugStatus(status) {
      this.debugSuccess = status;
    },
    unlock() {
      if (this.devlopParams.func_name) {
        this.bkRequest
          .httpRequest('udf/unloackUDF', {
            params: {
              function_name: this.devlopParams.func_name,
            },
          })
          ['catch'](err => {
            postMessage(err, 'error');
          });
      }
    },
    handScraftHandle() {
      let self = this.$refs.udfComponent;
      let params = this.paramsToSend();
      if (self.validateForm()) {
        this.isLoading = true;
        if (this.initStatus) {
          self.bkRequest.httpRequest('udf/updateUDF', { params }).then(res => {
            if (res.result) {
              self.$store.commit('udf/saveDevParams', self.params);
              postMethodWarning(this.$t('保存草稿成功'), 'success');
            } else {
              postMethodWarning(res.message, 'error');
            }
            this.isLoading = false;
          });
        } else {
          self.bkRequest.httpRequest('udf/createUDF', { params }).then(res => {
            if (res.result) {
              self.$store.commit('udf/saveDevParams', self.params);
              self.$store.commit('udf/changeInitStatus', true);
              postMethodWarning(this.$t('保存草稿成功'), 'success');
            } else {
              postMethodWarning(res.message, 'error');
            }
            this.isLoading = false;
          });
        }
      }
      Bus.$emit('handScraftClick');
    },
    publishHandle() {
      this.isPublishLoading = true;
      this.bkRequest
        .httpRequest('udf/publishUDF', {
          params: {
            function_name: this.devlopParams.func_name,
            release_log: this.$refs.udfComponent.publishLog,
          },
        })
        .then(res => {
          if (res.result) {
            this.isPublishLoading = false;
            Bus.$emit('getUDFList'); // 更新我的函数列表
            postMethodWarning(this.$t('函数发布成功'), 'success');
            this.isShow = false; // 关闭侧边栏
          } else {
            postMethodWarning(res.message, 'error');
          }
        })
        ['finally'](() => {
          this.isPublishLoading = false;
        });
    },
    nextStep() {
      if (this.curProcess === 1) {
        // 开发页面
        let self = this.$refs.udfComponent;
        let params = this.paramsToSend();
        self.validateForm().then(isValidate => {
          if (isValidate) {
            if (this.initStatus) {
              this.btnLoading = true;
              this.bkRequest.httpRequest('udf/updateUDF', { params }).then(res => {
                if (res.result) {
                  this.curProcess++;
                  this.$store.commit('udf/saveDevParams', this.$refs.udfComponent.params);
                } else {
                  postMethodWarning(res.message, 'error');
                }
                this.btnLoading = false;
              });
            } else {
              this.btnLoading = true;
              this.bkRequest.httpRequest('udf/createUDF', { params }).then(res => {
                if (res.result) {
                  this.curProcess++;
                  this.$store.commit('udf/saveDevParams', this.$refs.udfComponent.params);
                  this.$store.commit('udf/changeInitStatus', true);
                } else {
                  postMethodWarning(res.message, 'error');
                }
                this.btnLoading = false;
              });
            }
          }
          return;
        });
      } else if (this.curProcess === 2) {
        // 调试页面
        this.curProcess++;
      }
    },
    lastStep() {
      this.curProcess--;
    },
  },
};
</script>

<style lang="scss" scoped>
.sliderWrapper {
  .udfSlider {
    top: 60px;
    ::v-deep .bk-sideslider-content {
      max-height: none !important;
    }
  }
  .processBar {
    width: 883px;
    margin: 18px 0 15px 0;
  }
  .btnGroup {
    display: flex;
    justify-content: flex-start;
    .publishStatus {
      width: 633px;
      height: 31px;
    }
  }
}
</style>
