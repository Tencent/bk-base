

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
  <div class="explore-notebook-layout">
    <div class="notebook-toolbar-hidden">
      <div class="blank-placeholder" />
      <PopConfirm ref="versionPop"
        placement="bottom-start"
        :width="352"
        :confirmLoading="versionInfo.loading"
        :confirmFn="handleConfirmSave"
        :cancelFn="handleClearParams">
        <div slot="content"
          class="save-notebook-confirm">
          <span class="title">{{ $t('版本保存') }}</span>
          <div class="bk-form save-info">
            <div class="bk-form-item">
              <label class="bk-label pr20">{{ $t('保存至') }}</label>
              <div class="bk-form-content">
                <bkdata-radio-group v-model="storageMethod"
                  class="radio-group">
                  <bkdata-radio value="saveCur">
                    {{ $t('当前版本') }}
                  </bkdata-radio>
                  <bkdata-radio value="saveNew">
                    {{ $t('新版本') }}
                  </bkdata-radio>
                </bkdata-radio-group>
              </div>
            </div>
            <template v-if="storageMethod === 'saveNew'">
              <div class="bk-form-item is-required">
                <label class="bk-label pr20">{{ $t('版本号') }}</label>
                <div class="bk-form-content">
                  <bkdata-input v-model.trim="versionInfo.name"
                    :class="{ 'error-status': versionNameVerify.isError }" />
                </div>
                <div v-if="versionNameVerify.isError"
                  class="bk-error-tips">
                  {{ versionNameVerify.tipsText }}
                </div>
              </div>
              <div class="bk-form-item">
                <label class="bk-label pr20">{{ $t('描述') }}</label>
                <div class="bk-form-content">
                  <bkdata-input v-model="versionInfo.desc"
                    :type="'textarea'"
                    :maxlength="100" />
                </div>
              </div>
            </template>
          </div>
        </div>
        <span ref="triggerSavePop"
          class="btn-placeholder" />
      </PopConfirm>
    </div>

    <notebook-preview v-if="occupied" />
    <iframe v-else
      :id="'jupyter-notebook' + activeNotebook.notebook_id"
      width="100%"
      height="100%"
      style="border: none"
      :src="activeNotebook.notebook_url" />

    <!-- 当前编辑提示框 -->
    <div v-if="occupied && editor"
      v-bkloading="{ isLoading: textLoading, zIndex: 10 }"
      class="notify-tips">
      <i class="icon-close-line-2"
        @click="closePromptbox" />
      <i class="icon-exclamation-circle-shape-delete" />
      <p class="tips-title">
        {{ $t('暂无笔记编辑权') }}
      </p>
      <p v-if="notebookCode === '1580201'"
        class="tips-text">
        {{ $t('当前笔记正在被') }}
        <span class="userColor">{{ occupier }}</span>
        {{ $t('编辑中_您暂时只能浏览笔记内容无法编辑_如需获取笔记编辑权_请点击') }}
        <span class="textColor"
          @click="getEditPermission">
          {{ $t('获取编辑权') }}
        </span>
      </p>
      <p v-if="notebookCode === '1580200'"
        class="tips-text">
        {{ occupier }}
      </p>
      <p v-if="notebookCode === '1580202'"
        class="tips-text">
        {{ occupier }}
      </p>
    </div>

    <bkdata-sideslider :isShow.sync="versionSlider.isShow"
      :quickClose="true"
      :width="800"
      :transfer="true"
      :title="$t('版本管理')"
      extCls="version-side">
      <template slot="content">
        <div class="notebook-version-main">
          <bkdata-input v-model="versionSlider.search"
            class="version-search"
            rightIcon="bk-icon icon-search"
            :clearable="true"
            :placeholder="$t('搜索版本号_最后修改人_版本描述')" />
          <bkdata-table class="version-table mt20"
            :data="versionSlider.data"
            :height="appHeight - 168"
            :outerBorder="false"
            :headerBorder="false"
            :headerCellStyle="{ 'background-color': '#F5F6FA' }"
            :rowStyle="setRowStyle">
            <bkdata-table-column :label="$t('版本号')"
              prop="version"
              :width="260"
              :formatter="hanldeformatterCell" />
            <bkdata-table-column :label="$t('更新时间')"
              prop="commit_time" />
            <bkdata-table-column :label="$t('最后修改人')"
              prop="author" />
            <bkdata-table-column label="操作"
              width="160">
              <template slot-scope="props">
                <bkdata-button class="mr10"
                  theme="primary"
                  text
                  :disabled="props.row.is_default"
                  @click="handleShowDiff(props.row)">
                  {{ $t('差异对比') }}
                </bkdata-button>
                <PopConfirm placement="bottom-end"
                  :transfer="true"
                  :width="280"
                  :isEmit="true"
                  @confirm="handleDelVersion(props.row)">
                  <div slot="content"
                    class="custom-pop-info">
                    <span class="title">{{ $t('确认删除该版本') }}</span>
                    <span class="mt10">{{ $t('删除版本') }}{{ props.row.version }}</span>
                    <span class="mt5">{{ $t('删除操作无法撤回_请谨慎操作') }}</span>
                  </div>
                  <bkdata-button theme="primary"
                    text
                    :disabled="props.row.is_default">
                    {{ $t('删除') }}
                  </bkdata-button>
                </PopConfirm>
              </template>
            </bkdata-table-column>
          </bkdata-table>
        </div>
      </template>
    </bkdata-sideslider>

    <!-- /** 差异对比、版本弹框 */ -->
    <bkdata-dialog v-model="diffDialog.isShow"
      extCls="notebook-diff-dialog"
      renderDirective="if"
      width="80%"
      :closeIcon="false">
      <div slot="header"
        class="notebook-diff-dialog-header">
        <div class="title">
          <i class="icon-diff title-icon" />
          <span>{{ $t('差异对比') }}</span>
        </div>
        <div class="tips">
          <span class="desc">{{ $t('查看当前版本与回退版本的差异_确认是否回滚') }}</span>
          <span class="help"
            @click="handleHelp">
            <i class="icon-help-fill" />
            {{ $t('帮助文档') }}
          </span>
        </div>
        <i class="bk-icon icon-close"
          @click="handleCloseDiff" />
      </div>
      <notebook-diff :data-model="diffDialog.dataModel"
        :diffVersion="diffDialog.diffVersion"
        :curTime="diffDialog.curTime"
        :loading="diffDialog.loading"
        :isError="diffDialog.isError" />
      <div slot="footer"
        class="notebook-diff-dialog-btns">
        <span class="tips pr20">{{ $t('回滚默认会创建当前版本的快照') }}</span>
        <template v-if="!diffDialog.isError && !diffDialog.retreating">
          <PopConfirm placement="bottom-end"
            :transfer="true"
            :width="280"
            :isEmit="true"
            @confirm="handleRollBack">
            <div slot="content"
              class="custom-pop-info">
              <span class="title">{{ $t('确认回滚至该版本') }}</span>
              <span class="mt10">{{ $t('回滚版本') }}{{ diffDialog.diffVersion.name }}</span>
              <span class="mt5">{{ $t('回滚默认会创建当前版本的快照') }}</span>
            </div>
            <bkdata-button class="mr5"
              theme="primary">
              {{ $t('回滚') }}
            </bkdata-button>
          </PopConfirm>
        </template>
        <bkdata-button v-else
          class="mr5"
          theme="primary"
          :disabled="true"
          :loading="diffDialog.retreating">
          {{ $t('回滚') }}
        </bkdata-button>
        <bkdata-button theme="default"
          @click="handleCloseDiff">
          {{ $t('取消') }}
        </bkdata-button>
      </div>
    </bkdata-dialog>

    <!-- 重新连接服务器弹框 -->
    <bkdata-dialog v-model="reconnectionTipShow"
      theme="primary"
      :maskClose="false"
      :escClose="false"
      :closeIcon="false"
      :showFooter="false"
      :title="$t('笔记连接失败')">
      <p style="text-align: center">
        {{ $t('网络波动或服务端资源文件更新_与后台服务连接已断开') }}
      </p>
      <bkdata-button theme="primary"
        style="display: block; margin: 20px auto 0 auto"
        :loading="connecting"
        @click="reconnectNotebook">
        {{ $t('重连') }}
      </bkdata-button>
    </bkdata-dialog>

    <!-- MLSQL发布弹框 -->
    <PublishDialog ref="publishDialog" />

    <!-- 文件上传 -->
    <uploadFile ref="upload"
      :notebook="activeNotebook" />

    <!-- 生成报告 -->
    <bkdata-dialog v-model="report.primary.visible"
      theme="primary"
      :maskClose="false"
      :headerPosition="report.primary.headerPosition"
      :showFooter="false"
      title="生成报告"
      width="528">
      <bkdata-alert type="info"
        title="在项目中生成报告，可以将URL分享给项目成员；个人生成的报告只能自己访问。" />
      <bkdata-checkbox-group v-model="checkboxVal">
        <bkdata-checkbox :value="'addWatermark'">
          背景添加水印
        </bkdata-checkbox>
        <bkdata-checkbox :value="'hideCode'">
          隐藏 Cell 代码框
        </bkdata-checkbox>
        <bkdata-checkbox :value="'hideNum'">
          图表坐标模糊化
        </bkdata-checkbox>
      </bkdata-checkbox-group>
      <div v-show="flag === true"
        class="btn-box">
        <bkdata-button :theme="'primary'"
          :title="'生成报告'"
          class="mr10"
          @click="createdReport(checkboxVal)">
          生成报告
        </bkdata-button>
      </div>
      <div v-show="flag !== true"
        class="btn-box2">
        <bkdata-button :loading="newLoading"
          :theme="btnStyle"
          :title="'重新生成'"
          class="mr10"
          @click="createdReportAgain(checkboxVal)">
          重新生成
        </bkdata-button>
        <div class="copy-content">
          <div v-bk-tooltips.top="$t('复制')"
            class="copy-url"
            @click="copyUrl(reportVal)">
            <span>{{ reportVal }}</span>
            <i class="icon-data-copy" />
          </div>
          <div v-bk-tooltips.top="$t('新页面打开报告')"
            class="new-report">
            <i class="icon-jump-2"
              @click="toNewReport" />
          </div>
        </div>
      </div>
    </bkdata-dialog>
  </div>
</template>

<script>
import PopConfirm from './popConfirm.vue';
import NotebookDiff from './notebookDiff.vue';
import uploadFile from '@/pages/DataExplore/slider/FileUploadSlider';
import PublishDialog from './MLSQLPublishDialog';
import Bus from '@/common/js/bus.js';
import { clipboardCopy, showMsg } from '@/common/js/util.js';
import notebookPreview from '@/pages/DataExplore/notebookPreview.vue';
export default {
  components: {
    PopConfirm,
    NotebookDiff,
    PublishDialog,
    uploadFile,
    notebookPreview,
  },
  props: {
    activeNotebook: {
      type: Object,
      default: () => {},
    },
    isCommon: {
      type: Boolean,
      default: true,
    },
  },
  data() {
    return {
      connecting: false,
      reconnectionTipShow: false,
      versionNameVerify: {
        isError: false,
        tipsText: $t('必填项不可为空'),
      },
      appHeight: window.innerHeight,
      storageMethod: 'saveCur',
      versionInfo: {
        name: '',
        desc: '',
        loading: false,
      },
      versionSlider: {
        isShow: false,
        search: '',
        data: [],
        allData: [],
        delLoading: false,
      },
      diffDialog: {
        isShow: false,
        loading: true,
        isError: false,
        dataModel: {
          original: '',
          modified: '',
        },
        diffVersion: {
          name: '',
          time: '',
        },
        curTime: '',
        commitId: '',
        retreating: false,
      },
      occupyTimer: null,
      occupied: false,
      occupier: '',
      editor: true,
      textLoading: false,
      timer: null,
      report: {
        primary: {
          visible: false,
          headerPosition: 'left',
        },
      },
      checkboxVal: ['addWatermark', 'hideCode', 'hideNum'],
      notebookId: null,
      projectId: null,
      curType: '',
      reportVal: '',
      newLoading: false,
      flag: true,
      reportSecret: '', //转码后的密钥
      escapeReport: '', //未转码的密钥
      btnStyle: 'default',
      notebooks: [],
      notebookCode: '',
    };
  },
  watch: {
    'versionInfo.name'(val) {
      if (val) {
        this.versionNameVerify.isError = false;
      }
    },
    'versionSlider.search'(val, old) {
      if (val && val !== old) {
        this.versionSlider.data = this.versionSlider.allData.filter(item => {
          const includeVersion = item.version ? item.version.indexOf(val) : -1;
          const includeAuthor = item.author ? item.author.indexOf(val) : -1;
          const includeMessage = item.commit_message ? item.commit_message.indexOf(val) : -1;
          return includeVersion + includeAuthor + includeMessage > -3;
        });
      } else if (!val) {
        this.versionSlider.data = this.versionSlider.allData;
      }
    },
    storageMethod() {
      this.versionNameVerify.isError = false;
    },
    activeNotebook: {
      immediate: true,
      deep: true,
      handler(notebook) {
        this.occupyTimer && clearInterval(this.occupyTimer);
        this.occupied = false;
        if (notebook.notebook_id && this.isCommon) {
          this.handleGetOccupyStatus();
          this.handleOccupyPolling();
        } else if (notebook.notebook_id && !this.isCommon) {
          this.handleGetOccupyStatus();
        }
      },
    },
    checkboxVal(newVal, oldVal) {
      if (newVal !== oldVal) {
        this.btnStyle = 'primary';
      }
    },
  },
  mounted() {
    const mask = document.querySelector('.notebook-mask');
    window.addEventListener('message', this.handleReceiveMessage);
    window.addEventListener('resize', this.handleResetHeight);
    Bus.$on('header-nav-tippy-show', () => {
      mask.style.display = 'block';
    });
    Bus.$on('header-nav-tippy-close', () => {
      mask.style.display = 'none';
    });
    Bus.$on('reload-notebook', this.handleReloadNotebook);

    Bus.$on('notebook-upload-show', () => {
      this.$refs.upload.show = true;
    });
  },
  beforeDestroy() {
    this.occupyTimer && clearInterval(this.occupyTimer);
    window.removeEventListener('message', this.handleReceiveMessage);
    window.removeEventListener('resize', this.handleResetHeight);
    Bus.$off('header-nav-tippy-close');
    Bus.$off('header-nav-tippy-show');
    Bus.$off('reload-notebook');
    Bus.$off('notebook-upload-show');
  },
  methods: {
    reconnectNotebook() {
      this.connecting = true;
      this.bkRequest
        .httpRequest('dataExplore/startNotebook', {
          params: {
            notebook_id: this.activeNotebook.notebook_id,
          },
        })
        .then(res => {
          if (res.result && res.data) {
            this.reconnectionTipShow = false;
            document.getElementById('jupyter-notebook' + this.activeNotebook.notebook_id).src += ''; // 重载iframe
            this.$bkMessage({
              message: this.$t('笔记重连成功'),
              theme: 'success',
            });
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.connecting = false;
        });
    },
    handleResetHeight() {
      this.appHeight = window.innerHeight;
    },
    handleReceiveMessage(event) {
      const data = event.data;
      const type = data.eventType;
      const isShow = data.isShow;

      if (type === 'getUsername') {
        document.getElementById('jupyter-notebook' + this.activeNotebook.notebook_id).contentWindow.postMessage(
          {
            eventType: 'getUsername',
            username: this.$store.getters.getUserName,
          },
          '*'
        );
      } else if (type === 'readyToRecieveMsg') {
        this.handleSendBkUser();
      }
      if (data.notebook_id !== this.activeNotebook.notebook_id) return;

      /** 当事件抛出为当前笔记时，做相应的响应和处理 */
      switch (type) {
        case 'saveNoteConfirm':
          isShow && this.$refs.triggerSavePop.click();
          break;
        case 'hiddenSaveNotePop':
          data.hidden && this.handleCancel();
          break;
        case 'noteVersionManagement':
          const res = data.res;
          if (res.result) {
            const list = res.data || [];
            this.versionSlider.data = list;
            this.versionSlider.allData = list;
          } else {
            this.versionSlider.data = [];
            this.versionSlider.allData = [];
            this.$bkMessage({
              theme: 'error',
              message: res.message,
            });
          }
          isShow && (this.versionSlider.isShow = true);
          break;
        case 'deleteNoteVersion':
          if (data.res.result) {
            this.$bkMessage({
              theme: 'success',
              message: this.$t('删除成功'),
            });
            // data为搜索展示数据  allData为原始数据
            const delIndex = this.versionSlider.data.findIndex(item => item.commit_id === data.commitId);
            delIndex > -1 && this.versionSlider.data.splice(delIndex, 1);
            const delIndexInAll = this.versionSlider.allData.findIndex(item => item.commit_id === data.commitId);
            delIndexInAll > -1 && this.versionSlider.allData.splice(delIndexInAll, 1);
          } else {
            this.$bkMessage({
              theme: 'error',
              message: data.res.message,
            });
          }
          break;
        case 'saveNoteStatus':
          if (data.res.result) {
            this.$bkMessage({
              theme: 'success',
              message: this.$t('保存成功'),
            });
            this.handleCancel();
          } else {
            this.$bkMessage({
              theme: 'error',
              message: data.res.message,
            });
            this.versionInfo.loading = false;
            // 权限问题直接关闭pop
            const authCodes = ['1580015', '1580016', '1580017'];
            authCodes.includes(data.res.code) && this.handleCancel();
          }
          break;
        case 'diffNoteContents':
          if (data.res.result) {
            this.diffDialog.dataModel.original = data.res.data.file_content;
            this.diffDialog.dataModel.modified = data.res.data.diff_file_content;
            this.diffDialog.curTime = data.lastDate;
          } else {
            this.$bkMessage({
              theme: 'error',
              message: data.res.message,
            });
          }
          this.diffDialog.loading = false;
          this.diffDialog.isError = !data.res.result;
          break;
        case 'rollBackVersion':
          if (data.res.result) {
            this.$bkMessage({
              theme: 'success',
              message: this.$t('回滚成功'),
            });
            this.handleCloseDiff();
            this.handleReloadNotebook();
            this.versionSlider.isShow = false;
          } else {
            this.$bkMessage({
              theme: 'error',
              message: data.res.message,
            });
            this.diffDialog.retreating = false;
          }
          break;
        case 'connectionFailed':
          this.reconnectionTipShow = true;
          break;
        case 'releaseMLSQL':
          this.$refs.publishDialog.initDialog(data.cellContent, data.output);
          break;
        case 'bk_perfume':
          console.log(data.data.chartConfig, data.data.changedValue);
          const perfumeData = data.data;
          window.$bk_perfume.start('Chart_Changed_Action', { data: perfumeData.chartConfig });
          window.$bk_perfume.end('Chart_Changed_Action', { data: perfumeData.changedValue });
          break;
        case 'uploadFileShow':
          this.$refs.upload.show = true;
          break;
        case 'notebookReport':
          this.curType = this.$route.query.type;
          this.notebookId = this.$route.query.notebook;
          this.projectId = this.$route.query.projectId;
          this.getCurrentNotebook();
      }
    },
    handleSendJupyterMessage(params = {}) {
      const jupyter = document.getElementById('jupyter-notebook' + this.activeNotebook.notebook_id);
      jupyter.contentWindow.postMessage(params, '*');
    },
    handleSendBkUser() {
      this.handleSendJupyterMessage({
        eventType: 'postBkUserInfo',
        bkInfo: {
          username: this.$store.getters.getUserName,
          notebook_id: this.activeNotebook.notebook_id,
        },
      });
    },
    handleConfirmSave() {
      const saveNew = this.storageMethod === 'saveNew';
      if (!this.versionInfo.name && saveNew) {
        this.versionNameVerify.isError = true;
        this.versionNameVerify.tipsText = this.$t('必填项不可为空');
        return;
      }
      // 版本号不能包含空格和##
      const reg = /\s|##/;
      if (reg.test(this.versionInfo.name) && saveNew) {
        this.versionNameVerify.isError = true;
        this.versionNameVerify.tipsText = this.$t('版本号不能包括空格和##');
        return;
      }
      this.handleSendJupyterMessage({
        eventType: 'saveNoteConfirm',
        saveAsNew: saveNew,
        statusTips: true,
        params: {
          version: this.versionInfo.name,
          commit_message: this.versionInfo.desc,
        },
      });
      saveNew && (this.versionInfo.loading = true);
      return !saveNew;
    },
    handleCancel() {
      this.$refs.versionPop.hideHandler();
      this.handleClearParams();
      this.versionInfo.loading = false;
    },
    handleClearParams() {
      this.storageMethod = 'saveCur';
      this.versionInfo.name = '';
      this.versionInfo.desc = '';
    },
    setRowStyle({ rowIndex }) {
      return rowIndex === 0 ? { 'background-color': '#F0F1F5' } : {};
    },
    hanldeformatterCell(row, column, cellValue, index) {
      const tooltips = {
        content: row.commit_message || '--',
        placement: 'bottom',
        theme: 'light',
      };
      return (
        <span style="outline: 0;" v-bk-tooltips={tooltips}>
          {row.version}
        </span>
      );
    },
    handleDelVersion({ commit_id }) {
      this.handleSendJupyterMessage({
        eventType: 'deleteNoteVersion',
        params: { commit_id },
      });
    },
    handleGetDiffAgain() {
      this.diffDialog.dataModel.original = '';
      this.diffDialog.dataModel.modified = '';
      this.diffDialog.loading = true;
      this.handleSendJupyterMessage({
        eventType: 'noteDiff',
        commitId: this.diffDialog.commitId,
      });
    },
    handleShowDiff(row) {
      this.diffDialog.isShow = true;
      this.diffDialog.loading = true;
      this.diffDialog.diffVersion.name = row.version;
      this.diffDialog.diffVersion.time = row.commit_time;
      this.diffDialog.commitId = row.commit_id;
      this.handleSendJupyterMessage({
        eventType: 'noteDiff',
        commitId: row.commit_id,
      });
    },
    handleCloseDiff() {
      this.diffDialog.isShow = false;
      this.diffDialog.diffVersion.name = '';
      this.diffDialog.diffVersion.time = '';
      this.diffDialog.dataModel.original = '';
      this.diffDialog.dataModel.modified = '';
      this.diffDialog.curTime = '';
      this.diffDialog.commitId = '';
      this.diffDialog.retreating = false;
    },
    handleRollBack() {
      this.diffDialog.retreating = true;
      this.handleSendJupyterMessage({
        eventType: 'rollBackVersion',
        commitId: this.diffDialog.commitId,
      });
    },
    handleHelp() {
      const address = this.$store.getters['docs/notebookHelp'];
      window.open(address);
    },
    handleReloadNotebook() {
      document.getElementById(
        'jupyter-notebook'+ this.activeNotebook.notebook_id
      ).src = this.activeNotebook.notebook_url;
    },
    handleOccupyPolling() {
      this.occupyTimer = setInterval(this.handleGetOccupyStatus, 5000);
    },
    handleGetOccupyStatus(callback) {
      // this.bkRequest
      //   .httpRequest('dataExplore/getNotebookLock', {
      //     params: { notebook_id: this.activeNotebook.notebook_id },
      //   })
      //   .then(res => {
      //     if (res.result) {
      //       this.occupied = (res.data || {}).lock_status;
      //       this.occupier = (res.data || {}).lock_user;
      //       callback && callback();
      //     }
      //   });
      this.bkRequest
        .httpRequest('dataExplore/getNotebookEditRight', {
          params: { notebook_id: this.activeNotebook.notebook_id },
        })
        .then(res => {
          if (res.result) {
            this.occupied = false;
          } else {
            this.notebookCode = res.code;
            if (res.code === '1580200') {
              this.occupier = res.message;
              this.occupier = this.occupier.match(/】(\S*)/)[1];
              this.occupied = true;
            } else if (res.code === '1580201') {
              this.occupier = res.message;
              this.occupier = this.occupier.match(/】(\S*)/)[1];
              this.occupied = true;
            } else {
              this.occupier = res.message;
              this.occupier = this.occupier.match(/】(\S*)/)[1];
              this.occupied = true;
            }
          }
        });
    },
    getEditPermission(config) {
      this.textLoading = true;
      clearInterval(this.occupyTimer);
      this.bkRequest
        .httpRequest('dataExplore/getTackLock', {
          params: { notebook_id: this.activeNotebook.notebook_id },
        })
        .then(res => {
          if (res.result) {
            // 5s发送 handleGetOccupyStatus
            this.timer = setTimeout(() => {
              this.textLoading = false;
              this.handleGetOccupyStatus(() => {
                document.getElementById('jupyter-notebook' + this.activeNotebook.notebook_id).src += '';
                showMsg(this.$t('已成功获取编辑权'), 'success', { delay: 3000 });
              });
              if (this.isCommon) {
                this.handleOccupyPolling();
              }
            }, 5000);
          } else {
            this.getMethodWarning(res.message, res.code);
            this.textLoading = false;
          }
        });
    },
    closePromptbox() {
      this.editor = false;
    },
    getCurrentNotebook() {
      this.bkRequest
        .httpRequest('dataExplore/getCurrentNotebook/', {
          params: {
            notebook_id: this.notebookId,
          },
        })
        .then(res => {
          if (res.result) {
            this.notebooks = res.data;
            this.handleReport();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    //打开弹窗
    handleReport() {
      this.report.primary.visible = true;
      this.btnStyle = 'default';
      this.checkboxVal = JSON.parse(this.notebooks.report_config);
      if (this.notebooks.report_secret) {
        this.flag = false;
        this.bkRequest
          .httpRequest('dataExplore/createdGenerateReport', {
            params: {
              notebook_id: this.notebookId,
              report_config: JSON.stringify(this.checkboxVal),
            },
          })
          .then(res => {
            if (res.data) {
              this.reportSecret = encodeURIComponent(res.data.report_secret);
              this.escapeReport = res.data.report_secret;
              let url = window.location.href.substring(0, window.location.href.indexOf('#'));
              if (this.curType === 'common') {
                // eslint-disable-next-line max-len
                this.reportVal = `${url}#/datalab/notebook?type=${this.curType}&projectId=${this.projectId}&notebook=${this.notebookId}&key=${this.reportSecret}`;
              } else {
                // eslint-disable-next-line max-len
                this.reportVal = `${url}#/datalab/notebook?type=${this.curType}&notebook=${this.notebookId}&key=${this.reportSecret}`;
              }
              this.updateNootbookConfig(JSON.stringify(this.checkboxVal), this.reportSecret);
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          });
      } else {
        this.flag = true;
      }
    },
    //获取密钥
    getCreatedReport(val) {
      this.checkboxVal = val;
      return this.bkRequest
        .httpRequest('dataExplore/createdGenerateReport', {
          params: {
            notebook_id: this.notebookId,
            report_config: JSON.stringify(this.checkboxVal),
          },
        })
        .then(res => {
          if (res.data) {
            this.reportSecret = encodeURIComponent(res.data.report_secret);
            this.escapeReport = res.data.report_secret;
            let url = window.location.href.substring(0, window.location.href.indexOf('#'));
            if (this.curType === 'common') {
              // eslint-disable-next-line max-len
              this.reportVal = `${url}#/datalab/notebook?type=${this.curType}&projectId=${this.projectId}&notebook=${this.notebookId}&key=${this.reportSecret}`;
            } else {
              // eslint-disable-next-line max-len
              this.reportVal = `${url}#/datalab/notebook?type=${this.curType}&notebook=${this.notebookId}&key=${this.reportSecret}`;
            }
            this.updateNootbookConfig(JSON.stringify(this.checkboxVal), this.reportSecret);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    //生成报告
    createdReport(checkboxVal) {
      this.flag = false;
      this.getCreatedReport(checkboxVal);
      this.btnStyle = 'default';
    },
    copyUrl(reportVal) {
      clipboardCopy(reportVal, showMsg('复制成功', 'success', { delay: 3000 }));
    },
    //重新生成
    createdReportAgain(checkboxVal) {
      this.newLoading = true;
      this.reportVal = '';
      this.getCreatedReport(checkboxVal);
      this.newLoading = false;
      this.btnStyle = 'default';
      this.$bkMessage({
        message: this.$t('设置变更已生效'),
        theme: 'success',
      });
    },
    //更新笔记参数
    updateNootbookConfig(config, secret) {
      if (this.notebooks.notebook_id === this.notebookId) {
        this.notebooks.report_config = config;
        this.notebooks.report_secret = secret;
      }
    },
    toNewReport() {
      let newUrl = this.$router.resolve({
        name: 'DatalabNotebook',
        query: {
          type: this.curType,
          projectId: this.projectId,
          notebook: this.notebookId,
          key: this.escapeReport,
        },
      });
      window.open(newUrl.href, '_blank');
    },
  },
};
</script>

<style lang="scss" scoped>
@keyframes notify-tips {
  0% {
    right: -700px;
  }

  100% {
    right: -312px;
  }
}
.explore-notebook-layout {
  position: relative;
  flex: 1;
  .occupied-tips {
    position: absolute;
    top: 0;
    bottom: 0;
    left: 0;
    right: 0;
    line-height: 20em;
    font-size: 20px;
    text-align: center;
    color: #ffffff;
    background-color: rgba(0, 0, 0, 0.6);
  }
  //提示框样式
  .notify-tips {
    position: absolute !important;
    top: 10px;
    right: -312px;
    width: 360px;
    background-color: #fff;
    padding: 20px 21px 20px 49px;
    z-index: 9;
    border: 1px solid #f0f1f5;
    border-radius: 2px;
    box-shadow: 0px 3px 6px 0px rgba(65, 65, 65, 0.15);
    animation: notify-tips 1s;
    border-left: 4px solid #ff9c01;
    .icon-close-line-2 {
      width: 24px;
      height: 24px;
      color: #c4c6cc;
      position: absolute;
      top: 5px;
      right: 5px;
      cursor: pointer;
    }
    .icon-exclamation-circle-shape-delete {
      width: 18px;
      height: 18px;
      color: #ff9c01;
      position: absolute;
      top: 28px;
      left: 24px;
    }
    .tips-title {
      width: 112px;
      height: 21px;
      font-size: 16px;
      font-family: MicrosoftYaHei, MicrosoftYaHei-Bold;
      font-weight: 700;
      text-align: left;
      color: #313238;
      line-height: 21px;
    }
    .tips-text {
      width: 290px;
      font-size: 14px;
      font-family: MicrosoftYaHei;
      text-align: left;
      color: #63656e;
      line-height: 22px;
      margin-top: 6px;
      .userColor {
        color: #ea3636;
      }
      .textColor {
        color: #3a84ff;
        cursor: pointer;
      }
    }
  }

  // 样式与notebook头部导航样式相同，才可以保证pop位置正确
  .notebook-toolbar-hidden {
    position: absolute;
    top: 0;
    left: 50%;
    width: 100%;
    height: 44px;
    display: flex;
    align-items: center;
    padding-left: 16px;
    transform: translateX(-50%);
    z-index: -1;
    .blank-placeholder {
      display: inline-block;
      padding-left: 178px;
    }
    .btn-placeholder {
      display: inline-block;
      width: 28px;
      height: 28px;
      margin: 0 4px;
    }
  }
  .notebook-mask {
    width: 100%;
    height: 100%;
    background-color: rgba(0, 0, 0, 0);
    display: none;
    position: absolute;
    top: 0;
    left: 0;
  }
}
.save-notebook-confirm {
  padding: 15px 9px 0;
  .title {
    font-size: 16px;
  }
  .storage-methods {
    display: flex;
    align-items: center;
    font-size: 14px;
    line-height: 20px;
    color: #63656e;
    padding: 20px 0 12px;
  }
  .radio-group {
    flex: 1;
    .bk-form-radio {
      margin-right: 20px;
    }
  }
  .save-info {
    font-size: 14px;
    color: #63656e;
    padding: 16px 0 20px;
    .bk-label {
      width: 64px;
      font-weight: normal;
    }
    .bk-form-content {
      margin-left: 64px;
    }
    .error-status {
      ::v-deep .bk-form-input {
        border-color: #ff5656;
        background-color: #fff4f4;
        color: #ff5656;
      }
    }
    .bk-error-tips {
      position: absolute;
      bottom: -16px;
      left: 64px;
      font-size: 12px;
      color: #ff5656;
    }
    ::v-deep .bk-form-textarea {
      min-height: 62px;
    }
  }
}
.custom-pop-info {
  padding: 11px 5px 16px;
  span {
    display: block;
    color: #63656e;
    &.title {
      font-size: 16px;
      color: #313238;
    }
  }
}
::v-deep .notebook-diff-dialog {
  .bk-dialog {
    max-width: 1480px !important;
  }
  .bk-dialog-header {
    padding: 0;
  }
  .bk-dialog-body {
    padding: 0;
  }
  .bk-dialog-content {
    width: 100% !important;
    top: -100px;
  }
  .bk-dialog-tool {
    display: none;
  }
}
.notebook-diff-dialog-header {
  position: relative;
  text-align: left;
  color: #ffffff;
  padding: 18px 28px 20px 24px;
  background-color: #23243c;
  .icon-close {
    position: absolute;
    top: 5px;
    right: 5px;
    width: 32px;
    height: 32px;
    line-height: 32px;
    font-size: 28px;
    color: #979ba5;
    border-radius: 50%;
    cursor: pointer;
    &:hover {
      background-color: #f0f1f5;
    }
  }
  .title {
    display: flex;
    align-items: center;
    line-height: 28px;
    &::before {
      content: '';
      width: 5px;
      height: 24px;
      background-color: #3a84ff;
      margin-right: 22px;
    }
    .title-icon {
      font-size: 22px;
      margin-right: 6px;
    }
    span {
      font-size: 20px;
    }
  }
  .tips {
    display: flex;
    justify-content: space-between;
    color: #64656e;
    padding-top: 8px;
    font-size: 16px;
    .desc {
      padding-left: 27px;
    }
    .help {
      font-size: 14px;
      cursor: pointer;
    }
    .icon-help-fill {
      font-size: 16px;
      color: #c4c6cc;
      margin-right: 2px;
    }
  }
}
.notebook-diff-dialog-btns {
  .tips {
    font-size: 12px;
    color: #979ba5;
    line-height: 32px;
  }
  .bk-button {
    width: 86px;
  }
}
/deep/.bk-form-control {
  .bk-form-checkbox {
    width: 100%;
    margin: 17px 0 0 24px;
  }
}
/deep/.is-disabled {
  .bk-checkbox-text {
    color: #c4c6cc;
    font-family: MicrosoftYaHei;
  }
}
.btn-box {
  width: 100%;
  margin-top: 30px;
  height: 32px;
  .mr10 {
    float: right;
    margin-right: 0px !important;
  }
}
.btn-box2 {
  width: 100%;
  margin-top: 30px;
  height: 32px;
  line-height: 32px;
  display: flex;
  position: relative;
  .copy-content {
    width: 378px;
    border: 1px solid #979ba5;
    display: flex;
    i {
      font-size: 16px;
      display: inline-block;
      vertical-align: middle;
    }
    .copy-url {
      width: 350px;
      padding-left: 10px;
      span {
        display: inline-block;
        width: 300px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
      i {
        margin-bottom: 26px;
        margin-left: 5px;
      }
      &:hover {
        color: #3a84ff;
        cursor: pointer;
      }
    }
    .new-report {
      i {
        margin-bottom: 4px;
      }
      &:hover {
        color: #3a84ff;
        cursor: pointer;
      }
    }
  }
  .mr10 {
    margin-right: 10px !important;
  }
}
/deep/.bk-form-control.control-append-group .bk-form-input {
  border-right: 1px solid #fff;
}

/deep/.notebook-preview-wrapper {
  position: absolute !important;
  .notebook-preview-left {
    border-right: none;
    .left-header {
      border-bottom: none;
    }
    .left-content {
      padding: 0 15px;
    }
  }
  .notebook-preview-header {
    display: none !important;
  }
  .notebook-preview-main {
    margin: 0 !important;
    height: calc(100vh - 110px) !important;
    padding-right: 0 !important;
    .bk-alert-info {
      display: none;
    }
    .notebookReport-wrapper {
      padding: 20px 20px 20px 0;
      max-width: 1998px;
      overflow: hidden;
    }
  }
}
.version-side {
  .notebook-version-main {
    padding: 30px 30px 10px;
    text-align: right;
    .version-search {
      display: inline-block;
      width: 360px;
    }
    .version-table {
      &::before {
        background-color: transparent;
      }
    }
  }
}
</style>
