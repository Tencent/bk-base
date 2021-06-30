

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
  <div class="clean-sidebar"
    :style="{ top: top + 'px' }"
    :class="{ veil: !isShow }">
    <bkdata-dialog
      v-model="dialogShow"
      extCls="bkdata-dialog-1"
      width="600"
      height="300"
      :closeIcon="false"
      :showFooter="false"
      :title="$t('请选择时间格式')"
      :maskClose="false">
      <div>
        <div class="content">
          <div class="bk-form">
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('参考时间') }}：</label>
              <div class="bk-form-content">
                <bkdata-input v-model="referenceTime"
                  class="project-name-form"
                  :disabled="true" />
              </div>
            </div>
            <div class="bk-form-item">
              <label class="bk-label">{{ $t('时间格式') }}：</label>
              <div
                v-tooltip.notrigger.top="{
                  content: error.timeFormat,
                  visible: !!error.timeFormat && dialogShow,
                  class: 'float-dialog error-red',
                }"
                class="bk-form-content">
                <bkdata-selector
                  :placeholder="timeSelect.timeHoder"
                  :list="timeSelect.list"
                  :selected.sync="configuration.timeFormat"
                  @item-selected="checkTimeFormat" />
              </div>
            </div>

            <div class="bk-form-item"
              style="text-align: center">
              <bkdata-button
                :theme="'primary'"
                type="submit"
                :title="$t('确定')"
                class="mr10"
                @click.stop="dialogConfirm">
                {{ $t('确定') }}
              </bkdata-button>
              <bkdata-button :theme="'default'"
                :title="$t('取消')"
                class="mr10"
                @click.stop="dialogCancel">
                {{ $t('取消') }}
              </bkdata-button>
            </div>
          </div>
        </div>
      </div>
    </bkdata-dialog>
    <div :class="['close', { rotate: lan === 'en' }]"
      @click="collapse">
      {{ isShow ? $t('收起配置栏') : $t('展开配置栏') }}
    </div>
    <div class="header">
      <div>
        <p class="title">
          {{ $t('清洗配置设置') }}
        </p>
        <span
          :class="[
            'status',
            cleanStatus.status === 'started' ? 'running' : cleanStatus.status === 'failed' ? 'failed' : '',
          ]">
          {{ cleanStatus.status_display }}
        </span>
      </div>
      <div
        v-tooltip.notrigger.left="getValidateTips(error.clean_config_name, 'clean-config-name')"
        class="clean-config-name">
        <input
          v-if="isConfNameEdit"
          v-model="configuration.clean_config_name"
          type="text"
          :maxlength="15"
          :placeholder="$t('清洗任务名称')"
          @input="checkccName">
        <span v-else>{{ configuration.clean_config_name || $t('清洗任务名称') }}</span>
        <i
          :class="['bk-icon', (isConfNameEdit && 'icon-save-shape') || 'icon-edit2']"
          @click="isConfNameEdit = !isConfNameEdit" />
      </div>
      <!-- <span
            </span> -->
    </div>
    <div class="clean-info">
      <ul class="info-list">
        <li v-tooltip.notrigger.left="getValidateTips(error.resultTable)"
          class="clearfix">
          <p class="info-title">
            {{ $t('结果表英文名') }}
          </p>
          <div class="bk-form-content fl info result-en-name"
            :class="{ error: error.resultTable }">
            {{ bizId }}_
            <bkdata-input
              v-model="configuration.resultTable"
              :disabled="isEdit"
              :maxlength="50"
              :placeholder="info.resultHoder"
              @input="checkResultTable" />
            <!-- <div class="error-msg"
                            v-show="error.resultTable">{{ error.resultTable }}</div> -->
          </div>
        </li>
        <li v-tooltip.notrigger.left="getValidateTips(error.cleanName)"
          class="clearfix">
          <p class="info-title">
            {{ $t('结果表中文名') }}
          </p>
          <div class="bk-form-content fl info"
            :class="{ error: error.cleanName }">
            <bkdata-input
              v-model="configuration.cleanName"
              :maxlength="50"
              :placeholder="info.configurationHoder"
              @input="checkName"
              @blur.stop="setConfigName" />
            <!-- <div class="error-msg"
                            v-show="error.cleanName">{{ error.cleanName }}</div> -->
          </div>
        </li>

        <li v-tooltip.notrigger.left="getValidateTips(error.timeFormat)"
          class="clearfix">
          <p class="info-title">
            {{ $t('时间格式') }}
          </p>
          <div class="bk-form-content fl info"
            :class="{ error: error.timeFormat }">
            <bkdata-selector
              :placeholder="timeSelect.timeHoder"
              :list="timeSelect.list"
              :selected.sync="configuration.timeFormat"
              :disabled="!isEdit"
              @item-selected="checkTimeFormat" />
          </div>
          <!-- <div class="error-msg"
                        v-show="error.timeFormat">{{ error.timeFormat }}</div> -->
        </li>
        <li class="clearfix">
          <p class="info-title">
            {{ $t('时区') }}
          </p>
          <div class="bk-form-content fl info">
            <bkdata-selector
              :placeholder="zoneSelect.zoneHoder"
              :list="zoneSelect.zoneList"
              :selected.sync="configuration.timeZone" />
          </div>
        </li>
        <li v-tooltip.notrigger.left="getValidateTips(error.description)"
          class="clearfix">
          <p class="info-title">
            {{ $t('描述') }}
          </p>
          <div class="bk-form-content fl info"
            :class="{ error: error.description }">
            <textarea v-model="configuration.description"
              class="bk-form-textarea"
              @input="checkDescription" />
            <!-- <div class="error-msg"
                            v-show="error.description">{{ error.description }}</div> -->
          </div>
        </li>
      </ul>
    </div>
    <div class="clean-table">
      <div class="clean-table-wrapper header">
        <table class="bk-table has-thead-bordered">
          <thead>
            <tr>
              <th width="130"
                class="name">
                <i class="bk-icon mr5" />{{ $t('名称') }}
              </th>
              <th width="70">
                {{ $t('类型') }}
              </th>
              <th>
                {{ $t('中文名称') }}
                <template v-if="$modules.isActive('clean_auto_fill')">
                  <a href="javascript:void(0);"
                    style="margin-left: 15px"
                    @click="handlerAutoFillZhName">
                    智能填充
                  </a>
                </template>
              </th>
              <th width="35"
                class="item-center">
                <bkdata-popover
                  v-tooltip.notrigger.top="{
                    content: error.timePicked,
                    visible: error.timePicked !== '',
                    class: 'clean-side-tips error-red',
                  }"
                  class="item"
                  effect="dark"
                  placement="top"
                  :content="$t('用于系统内部数据流转的时间')">
                  <i class="bk-icon icon-time" />
                </bkdata-popover>
              </th>

              <th width="35"
                class="item-center">
                <bkdata-popover class="item"
                  effect="dark"
                  placement="top"
                  :content="$t('维度字段')">
                  <i class="bk-icon icon-dimension" />
                </bkdata-popover>
              </th>
            </tr>
          </thead>
        </table>
      </div>
      <div class="clean-table-wrapper body">
        <table class="bk-table has-thead-bordered">
          <tbody>
            <template v-if="configuration.fields.length">
              <tr v-for="(list, index) in configuration.fields"
                :key="index">
                <td class="name pointer"
                  width="130"
                  @click="focusAssign(list)">
                  <template v-if="configuration.timePicked === list.id">
                    <bkdata-popover
                      class="item"
                      effect="dark"
                      placement="top"
                      :content="$t('时间字段')"
                      :disabled="configuration.timePicked !== list.id">
                      <span class="icon mr5 fl">
                        <i
                          class="bk-icon"
                          :class="{
                            'icon-clock-shape': configuration.timePicked === list.id,
                          }" />
                      </span>
                    </bkdata-popover>
                  </template>
                  <p class="text-name"
                    :class="{ pl20: configuration.timePicked !== list.id }"
                    :title="list.field_name">
                    {{ list.field_name }}
                  </p>
                </td>
                <td class="type"
                  width="70">
                  {{ list.field_type }}
                </td>
                <td class="chinese-name"
                  width="164">
                  <div :class="['bk-form-content', { error: error['fields#' + index + '#field_alias'] }]">
                    <bkdata-input
                      v-model="list.field_alias"
                      :placeholder="info.fieldsHoder"
                      @keydown.tab.prevent="nextFocus(index + 1)"
                      @input="checkFieldDesc(index, list.field_alias)" />
                    <div v-show="error['fields#' + index + '#field_alias']"
                      class="error-msg">
                      {{ error['fields#' + index + '#field_alias'] }}
                    </div>
                  </div>
                </td>
                <td width="35"
                  class="item-center">
                  <span @click.stop="e => openTimeFormatSelector($event, list.field_name, list.id)">
                    <bkdata-radio
                      v-bk-tooltips.right="$t('指定为时间字段')"
                      :value="list.id"
                      name="time_field_config"
                      :checked="configuration.timePicked === list.id" />
                  </span>
                </td>
                <td width="35"
                  class="item-center">
                  <bkdata-checkbox
                    v-bk-tooltips.right="$t('指定为维度字段')"
                    :value="list.id"
                    :checked="dimensions.includes(list.id)"
                    @change="e => handleDimensionsChanged(e, list)" />
                </td>
              </tr>
            </template>
            <template v-else>
              <tr>
                <td>{{ $t('请在左侧添加赋值算子') }}</td>
              </tr>
            </template>
          </tbody>
        </table>
      </div>
    </div>
    <!-- "debugInfoTipShow && !isReadonly" -->
    <!-- 调试成功，请先确认输出（结果预览），然后填写清洗配置设置 -->
    <div v-show="submitTips.visible"
      class="debug-result-info"
      :class="[submitTips.class]">
      <i class="left bk-icon"
        :class="[submitTips.iconClass]" /> {{ submitTips.content }}
      <i v-if="!isReadonly"
        class="clean-tool-right bk-icon icon-close"
        @click="debugInfoTipShow = !debugInfoTipShow" />
    </div>
    <!-- v-tooltip.notrigger="readonlyTips" -->
    <div class="button">
      <bkdata-button theme="primary"
        :disabled="isReadonly"
        size="large"
        :loading="isLoading"
        @click="save">
        {{ $t('保存并执行') }}
      </bkdata-button>
      <bkdata-button
        v-if="isEdit && cleanStatus.status !== 'stopped'"
        class="stop"
        theme="default"
        size="large"
        :loading="loading.stop"
        @click="confirmStopTask">
        {{ $t('停止') }}
      </bkdata-button>
    </div>
  </div>
</template>
<script>
import Bus from '@/common/js/bus.js';
import { confirmMsg } from '@/common/js/util';
import Cookies from 'js-cookie';
import { showMsg, postMethodWarning } from '@/common/js/util.js';
import { JValidator } from '@/common/js/check';
export default {
  props: {
    bizId: {
      type: String,
      default: '',
    },
    isReadonly: {
      type: Boolean,
      default: true,
    },
    isLoading: {
      type: Boolean,
      default: false,
    },
    referenceFormat: {
      type: Object,
    },
  },
  data() {
    return {
      // referenceTime: '',
      dialogShow: false,
      dialogLoading: false,
      isSidebarOpened: false,
      debugInfoTipShow: true,
      setConfigNameId: 0,
      timePickedId: 0,
      timePickedName: '',
      isConfNameEdit: false,
      isShow: false,
      loading: {
        stop: false,
      },
      top: 60, // 定位
      cleanStatus: {
        status: '',
        status_display: '',
      }, // 清洗配置设置状态
      timeSelect: {
        timeHoder: this.$t('请选择时间格式'),
        list: [],
      },
      zoneSelect: {
        zone: this.$t('请选择时间时区'),
        zoneList: [
          { id: 12, name: 'UTC+12' },
          { id: 11, name: 'UTC+11' },
          { id: 10, name: 'UTC+10' },
          { id: 9, name: 'UTC+9' },
          { id: 8, name: this.$t('UTC_plus8') },
          { id: 7, name: 'UTC+7' },
          { id: 6, name: 'UTC+6' },
          { id: 5, name: 'UTC+5' },
          { id: 4, name: 'UTC+4' },
          { id: 3, name: 'UTC+3' },
          { id: 2, name: 'UTC+2' },
          { id: 1, name: 'UTC+1' },
          { id: 0, name: 'UTC+0' },
          { id: -1, name: 'UTC-1' },
          { id: -2, name: 'UTC-2' },
          { id: -3, name: 'UTC-3' },
          { id: -4, name: 'UTC-4' },
          { id: -5, name: 'UTC-5' },
          { id: -6, name: 'UTC-6' },
          { id: -7, name: 'UTC-7' },
          { id: -8, name: 'UTC-8' },
          { id: -9, name: 'UTC-9' },
          { id: -10, name: 'UTC-10' },
          { id: -11, name: 'UTC-11' },
          { id: -12, name: 'UTC-12' },
        ],
      },
      dimensions: [], // 维度字段列表
      configuration: {
        description: '',
        clean_config_name: '',
        cleanName: '',
        resultTable: '',
        timeFormat: '',
        timeZone: 8,
        timePicked: '',
        fields: [],
      },
      info: {
        fieldsHoder: this.$t('不可为空'),
        configurationHoder: this.$t('请输入任务名称_50个字符限制'),
        resultHoder: this.$t('由英文字母_下划线和数字组成_且字母开头'),
      },
      error: {
        clean_config_name: '',
        cleanName: '',
        resultTable: '',
        timePicked: '',
        timeFormat: '',
        description: '',
      },
      timeFormatList: [],
    };
  },
  computed: {
    readonlyTips() {
      const isEditTips = this.debugInfoTipShow && !this.isReadonly;
      const isReadonlyTips = this.isReadonly && this.isShow && this.isSidebarOpened;
      const showMsg = this.$t('清洗逻辑执行提示');
      return { content: showMsg, visible: isReadonlyTips, class: 'clean-side-tips error-red' };
    },
    submitTips() {
      const isEditTips = this.debugInfoTipShow && !this.isReadonly;
      const isReadonlyTips = this.isReadonly && this.isShow && this.isSidebarOpened;
      const showMsg = (isReadonlyTips && this.$t('清洗逻辑执行提示')) || this.$t('清洗逻辑调试成功提示');
      return {
        content: showMsg,
        visible: this.debugInfoTipShow || isReadonlyTips,
        class: `${(isEditTips && 'info') || 'warning'}`,
        iconClass: `${(isEditTips && 'icon-check-1') || 'icon-close'}`,
      };
    },
    isEdit() {
      return this.$route.name === 'edit_clean';
    },
    lan() {
      return Cookies.get('blueking_language');
    },
    referenceTime: {
      get() {
        return this.referenceFormat[this.timePickedName];
      },

      set(val) {
        // eslint-disable-next-line vue/no-mutating-props
        this.referenceFormat[this.timePickedName] = val;
      },
    },
  },
  watch: {
    isReadonly(val) {
      !val && this.$set(this, 'debugInfoTipShow', true);
    },
  },
  mounted() {
    window.addEventListener('scroll', this.sidebarScroll);
    // window.addEventListener('resize', this.sidebarResize)
    Bus.$on('clean.fields-update', fields => {
      let newFieldIds = fields.map(f => {
        return f.id;
      });
      // 选中的值改变的话清掉
      if (this.configuration.timePicked && newFieldIds.length && !newFieldIds.includes(this.configuration.timePicked)) {
        this.configuration.timePicked = '';
        this.timePickedId = '';
        this.timePickedName = '';
      }

      let list = [];
      fields.forEach((field, index) => {
        let wrapper = {
          id: field.id,
          field_name: field.assign_to,
          field_type: field.type,
          field_alias: field.field_alias === undefined ? '' : field.field_alias,
          is_dimension: 0,
          field_index: index + 1, // 索引0保留给系统固定字段timestamp，这里索引从1开始
        };
        for (let oldfield of this.configuration.fields) {
          if (field.id === oldfield.id) {
            wrapper = oldfield;
            wrapper.field_name = field.assign_to;
            wrapper.field_type = field.type;
            break;
          }
        }
        list.push(wrapper);
      });
      this.configuration.fields = list;
    });
    this.getTimeFormats();
  },
  methods: {
    handlerAutoFillZhName() {
      const postData = {
        kwargs: { field_names: [this.configuration.fields.map(field => field.field_name)] },
        type: 'field_alias',
      };
      this.bkRequest.httpRequest('meta/zhNameSuggest', { params: postData }).then(res => {
        if (res.result) {
          const responseData = res.data[0] || {};
          this.configuration.fields.forEach(field => {
            if (responseData[field.field_name]) {
              this.$set(field, 'field_alias', responseData[field.field_name]);
            }
          });
        } else {
          this.postMethodWarning(res.message);
        }
      });
    },
    handleDimensionsChanged(checked, item) {
      if (checked) {
        this.dimensions.push(item.id);
      } else {
        this.dimensions = this.dimensions.filter(id => id !== item.id);
      }
    },
    dialogCancel() {
      this.dialogShow = false;
      this.timePickedId = '';
      this.timePickedName = '';
      this.error.timeFormat = '';
    },
    dialogConfirm() {
      const result = this.checkTimeFormat(true);
      if (result) {
        this.configuration.timePicked = this.timePickedId;
        this.dialogShow = false;
      }
    },
    getValidateTips(validate, customClass = '') {
      return { visible: validate.length > 0, content: validate, class: `error-red ${customClass}` };
    },
    focusAssign(list) {
      this.$emit('notifyChangeHighlight', list.id);
    },
    nextFocus(index) {
      let focus = document.querySelectorAll('.chinese-name')[index];
      if (focus) {
        focus.querySelector('.bk-form-input').focus();
      }
    },
    checkName() {
      this.error.cleanName = '';
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(this.configuration.cleanName)) {
        this.error.cleanName = validator.errMsg;
        return false;
      }
      return true;
    },
    checkDescription() {
      this.error.description = '';
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(this.configuration.description)) {
        this.error.description = validator.errMsg;
        return false;
      }
      return true;
    },
    checkccName() {
      this.error.clean_config_name = '';
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(this.configuration.clean_config_name)) {
        this.error.clean_config_name = validator.errMsg;
        return false;
      }
      return true;
    },
    setConfigName() {
      this.setConfigNameId && clearTimeout(this.setConfigNameId);
      this.setConfigNameId = setTimeout(() => {
        const val = this.configuration.cleanName;
        if (val && !this.configuration.clean_config_name) {
          this.configuration.clean_config_name = val;
        }
      }, 500);
    },
    checkResultTable() {
      this.error.resultTable = '';
      let validator = new JValidator({
        required: true,
        wordFormat: true,
      });
      if (!validator.valid(this.configuration.resultTable)) {
        this.error.resultTable = validator.errMsg;
        return false;
      }
      return true;
    },
    checkTimeFormat(isExampleSet = false) {
      const TIME_FORMAT_REG = {
        yyyyMMddHHmmss: /^\d{14}/,
        yyyyMMddHHmm: /^\d{12}/,
        'yyyyMMdd HH:mm:ss.SSSSSS': /^(\d{8}\s\d{1,2}:\d{1,2}:\d{1,2}\.\d{6})/,
        'yyyyMMdd HH:mm:ss': /^(\d{8}\s\d{1,2}:\d{1,2}:\d{1,2})/,
        yyyyMMdd: /^\d{8}/,
        'yyyy-MM-dd+HH:mm:ss': /^(\d{4}-\d{1,2}-\d{1,2}\+\d{1,2}:\d{1,2}:\d{1,2})/,
        'yyyy-MM-dd"T"HH:mm:ssXXX': /^(\d{4}-\d{1,2}-\d{1,2}T\d{1,2}:\d{1,2}:\d{1,2}\+\d{1,2}:\d{1,2})/,
        'yyyy-MM-dd HH:mm:ss.SSSSSS': /^(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}.\d{6})/,
        'yyyy-MM-dd HH:mm:ss': /^(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})/,
        'yyyy-MM-dd': /^(\d{4}-\d{1,2}-\d{1,2})/,
        'yy-MM-dd HH:mm:ss': /^(\d{2}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})/,
        'Unix Time Stamp(seconds)': /^\d{10}/,
        'Unix Time Stamp(mins)': /^\d{8}/,
        'Unix Time Stamp(milliseconds)': /^\d{13}/,
        'MM/dd/yyyy HH:mm:ss': /^(\d{2}\/\d{1,2}\/\d{4}\s\d{1,2}:\d{1,2}:\d{1,2})/,
        'dd/MMM/yyyy:HH:mm:ss': /^(\d{2}\/[A-Za-z]{3}\/\d{4}:\d{1,2}:\d{1,2}:\d{1,2})/,
      };
      this.error.timeFormat = '';
      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(this.configuration.timeFormat)) {
        this.error.timeFormat = validator.errMsg;
        return false;
      }
      if (isExampleSet) {
        const validate = new RegExp(TIME_FORMAT_REG[this.configuration.timeFormat]).test(this.referenceTime);
        if (!validate) {
          this.error.timeFormat = this.$t('请选择正确格式');
        }

        return validate;
      }
      return true;
    },
    checkFields() {
      let ret = true;

      if (this.configuration.fields.length === 0) {
        showMsg(this.$t('请在清洗配置中选择赋值算子_生成清洗字段'), 'warning');
        return false;
      }

      for (let [index, field] of this.configuration.fields.entries()) {
        if (!this.checkFieldDesc(index, field.field_alias)) {
          ret = false;
        }
      }

      if (!this.checkTimePicked()) {
        ret = false;
      }

      return ret;
    },
    openTimeFormatSelector(event, key, id) {
      if (this.timePickedId !== id) {
        // this.referenceTime = this.referenceFormat[key]
        this.timePickedId = id;
        this.timePickedName = key;
        this.dialogShow = true;
      }
      event.stopPropagation();
      event.preventDefault();
    },
    checkTimePicked(checked, key, id) {
      this.error.timePicked = '';
      if (this.configuration.timePicked.length === 0) {
        this.error.timePicked = this.$t('请指定时间字段');
        return false;
      }
      return true;
    },
    checkFieldDesc(index, value) {
      let errKey = `fields#${index}#field_alias`;
      this.$set(this.error, errKey, '');

      let validator = new JValidator({
        required: true,
      });
      if (!validator.valid(value)) {
        this.$set(this.error, errKey, validator.errMsg);
        return false;
      }
      return true;
    },
    check() {
      let isValidate = true;
      for (let i of [
        'checkName',
        'checkResultTable',
        'checkTimeFormat',
        'checkFields',
        'checkccName',
        'checkDescription',
      ]) {
        if (!this[i]()) {
          isValidate = false;
        }
      }
      return isValidate;
    },
    collapse() {
      this.isSidebarOpened = false;
      this.isShow = !this.isShow;
      this.isShow
        && this.$nextTick(() => {
          setTimeout(() => {
            this.isSidebarOpened = true;
          }, 300);
        });
    },

    showSideBar() {
      const that = this;
      this.isSidebarOpened = false;
      this.isShow = true;
      setTimeout(() => {
        that.isSidebarOpened = true;
        that.configuration.timeFormat && that.checkTimeFormat(true); // 编辑态下，如果有时间格式，做时间格式校验
      }, 1000);
    },

    /*
     * 保存并提示
     */
    save() {
      this.$emit('save');
    },
    getParams() {
      let setting = JSON.parse(JSON.stringify(this.configuration));
      setting.fields.forEach(field => {
        field.is_dimension = this.dimensions.includes(field.id);
      });
      for (let format of this.timeFormatList) {
        if (format.time_format_name === setting.timeFormat) {
          setting.timestamp_len = format.timestamp_len;
          break;
        }
      }
      let timepicker = setting.timePicked;
      for (let field of setting.fields) {
        if (field.id === timepicker) {
          setting.timePicked = field.field_name;
          break;
        }
      }
      return setting;
    },
    /*
     * 回填
     */
    Backfill(data) {
      this.cleanStatus.status = data.status;
      this.cleanStatus.status_display = data.status_display;
      this.configuration.resultTable = data.result_table_name;
      this.configuration.cleanName = data.result_table_name_alias;
      this.configuration.clean_config_name = data.clean_config_name || data.result_table_name_alias;
      this.configuration.description = data.description;
      this.configuration.timeFormat = data.time_format;
      this.configuration.timeZone = data.timezone;
      const timeField = data.fields.find(d => d.field_name === data.time_field_name);
      if (timeField) {
        this.$set(this.configuration, 'timePicked', timeField.id);
        this.$set(this, 'timePickedId', timeField.id);
        this.$set(this, 'timePickedName', timeField.field_name);
      }
      this.configuration.fields = data.fields;
      data.fields.forEach(field => {
        if (field.is_dimension) {
          this.dimensions.push(field.id);
        }
      });
    },
    /*
     * 清洗配置设置定位
     */
    sidebarScroll() {
      let scroll = document.documentElement.scrollTop || document.body.scrollTop;
      if (scroll < 60) {
        this.top = 60 - scroll;
      } else {
        this.top = 0;
      }
    },
    confirmStopTask() {
      confirmMsg(this.$t('确认停止该流程'), '', this.stopEtl);
    },
    /**
     * 停止清洗
     */
    stopEtl() {
      let apiParams = {
        rid: this.$route.params.rtid,
      };
      this.loading.stop = true;
      this.$store
        .dispatch('api/stopEtl', apiParams)
        .then(res => {
          if (res.result) {
            this.cleanStatus.status = res.data.status;
            this.cleanStatus.status_display = res.data.status_display;
          } else {
            this.postMethodWarning(res.message);
          }
        })
        ['finally'](() => {
          this.loading.stop = false;
        });
    },
    sidebarResize() {
      // if (document.body.clientWidth > 1780) {
      //     this.isShow = true
      // }
    },
    /**
     * 获取时间格式列表
     */
    getTimeFormats() {
      this.$store.dispatch('api/listEtlTimeFormat').then(res => {
        this.timeFormatList = res.data;
        this.timeSelect.list = res.data.map(d => {
          return { id: d.time_format_name, name: d.time_format_alias };
        });
      });
    },
  },
};
</script>
<style lang="scss">
.vue-tooltip {
  &.error-red {
    &.float-dialog {
      z-index: 9999;
    }
  }
}

.clean-config-name,
.clean-side-tips {
  z-index: 1000 !important;
}
.clean-sidebar {
  position: fixed;
  right: 0;
  bottom: 0;
  width: 480px;
  z-index: 1000;
  background: #fff;
  border: 1px solid #c3cdd7;
  box-shadow: -4px 0 5px rgba(33, 34, 50, 0.1);
  transition: all 0.3s ease;
  /* overflow-y: auto; */
  &.veil {
    right: -480px;
  }

  .content {
    max-height: 500px;
    padding: 30px 0 45px 0;
    overflow-y: auto;
    .bk-form {
      width: 480px;
      .bk-label {
        padding-right: 16px;
        width: 165px;
      }
      .bk-selector-create-item {
        width: 100%;
        float: left;
        padding: 10px 10px;
        background-color: #fafbfd;
        cursor: pointer;
        .text {
          font-style: normal;
          color: #666bb4;
        }
        &:hover {
          background: #cccccc;
        }
      }
      .bk-form-action {
        margin-top: 20px;
      }
      .bk-form-content {
        margin-left: 165px;
        &.is-danger {
          .bk-data-wrapper {
            border-color: #ff5656;
          }
        }
        .no-data {
          padding: 5px;
        }
        .bk-icon.icon-exclamation-circle {
          position: absolute;
          left: 105%;
          top: 8px;
          font-size: 16px;
          cursor: pointer;
          .tips {
            display: none;
            margin-left: 0;
            position: absolute;
            width: 200px;
            border-radius: 4px;
            background: #000;
            color: #fff;
            font-size: 14px;
            right: 30px;
            top: 3px;
            transform: translate(0%, -50%);
            &::after {
              content: '';
              border: 5px solid #000;
              border-color: transparent transparent transparent #000;
              position: absolute;
              right: -10px;
              top: 50%;
            }
          }
        }
        .bk-icon.icon-exclamation-circle:hover .tips {
          display: block;
        }
      }
      .bk-selector.is-danger {
        .bk-selector-input {
          border-color: #ff5656;
          &[placeholder],
          [placeholder],
          *[placeholder] {
            /* Chrome/Opera/Safari */
            color: #ff5656;
          }
        }
      }
      .bk-form-not-power {
        line-height: 36px;
        a {
          vertical-align: sub;
          text-decoration: underline;
        }
      }
      .bk-form-power-loading {
        height: 36px;
      }
      .bk-form-tip .bk-tip-text {
        color: #ff5656;
      }
      .bk-badge {
        line-height: 18px;
      }
    }
    .tips {
      background: #e1f3ff;
      min-height: 42px;
      line-height: 22px;
      color: #3c96ff;
      padding: 10px 10px;
      margin-left: 165px;
    }
    .required {
      margin-left: 165px;
      padding-top: 5px;
      color: #f64646;
      display: inline-block;
      vertical-align: -2px;
      margin-right: 5px;
    }
    .bk-button {
      min-width: 120px;
    }
  }
  .debug-result-info {
    display: flex;
    flex-direction: row;
    align-items: center;
    padding: 10px 15px;
    border-radius: 2px;
    transition: top 0.3s linear, bottom 0.3s linear;
    padding-right: 5px;
    justify-content: flex-start;

    &.warning {
      background: #ffeded;
      border: 1px solid #fd9c9c;

      i {
        &.left {
          background: #ea3636;
        }
      }
    }
    &.info {
      background: #eaffed;
      border: 1px solid #45e35f;
    }

    i {
      &.left {
        background: #45e35f;
        display: block;
        width: 18px;
        height: 18px;
        line-height: 18px;
        font-size: 12px;
        text-align: center;
        color: #ffffff;
        border-radius: 50%;
        margin-right: 10px;
      }

      &.clean-tool-right {
        font-size: 12px;
        margin-left: 5px;
        cursor: pointer;
        position: absolute;
        right: 10px;
      }
    }
  }

  .header {
    padding: 14px 20px;
    border-bottom: 1px solid #dbe1e7;
    display: flex;
    justify-content: space-between;
    align-items: center;
    .title {
      line-height: 20px;
      font-weight: bold;
      padding-left: 18px;
      position: relative;
      display: inline-block;
      margin-right: 15px;
      &:before {
        content: '';
        width: 4px;
        height: 20px;
        position: absolute;
        left: 0px;
        top: 50%;
        background: #3a84ff;
        transform: translate(0%, -50%);
      }
    }
    .status {
      display: inline-block;
      padding: 0px 6px;
      background: #737987;
      color: #fff;
      border-radius: 2px;
      font-size: 12px;
      line-height: 20px;
      &.running {
        background: #9dcb6b;
      }
      &.failed {
        background: #ff5555;
      }
    }

    .clean-config-name {
      display: flex;
      align-items: center;

      input {
        line-height: 25px;
        height: 25px;
      }
      i {
        cursor: pointer;
        margin-left: 3px;
      }
    }
  }
  .clean-info {
    .info-list > li {
      border-bottom: 1px solid #dbe1e7;
      background: #fafafa;
      display: flex;
      align-items: center;
    }
    .info-title {
      width: 130px;
      float: left;
      text-align: right;
      padding: 10px;
      //   border-right: 1px solid #dbe1e7;
      background: #fafafa;
    }
    .info {
      width: calc(100% - 130px);
    }
    .bk-form-content {
      line-height: 40px;
      border-left: 1px solid #dbe1e7;
    }
    .bk-form-input,
    .bk-form-textarea {
      border: none;
      height: 40px;
      line-height: 40px;
    }
    .bkdata-selector-input,
    .bkdata-combobox-input {
      border: none;
    }
  }
  .clean-table {
    padding: 20px 5px 20px 15px;
    height: calc(100% - 390px);
    .clean-table-wrapper {
      &.body {
        overflow: scroll;
        min-height: 50px;
        height: calc(100% - 25px);
        margin-top: -1px;
        table {
          border-top: 0;
        }
      }

      &.header {
        margin-right: 8px;
        padding: 0;
        border-bottom: 0;
        table {
          border-bottom: 0;
          thead {
            border-bottom: 0;
            tr {
              border-bottom: 0;
              th {
                border-bottom: 0;
              }
            }
          }
        }
      }

      &::-webkit-scrollbar {
        width: 8px;
        background-color: transparent;
      }

      &::-webkit-scrollbar-thumb {
        border-radius: 8px;
        background-color: #a0a0a0;
      }
      .bk-table {
        table-layout: fixed;
        //   height: 100%;
      }
      thead {
        width: calc(100% - 1em);
        > tr > th {
          padding: 5px;
          &:first-of-type {
            border-left: none;
          }
          &:last-of-type {
            border-right: none;
          }
        }
        tr {
          background: #efefef;
          display: table;
          width: 100%;
          table-layout: fixed;
        }
      }
      tbody {
        display: block;
        height: 100%;
        // overflow-y: scroll;
        tr {
          display: table;
          width: 100%;
          table-layout: fixed;
          border-bottom: 1px solid #dfe0e5;
          padding-top: 1px;
          &:last-child {
            border-bottom: none;
          }
        }
        > tr > td {
          font-size: 14px;
          padding: 0;
          line-height: 1;
          height: 30px;
          border-bottom: none;
          &.name {
            &.pointer {
              display: flex;
              align-items: center;
              .bk-tooltip {
                &.item {
                  display: block;
                }
              }
            }
          }
        }
        tr:hover {
          background: #fafafa;
        }
      }

      .item-center {
        text-align: center;
      }
    }
    .name {
      .icon {
        width: 14px;
        height: 30px;
        line-height: 30px;
      }
      .icon-clock-shape {
        color: #3c96ff;
        vertical-align: -1px;
      }
      .text-name {
        width: calc(100% - 20px);
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
      }
    }
    .chinese-name {
      .bk-form-input {
        height: 32px;
        line-height: 1;
        font-size: 12px;
      }
    }
    .type {
      color: #c3cdd7;
    }
    .bk-form-radio,
    .bk-form-checkbox {
      margin: 0;
      padding: 0;
      input[type='checkbox'],
      input[type='radio'] {
        margin: 0;
      }
    }
    .error {
      .error-msg {
        top: 50%;
        left: -10px;
        transform: translate(-100%, -50%);
        &:after {
          border-color: transparent transparent transparent #212232;
          left: initial;
          right: -10px;
          top: 50%;
          bottom: initial;
          transform: translate(0, -50%);
        }
      }
    }
  }
  .button {
    border-top: 1px solid #dbe1e7;
    padding: 15px 20px;
    background: #fafafa;
    .stop {
      margin-left: 15px;
    }
  }
  .error {
    position: relative;
    input,
    textarea {
      outline: 1px solid #f00;
      line-height: 1.5;
    }
    /* &:after {
            content: '';
            top:0;
            position: absolute;
            width: 99%;
            height: 100%;
            border: 1px solid #f00;
        } */
    .error-msg {
      max-width: 300px;
      position: absolute;
      top: -33px;
      left: 50%;
      padding: 5px 5px;
      font-size: 12px;
      white-space: nowrap;
      background: #212232;
      color: #fff;
      border-radius: 2px;
      line-height: 14px;
      transform: translate(-50%, 0);
      &:after {
        content: '';
        position: absolute;
        /* right: 70%; */
        left: 50%;
        bottom: -10px;
        border: 5px solid;
        border-color: #212232 transparent transparent transparent;
      }
    }
  }
  .close {
    display: none;
    width: 34px;
    position: absolute;
    top: 50%;
    left: -34px;
    font-size: 12px;
    padding: 10px;
    background: #fff;
    border: 1px solid #c3cdd7;
    color: #737987;
    border-right: none;
    box-shadow: -4px 0 5px rgba(33, 34, 50, 0.1);
    cursor: pointer;
    transform: translate(0, -50%);
    &.rotate {
      transform: rotate(0.25turn) translate(0, 52%);
      width: 70px;
      text-align: center;
      border-right: 1px solid #c3cdd7;
      border-top: none;
      box-shadow: 0px 4px 5px rgba(33, 34, 50, 0.1);
    }
  }

  .result-en-name {
    display: flex;
    padding: 0 0 0 10px;
  }

  @media (max-width: 1780px) {
    .close {
      display: block;
    }
  }
}
</style>
