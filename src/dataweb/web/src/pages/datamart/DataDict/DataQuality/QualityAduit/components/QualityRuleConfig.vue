

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
    <bkdata-sideslider :isShow.sync="isShow"
      extCls=""
      :quickClose="true"
      :width="850"
      @hidden="$emit('close')">
      <div slot="header"
        class="head-container">
        <i class="icon icon-cog" />
        <span class="title">
          {{ $t('数据质量审核规则配置') }}
        </span>
        <span v-if="isAllDisabled"
          class="bk-text-danger">
          （{{ $t('运行中的规则不能被修改') }}）
        </span>
      </div>
      <div slot="content">
        <bkdata-form ref="validateForm1"
          :labelWidth="100"
          :model="formData"
          :rules="rules">
          <HeaderType :title="$t('规则配置')">
            <bkdata-form-item :label="$t('规则名称')"
              :required="true"
              :property="'rule_name'">
              <bkdata-input v-model="formData.rule_name"
                :disabled="isAllDisabled"
                extCls="width200"
                :placeholder="$t('请输入规则名称')"
                @blur="handleRuleNameChange" />
              <bkdata-input v-model="formData.rule_description"
                :disabled="isAllDisabled"
                :placeholder="$t('请输入规则描述信息，便于后续继续查找、分析')" />
            </bkdata-form-item>
            <bkdata-form-item :label="$t('规则模板')"
              :required="true"
              :property="'rule_template_id'"
              extCls="flex-wrap">
              <bkdata-selector
                :disabled="isAllDisabled"
                :isLoading="isRuleTemplateLoading"
                :placeholder="$t('选择系统内置模板快速创建规则，默认为自定义模板')"
                :displayKey="'template_alias'"
                :list="ruleTemplateList"
                :searchable="true"
                :selected.sync="formData.rule_template_id"
                :settingKey="'id'"
                :searchKey="'template_alias'"
                @item-selected="handleTempSelect" />
            </bkdata-form-item>
            <bkdata-form-item :label="$t('规则内容')"
              :required="true"
              :property="'rule_config'"
              extCls="flex-wrap">
              <bkdata-input v-model="ruleContent"
                disabled
                type="textarea"
                :rows="formData.rule_config.length"
                :placeholder="$t('请进行规则逻辑配置')" />
              <div class="rule-content-container">
                <transition name="fade">
                  <RuleContentConfig :disabled="isAllDisabled"
                    :isCoveredConfig.sync="isCoveredConfig"
                    :ruleContent.sync="ruleContent"
                    :ruleList="formData.rule_config"
                    :limitMetrics="modelLimitMetrics" />
                </transition>
              </div>
            </bkdata-form-item>
          </HeaderType>
          <HeaderType :title="$t('事件信息')">
            <div class="bk-item-container">
              <bkdata-form-item :label="$t('事件名称')"
                extCls="flex5"
                :required="true"
                :property="'event_name'">
                <bkdata-input v-model="formData.event_name"
                  :disabled="isAllDisabled"
                  :placeholder="$t('由英文字母、下划线和数字组成，且以字母开头')" />
              </bkdata-form-item>
              <bkdata-form-item extCls="flex5"
                :label="$t('事件别名')"
                :required="true"
                :property="'event_alias'">
                <bkdata-input v-model="formData.event_alias"
                  :disabled="isAllDisabled"
                  :placeholder="$t('中文名称')" />
              </bkdata-form-item>
            </div>
            <bkdata-form-item :label="$t('事件描述')"
              :required="true"
              :property="'event_description'">
              <bkdata-input v-model="formData.event_description"
                :disabled="isAllDisabled"
                :placeholder="$t('请输入事件描述信息，便于后续准确查找、分析，比如：波动率微弱')" />
            </bkdata-form-item>
            <bkdata-form-item extCls="event-style"
              :label="$t('事件详情')"
              :property="'event_detail_template'">
              <bkdata-dropdown-menu ref="dropdown"
                :extCls="!isDropdownShow ? 'hide-content' : ''"
                @show="handleStopOver">
                <bkdata-input
                  ref="textareaInput"
                  slot="dropdown-trigger"
                  v-model="formData.event_detail_template"
                  :disabled="isAllDisabled"
                  :placeholder="$t('事件详情支持模板变量，请输入$获取输入提示，不填写系统将使用默认模板')"
                  type="textarea"
                  :showControls="false"
                  extCls="width350"
                  :rows="6"
                  @mouseover.stop="handleStopOver"
                  @change="handleInputChange" />
                <ul v-if="isDropdownShow"
                  slot="dropdown-content"
                  class="bk-dropdown-list">
                  <li v-for="(item, index) in auditEventTempList"
                    :key="index"
                    @click="triggerHandler(item)">
                    <a href="javascript:void(0);">
                      ${
                      {{ item.varName }}
                      }
                    </a>
                    <a href="javascript:void(0);">
                      {{ item.varAlias }}
                    </a>
                  </li>
                </ul>
              </bkdata-dropdown-menu>
              <div class="default-model">
                <div class="default-name">
                  {{ $t('参考模板') }}
                </div>
                <ul>
                  <li>事件名称: ${event_name}</li>
                  <li>事件内容: ${event_status_alias}</li>
                  <li>事件时间: ${event_time}</li>
                  <li>事件级别: ${event_level}</li>
                </ul>
              </div>
            </bkdata-form-item>
            <div class="advance-config">
              <div class="config-title">
                <span @click="isShowEventAdvance = !isShowEventAdvance">
                  {{ $t('高级选项') }}
                </span>
                <span :class="[`icon-${isShowEventAdvance ? 'up-big' : 'down-big'}`]" />
              </div>
              <transition-group name="fade">
                <div v-show="isShowEventAdvance"
                  key="1"
                  class="bk-item-container">
                  <bkdata-form-item extCls="flex5"
                    :label="$t('事件类型')"
                    :required="true"
                    :property="'event_type'">
                    <bkdata-selector :disabled="isAllDisabled"
                      :displayKey="'name'"
                      :list="eventTypeList"
                      :multiSelect="false"
                      :searchable="true"
                      :selected.sync="formData.event_sub_type"
                      :settingKey="'id'"
                      :searchKey="'name'" />
                  </bkdata-form-item>
                  <bkdata-form-item extCls="flex5"
                    :label="$t('事件极性')"
                    :required="true"
                    :property="'event_polarity'">
                    <bkdata-selector :disabled="isAllDisabled"
                      :placeholder="$t('事件极性，默认为中性')"
                      :displayKey="'name'"
                      :list="eventPolarityList"
                      :multiSelect="false"
                      :searchable="true"
                      :selected.sync="formData.event_polarity"
                      :settingKey="'id'"
                      :searchKey="'name'" />
                  </bkdata-form-item>
                </div>
                <div v-show="isShowEventAdvance"
                  key="2"
                  class="bk-item-container">
                  <bkdata-form-item extCls="flex5"
                    :label="$t('事件时效')"
                    :required="true"
                    :property="'event_currency'">
                    <bkdata-input v-model="formData.event_currency"
                      :disabled="isAllDisabled"
                      :placeholder="$t('单位：秒')"
                      :clearable="true"
                      type="number"
                      :max="1000"
                      :min="0" />
                  </bkdata-form-item>
                  <bkdata-form-item extCls="flex5"
                    :label="$t('敏感度')"
                    :required="true"
                    :property="'sensitivity'">
                    <bkdata-selector :disabled="isAllDisabled"
                      :placeholder="$t('事件敏感级别，默认为一般')"
                      :displayKey="'name'"
                      :list="influenceLevelList"
                      :multiSelect="false"
                      :searchable="true"
                      :selected.sync="formData.sensitivity"
                      :settingKey="'id'"
                      :searchKey="'name'" />
                  </bkdata-form-item>
                </div>
              </transition-group>
            </div>
          </HeaderType>
          <HeaderType :title="$t('通知配置')">
            <bkdata-form-item :label="$t('通知类型')"
              :required="true"
              :property="'notify_ways'">
              <bkdata-checkbox-group v-model="formData.notify_ways"
                v-bkloading="{ isLoading: isNotifyLoading }">
                <bkdata-checkbox v-for="(item, index) in notifyWays"
                  :key="index"
                  :disabled="isAllDisabled"
                  :value="item.notifyWay"
                  class="mr20">
                  <div class="check-img-container">
                    <img :src="`data:image/png;base64,${item.icon}`"
                      :title="item.notifyWayAlias">
                    {{ item.notifyWayAlias }}
                  </div>
                </bkdata-checkbox>
              </bkdata-checkbox-group>
            </bkdata-form-item>
            <bkdata-form-item :label="$t('通知接收人')"
              :required="true"
              :property="'receivers'">
              <bkdata-selector
                :multiSelect="true"
                :searchable="true"
                :isLoading="isReceiverLoading"
                :displayKey="'name'"
                :allowClear="true"
                :settingKey="'name'"
                :list="userList"
                :autoSort="true"
                :placeholder="$t('告警接收人')"
                :selected.sync="receivers"
                :disabled="isReceiverLoading || isAllDisabled"
                style="width: 500px"
                @clear="clearReceivers" />
              <!-- <receiver-form :disabled="!isRefresh" v-model="receivers" :roles="roles"> </receiver-form> -->
            </bkdata-form-item>
            <div class="advance-config">
              <div class="config-title">
                <span @click="isShowAdvance = !isShowAdvance">
                  {{ $t('高级选项') }}
                </span>
                <span :class="[`icon-${isShowAdvance ? 'up-big' : 'down-big'}`]" />
              </div>
              <transition name="fade">
                <bkdata-form-item v-show="isShowAdvance"
                  :label="$t('收敛策略')"
                  :required="true"
                  :property="'convergence_config'">
                  <div class="config-list mt10 f13">
                    <alert-trigger-form v-model="alertConfigInfo.trigger_config"
                      :desLan="'分钟内满足次审核规则'"
                      class="alert-rule-form"
                      :disabled="!isRefresh || isAllDisabled" />
                    <alert-convergence-form v-model="alertConfigInfo.convergence_config"
                      class="alert-rule-form"
                      :disabled="!isRefresh || isAllDisabled" />
                  </div>
                </bkdata-form-item>
              </transition>
            </div>
          </HeaderType>
          <bkdata-form-item v-if="isEdit"
            extCls="flex-right">
            <bkdata-button :loading="isChecking"
              extCls="mb30 mr30"
              theme="primary"
              :title="isCreated ? $t('更新') : $t('提交')"
              @click.stop.prevent="submitData">
              {{ isCreated ? $t('更新') : $t('提交') }}
            </bkdata-button>
            <bkdata-button extCls="mr5 mb30 mr15"
              theme="default"
              title="取消"
              @click="$emit('close')">
              {{ $t('取消') }}
            </bkdata-button>
          </bkdata-form-item>
        </bkdata-form>
      </div>
    </bkdata-sideslider>
  </div>
</template>

<script lang="ts" src="./QualityRuleConfig.ts"></script>

<style lang="scss" scoped>
.check-img-container {
  display: flex;
  align-items: center;
  img {
    width: 25px;
  }
}
.flex5 {
  flex: 5;
}
.width200 {
  width: 200px;
}
.width350 {
  width: 350px;
}
.head-container {
  .icon {
    font-size: 24px;
  }
  .bk-text-danger {
    font-size: 14px;
  }
}
.bk-item-container {
  display: flex;
  justify-content: space-between;
  margin: 20px 0;
  ::v-deep .bk-form-item {
    margin-top: 0;
  }
}
.config-list {
  color: #606266;
  padding: 0px 0 10px 20px;
  line-height: normal;

  .alert-rule-form {
    display: flex;
    margin-left: 10px;
    margin-bottom: 6px;
  }

  .content-item {
    display: flex;
    padding: 10px 0;
    .monitor-title {
      display: flex;
      justify-content: flex-end;
      align-items: flex-start;
      width: 65px;
      margin: 7px 0;
    }
    .config-list-item {
      display: flex;
      align-items: center;
      width: 100%;
      margin: 0 15px;
      ::v-deep .bk-tag-selector .bk-tag-input .tag-list > li {
        margin: 4px;
      }
      .el-checkbox + .el-checkbox {
        margin-left: 15px;
      }
      ::v-deep .bk-tag-selector {
        width: 100%;
        .bk-tag-input {
          height: 100px;
        }
      }
      .config-cont {
        display: flex;
        align-items: center;
        &.disabled {
          color: #ccc;
        }
      }
      .bk-form-input {
        width: 69px;
        height: 20px;
      }
      .bk-checkbox-text {
        font-style: normal;
        font-weight: normal;
        cursor: pointer;
        vertical-align: middle;
      }
    }
    .config-list-item + .config-list-item {
      margin-bottom: 0;
    }
  }
}
::v-deep .flex-right {
  .bk-form-content {
    justify-content: flex-end;
  }
}
::v-deep .flex-wrap {
  .bk-form-content {
    flex-wrap: wrap;
  }
}
.rule-content-container {
  width: 100%;
}
::v-deep .event-style {
  .bk-form-content {
    align-items: flex-start;
  }
  .default-model {
    display: flex;
    height: 120px;
    line-height: normal;
    border: 1px solid #c4c6cc;
    padding: 8px 10px;
    font-size: 12px;
    .default-name {
      white-space: nowrap;
      margin-right: 10px;
      font-size: 14px;
      font-weight: bold;
    }
  }
}
.hide-content {
  ::v-deep .bk-dropdown-content {
    opacity: 0;
  }
}
.bk-dropdown-list {
  li {
    display: flex;
    flex-wrap: nowrap;
    justify-content: space-between;
    &:hover {
      color: #3a84ff;
      background-color: #eaf3ff;
    }
  }
}
.fade-enter-active,
.fade-leave-active {
  z-index: 900;
  transition: height 5s;
}
.fade-enter, .fade-leave-to /* .fade-leave-active below version 2.1.8 */ {
  height: 0;
  margin: 0;
  z-index: -1;
  padding: 0 !important;
}
.advance-config {
  .config-title {
    cursor: pointer;
    color: #699df4;
    font-weight: normal;
    padding: 10px;
    text-align: center;
  }
}
</style>
