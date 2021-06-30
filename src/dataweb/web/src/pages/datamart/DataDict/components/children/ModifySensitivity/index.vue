

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
    <dialogWrapper :extCls="'header-style'"
      :dialog="dialogConfig"
      :footerConfig="footerConfig"
      :defaultFooter="true"
      @confirm="confirm"
      @cancle="cancle">
      <template #header>
        <div class="text fl">
          <p class="title">
            {{ $t('修改数据敏感度') }}
          </p>
        </div>
      </template>
      <template #content>
        <div class="sensitivity-container">
          <bkdata-radio-group v-model="relueMode">
            <bkdata-radio :disabled="isEditDefaultSet"
              :value="'default'">
              {{ `【${$t('默认规则')}】${$t('通过血缘关系继承父表的敏感度')}` }}
            </bkdata-radio>
            <bkdata-radio :value="'user'">
              {{ `【${$t('用户定义')}】${$t('优先以用户指定的敏感度为准，子表按照规则继承下去')}` }}
            </bkdata-radio>
          </bkdata-radio-group>
          <div v-show="relueMode === 'user'"
            class="manager-select-container">
            <SelectManager v-bind="$attrs"
              :disabled="true"
              :bkBizId="bkBizId"
              :sensitivity="sensitivity" />
            <div class="arrow-bar">
              <div class="arrow-bar-right" />
            </div>
            <SelectManager v-bind="$attrs"
              :sensitivity="sensitivity"
              :bkBizId="bkBizId"
              @changeSecretLevel="changeSecretLevel"
              @changeWarnTipsShow="changeWarnTipsShow" />
          </div>
        </div>
      </template>
      <template v-if="isHiddenTips"
        #footer>
        <div class="warn-tip">
          升级后，您将不在管理人员里，是否继续操作
        </div>
      </template>
    </dialogWrapper>
  </div>
</template>

<script>
import dialogWrapper from '@/components/dialogWrapper';
import SelectManager from './SelectManager';

export default {
  name: 'ModifySensitivity',
  components: {
    dialogWrapper,
    SelectManager,
  },
  props: {
    sensitivity: {
      type: String,
      default: '',
    },
    dataSetId: {
      type: [Number, String],
      default: '',
    },
    dataType: {
      type: String,
      default: '',
    },
    bkBizId: {
      type: [Number, String],
      default: '',
    },
  },
  data() {
    return {
      dialogConfig: {
        width: '900px',
        isShow: false,
        quickClose: false,
        loading: false,
      },
      relueMode: 'default',
      footerConfig: {
        confirmText: this.$t('确定'),
        cancleText: this.$t('取消'),
        loading: false,
      },
      secretLevel: this.sensitivity,
      isShowWarnTips: false,
    };
  },
  computed: {
    isHiddenTips() {
      return this.isShowWarnTips && this.relueMode === 'user';
    },
    isEditDefaultSet() {
      // 数据类型为数据源时，禁用默认规则
      return this.dataType === 'raw_data';
    },
  },
  methods: {
    changeWarnTipsShow(isShow) {
      // 数据管理员和运维任务列表里，如果都不包含当前用户，需要显示提示语
      this.isShowWarnTips = isShow;
    },
    changeSecretLevel(id) {
      this.secretLevel = id;
    },
    cancle() {
      this.dialogConfig.isShow = false;
    },
    confirm() {
      this.footerConfig.loading = true;
      this.bkRequest
        .httpRequest('dataDict/updateDataSetSensitivity', {
          params: {
            data_set_type: this.dataType,
            data_set_id: this.dataSetId,
            sensitivity: this.secretLevel,
            tag_method: this.relueMode,
          },
        })
        .then(res => {
          if (res.result) {
            this.$bkMessage({
              message: this.$t('更新数据敏感度成功！'),
              theme: 'success',
            });
            this.$emit('changeSensitivityStatus', this.secretLevel, this.relueMode);
            this.cancle();
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](() => {
          this.footerConfig.loading = false;
        });
    },
    open(tagMethod) {
      this.relueMode = tagMethod;
      this.dialogConfig.isShow = true;
    },
  },
};
</script>

<style lang="scss" scoped>
::v-deep .header-style {
  .hearder {
    background-color: white !important;
    .text {
      .title {
        padding-left: 15px !important;
        color: #63656e !important;
      }
    }
  }
  .content {
    max-height: 600px;
  }
  .footer {
    position: relative;
    .warn-tip {
      position: absolute;
      right: 200px;
      top: 50%;
      transform: translateY(-50%);
      line-height: 36px;
      color: #ff9c01;
    }
  }
}
.sensitivity-container {
  padding: 0 25px 25px 25px;
  ::v-deep .bk-form-radio {
    display: block;
    &:last-of-type {
      margin-top: 10px;
    }
  }
  .manager-select-container {
    margin-top: 20px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    .wrapper {
      padding: 30px 20px;
      flex: 0.49;
    }
    .arrow-bar {
      width: 24px;
      position: relative;
      height: 0px;
      margin: 0 20px;
      border-bottom: 1px dashed #63656e;
      .arrow-bar-right {
        position: absolute;
        top: -9px;
        right: -8px;
        width: 20px;
        height: 20px;
        border: 2px solid #c4c6cc;
        border-left: none;
        border-bottom: none;
        transform: rotateZ(45deg);
      }
    }
  }
}
</style>
