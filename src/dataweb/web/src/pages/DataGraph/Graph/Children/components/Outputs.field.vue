

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
  <div class="outputs-wrapper mt20">
    <bkdata-form ref="outputsFrameForm"
      :labelWidth="125"
      :model="nodeParams.config"
      extCls="data-table">
      <bkdata-form-item :label="$t('数据输出')"
        :required="true"
        property="outputs"
        :rules="validator">
        <bkdata-button
          v-tooltip.notrigger="validator"
          icon="plus"
          theme="primary"
          size="small"
          class="add-btn"
          @click="addOutputsHandle">
          {{ $t('新增') }}
        </bkdata-button>
        <bkdata-table :data="beforeUpdateValue.outputs">
          <bkdata-table-column :label="$t('输出表')">
            <template slot-scope="props">
              <span v-bk-tooltips="{ content: props.row.table_name, placement: 'top' }">
                {{
                  props.row.table_name
                }}
              </span>
            </template>
          </bkdata-table-column>
          <bkdata-table-column :label="$t('中文名')">
            <template slot-scope="props">
              <span v-bk-tooltips="{ content: props.row.output_name, placement: 'top' }">
                {{
                  props.row.output_name
                }}
              </span>
            </template>
          </bkdata-table-column>
          <bkdata-table-column label="操作"
            width="95">
            <template slot-scope="props">
              <bkdata-button theme="primary"
                text
                @click="editOutputs(props)">
                {{ $t('编辑') }}
              </bkdata-button>
              <bkdata-popover :ref="`popover${props.$index}`"
                placement="top"
                theme="light"
                trigger="click">
                <bkdata-button theme="primary"
                  text
                  class="del-btn">
                  {{ $t('删除') }}
                </bkdata-button>
                <div slot="content">
                  <bkdata-button theme="primary"
                    text
                    style="font-size: 12px"
                    @click="delOutputs(props)">
                    {{ $t('确定') }}
                  </bkdata-button>
                  <span style="color: #dcdee5">|</span>
                  <bkdata-button
                    theme="primary"
                    text
                    class="del-btn"
                    style="font-size: 12px"
                    @click="cancleDel(props.$index)">
                    {{ $t('取消') }}
                  </bkdata-button>
                </div>
              </bkdata-popover>
            </template>
          </bkdata-table-column>
        </bkdata-table>
      </bkdata-form-item>
      <!-- 点击编辑后弹出框 -->
      <div v-show="showEdit"
        class="dialog-edit">
        <div class="dialog-edit-head">
          {{ $t('输出编辑') }}
          <i class="bk-icon icon-close"
            @click="$emit('update:showEdit', false)" />
        </div>
        <div class="dialog-edit-body">
          <slot />
          <div class="outputs-foot">
            <bkdata-button theme="primary"
              class="mr10"
              @click="confirmHandle">
              {{ $t('确定') }}
            </bkdata-button>
            <bkdata-button theme="default"
              @click="cancleHandle">
              {{ $t('取消') }}
            </bkdata-button>
          </div>
        </div>
      </div>
    </bkdata-form>
  </div>
</template>

<script>
import mixin from './Node.components.mixin';
export default {
  mixins: [mixin],
  props: {
    showEdit: {
      type: Boolean,
      default: false,
    },
    hasNodeId: {
      type: Boolean,
      default: false,
    },
    addOutputs: {
      type: Function,
      default: () => {},
    },
    confirmHandle: {
      type: Function,
      default: () => {},
    },
    cancleHandle: {
      type: Function,
      default: () => {},
    },
    editOutputs: {
      type: Function,
      default: () => {},
    },
  },
  data() {
    return {
      beforeUpdateValue: {},
      validator: [
        {
          required: true,
          message: this.$t('该值不能为空'),
          trigger: 'blur',
        },
      ],
    };
  },
  methods: {
    addOutputsHandle() {
      this.$refs.outputsFrameForm.clearError(); // 清除错误提示
      this.addOutputs();
    },
    validateForm() {
      this.$refs.outputsFrameForm.validate().then(
        validator => {
          return Promise.resolve(validator);
        },
        reject => {
          return Promise.reject(reject);
        }
      );
    },
    setBackupData(config) {
      this.beforeUpdateValue = JSON.parse(JSON.stringify(config || {}));
    },
    // jar包确认删除数据输出
    delOutputs(props) {
      this.beforeUpdateValue.outputs = this.beforeUpdateValue.outputs.filter((item, index) => index !== props.$index);
      this.nodeParams.config = this.beforeUpdateValue;
    },
    cancleDel(index) {
      this.$refs[`popover${index}`].instance.hide();
    },
  },
};
</script>
<style lang="scss" scoped>
.outputs-foot {
  margin-top: 10px;
  margin-left: 125px;
}
</style>
