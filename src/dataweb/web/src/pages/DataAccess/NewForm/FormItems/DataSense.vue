

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
  <Container>
    <FieldX2 :hideLabel="true">
      <div class="sense">
        <bkdata-radio-group
          v-for="item in sourceList"
          :key="item.id"
          v-model="params.sensitivity"
          :class="[
            'sense-degree',
            { 'sense-check': params.sensitivity === item.value, diasbled: $route.name === 'updateDataid' },
          ]">
          <div class="type">
            <bkdata-radio :value="item.value"
              :disabled="item.disabled">
              <i :class="['bk-radio-text', item.value]">{{ $t(item.label) }}</i>
              <span class="visible">{{ $t(item.des) }}</span>
            </bkdata-radio>
          </div>
        </bkdata-radio-group>
      </div>
    </FieldX2>
  </Container>
</template>
<script>
import Container from './ItemContainer';
import Item from './Item';
import FieldX2 from './FieldComponents/FieldX2';
export default {
  components: { Container, FieldX2 },
  data() {
    return {
      params: {
        sensitivity: 'private',
      },

      sourceList: [
        {
          label: '绝密数据',
          des: '由业务负责人接入平台_需要GM审批接入_仅授权人员可见_数据申请方需要总监级别',
          value: 'topsecret',
          id: 'sense_top_secret',
          disabled: true,
        },
        {
          label: '机密数据',
          des: '由业务负责人接入平台_需要总监审批接入_业务成员不可见_数据申请方需要leader级别',
          value: 'confidential',
          id: 'sense_secret',
          disabled: !this.$modules.isActive('sensitivity'),
        },
        {
          label: '私有数据',
          des: '由业务负责人直接接入至平台_业务人员可见_平台用户均可申请此数据',
          value: 'private',
          id: 'sense_private',
          disabled: false,
        },
        {
          label: '公开数据',
          des: '接入后平台所有用户均可使用此数据',
          value: 'public',
          id: 'sense_public',
          disabled: !this.$modules.isActive('sensitivity'),
        },
      ],
    };
  },
  methods: {
    formatFormData() {
      return {
        group: '',
        identifier: 'access_raw_data',
        data: this.params,
      };
    },
    renderData(data) {
      Array.prototype.forEach.call(Object.keys(this.params), key => {
        data['access_raw_data'][key] && this.$set(this.params, key, data['access_raw_data'][key]);
      });
    },
  },
};
</script>
<style lang="scss" scoped>
.sense {
  flex-direction: column;
  .visible {
    padding-left: 10px;
    font-size: 12px;
    line-height: 38px;
    height: 38px;
    min-width: 120px;
    color: #a5a5a5;
    vertical-align: middle;
  }
  .sense-check {
    .bk-radio-text {
      &.general {
        color: #9dcb6b;
      }

      &.private,
      .confidential,
      .public {
        color: #f6ae00;
      }

      &.senstive {
        color: #ff5555;
      }
    }
  }
  .bk-form-radio {
    // width: 350px;
    display: flex;
    align-items: center;
    padding: 0;
    .bk-radio-text {
      font-weight: 600;
    }
  }
  .sense-text {
    cursor: pointer;

    input[disabled='disabled'] {
      cursor: not-allowed;
    }
  }
}
</style>
