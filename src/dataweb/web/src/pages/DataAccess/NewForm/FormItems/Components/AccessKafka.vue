

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
  <div class="access-file">
    <!-- <span v-if="!display"
            class="bk-item-des">{{"接入成功后是不允许修改的，请慎重填写！"}}</span> -->
    <span v-if="!display"
      class="bk-item-des mt15">
      {{ '切换集群可能导致数据拉取异常' }}
    </span>
    <Container>
      <FieldX2 :label="$t('broker_地址')">
        <bkdata-input
          v-model="params.scope[0]['master']"
          v-tooltip.notrigger.right="validate['master']"
          :disabled="display"
          :placeholder="$t('请填写broker地址')" />
      </FieldX2>
      <FieldX2 :label="$t('消费组')">
        <bkdata-input
          v-model="params.scope[0]['group']"
          v-tooltip.notrigger.right="validate['group']"
          :disabled="display"
          :placeholder="$t('请填写消费组')" />
      </FieldX2>
      <FieldX2 :label="$t('消费Topic')">
        <bkdata-input
          v-model="params.scope[0]['topic']"
          v-tooltip.notrigger.right="validate['topic']"
          :disabled="display"
          :placeholder="$t('请填写消费Topic')" />
      </FieldX2>
      <FieldX2 :label="$t('偏移量选项')">
        <bkdata-select v-model="params.scope[0]['auto_offset_reset']"
          :clearable="false"
          style="width: 669px">
          <bkdata-option v-for="item in offsetList"
            :id="item.id"
            :key="item.id"
            :name="item.name" />
        </bkdata-select>
      </FieldX2>
      <FieldX2 :label="$t('最大并发度')">
        <bkdata-input
          v-model="params.scope[0]['tasks']"
          v-tooltip.notrigger.right="validate['tasks']"
          :min="1"
          :max="24"
          type="number"
          :disabled="display"
          :placeholder="$t('请填写最大并发度')" />
      </FieldX2>
      <FieldX2 :label="$t('是否使用加密')">
        <bkdata-radio-group v-model="params.scope[0]['use_sasl']">
          <bkdata-radio :disabled="display"
            :value="true">
            {{ $t('加密') }}
          </bkdata-radio>
          <bkdata-radio :disabled="display"
            :value="false">
            {{ $t('非加密') }}
          </bkdata-radio>
        </bkdata-radio-group>
      </FieldX2>
      <template v-if="params.scope[0]['use_sasl']">
        <FieldX2 :label="$t('安全协议')">
          <bkdata-input
            v-model="params.scope[0]['security_protocol']"
            v-tooltip.notrigger.right="validate['security_protocol']"
            :disabled="display"
            :placeholder="$t('请填写安全协议')" />
        </FieldX2>
        <FieldX2 :label="$t('SASL机制')">
          <bkdata-input
            v-model="params.scope[0]['sasl_mechanism']"
            v-tooltip.notrigger.right="validate['sasl_mechanism']"
            :disabled="display"
            :placeholder="$t('请填写传递途径')" />
        </FieldX2>
        <FieldX2 :label="$t('用户名')">
          <bkdata-input
            v-model="params.scope[0]['user']"
            v-tooltip.notrigger.right="validate['user']"
            :disabled="display"
            :placeholder="$t('请填写用户名')" />
        </FieldX2>
        <FieldX2 :label="$t('密码')">
          <bkdata-input
            v-model="params.scope[0]['password']"
            v-tooltip.notrigger.right="validate['password']"
            type="password"
            :disabled="display"
            :placeholder="$t('请填写密码')" />
        </FieldX2>
      </template>
    </Container>
  </div>
</template>
<script>
import Container from '../ItemContainer';
import FieldX2 from '../FieldComponents/FieldX2';
import { validateScope } from '../../SubformConfig/validate.js';
import mixin from '@/pages/DataAccess/Config/mixins.js';

export default {
  components: {
    Container,
    FieldX2,
  },
  mixins: [mixin],
  props: {
    display: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      loading: false,
      asciiSelceted: false,
      isFirstValidate: true,
      params: {
        scope: [
          {
            master: '',
            group: '',
            topic: '',
            tasks: 1,
            use_sasl: false,
            security_protocol: 'SASL_PLAINTEXT',
            sasl_mechanism: 'SCRAM-SHA-512',
            user: '',
            password: '',
            auto_offset_reset: 'latest',
          },
        ],
      },
      validate: {
        master: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        group: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        topic: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        tasks: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        security_protocol: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        sasl_mechanism: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        user: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
        password: {
          content: window.$t('不能为空'),
          visible: false,
          class: 'error-red',
        },
      },
      offsetList: [
        { id: 'latest', name: 'latest（当前消费组无提交的offset时，消费该分区下的最新数据）' },
        { id: 'earliest', name: 'earliest（当前消费组无提交的offset时，消费该分区下的最早的一条数据）' },
      ],
    };
  },
  computed: {
    topicWatchHandel() {
      return this.params.scope[0].topic;
    },
  },
  methods: {
    validateForm(validateFunc, isSubmit = true) {
      if (isSubmit) {
        this.isFirstValidate = false;
      }
      let isValidate = true;
      let params = {};
      if (this.params.scope[0]['use_sasl']) {
        params = this.params.scope[0];
      } else {
        // 不加密时，需要过滤掉安全协议、SASL机制、用户、密码这4个参数，避免校验
        const paramsObj = {};
        Object.keys(this.params.scope[0]).forEach(item => {
          if (!['security_protocol', 'sasl_mechanism', 'user', 'password'].includes(item)) {
            paramsObj[item] = this.params.scope[0][item];
          }
        });
        params = paramsObj;
      }
      if (!validateScope(params, this.validate)) {
        isValidate = false;
      }
      this.$forceUpdate();
      return isValidate;
    },

    formatFormData() {
      return {
        group: 'access_conf_info',
        identifier: 'resource',
        data: this.params,
      };
    },

    renderData(data) {
      Object.keys(this.params).forEach(key => {
        data['access_conf_info'].resource[key] && this.$set(this.params, key, data['access_conf_info'].resource[key]);
      });

      /** 偏移量选项如果没有，默认选择第一个 */
      if (this.params.scope[0].auto_offset_reset === '' || this.params.scope[0].auto_offset_reset === undefined) {
        this.$set(this.params.scope[0], 'auto_offset_reset', 'latest');
      }
    },
  },
};
</script>
