

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
  <div class="access-http mb20">
    <span v-if="!display"
      class="bk-item-des">
      {{ $t('为了保护数据对象所在系统_只支持一个URL请求_而且单次返回数据量小于等于1MB') }}
    </span>
    <Container>
      <Item class="x2">
        <label class="bk-label">URL：</label>
        <div class="bk-form-content url-tips">
          <bkdata-input
            v-model="params.scope.url"
            v-tooltip.notrigger="validate['url']"
            :disabled="display"
            type="text" />
          <template v-if="!display">
            <bkdata-selector
              class="type-lists"
              :list="typeLists"
              disabled="disabled"
              :selected.sync="params.scope.method" />
            <div class="url-icon">
              <i class="bk-icon icon-exclamation-circle-shape bk-icon-new" />
            </div>
          </template>
        </div>
      </Item>
      <Item>
        <label class="bk-label">{{ $t('时间参数') }}：</label>
        <div class="bk-form-content">
          <bkdata-input v-model="params.scope.time_range"
            disabled="disabled"
            type="text"
            placeholder="start，end" />
        </div>
      </Item>
      <Item>
        <label class="bk-label">{{ $t('时间参数格式') }}：</label>
        <div class="bk-form-content">
          <template v-if="!display">
            <bkdata-selector
              v-tooltip.notrigger="validate['time_format']"
              :placeholder="$t('请选择时间格式')"
              :disabled="display"
              :list="timeLists"
              :selected.sync="params.scope.time_format"
              @item-selected="handleFormatSelected" />
          </template>
          <template v-else>
            <bkdata-input disabled="disabled"
              :value="params.scope['time_format']" />
          </template>
        </div>
      </Item>
    </Container>
  </div>
</template>
<script>
/* eslint-disable */
import Bus from '@/common/js/bus';
import Container from '../ItemContainer';
import Item from '../Item';
import { validateScope } from '../../SubformConfig/validate.js';
import { getDateFormat } from './fun.js';
import mixin from '@/pages/DataAccess/Config/mixins.js';

export default {
  mixins: [mixin],
  components: {
    Container,
    Item,
  },
  props: {
    display: {
      type: Boolean,
      default: false,
    },
  },
  data() {
    return {
      isFirstValidate: true,
      isDeepTarget: true,
      typeLists: [
        {
          id: 'get',
          name: 'GET',
        },
      ],
      timeLists: [],
      time: {
        range: '',
      },
      params: {
        scope: {
          url: `http://www.qq.com/bk?start=<start>&end=<end>`,
          method: 'get',
          time_format: '',
          time_range: '',
        },
      },
      validate: {
        url: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
        time_format: {
          regs: { required: true, type: 'string', error: window.$t('不能为空') },
          content: '',
          visible: false,
          class: 'error-red',
        },
      },
    };
  },
  watch: {
    params: {
      handler(newVal) {
        !this.isFirstValidate && this.validateForm(null, false);
      },
      deep: true,
    },
    'params.scope': {
      deep: true,
      immediate: true,
      handler(val, oldVal) {
        const start = this.getTimeRange(val.url);
        const end = this.getTimeRange(val.url, /[\?|&]([_a-zA-Z0-9]*)=<end>/gi);
        val.time_range = new Array([start, end]).filter(v => v !== '').join(',');
        const timeFormat = this.timeLists.find(t => t.id === val.time_format) || {};
        Bus.$emit('access.http', {
          url: val.url || '',
          timeUnit: timeFormat.format_unit,
          timeFormat: timeFormat.name,
        });
      },
    },
  },
  methods: {
    handleFormatSelected(val) {
      this.validate['time_format'].visible = false;
      Bus.$emit('access.http.timeformat', val);
    },
    validateForm(validateFunc, isSubmit = true) {
      let isvalidate = true;
      if (isSubmit) {
        this.isFirstValidate = false;
      }
      isvalidate = validateScope(this.params.scope, this.validate);
      if (!isvalidate) {
        this.$forceUpdate();
      }
      return isvalidate;
    },
    getTimeRange(url, reg = /[\?|&]([_a-zA-Z0-9]*)=<start>/gi) {
      const matchs = reg.exec(url);
      let timeRange = '';
      if (matchs && matchs.length === 2) {
        timeRange = matchs[1];
      }

      return timeRange;
    },
    initDateFormat() {
      getDateFormat().then(res => {
        if (res.result) {
          this.timeLists = res.data;
        }
      });
    },
    formatFormData() {
      return [
        {
          group: 'access_conf_info',
          identifier: 'resource',
          data: {
            scope: [this.params.scope],
          },
        },
        {
          group: 'access_conf_info',
          identifier: 'collection_model',
          data: { time_format: this.params.scope.time_format, increment_field: this.params.scope.time_range },
        },
      ];
    },
    renderData(data) {
      data['access_conf_info']['resource']['scope'] &&
        Object.assign(this.params.scope, data['access_conf_info']['resource']['scope'][0]);
      this.params.scope['time_format'] = data['access_conf_info']['collection_model']['time_format'];
    },
  },
  created() {
    this.initDateFormat();
  },
  mounted() {
    Bus.$on('Access-httpExample-DebugValidate1', validate => {
      this.$set(this.validate.time_format, 'visible', !!validate.visible);
      this.$set(this.validate.time_format, 'content', validate.content);
      this.$forceUpdate();
    });
  },
};
</script>
<style lang="scss">
.access-http {
  .url-tips {
    position: relative;
    display: flex;
  }
  .url-icon {
    position: absolute;
    right: -25px;
    margin-left: 10px;
    line-height: 30px;
    cursor: pointer;
  }
  .type-lists {
    width: 100px;
    height: 30px;
    .bkdata-selector-input {
      border-left: 0;
    }
  }
}
</style>
