

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
  <tr v-if="is_show">
    <th>
      {{ config[arg_name] }}
      <i class="fa fa-question-circle f20 fr"
        aria-hidden="true"
        data-toggle="tooltip"
        :title="config.description" />
    </th>
    <td :class="{ 'has-error': has_error }">
      <div class="sample-box">
        <div class="sample-select">
          <bkdata-selector
            :selected.sync="selected_sample"
            :searchable="true"
            :list="cleanedSamples"
            :settingKey="'id'"
            :displayKey="'text'"
            :searchKey="'text'" />
        </div>
        <div v-if="selected_sample_display"
          class="king-instruction king-instruction-info mt10">
          {{ selected_sample_display }}
        </div>
      </div>
      <div class="text-danger mt5">
        {{ errMsg }}
      </div>
    </td>
  </tr>
</template>

<script>
/* eslint-disable */
import { JValidator } from '@/common/js/check';
import mixin from './prop.mixin.js';
export default {
  mixins: [mixin],
  data() {
    return {
      samples: [],
      selected_sample: '',
      errMsg: '',
      is_training_serving_same_source: false,
      trained_reused_instance: 0,
    };
  },
  watch: {
    selected_sample(prev, cur) {
      this.check();
    },
  },
  computed: {
    is_show() {
      return this.trained_reused_instance == 0 && !this.is_training_serving_same_source;
    },
    has_error() {
      return this.errMsg.length > 0;
    },
    selected_sample_info: function() {
      for (var i = 0; i < this.samples.length; i++) {
        if (this.samples[i].sample_db_name == this.selected_sample) {
          return this.samples[i];
        }
      }
      return null;
    },
    selected_sample_display() {
      if (this.selected_sample_info === null) {
        return '';
      }

      if (this.selected_sample_info.is_dynamic) {
        let name = this.selected_sample_info.sample_db_name;
        let rt_id = this.selected_sample_info.result_table_id;
        let start = this.selected_sample_info.sample_start_time;
        let end = this.selected_sample_info.sample_end_time;

        return `${name} 样本为用户自定义的动态样本库，取值于 ${rt_id} 从 ${start} 至 ${end} 时间区间内的数据`;
      } else {
        let name = this.selected_sample_info.sample_db_name;
        return `${name} 样本为平台默认提供的静态样本库`;
      }
    },
    cleanedSamples() {
      return this.samples.map(sam => {
        return {
          id: sam.sample_db_name,
          text: (sam.is_dynamic ? '【动态】' : '【静态】') + `${sam.sample_db_name} (${sam.description})`,
        };
      });
    },
    arg_name() {
      return `arg_${this.$getLocale() === 'en' ? 'en' : 'zh'}_name`;
    },
  },
  methods: {
    async load_samples() {
      this.bkRequest
        .httpRequest('modelFlow/getModeListSample', {
          query: {
            project_id: this.projectId,
          },
        })
        .then(res => {
          if (res.result) {
            this.samples = res.data;
          } else {
            this.getMethodWarning(res.message, res.code);
          }
        });
    },
    set_value(val) {
      this.selected_sample = val;
    },
    get_value() {
      return {
        category: this.category,
        nodeid: this.nodeid,
        arg: this.config.arg_en_name,
        value: this.selected_sample,
      };
    },
    check() {
      if (!this.is_show) {
        return true;
      }

      var rules = { required: true };
      var validator = new JValidator(rules);
      if (!validator.valid(this.selected_sample)) {
        this.errMsg = validator.errMsg;
        return false;
      } else {
        this.errMsg = '';
      }
      return true;
    },
  },
  mounted() {
    var top_this = this;
    top_this.load_samples();

    var sameSourceArg = this.tools().get_arg_config(
      this.configTemplate,
      'input',
      this.nodeid,
      'is_training_serving_same_source'
    );

    if (typeof sameSourceArg.value === 'boolean') {
      this.is_training_serving_same_source = sameSourceArg.value;
    } else {
      this.is_training_serving_same_source = sameSourceArg.value['is_training_serving_same_source'];
    }

    var val = this.tools().get_args_value(top_this.config);
    if (val) {
      top_this.set_value(val);
    }
  },
};
</script>
