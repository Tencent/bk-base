

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
  <div class="newDataID">
    <div>
      <div class="bk-form-item sideslider-select">
        <label class="bk-label">{{ $t('选择数据类型') }}</label>
        <div class="bk-form-content">
          <bkdata-selector
            :list="dataType"
            :selected.sync="defaultDemo.dataTypeSelected"
            :settingKey="'name'"
            @item-selected="dataTypeSelected" />
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('业务列表') }}</label>
        <div class="bk-form-content">
          <bkdata-input class="bk-form-input data-set"
            name="validation_name"
            :placeholder="$t('英文标识_如login')" />
        </div>
        <div class="bk-form-tip">
          <span class="bk-badge">
            <i class="bk-icon icon-info-tip" />
          </span>
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('数据标识') }}</label>
        <div class="bk-form-content">
          <bkdata-input class="bk-form-input data-set"
            name="validation_name"
            :placeholder="$t('英文标识_如login')" />
        </div>
        <div class="bk-form-tip">
          <span class="bk-badge">
            <i class="bk-icon icon-info-tip" />
          </span>
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('数据描述') }}</label>
        <div class="bk-form-content">
          <bkdata-input class="bk-form-input data-desc"
            :placeholder="$t('描述该数据的作用')" />
        </div>
        <div class="bk-form-tip">
          <span class="bk-badge">
            <i class="bk-icon icon-info-tip" />
          </span>
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('数据标签') }}</label>
        <div class="bk-form-content">
          <bkdata-input class="bk-form-input data-tags"
            :placeholder="'login/' + $t('大区')" />
        </div>
        <div class="bk-form-tip">
          <span class="bk-badge">
            <i class="bk-icon icon-info-tip" />
          </span>
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('是否敏感') }}</label>
        <div class="bk-form-content">
          <label class="bk-form-radio bk-radio-small">
            <input type="radio"
              name="radio1"
              class="data-sensitive"
              checked="checked"
              value="1">
            <i class="bk-radio-text">{{ $t('敏感数据') }}</i>
          </label>
          <label class="bk-form-radio bk-radio-small">
            <input type="radio"
              name="radio1"
              class="data-sensitive"
              value="2">
            <i class="bk-radio-text">{{ $t('非敏感数据') }}</i>
          </label>
        </div>
      </div>
      <div class="bk-form-item">
        <label class="bk-label">{{ $t('日志路径') }}</label>
        <div class="bk-form-content">
          <bkdata-input class="bk-form-input data-log"
            placeholder="/data/home/user00/abc.log" />
        </div>
        <div class="bk-form-tip">
          <span class="bk-badge">
            <i class="bk-icon icon-info-tip" />
          </span>
        </div>
      </div>
      <div class="bk-form-item sideslider-select">
        <label class="bk-label">{{ $t('日志频率') }}</label>
        <div class="bk-form-content">
          <bkdata-selector
            :list="logFreq"
            :selected.sync="defaultDemo.dataFreqSelected"
            :settingKey="'name'"
            @item-selected="logFreqSelected" />
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import $ from 'jquery';
export default {
  data() {
    return {
      /* 数据源列表*/
      dataSourceList: [],
      /* 日志频率list */
      logFreq: [],
      /* 下拉列表默认显示第一项 */
      defaultDemo: {
        dataSourceSelected: 0,
        dataTypeSelected: 0,
        dataFreqSelected: 0,
      },
      isShow: false,
      selectedList: '',

      /* 选择源数据传入的参数 */
      params1: {
        node_type: 'rawsource',
        config: {
          data_id: 0,
        },
        from_links: [],
        frontend_info: {},
      },
      /* 获取配置源数据新增数据源的参数 */
      params: {
        couplein_type: '', // 接入类型
        biz_id: 3, // 业务ID
        data_desc: '', // 数据描述
        data_set: '', // 数据标识
        sensitive: 1, // 是否敏感
        log_path: '',
        tags: [],
        file_frequency: 0,
        hosts: [
          {
            ip: '127.0.0.1',
            plat_id: 1,
          },
        ],
      },
      /* 接入数据类型列表 */
      dataType: [
        {
          id: 0,
          name: '',
          data_src_item: '',
        },
      ],
      data: '',
    };
  },
  methods: {},
};
</script>

<style media="screen" scoped>
.bk-label {
  width: 125px;
  padding-right: 20px;
  color: #737987;
}
.bk-form-item {
  width: 442px;
}
</style>
