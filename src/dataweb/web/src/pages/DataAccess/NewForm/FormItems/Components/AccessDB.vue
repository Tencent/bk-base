

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
    <span class="bk-item-des">{{ $t('多个接入对象的内容必须一致_否则会出错') }}</span>
    <Container>
      <Item>
        <label class="bk-label">{{ $t('DB类型') }}：</label>
        <div class="bk-form-content">
          <bkdata-selector
            v-tooltip.notrigger="validate['db_type']"
            :list="dbLists"
            :selected.sync="params.db_type"
            settingKey="id"
            displayKey="db_type_alias" />
        </div>
      </Item>
      <CollectRange :dbTypeId="params.db_type"
        :bkBizid="bizid"
        :permissionHostList="permissionHostList" />
    </Container>
  </div>
</template>
<script>
import Container from '../ItemContainer';
import Item from '../Item';
import CollectRange from './CollectRange';
import { validateComponentForm } from '../../SubformConfig/validate.js';
import { postMethodWarning } from '@/common/js/util.js';
import mixin from '@/pages/DataAccess/Config/mixins.js';
export default {
  components: {
    Container,
    Item,
    CollectRange,
  },
  mixins: [mixin],
  props: {
    bizid: {
      type: Number,
      default: 0,
    },
    /** 需要申请权限IP列表 */
    permissionHostList: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      isFirstValidate: true,
      dbLists: [
        {
          id: 1,
          db_type_alias: 'MySql',
        },
      ],
      params: {
        db_type: 1,
      },
      validate: {
        db_type: {
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
        !this.isFirstValidate && this.validateComponentForm(this.validate, newVal);
      },
      deep: true,
    },
  },
  mounted() {
    this.getDbTypes();
  },
  methods: {
    renderData(data) {
      const key = 'scope';
      const scope = data['access_conf_info'].resource[key][0];
      this.params.db_type = (scope && scope['db_type_id']) || 1;
    },
    validateForm(validateFunc) {
      this.isFirstValidate = false;
      return validateFunc(this.validate, this.params);
    },
    getDbTypes() {
      this.bkRequest.httpRequest('dataAccess/getDBTypelists').then(res => {
        if (res.result) {
          this.dbLists = res.data;
        } else {
          postMethodWarning('获取数据库类型失败:' + res.message, 'error');
        }
      });
    },
  },
};
</script>
