

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
  <bkdata-dialog
    v-model="isShow"
    width="1200"
    height="900"
    :title="$t('清洗数据导入')"
    :maskClose="false"
    @confirm="importData">
    <div class="contentWrapper">
      <textarea
        v-model="sheetVal"
        :placeholder="$t('将excel表格内容粘贴至本区域')"
        rows="10"
        cols="70"
        @input="formatSheetVal" />
      <bkdata-table :data="data"
        :emptyText="$t('预览内容为空')"
        :height="495">
        <bkdata-table-column :label="$t('key/index')"
          prop="assign_method" />
        <bkdata-table-column :label="$t('关键字')"
          prop="location" />
        <bkdata-table-column :label="$t('字段')"
          prop="assign_to" />
        <bkdata-table-column :label="$t('类型')"
          width="65"
          prop="type" />
        <bkdata-table-column :label="$t('中文名称')"
          prop="field_alias" />
      </bkdata-table>
    </div>
    <div v-show="validata.status"
      class="help-block"
      style="margin-left: 125px"
      v-text="validata.errorMsg" />
  </bkdata-dialog>
</template>
<script>
export default {
  props: {
    header: {
      type: Array,
      default: () => ['assign_method', 'location', 'assign_to', 'type', 'field_alias'],
    },
    innerParams: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      types: ['double', 'int', 'long', 'string', 'text'],
      validata: {
        status: false,
        errorMsg: '',
      },
      isShow: false,
      data: [],
      sheetVal: '',
      output: {},
    };
  },
  watch: {
    innerParams(val) {
      let key = val.assign_method;
      val.fields.forEach(item => {
        let obj = Object.assign({ assign_method: key }, item);
        this.data.push(obj);
      });
    },
  },
  methods: {
    importData() {
      if (this.validata.status) return;
      let key = this.data[0].assign_method;
      let formatData = { assign_method: key };
      formatData.fields = Array.from(this.data);
      formatData.fields.forEach((item, index) => {
        item = JSON.parse(
          JSON.stringify(item, function (key, value) {
            console.log('key', key, key === 'assign_method');
            if (key === 'assign_method') {
              return undefined;
            } else {
              return value;
            }
          })
        );
        formatData.fields[index] = item;
      });
      console.log(formatData);
      this.$emit('importData', formatData);
    },
    formatSheetVal() {
      this.data = [];
      this.validata.status = false;
      const sheetTable = this.sheetVal
        .split(/\n/)
        .map(row => row.split(/\s/))
        .filter(item => item.length !== 1);
      const key = sheetTable[0][0];
      for (let i = 0; i < sheetTable.length; i++) {
        if (sheetTable[i].length !== 5) {
          this.validata.errorMsg = this.$t('请检查数据格式，每行必须有5列');
          this.validata.status = true;
          this.data = [];
          break;
        } else if (this.types.indexOf(sheetTable[i][3]) === -1) {
          this.validata.errorMsg = this.$t('类型必须是“string/long/int/text/double”的一种');
          this.validata.status = true;
          this.data = [];
          break;
        } else if (sheetTable[i][0] !== key || (sheetTable[i][0] !== 'key' && sheetTable[i][0] !== 'index')) {
          this.validata.errorMsg = this.$t('每条数据的关键字必须统一为key或index');
          this.validata.status = true;
          this.data = [];
          break;
        }
        let dataObj = {};
        sheetTable[i].forEach((item, index) => {
          dataObj[this.header[index]] = item;
        });
        this.data.push(dataObj);
      }
    },
  },
};
</script>
<style lang="scss" scoped>
.contentWrapper {
  display: flex;
  max-height: 500px;
  padding: 3px 25px;
  textarea {
    margin-right: 50px;
    resize: none;
  }
  textarea::-webkit-input-placeholder:after {
    display: block;
    content: 'line@ \A line#'; /*  \A 表示换行  */
    color: red;
  }
}
.help-block {
  // width: 100%;
  margin-top: 10px;
  color: red;
}
</style>
