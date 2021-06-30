

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
    v-model="openModal"
    :maskClose="true"
    :headerPosition="discardedDataDialogSetting.headerPosition"
    :width="discardedDataDialogSetting.width"
    :title="discardedDataDialogSetting.title"
    @cancel="close">
    <div class="discardDataTable">
      <bkdata-table v-bkloading="{ isLoading: loading }"
        :data="discardData"
        :emptyText="$t('暂无数据')">
        <bkdata-table-column
          v-for="(item, index) in column"
          :key="index"
          :width="item.width"
          :label="item.noI18n ? item.label : $t(item.label)"
          :showOverflowTooltip="!item.showMore"
          :prop="item.prop">
          <template slot-scope="{ row }">
            <cell-click v-if="item.showMore"
              :content="row[item.prop]" />
            <span v-else>{{ row[item.prop] }}</span>
          </template>
        </bkdata-table-column>
      </bkdata-table>
    </div>
    <div slot="footer"
      class="discardDataTable-footer">
      <div class="bk-default bk-button-normal bk-button"
        @click="close">
        {{ $t('关闭') }}
      </div>
    </div>
  </bkdata-dialog>
</template>

<script>
import cellClick from '@/components/cellClick/cellClick.vue';
import { mapState, mapActions } from 'vuex';
export default {
  components: {
    cellClick,
  },
  inject: ['userName'],
  props: {
    value: {
      type: Boolean,
      default: false,
    },
    rid: {
      type: String,
    },
  },
  data() {
    return {
      // 打开弹窗
      openModal: false,
      // 弹窗设置
      discardedDataDialogSetting: {
        title: window.$t('查看最近丢弃数据'),
        headerPosition: 'left',
        width: '1000px',
      },
      // 加载
      loading: false,
      // 列表项
      column: [
        {
          label: '清洗时间',
          prop: 'timestamp',
          width: '160',
        },
        {
          label: '原始记录',
          prop: 'msg_value',
          showMore: true,
        },
        {
          label: '清洗错误信息',
          prop: 'error',
          showMore: true,
        },
        {
          label: '结果表',
          prop: 'result_table_id',
        },
        {
          label: '分区',
          prop: 'partition',
          width: '80',
        },
        {
          label: '消息key值',
          prop: 'msg_key',
        },
        {
          label: 'kafka_offset',
          prop: 'offset',
          width: '100',
          noI18n: true,
        },
      ],
    };
  },
  computed: {
    ...mapState('accessDetail', ['discardData']),
  },
  watch: {
    value(newValue) {
      this.openModal = newValue;
      newValue && this.getDiscardedData();
    },
  },
  methods: {
    ...mapActions('accessDetail', ['setDiscardData']),
    /**
     *  获取数据
     */
    getDiscardedData() {
      this.loading = true;
      this.bkRequest
        .httpRequest('dataClear/getRawdatas/', {
          params: {
            raw_data_id: this.$route.params.did,
          },
          query: {
            bk_username: this.userName,
            bkdata_authentication_method: 'user',
            bk_app_code: 'data',
          },
        })
        .then(res => {
          if (res.result) {
            const tableId = res.data.filter(item => {
              return item.result_table_id === this.rid;
            });
            this.setDiscardData(tableId);
          } else {
            this.getMethodWarning(res.message, res.code);
          }
          this.loading = false;
        });
    },
    close() {
      this.$emit('input', false);
    },
  },
};
</script>

<style lang="scss" scoped>
.discardDataTable {
  &-footer {
    text-align: right;
  }
}
</style>
