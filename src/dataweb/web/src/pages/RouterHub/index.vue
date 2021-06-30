

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
  <div v-bkloading="{ isLoading: loading, title: '地址解析中' }"
    class="route-hub-container">
    {{ responseMsg }}
  </div>
</template>

<script>
export default {
  data() {
    return {
      loading: false,
      responseMsg: '',
      rtid: '',
    };
  },
  mounted() {
    const rtid = this.$route.query.rtid || this.$route.params.rtid;
    if (!rtid) {
      this.loading = false;
      this.responseMsg = '';
    } else {
      this.responseMsg = '';
      this.resolveRtidTarget(rtid);
    }
  },
  methods: {
    resolveRtidTarget(resultId) {
      if (this.loading) return;
      this.loading = true;
      const opt = {
        params: {
          rtid: resultId,
        },
      };
      this.bkRequest
        .httpRequest('meta/getResultTablesStorages', opt)
        .then(res => {
          if (res.result) {
            if (res.data && res.data.processing_type === 'clean') {
              const instanceOption = {
                params: {
                  processing_id: res.data.data_processing.processing_id,
                },
              };
              this.bkRequest
                .httpRequest('meta/getProcessingInstance', instanceOption)
                .then(res => {
                  if (res.result) {
                    const dataid = (res.data.inputs && res.data.inputs[0] && res.data.inputs[0].data_set_id) || 0;
                    if (dataid) {
                      this.$router.replace(`/data-access/edit-clean/${dataid}/${resultId}`);
                    } else {
                      this.responseMsg = '获取DataID失败';
                      this.getMethodWarning('获取DataID失败', 404);
                    }
                  } else {
                    this.responseMsg = `${res.code}: ${res.message}`;
                    this.getMethodWarning(res.message, res.code);
                  }
                })
                ['finally'](_ => {
                  this.loading = false;
                });
            } else {
              // getStorageInfo
              this.bkRequest
                .httpRequest('dataFlow/getProcessingNodes', {
                  query: {
                    result_table_id: resultId,
                  },
                })
                .then(res => {
                  if (res.result) {
                    if (res.data && res.data[0]) {
                      const task = res.data[0] || {};
                      this.$router.replace({
                        name: 'dataflow_ide',
                        params: {
                          fid: task.flow_id,
                        },
                        query: {
                          NID: task.node_id,
                        },
                      });
                    } else {
                      this.responseMsg = '任务不存在';
                      this.getMethodWarning(this.$t('任务不存在'), 'error');
                    }
                  } else {
                    this.responseMsg = `${res.code}: ${res.message}`;
                    this.getMethodWarning(res.message, res.code);
                  }
                });
            }
          } else {
            this.responseMsg = `${res.code}: ${res.message}`;
            this.getMethodWarning(res.message, res.code);
          }
        })
        ['finally'](_ => {
          this.loading = false;
        });
    },
  },
};
</script>
<style lang="scss">
.route-hub-container {
  display: flex;
  justify-content: center;
  align-items: center;
  width: 100%;
  min-height: 600px;
  height: auto;
  font-size: 20px;
}
</style>
