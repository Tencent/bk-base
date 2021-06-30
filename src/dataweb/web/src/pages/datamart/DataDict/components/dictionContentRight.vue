

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
  <!-- 数据源tab有生命周期，但只要存储列表
    tdw，tab不要生命周期
-->
  <div class="dict-detail-content">
    <bkdata-tab :active.sync="active">
      <bkdata-tab-panel v-bind="rightTabPanels.basicInfo">
        <div class="wrap">
          <div class="diction-content-right bk-scroll-y">
            <div v-if="!$route.query.tdw_table_id"
              class="block-item">
              <div class="block-name">
                {{ $t('数据趋势') }}
              </div>
              <data-trend :id="chartId" />
            </div>
            <div v-if="$route.query.dataType === 'result_table' || $route.query.dataType === 'tdw_table'"
              class="block-item">
              <div class="block-name">
                {{ $t('字段信息') }}
              </div>
              <field-info :resultTableInfo="resultTableInfo"
                @changeFieldDes="changeFieldDes" />
            </div>
            <div v-if="!$route.query.tdw_table_id"
              class="block-item">
              <div class="block-name">
                {{ $route.query.dataType === 'result_table' ? $t('关联任务') : $t('清洗任务') }}
              </div>
              <use-task :resultTableInfo="resultTableInfo" />
            </div>
            <!-- 结果表数据预览 -->
            <div v-if="!isShowRecentData && resultTableInfo.can_search"
              class="block-item">
              <div class="block-name look-more-wrap">
                <div class="preview-text-wrap">
                  <span class="title"> {{ $t('数据预览') }}</span><span :title="dataPreviewError"
                    class="error-text text-overflow">
                    {{ dataPreviewError }}
                  </span>
                  <bkdata-popover v-if="failedError && failedError.error"
                    placement="top"
                    :theme="'light'">
                    <a href="jacascript:;">{{ $t('详情') }}</a>
                    <div slot="content"
                      style="white-space: normal">
                      <div class="bk-text-danger error-tooltips-detail bk-scroll-y pt10 pb5 pl10 pr10">
                        {{ failedError.error }}
                      </div>
                    </div>
                  </bkdata-popover>
                </div>

                <div v-if="resultTableInfo.has_permission || !isShowRecentData"
                  class="fr">
                  <bkdata-button :theme="'primary'"
                    :title="$t('查询更多数据')"
                    @click="lookMore">
                    {{ $t('查询更多数据') }}
                  </bkdata-button>
                </div>
              </div>
              <data-preview :resultTableInfo="resultTableInfo"
                :isShowPermission="isShowPermission"
                :isPermissionProcessing="isPermissionProcessing"
                :hasPermission="hasPermission"
                @applyPermission="applyPermission"
                @requestError="requestError" />
            </div>
            <!-- 数据源数据预览 -->
            <div v-if="isShowRecentData"
              class="block-item">
              <div class="block-name">
                <div class="preview-text-wrap">
                  <span class="title"> {{ $t('数据预览') }}</span><span :title="dataPreviewError"
                    class="error-text text-overflow">
                    {{ dataPreviewError }}
                  </span>
                  <bkdata-popover v-if="failedError && failedError.error"
                    placement="top"
                    :theme="'light'">
                    <a href="jacascript:;">{{ $t('详情') }}</a>
                    <div slot="content"
                      style="white-space: normal">
                      <div class="bk-text-danger error-tooltips-detail bk-scroll-y pt10 pb5 pl10 pr10">
                        {{ failedError.error }}
                      </div>
                    </div>
                  </bkdata-popover>
                </div>
              </div>
              <data-trend :id="chartId"
                :tail="true"
                :isShowPermission="isShowPermission"
                :hasPermission="hasPermission"
                :isPermissionProcessing="isPermissionProcessing"
                :isShowRecentData="isShowRecentData"
                @requestError="requestError" />
            </div>
            <PermissionApplyWindow v-if="isApplyPermissionShow"
              ref="apply" />
          </div>
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel v-if="$route.query.dataType !== 'tdw_table'"
        v-bind="rightTabPanels.blood">
        <div class="blood-container">
          <div class="blood-wrap">
            <BloodAnalysis v-if="active === 'blood'" />
          </div>
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel v-if="$modules.isActive('data_quality') && $route.query.dataType === 'result_table'"
        v-bind="rightTabPanels.dataQuality">
        <div class="blood-container">
          <dataQuality :processingType="processingType" />
        </div>
      </bkdata-tab-panel>
      <bkdata-tab-panel v-if="$route.query.dataType !== 'tdw_table'"
        v-bind="rightTabPanels.lifeCycle">
        <DataLifeCycle v-bind="$attrs"
          :resultTableInfo="resultTableInfo" />
        <!-- <div class="life-cycle-container"> -->
        <!-- <StatisticsChart v-if="isShowStandard"
                        :correctHeatScore="resultTableInfo.heat_score || 0"
                        :correctRangeScore="resultTableInfo.range_score || 13.1"
                    />
                    <StorageList
                        @refreshRemovalList="refreshRemovalList"
                        v-if="$route.query.dataType !== 'raw_data'"
                    />
                    <DataSimilarity
                        v-if="$route.query.dataType === 'result_table'"
                    /> -->
        <!-- <DataRemoval :details="removalDetails" v-if="$route.query.dataType !== 'raw_data'" /> -->
        <!-- <DataRemovalList
                        ref="removalList"
                        v-if="
                            $route.query.dataType !== 'raw_data' &&
                            isShowStandard
                        "
                    /> -->
        <!-- </div> -->
      </bkdata-tab-panel>
    </bkdata-tab>
  </div>
</template>
<script>
import dataTrend from './children/DataTrend';
import fieldInfo from './children/FieldInfo';
import useTask from './children/UseTask';
import dataPreview from './children/DataPreview';
import PermissionApplyWindow from '@/pages/authCenter/permissions/PermissionApplyWindow';
import DataLifeCycle from './../DataLifeCycle/DataLifeCycle.index';

export default {
  components: {
    dataTrend,
    fieldInfo,
    useTask,
    dataPreview,
    PermissionApplyWindow,
    BloodAnalysis: () => import('@/pages/datamart/bloodAnalysis'),
    DataQuality: () => import('@/pages/datamart/DataDict/DataQuality'),
    DataLifeCycle,
  },
  props: {
    hasPermission: {
      type: Boolean,
      default: false,
    },
    isPermissionProcessing: {
      type: Boolean,
      dafault: false,
    },
    resultTableInfo: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {
      chartId: '',
      isShowPermission: false,
      isApplyPermissionShow: false,
      dataPreviewError: '',
      failedError: null,
      rightTabPanels: {
        basicInfo: {
          name: 'basicInfo',
          label: this.$t('基本信息'),
        },
        blood: {
          name: 'blood',
          label: this.$t('血缘分析'),
        },
        dataQuality: {
          name: 'dataQuality',
          label: this.$t('数据质量'),
        },
        lifeCycle: {
          name: 'lifeCycle',
          label: this.$t('生命周期'),
        },
      },
      active: 'basicInfo',
      removalDetails: {},
      isShowStandard: true,
    };
  },
  computed: {
    isShowRecentData() {
      return this.$route.query.dataType === 'raw_data';
    },
    fieldList() {
      return this.resultTableInfo.fields || [];
    },
    isShowDataQuality() {
      return this.$modules.isActive('data_quality') && this.$route.query.dataType === 'result_table';
    },
    processingType() {
      return this.resultTableInfo.processing_type;
    },
  },
  watch: {
    resultTableInfo(val) {
      if (Object.keys(val).length) {
        this.isShowPermission = true;
      }
    },
    active(val) {
      if (val === 'lifeCycle' && this.$route.query.dataType === 'result_table') {
        this.getRemovalDetails();
      }
    },
  },
  created() {
    if (this.$route.query.dataType === 'result_table') {
      this.chartId = this.$route.query.result_table_id;
    } else {
      this.chartId = this.$route.query.data_id;
    }
    if (!this.$modules.isActive('lifecycle')) {
      this.isShowStandard = false;
    }
  },
  methods: {
    refreshRemovalList() {
      this.$refs.removalList.getDataRemovalList();
    },
    getRemovalDetails() {
      // this.bkRequest.httpRequest('dataDict/modifyTdwFieldDes', options).then(res => {
      //     if (res.result) {
      //     } else {
      //         this.getMethodWarning(res.message, res.code)
      //     }
      // })
    },
    changeFieldDes(value, rowData) {
      const options = {
        params: {
          cluster_id: this.resultTableInfo.cluster_id,
          db_name: this.resultTableInfo.db_name,
          table_name: this.resultTableInfo.table_name,
          ddls: [
            {
              col_name: this.resultTableInfo.cols_info[rowData.col_index].col_name,
              col_index: rowData.col_index,
              col_comment: value,
            },
          ],
          bk_username: this.$store.getters.getUserName,
        },
      };
      this.bkRequest.httpRequest('dataDict/modifyTdwFieldDes', options).then(res => {
        if (res.result) {
          if (res.data === 'ok') {
            this.$emit('changeFieldDes', value, rowData.col_index);
          }
        } else {
          this.getMethodWarning(res.message, res.code);
        }
      });
    },
    requestError(data) {
      this.dataPreviewError = data.message;
      if (data.errors) {
        this.failedError = data.errors;
      }
    },
    lookMore() {
      this.$router.push({
        path: `/data-access/query/${this.$route.query.bk_biz_id}/${this.$route.query.result_table_id}/`,
      });
    },
    applyPermission() {
      let roleId, id;
      if (this.$route.query.dataType === 'raw_data') {
        roleId = 'raw_data.viewer';
        id = this.$route.query.data_id;
      } else if (this.$route.query.dataType === 'result_table') {
        roleId = 'result_table.viewer';
        id = this.$route.query.result_table_id;
      }
      this.isApplyPermissionShow = true;
      this.$nextTick(() => {
        this.$refs.apply
          && this.$refs.apply.openDialog({
            data_set_type: this.$route.query.dataType,
            bk_biz_id: this.$route.query.bk_biz_id,
            data_set_id: id,
            roleId: roleId,
          });
      });
    },
  },
};
</script>
<style lang="scss" scoped>
::v-deep .bk-tab-section {
  padding: 0;
}
.dict-detail-content {
  width: calc(100% - 400px);
  min-width: 1250px;
  .wrap {
    width: calc(100% - 400px);
    padding: 5px 5px 20px;
    min-width: 1250px;
    .diction-content-right {
      padding: 0 15px;
      .block-item {
        .block-name {
          height: 55px;
          line-height: 55px;
          font-size: 15px;
          font-weight: 700;
          .data-preview {
            font-size: 12px;
            font-weight: normal;
            span {
              color: #3a84ff;
              cursor: pointer;
            }
          }
          .preview-text-wrap {
            width: 80%;
            display: flex;
            align-items: center;
            .title {
              white-space: nowrap;
            }
            .error-text {
              color: red;
              display: inline-block;
            }
          }
        }
        .look-more-wrap {
          overflow: hidden;
          display: flex;
          align-items: flex-end;
          justify-content: space-between;
        }
      }
    }
  }
  .life-cycle-container {
    padding: 5px 5px 20px;
    .type {
      height: 55px;
      line-height: 55px;
      padding: 0 15px;
      font-weight: 700;
      &::before {
        content: '';
        width: 2px;
        height: 19px;
        background: #3a84ff;
        display: inline-block;
        margin-right: 15px;
        position: relative;
        top: 4px;
      }
    }
  }
  .blood-container {
    position: relative;
    min-height: 800px;
    .blood-wrap {
      position: absolute;
      left: 0;
      right: 0;
      top: 0;
      bottom: 0;
    }
  }
}
</style>
