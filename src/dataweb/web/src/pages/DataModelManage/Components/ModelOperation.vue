

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
  <div class="model-operation-layout">
    <div class="input-demo">
      <bkdata-search-select
        ref="searchSelect"
        v-model="searchSelectData"
        clearable
        class="searchType"
        :placeholder="$t('操作类型、操作对象类别、操作对象、操作者、描述')"
        :showPopoverTagChange="true"
        :wrapZindex="100"
        :data="renderCoditionsData"
        :showCondition="false"
        :remoteMethod="handleRemoteMethod"
        @change="handleSearchSelectChange" />
      <bkdata-date-picker
        v-model="initDateTimeRange"
        class="searchDate"
        :placeholder="$t('请选择')"
        :type="'datetimerange'" />
    </div>
    <div class="tableWrap">
      <bkdata-table
        ref="operationsTable"
        v-bkloading="{ isLoading, opacity: 1, zIndex: 99 }"
        :data="operationData.data"
        :maxHeight="appHeight - 260"
        :rowStyle="{ cursor: 'pointer' }"
        :pagination="operationData.pagination"
        @row-click="handleShowDiffSlider"
        @filter-change="handleFilterChange"
        @page-change="handlePageChange"
        @page-limit-change="handleLimitChange"
        @sort-change="handleSortChange">
        <bkdata-table-column
          :key="generateId('operation_table_column_id')"
          label="操作类型"
          prop="objectOperation"
          columnKey="object_operation"
          :filters="objectOperationFilters"
          :filterMultiple="true"
          :filteredValue="operationFiltered">
          <template slot-scope="{ row }">
            {{ operationMap[row.objectOperation] }}
          </template>
        </bkdata-table-column>
        <bkdata-table-column
          :key="generateId('operation_table_column_id')"
          label="操作对象类别"
          prop="objectType"
          columnKey="object_type"
          :filters="objectTypeFilters"
          :filterMultiple="true"
          :filteredValue="typeFiltered">
          <template slot-scope="{ row }">
            {{ operationMap[row.objectType] }}
          </template>
        </bkdata-table-column>
        <bkdata-table-column label="操作对象"
          prop="actionObject">
          <template slot-scope="{ row }">
            {{ `${row.objectName} (${row.objectAlias})` }}
          </template>
        </bkdata-table-column>
        <bkdata-table-column label="描述"
          prop="description">
          <template slot-scope="{ row }">
            {{ row.description || '--' }}
          </template>
        </bkdata-table-column>
        <bkdata-table-column label="操作者"
          prop="createdBy" />
        <bkdata-table-column label="操作时间"
          prop="createdAt"
          sortable="custom" />
        <bkdata-table-column label="操作"
          prop="operation"
          width="100">
          <template slot-scope="{ row }">
            <bkdata-button theme="primary"
              text
              @click.stop="handleShowDiffSlider(row)">
              {{ $t('详情') }}
            </bkdata-button>
          </template>
        </bkdata-table-column>
      </bkdata-table>
    </div>

    <!-- diff slider -->
    <bkdata-sideslider
      extCls="diff-slider-cls"
      :isShow.sync="diffSlider.isShow"
      :title="$t('变更详情')"
      :quickClose="true">
      <template slot="content">
        <BizDiffContents
          v-bkloading="{ isLoading: diffSlider.isLoading, opacity: 1 }"
          :diff="diffData"
          :newContents="newContents"
          :origContents="origContents"
          :fieldContraintConfigList="fieldContraintConfigList"
          :onlyShowDiff.sync="diffSlider.onlyShowDiff"
          :isLoading.sync="diffSlider.contentLoading"
          :showResult="diffSlider.showResult">
          <template v-if="diffSlider.showResult">
            <div slot="before-verison-desc"
              class="version-wrapper">
              <div class="version-info">
                <span class="version-tag">【{{ $t('源版本') }}】</span>
                <bkdata-select
                  v-model="diffSlider.origVersionId"
                  class="version-selector"
                  :clearable="false"
                  :disabled="diffSlider.contentLoading"
                  @change="handleOrigVersionChange">
                  <template v-for="item in releaseList">
                    <bkdata-option
                      :id="item.versionId"
                      :key="item.versionId + '_before'"
                      :disabled="item.versionId === diffSlider.newVersionId"
                      :name="`${item.createdBy} (${item.createdAt})`" />
                  </template>
                </bkdata-select>
              </div>
              <span v-bk-overflow-tips="{ content: versionLog }"
                class="release-desc text-overflow">
                发布描述：{{ versionLog }}
              </span>
            </div>
            <div slot="after-verison-desc"
              class="version-wrapper">
              <div class="version-info">
                <span class="version-tag">【{{ $t('目标版本') }}】</span>
                <bkdata-select
                  v-model="diffSlider.newVersionId"
                  class="version-selector target-selector"
                  :clearable="false"
                  :disabled="diffSlider.contentLoading"
                  @change="handleNewVersionChange">
                  <template v-for="item in releaseList">
                    <bkdata-option
                      :id="item.versionId"
                      :key="item.versionId + '_after'"
                      :disabled="item.versionId === diffSlider.origVersionId"
                      :name="`${item.createdBy} (${item.createdAt})`" />
                  </template>
                </bkdata-select>
              </div>
              <span v-bk-overflow-tips="{ content: newVersionLog }"
                class="release-desc text-overflow">
                发布描述：{{ newVersionLog }}
              </span>
            </div>
          </template>
          <template v-else>
            <span slot="before-verison-desc">【变更前】{{ versionInfo }}</span>
            <span slot="after-verison-desc">【变更后】{{ newVersionInfo }}</span>
          </template>
        </BizDiffContents>
      </template>
    </bkdata-sideslider>
  </div>
</template>

<script lang="ts" src="./ModelOperation.ts"></script>

<style lang="scss" scoped>
.model-operation-layout {
  background: #f5f6fa;
  margin: 0 auto;
  .input-demo {
    padding: 30px 0 16px 24px;
    display: flex;
    .searchType {
      width: 520px;
      background: #fff;
    }
    .searchDate {
      width: 300px;
      margin-left: 10px;
    }
  }
  .tableWrap {
    margin: 0 24px;
    background-color: #fff;
  }
  .diff-slider-cls {
    ::v-deep .bk-sideslider-wrapper {
      min-width: 1200px;
      max-width: 2000px;
      width: 80% !important;
    }
  }
}
::v-deep .diff-wrapper {
  margin-top: 10px !important;
}
.version-wrapper {
  padding: 10px 2px;
  .version-info {
    position: relative;
  }
  .version-tag {
    position: absolute;
    top: 48%;
    left: 10px;
    transform: translateY(-50%);
    z-index: 1;
  }
  .version-selector {
    background-color: #fafbfd;
    border-color: transparent;
    margin-bottom: 10px;
    &.is-focus {
      border-color: #3a84ff;
      background-color: #ffffff;
    }
    &.target-selector {
      ::v-deep &.is-default-trigger.is-unselected:before {
        left: 94px;
      }
      ::v-deep .bk-select-name {
        padding-left: 94px;
      }
    }
    ::v-deep &.is-default-trigger.is-unselected:before {
      left: 80px;
    }
    ::v-deep .bk-select-name {
      padding-left: 80px;
    }
  }
  .release-desc {
    font-size: 12px;
    padding-left: 18px;
  }
}
</style>
