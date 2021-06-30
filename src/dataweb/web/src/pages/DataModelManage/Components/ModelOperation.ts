/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE
 */

import { Component, Ref, Watch } from 'vue-property-decorator';
import {
  operationRecords,
  getOperators,
  getOperationDiff,
  getFieldContraintConfigs,
  getModelVersionDiff,
  getReleaseList,
} from '../Api/index';
import { DataModelManageBase } from '../Controller/DataModelManageBase';
import moment from 'moment';
import { BizDiffContents } from './Common/index';
import { VNode } from 'vue/types/umd';
import { generateId } from '@/common/js/util';
import { bkOverflowTips } from 'bk-magic-vue';

interface IConditions {
  key: string;
  value: string[];
}
@Component({
  components: { BizDiffContents },
  directives: { bkOverflowTips },
})
export default class ModelOperation extends DataModelManageBase {
  @Ref('searchSelect') public readonly searchSelectNode!: VNode;
  @Ref() public readonly operationsTable!: VNode;

  /**
     * 获取操作记录接口参数
     */
  get apiParams() {
    const params = {
      modelId: this.modelId,
      page: this.operationData.pagination.current,
      pageSize: this.operationData.pagination.limit,
    };
    if (this.startTime && this.endTime) {
      params.startTime = this.startTime;
      params.endTime = this.endTime;
    }
    const conditions = this.getConditions();
    if (conditions.length) {
      params.conditions = conditions;
    }
    if (this.order) {
      params.orderByCreatedAt = this.order;
    }
    return params;
  }

  /**
     * 获取旧版本信息
     */
  get versionInfo() {
    const { createdAt, createdBy } = this.origContents;
    return createdAt && createdBy ? `${createdBy} (${createdAt})` : '';
  }

  /**
     * 获取新版本信息
     */
  get newVersionInfo() {
    const { createdAt, createdBy } = this.newContents;
    return createdAt && createdBy ? `${createdBy} (${createdAt})` : '';
  }

  /**
     * 获取旧版本日志
     */
  get versionLog() {
    const item = this.releaseList.find(item => item.versionId === this.diffSlider.origVersionId);
    return item ? item.versionLog : '--';
  }

  /**
     * 获取新版本日志
     */
  get newVersionLog() {
    const item = this.releaseList.find(item => item.versionId === this.diffSlider.newVersionId);
    return item ? item.versionLog : '--';
  }

  get renderCoditionsData() {
    const renderCoditionsData = [...this.conditionsData];
    for (const item of this.searchSelectData) {
      const id = item.id;
      const conditionIndex = renderCoditionsData.findIndex(condition => condition.id === id);
      if (conditionIndex >= 0) {
        renderCoditionsData.splice(conditionIndex, 1);
      }
    }
    return renderCoditionsData;
  }

  /**
     * 获取操作对象类别已选过滤 values
     */
  get operationFiltered() {
    const item = this.searchSelectData.find(item => item.id === 'object_operation');
    return item ? item.values.map(value => value.id) : [];
  }

  /**
     * 获取类型已选过滤 values
     */
  get typeFiltered() {
    const item = this.searchSelectData.find(item => item.id === 'object_type');
    return item ? item.values.map(value => value.id) : [];
  }

  public generateId = generateId;
  public initDateTimeRange: Array<string> = [];
  public diffData = {};
  public newContents: object = {};
  public origContents: object = {};
  public fieldContraintConfigList: object[] = [];
  public diffSlider: object = {
    isShow: false,
    isLoading: false,
    onlyShowDiff: true,
    showResult: true,
    origVersionId: null,
    newVersionId: null,
    contentLoading: false,
  };
  public releaseList: any[] = [];
  cancelKey: string = generateId('operation_diff_api');
  appHeight: number = window.innerHeight;
  isLoading = false;
  startTime = '';
  endTime = '';
  order = '';
  searchSelectData: any[] = [];
  // 操作 map
  operationMap = {
    create: this.$t('新增'),
    update: this.$t('更新'),
    delete: this.$t('删除'),
    release: this.$t('发布'),
    model: this.$t('数据模型'),
    master_table: this.$t('主表'),
    calculation_atom: this.$t('统计口径'),
    indicator: this.$t('指标'),
  };
  // search-select conditions data
  public conditionsData = [
    {
      name: '操作类型',
      id: 'object_operation',
      multiable: true,
      children: [
        {
          name: '更新',
          id: 'update',
        },
        {
          name: '新增',
          id: 'create',
        },
        {
          name: '删除',
          id: 'delete',
        },
        {
          name: '发布',
          id: 'release',
        },
      ],
    },
    {
      name: '操作对象类型',
      id: 'object_type',
      multiable: true,
      children: [
        {
          name: '数据模型',
          id: 'model',
        },
        {
          name: '主表',
          id: 'master_table',
        },
        {
          name: '统计口径',
          id: 'calculation_atom',
        },
        {
          name: '指标',
          id: 'indicator',
        },
      ],
    },
    {
      name: '操作对象',
      id: 'object',
    },
    {
      name: '操作者',
      id: 'created_by',
      remote: true,
    },
  ];
  public objectOperationFilters: any[] = [
    { text: '新增', value: 'create' },
    { text: '删除', value: 'delete' },
    { text: '更新', value: 'update' },
    { text: '发布', value: 'release' },
  ];
  public objectTypeFilters: any[] = [
    { text: '数据模型', value: 'model' },
    { text: '主表', value: 'master_table' },
    { text: '统计口径', value: 'calculation_atom' },
    { text: '指标', value: 'indicator' },
  ];
  public operationData = {
    data: [],
    pagination: {
      current: 1,
      count: 0,
      limit: 10,
    },
  };

  @Watch('initDateTimeRange')
  onChangeValue(newVal: any[], oldVal: any[]) {
    if (newVal === oldVal) return;
    if (this.initDateTimeRange[0] && this.initDateTimeRange[1]) {
      this.startTime = moment(newVal[0]).format('YYYY-MM-DD HH:mm:ss');
      this.endTime = moment(newVal[1]).format('YYYY-MM-DD HH:mm:ss');
    } else {
      this.startTime = '';
      this.endTime = '';
    }
    this.handlePageChange(1);
  }

  @Watch('searchSelectData', { deep: true })
  handleChangeSearchSelectData() {
    this.handlePageChange(1);
  }

  /**
     * 处理过滤条件
     */
  public getConditions() {
    if (!this.searchSelectData.length) return [];
    const conditions = {
      object_operation: [],
      object_type: [],
      created_by: [],
      object: [],
      query: [],
    };
    for (const item of this.searchSelectData) {
      const key = item.id;
      if (conditions[key]) {
        const values = item.values.map(value => value.id);
        conditions[key].push(...values);
      } else {
        conditions.query.push(item.name);
      }
    }
    const res: IConditions[] = [];
    // 去重，获取结果
    Object.keys(conditions).forEach(key => {
      const value = Array.from(new Set(conditions[key]));
      value.length && res.push({ key, value });
    });
    return res;
  }

  /**
     * 远程加载操作者过滤条件
     */
  public handleRemoteMethod() {
    return getOperators(this.modelId).then(res => {
      if (res.result) {
        const list = res.data.results || [];
        return list.map((operator: string) => ({ id: operator, name: operator }));
      }
      return [];
    });
  }

  /**
     * 处理 table filters 事件
     * @param filters
     */
  public handleFilterChange(filters) {
    const nameMap = {
      object_operation: '操作类型',
      object_type: '操作对象类别',
    };
    const columnKeys = Object.keys(filters);
    for (const columnKey of columnKeys) {
      const keys = filters[columnKey];
      const values = keys.reduce((res, key) => res.concat([{ id: key, name: this.operationMap[key] }]), []);
      const index = this.searchSelectData.findIndex(item => item.id === columnKey);
      if (index >= 0) {
        if (!keys.length) {
          this.searchSelectData.splice(index, 1);
        } else {
          const item = this.searchSelectData[index];
          item.values = values;
          this.searchSelectData.splice(index, 1, item);
        }
      } else {
        this.searchSelectData.push({
          id: columnKey,
          name: nameMap[columnKey],
          multiable: true,
          values,
        });
      }
    }
  }

  /**
     * 处理翻页
     * @param page
     */
  public handlePageChange(page: number) {
    this.operationData.pagination.current = page;
    this.operationRecords();
  }

  /**
     * 处理每页显示数量
     * @param limit
     */
  public handleLimitChange(limit: number) {
    this.operationData.pagination.limit = limit;
    this.handlePageChange(1);
  }

  /**
     * 处理 table 排序
     */
  public handleSortChange({ order }) {
    const map = {
      ascending: 'asc',
      descending: 'desc',
    };
    this.order = map[order];
    this.handlePageChange(1);
  }

  /**
     * 处理详情源版本选择
     * @param id
     */
  public handleOrigVersionChange(id: string) {
    this.diffSlider.origVersionId = id;
    this.diffSlider.contentLoading = true;
    this.getModelVersionDiff(id, this.diffSlider.newVersionId);
  }

  /**
     * 处理详情新版本选择
     * @param id
     */
  public handleNewVersionChange(id: string) {
    this.diffSlider.newVersionId = id;
    this.diffSlider.contentLoading = true;
    this.getModelVersionDiff(this.diffSlider.origVersionId, id);
  }

  /**
     * 展示操作记录 diff
     * @param row
     */
  public handleShowDiffSlider(row: object) {
    /** 设置Cancel请求 */
    this.handleCancelRequest(this.cancelKey);

    this.diffSlider.showResult = row.objectOperation === 'release';
    this.diffSlider.onlyShowDiff = true;
    this.diffSlider.origVersionId = row.origVersionId;
    this.diffSlider.newVersionId = row.newVersionId;
    this.diffSlider.isShow = true;
    this.diffSlider.isLoading = true;
    row.objectOperation === 'release'
      ? this.getModelVersionDiff(row.origVersionId, row.newVersionId)
      : this.getOperationDiff(row.id);
  }

  /**
     * 获取操作记录列表
     */
  public operationRecords() {
    this.isLoading = true;
    const { modelId, page, pageSize, conditions, startTime, endTime, orderByCreatedAt } = this.apiParams;
    operationRecords(modelId, page, pageSize, conditions, startTime, endTime, orderByCreatedAt)
      .then(res => {
        res.setDataFn(data => {
          const { results, count } = data;
          this.operationData.data = results;
          this.operationData.pagination.count = count;
        });
      })
      ['finally'](() => {
        this.isLoading = false;
      });
  }

  /**
     * 获取值约束条件
     */
  public getFieldContraintConfigs() {
    getFieldContraintConfigs().then(res => {
      res.setData(this, 'fieldContraintConfigList');
    });
  }

  /**
     * 模型发布 diff
     * @param origVersionId
     * @param newVersionId
     */
  public getModelVersionDiff(origVersionId: string | null, newVersionId: string) {
    getModelVersionDiff(this.modelId, origVersionId, newVersionId, this.getCancelToken(this.cancelKey))
      .then(res => {
        res.setDataFn(data => {
          const { diff = {}, newContents = {}, origContents = {} } = data || {};
          this.diffData = diff;
          this.newContents = newContents;
          this.origContents = origContents;
        });
      })
      ['finally'](() => {
        this.diffSlider.isLoading = false;
        this.diffSlider.contentLoading = false;
      });
  }

  /**
     * 除发布外的操作 diff
     * @param operationId
     */
  public getOperationDiff(operationId: number) {
    getOperationDiff(this.modelId, operationId, this.getCancelToken(this.cancelKey))
      .then(res => {
        res.setDataFn(data => {
          const { diff = {}, newContents = {}, origContents = {} } = data || {};
          this.diffData = diff;
          this.newContents = newContents;
          this.origContents = origContents;
        });
      })
      ['finally'](() => {
        this.diffSlider.isLoading = false;
      });
  }

  /**
     * 获取版本列表
     */
  public getReleaseList() {
    getReleaseList(this.modelId).then(res => {
      res.setDataFn(data => {
        this.releaseList = data.results || [];
      });
    });
  }

  /**
     * 过滤条件改变
     * @param list
     */
  public handleSearchSelectChange(list: any[]) {
    // search-select 没有 conditions 后取消显示 menu popper
    this.$nextTick(() => {
      !this.renderCoditionsData.length && this.searchSelectNode && this.searchSelectNode.hidePopper();
    });
  }

  /**
     * 重置高度
     */
  public handleResetHeight() {
    this.appHeight = window.innerHeight;
  }

  public async created() {
    window.addEventListener('resize', this.handleResetHeight);
    this.operationRecords();
    this.getFieldContraintConfigs();
    await this.getReleaseList();
  }

  public beforeDestroy() {
    window.removeEventListener('resize', this.handleResetHeight);
  }
}
