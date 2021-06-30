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

import { serveFormatToWebFormat } from '@/common/js/Utils';
import DiffContents from '@/components/diffContents/DiffContents.vue';
import EmptyView from '@/components/emptyView/EmptyView.vue';
import popContainer from '@/components/popContainer';
import utils from '@/pages/DataGraph/Common/utils.js';
import { bkDiff } from 'bk-magic-vue';
import 'bk-magic-vue/lib/ui/diff.min.css';
import { Component, Emit, Prop, PropSync, Ref, Vue, Watch } from 'vue-property-decorator';
import { DataModelDiff } from '../../Controller/DataModelDiff';

// 获取字段加工 sql
const getCode = (data: object, target: string): string => {
  if (data.isEmpty) {
    return '';
  }
  const { fields = [] } = data;
  const field = fields.find(field => field.fieldName === target);
  return field?.fieldCleanContent?.cleanContent || '';
};

// 获取值约束内容
const getFieldConstraintContent = (data: object, target: string) => {
  if (data.isEmpty) {
    return '';
  }
  const { fields = [] } = data;
  const field = fields.find(field => field.fieldName === target);
  return field?.fieldConstraintContent || '';
};

// 将 diff key value 格式化为驼峰
const diffObjectsformatter = (objects: any[] = []): any[] => {
  for (const item of objects) {
    if (item.diffKeys) {
      item.diffKeys = item.diffKeys.map((value: string) => serveFormatToWebFormat(value));
    }
    if (item.diffObjects) {
      item.diffObjects = diffObjectsformatter(item.diffObjects);
    }
  }
  return objects;
};

// window type 对应展示字段信息
const extraConfigMap = {
  scroll: [
    { label: window.$t('统计频率'), prop: 'schedulingContent.countFreq' },
    { label: window.$t('等待时间'), prop: 'schedulingContent.waitingTime' },
    { label: window.$t('依赖计算延迟数据'), prop: 'schedulingContent.windowLateness.allowedLateness' },
  ],
  slide: [
    { label: window.$t('统计频率'), prop: 'schedulingContent.countFreq' },
    { label: window.$t('窗口长度'), prop: 'schedulingContent.windowTime' },
    { label: window.$t('等待时间'), prop: 'schedulingContent.waitingTime' },
    { label: window.$t('依赖计算延迟数据'), prop: 'schedulingContent.windowLateness.allowedLateness' },
  ],
  accumulate: [
    { label: window.$t('统计频率'), prop: 'schedulingContent.countFreq' },
    { label: window.$t('窗口长度'), prop: 'schedulingContent.windowTime' },
    { label: window.$t('等待时间'), prop: 'schedulingContent.waitingTime' },
    { label: window.$t('依赖计算延迟数据'), prop: 'schedulingContent.windowLateness.allowedLateness' },
  ],
  accumulate_by_hour: [
    { label: window.$t('统计频率'), prop: 'schedulingContent.countFreq' },
    { label: window.$t('统计延迟'), prop: 'schedulingContent.delay' },
    { label: window.$t('窗口起点'), prop: 'schedulingContent.dataStart' },
    { label: window.$t('窗口终点'), prop: 'schedulingContent.dataEnd' },
    { label: window.$t('依赖策略'), prop: 'schedulingContent.unifiedConfig.dependencyRule' },
    { label: window.$t('调度失败重试'), prop: 'schedulingContent.advanced.recoveryEnable' },
  ],
  fixed: [
    { label: window.$t('统计频率'), prop: 'schedulingContent.countFreq' },
    { label: window.$t('统计延迟'), prop: 'schedulingContent.fixedDelay' },
    { label: window.$t('窗口长度'), prop: 'schedulingContent.unifiedConfig.windowSize' },
    { label: window.$t('依赖策略'), prop: 'schedulingContent.unifiedConfig.dependencyRule' },
    { label: window.$t('调度失败重试'), prop: 'schedulingContent.advanced.recoveryEnable' },
  ],
};

@Component({
  components: { DiffContents, bkDiff, EmptyView, popContainer },
})
export default class BizDiffContents extends Vue {
  @PropSync('isLoading', { default: false }) public localIsLoading!: boolean;
  @PropSync('onlyShowDiff', { default: false }) public localOnlyShowDiff!: boolean;
  @Prop({ default: true }) public showResult!: boolean;
  @Prop({ required: true }) public newContents!: object;
  @Prop({ required: true }) public origContents!: object;
  @Prop({ required: true }) public diff!: object;
  @Prop({ default: () => [] }) public fieldContraintConfigList!: any[];

  @Ref() public readonly diffConditions!: popContainer;
  @Ref() public readonly diffRelation!: popContainer;

  get diffObjects() {
    return diffObjectsformatter(this.diff.diffObjects || []);
  }

  get diffResult() {
    const { update: updateNum, delete: deleteNum, create: createNum, hasDiff } = this.diff.diffResult || {};
    return Object.assign({}, new DataModelDiff(), { updateNum, deleteNum, createNum, hasDiff });
  }

  get tableDiffResult() {
    return this.diff.diffResult?.field || {};
  }

  get versionInfo() {
    const { createdAt, createdBy } = this.origContents;
    return createdAt && createdBy ? `${createdBy} (${createdAt})` : '';
  }

  get conditionMap() {
    const map: any = {};
    for (const condition of this.fieldContraintConfigList) {
      map[condition.constraintId] = condition;
      if (condition.children) {
        for (const child of condition.children) {
          child.parentId = condition.constraintId;
          map[child.constraintId] = child;
        }
      }
    }
    return map;
  }

  get newContentObjects() {
    return this.newContents.objects || [];
  }

  get originContentObjects() {
    return this.origContents.objects || [];
  }

  get diffList() {
    return this.resolveContentObject();
  }

  get renderDiffList() {
    if (this.localOnlyShowDiff) {
      return this.diffList.filter(
        (diff: any) => diff.newContent.diffType !== 'default' || diff.originContent.diffType !== 'default'
      );
    }
    return this.diffList;
  }

  /**
     * 获取内容对象数据
     */
  getContentsObjectMap() {
    const newContentsObjectIds = this.newContentObjects.map((item: any) => item.objectId);
    const origContentsObjectIds = this.originContentObjects.map((item: any) => item.objectId);
    const uniqueObjectId = Array.from(new Set([...newContentsObjectIds, ...origContentsObjectIds]));
    const diffObjectsMap = this.diffObjects
      .reduce((map, item) => Object.assign(map, { [item.objectId]: item }), {});
    const newContentsObjectMap = this.newContentObjects
      .reduce((map: any, item: any) => Object.assign(map, { [item.objectId]: item }), {});
    const origContentsObjectMap = this.originContentObjects
      .reduce((map: any, item: any) => Object.assign(map, { [item.objectId]: item }), {});

    return {
      newContentsObjectIds,
      origContentsObjectIds,
      uniqueObjectId,
      diffObjectsMap,
      newContentsObjectMap,
      origContentsObjectMap
    };
  }

  /**
     * 对数据进行格式化处理
     */
  resolveContentObject() {
    const list = [];
    const {
      uniqueObjectId,
      diffObjectsMap,
      newContentsObjectMap,
      origContentsObjectMap
    } = this.getContentsObjectMap();
    for (const id of uniqueObjectId) {
      const newContent = newContentsObjectMap[id];
      const originContent = origContentsObjectMap[id];

      if (!newContent && !originContent) {
        continue;
      }

      const {
        diffInfo,
        type,
        objectType,
        config,
        newCollapseTitle,
        originCollapseTitle
      } = this.resolveDiffInfos(diffObjectsMap, id, newContent, originContent);
      const oTitle = originCollapseTitle;
      const nTitle = newCollapseTitle;
      const defaultItem = { configs: config?.configs || [], type };
      let item = this.resolveObjectItem(newContent, originContent, oTitle, diffInfo, nTitle);
      this.resolveTableField(type, defaultItem, config, item);
      this.resolveDiffTableFields(diffInfo, type, item);
      this.resolveIndicatorField(objectType, item, extraConfigMap, defaultItem);
      list.push(Object.assign(defaultItem, item));
    }
    return list;
  }

  /**
     * 处理 diff 信息
     * @param diffObjectsMap
     * @param id
     * @param newContent
     * @param originContent
     */
  resolveDiffInfos(diffObjectsMap: any, id: any, newContent: any, originContent: any,) {
    const diffInfo = diffObjectsMap[id] || { diffType: 'default' };
    const type = (newContent?.objectType || originContent?.objectType || '')
      .indexOf('table') >= 0 ? 'table' : 'list';
    const objectType = newContent?.objectType || originContent?.objectType;
    const config = objectType ? this.config[objectType] : null;
    const newCollapseTitle = newContent
      ? `【${config.title}】${newContent[config.nameKey]} (${newContent[config.aliasKey]})`
      : '--';
    const originCollapseTitle = originContent
      ? `【${config.title}】${originContent[config.nameKey]} (${originContent[config.aliasKey]})`
      : '--';
    return { diffInfo, type, objectType, config, newCollapseTitle, originCollapseTitle };
  }

  /**
     * 处理新旧内容 diff 信息
     * @param newContent
     * @param originContent
     * @param originCollapseTitle
     * @param diffInfo
     * @param newCollapseTitle
     */
  resolveObjectItem(newCxt: any, originCxt: any, originCollapseTitle: any, diffInfo: any, newCollapseTitle: any) {
    let item = {};
    if (newCxt && originCxt) {
      item = {
        originCxt: Object.assign({ collapseTitle: originCollapseTitle }, originCxt, diffInfo),
        newCxt: Object.assign({ collapseTitle: newCollapseTitle }, newCxt, diffInfo),
      };
    } else if (newCxt && !originCxt) {
      item = {
        originCxt: { isEmpty: true, diffType: 'default', collapseTitle: originCollapseTitle },
        newCxt: Object.assign({ collapseTitle: newCollapseTitle }, newCxt, diffInfo),
      };
    } else if (!newCxt && originCxt) {
      item = {
        originCxt: Object.assign({ collapseTitle: originCollapseTitle }, originCxt, diffInfo),
        newCxt: { isEmpty: true, diffType: 'default', collapseTitle: newCollapseTitle },
      };
    }

    return item;
  }

  /**
     * 处理 table field 信息
     * @param type
     * @param defaultItem
     * @param config
     * @param item
     */
  resolveTableField(type: string, defaultItem: any, config: any, item: any) {
    if (type === 'table') {
      Object.assign(defaultItem, { tableParams: config?.tableParams || {} });
      if (item.newContent.fields?.length) {
        item.newContent.fields = this.tableDataFormatter(item.newContent.fields);
      }
      if (item.originContent.fields?.length) {
        item.originContent.fields = this.tableDataFormatter(item.originContent.fields);
      }
    }
  }

  /**
     * 处理指标变动字段
     * @param objectType
     * @param item
     * @param extraConfigMap
     * @param defaultItem
     */
  resolveIndicatorField(objectType: string, item: any, extraConfigMap: any, defaultItem: any) {
    if (objectType === 'indicator') {
      const newExtraConfigs = item.newContent.isEmpty
        ? []
        : extraConfigMap[item.newContent.schedulingContent?.windowType] || [];
      const originExtraConfigs = item.originContent.isEmpty
        ? []
        : extraConfigMap[item.originContent.schedulingContent?.windowType] || [];
      Object.assign(defaultItem, { newExtraConfigs, originExtraConfigs });
    }
  }

  resolveDiffTableFields(diffInfo: any, type: string, item: any,) {
    // 处理数据 table fields diff 状态
    if (diffInfo.diffType !== 'default' && type === 'table') {
      const fieldDiffObjects = diffInfo.diffObjects || [];
      const newFields = item.newContent.fields || [];
      const originFields = item.originContent.fields || [];
      for (const item of fieldDiffObjects) {
        const newField = newFields.find((field: any) => field.objectId === item.objectId
                    || `model_relation-${field.modelId}-${field.fieldName}` === item.objectId);
        const originField = originFields.find((field: any) => field.objectId === item.objectId
                    || `model_relation-${field.modelId}-${field.fieldName}` === item.objectId);
        this.resolveDiffFieldItem(item, newField, originField);
      }
    }
  }

  resolveDiffFieldItem(item: any, newField: any, originField: any) {
    if (item.objectId.indexOf('model_relation') === 0) {
      if (newField) {
        !newField.diffType && (newField.diffType = 'update');
        newField.diffKeys = [].concat(item.diffKeys || [], newField.diffKeys || []);
      }
      if (originField) {
        !originField.diffType && (originField.diffType = 'update');
        originField.diffKeys = [].concat(item.diffKeys || [], originField.diffKeys || []);
      }
    } else {
      newField && Object.assign(newField, item);
      originField && Object.assign(originField, item);
    }
  }

  public diffConditionsContent = {
    origin: '',
    target: '',
    cls: '',
  };
  public diffRelationContent = {
    origin: {},
    target: {},
    modelCls: '',
    fieldCls: '',
  };
  public activeHoverFieldName: string = '';
  public diffCodeDialog: object = {
    isShow: false,
    title: '',
    subTitle: '',
    originCode: '',
    targetCode: '',
  };
  public fieldCategoryMap: object = {
    measure: this.$t('度量'),
    dimension: this.$t('维度'),
  };
  public dataStartList: any[] = utils.getTimeList();
  public dataEndList: any[] = utils.getTimeList('59');
  public dependencyRule = {
    no_failed: this.$t('无失败'),
    all_finished: this.$t('全部成功'),
    at_least_one_finished: this.$t('一次成功'),
  };
  public config: object = {
    model: {
      title: this.$t('数据模型基本信息'),
      nameKey: 'modelName',
      aliasKey: 'modelAlias',
      configs: [
        { label: this.$t('中文名'), prop: 'modelAlias', resizable: false },
        { label: this.$t('标签'), prop: 'tags', resizable: false },
        { label: this.$t('描述'), prop: 'description', resizable: false },
      ],
    },
    master_table: {
      title: this.$t('主表'),
      nameKey: 'modelName',
      aliasKey: 'modelAlias',
      tableParams: {
        bind: {
          rowClassName: this.rowClassName,
          cellClassName: this.cellClassName,
        },
        on: {
          'row-mouse-enter': this.handleRowMouseEnter,
          'row-mouse-leave': this.handleRowMouseLeave,
        },
      },
      configs: [
        { label: this.$t('字段名'), prop: 'fieldName', resizable: false },
        { label: this.$t('字段中文名'), prop: 'fieldAlias', resizable: false },
        { label: this.$t('数据类型'), prop: 'fieldType', resizable: false },
        { label: this.$t('字段角色'), prop: 'fieldCategory', resizable: false },
        { label: this.$t('字段描述'), prop: 'description', resizable: false },
        { label: this.$t('值约束'), prop: 'fieldConstraintContent', resizable: false },
        { label: this.$t('字段加工逻辑'), prop: 'fieldCleanContent', resizable: false },
        { label: this.$t('主键'), prop: 'isPrimaryKey', width: 70, resizable: false },
      ],
    },
    calculation_atom: {
      title: this.$t('指标统计口径'),
      nameKey: 'calculationAtomName',
      aliasKey: 'calculationAtomAlias',
      configs: [
        { label: this.$t('口径名称'), prop: 'calculationDisplayName' },
        { label: this.$t('聚合逻辑'), prop: 'calculationFormulaStrippedComment' },
        { label: this.$t('字段类型'), prop: 'fieldType' },
        { label: this.$t('描述'), prop: 'description' },
      ],
    },
    indicator: {
      title: this.$t('指标'),
      nameKey: 'indicatorName',
      aliasKey: 'indicatorAlias',
      configs: [
        { label: this.$t('指标英文'), prop: 'indicatorName' },
        { label: this.$t('指标中文'), prop: 'indicatorAlias' },
        { label: this.$t('描述'), prop: 'description' },
        { label: this.$t('指标统计口径'), prop: 'calculationDisplayName' },
        { label: this.$t('聚合字段'), prop: 'aggregationFieldsStr' },
        { label: this.$t('过滤条件'), prop: 'filterFormula' },
        { label: this.$t('计算类型'), prop: 'schedulingType' },
        { label: this.$t('窗口类型'), prop: 'schedulingContent.windowType' },
      ],
    },
  };
  public dataMap: object = {
    schedulingType: {
      stream: this.$t('实时计算'),
      batch: this.$t('离线计算'),
    },
    windowType: {
      scroll: this.$t('滚动窗口'),
      accumulate: this.$t('累加窗口'),
      slide: this.$t('滑动窗口'),
      accumulate_by_hour: this.$t('按小时累加窗口'),
      fixed: this.$t('固定窗口'),
    },
    booleanMap: {
      true: this.$t('是'),
      false: this.$t('否'),
    },
    unitMap: {
      s: this.$t('秒'),
      min: this.$t('分钟'),
      h: this.$t('小时'),
      d: this.$t('天'),
      w: this.$t('周'),
      m: this.$t('月'),
    },
    timeformatter(value: string | number, unit: string | undefined) {
      const empty = [undefined, null, ''];
      if (empty.includes(value)) {
        return '--';
      }
      if (!unit) {
        return value;
      }
      if (unit === 'min') {
        return value + this.unitMap.min;
      }
      const key = unit.charAt(0).toLocaleLowerCase();
      return value + this.unitMap[key];
    },
  };

  /**
     * 格式化值约束渲染的条件
     * @param data
     */
  public formatRenderConditions(data: object) {
    const renderConditions = [];
    const { op: groupCondition, groups } = data;
    if (groups) {
      for (const group of groups) {
        const itemOp = group.op;
        const groupItem = {
          op: groupCondition,
          items: [],
        };
        for (const item of group.items) {
          const condition = this.conditionMap[item.constraintId];
          const name = condition.constraintName;
          if (name) {
            const content = condition.editable ? item.constraintContent : '';
            groupItem.items.push({
              op: itemOp,
              content: name + ' ' + content,
            });
          }
        }
        renderConditions.push(groupItem);
      }
    }
    return renderConditions;
  }

  /**
     * 展示值约束 diff
     * @param e
     * @param item
     * @param data
     */
  public handleShowContentDiff(e: Event, item: object, data: object) {
    const { newContent, originContent } = item;
    const newConditions = getFieldConstraintContent(newContent, data.fieldName);
    const originConditions = getFieldConstraintContent(originContent, data.fieldName);
    this.diffConditionsContent.origin = originConditions ? this.formatRenderConditions(originConditions) : '';
    this.diffConditionsContent.target = newConditions ? this.formatRenderConditions(newConditions) : '';
    this.diffConditionsContent.cls = this.getItemStatus(data, ['fieldConstraintContent']) ? 'is-update' : '';
    this.diffConditions.handlePopShow(e, {
      theme: 'light diff-conditions',
      placement: 'bottom-end',
      maxWidth: 568,
      offset: 100,
      interactive: false,
    });
  }

  /**
     * 隐藏值约束 diff
     */
  public handleHideContentDiff() {
    this.diffConditionsContent.cls = '';
    this.diffConditions.handlePopHidden();
  }

  /**
     * 展示关联关系 diff
     * @param e
     * @param item
     * @param data
     */
  public handleShowRelationDiff(e: Event, item: object, data: object) {
    const { newContent = {}, originContent = {} } = item;
    const originModelRelation = originContent.modelRelation || [];
    const targetModelRelation = newContent.modelRelation || [];
    this.diffRelationContent.origin = originModelRelation.find(relation => relation.fieldName === data.fieldName)
            || {};
    this.diffRelationContent.target = targetModelRelation.find(relation => relation.fieldName === data.fieldName)
            || {};
    this.diffRelationContent.modelCls = this.getItemStatus(data, ['isJoinField', 'relatedModelId']) ? 'is-update'
      : '';
    this.diffRelationContent.fieldCls = this.getItemStatus(data, ['isJoinField', 'relatedFieldName'])
      ? 'is-update'
      : '';
    this.diffRelation.handlePopShow(e, {
      theme: 'light diff-conditions',
      placement: 'bottom-end',
      maxWidth: 568,
      offset: 100,
      interactive: false,
    });
  }

  /**
     * 隐藏关联关系 diff
     */
  public handleHideRelationDiff() {
    this.diffRelationContent.modelCls = '';
    this.diffRelationContent.fieldCls = '';
    this.diffRelation.handlePopHidden();
  }

  /**
     * 格式化 table 数据
     * @param data
     */
  public tableDataFormatter(data: []) {
    for (let i = 0; i < data.length; i++) {
      const cur = data[i];
      const next = data[i + 1];
      if (cur?.isExtendedField && next?.isExtendedField) {
        cur.isExtendedFieldCls = true;
      }
    }
    return data;
  }

  /**
     * 设置主表 table row 样式
     * @param item
     */
  public rowClassName(item: any) {
    const { row } = item;
    const cls = {
      create: 'create-row ',
      delete: 'delete-row ',
      update: 'update-row ',
    };
    const isOnlyIndexUpdate = row.diffKeys && row.diffKeys.length === 1 && row.diffKeys[0] === 'fieldIndex';
    const basicCls = row.diffType && cls[row.diffType] ? (isOnlyIndexUpdate ? '' : cls[row.diffType]) : '';
    if (row.fieldName === this.activeHoverFieldName) {
      return basicCls + 'is-hover-row';
    }
    return basicCls + (row.isExtendedField ? 'is-extended-row' : '');
  }

  /**
     * 设置主表 table cell 样式
     */
  public cellClassName({ row, column, rowIndex, columnIndex }) {
    return columnIndex === 0 && row?.isExtendedFieldCls ? 'cell-border-bottom-none' : '';
  }

  /**
     * table row mouseenter
     * @param index
     * @param event
     * @param row
     */
  public handleRowMouseEnter(index, event, row) {
    this.activeHoverFieldName = row.fieldName;
  }

  /**
     * table row mouseleave
     */
  public handleRowMouseLeave() {
    this.activeHoverFieldName = '';
  }

  /**
     * 获取每项的状态
     * @param data
     * @param keys
     */
  public getItemStatus(data: object = {}, keys: string[]) {
    return (data.diffKeys || []).some(key => keys.includes(key));
  }

  /**
     * 获取聚合字段信息
     * @param data
     */
  public getAggregationFieldsStr(data: object) {
    // 聚合字段（aggregationFieldsStr），首先用aggregationFieldsAlias，不存在用aggregationFields
    const arr = data.aggregationFieldsAlias || data.aggregationFields || [];
    return arr.join(', ') || '--';
  }

  /**
     * 字段加工逻辑diff
     * @param data
     * @param itemData
     */
  public handleShowFieldCleanContent(data: object, itemData: object = {}) {
    const { originContent, newContent } = itemData;
    this.diffCodeDialog.originCode = getCode(originContent, data.fieldName);
    this.diffCodeDialog.targetCode = getCode(newContent, data.fieldName);
    this.diffCodeDialog.title = this.$t('字段加工逻辑对比');
    this.diffCodeDialog.subTitle = `${data.fieldName} (${data.fieldAlias})`;
    this.diffCodeDialog.isShow = true;
  }

  /**
     * 指标过滤条件diff
     * @param data
     * @param itemData
     */
  public handleShowFilterFormula(data: object, itemData: object = {}) {
    const { originContent, newContent } = itemData;
    this.diffCodeDialog.originCode = originContent?.filterFormula;
    this.diffCodeDialog.targetCode = newContent?.filterFormula;
    this.diffCodeDialog.title = this.$t('指标');
    this.diffCodeDialog.subTitle = `${data.indicatorName} (${data.indicatorAlias})`;
    this.diffCodeDialog.isShow = true;
  }
}
