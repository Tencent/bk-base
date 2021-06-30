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

export namespace IDataModelManage {
  export interface IModeRightTabItem {
    /** 是否新增 */
    isNew: boolean;

    /** 是否当前激活条目 */
    isActive: boolean;

    /** 唯一ID */
    id: string;

    /** 缓存当前激活步骤 */
    activeStep: number;

    /** name */
    name: string;

    displayName: string;

    /** type */
    type: string;

    /** 模型类型 */
    modelType: string;

    /** 模型ID */
    modelId: number;

    projectId?: number;

    icon: string;

    /** 当前模型最后已完成步骤 */
    lastStep: number;

    /** 模型发布状态 */
    publishStatus: string;

    /** 子路由 name */
    routeName?: string;
  }

  export interface IModeRightTabItems {
    public static getActiveItem: () => IModeRightTabItem[];
    public static activeItem: (item: IModeRightTabItem) => IModeRightTabItem;
    public static activeItemById: (id: string) => IModeRightTabItem;
    public static activeItemByIndex: (index: number) => IModeRightTabItem;
    public static deActiveItem: (item?: IModeRightTabItem) => void;
    public static appendItem: (item: IModeRightTabItem) => void;
    public static insertItem: (item: IModeRightTabItem) => void;
    public static removeItem: (item: IModeRightTabItem) => number;
    private static items: IModeRightTabItem[];
  }

  export interface IModelList {
    modelId: number;
    description: string;
    tags: ITags[];
    tableAlias: string;
    createdBy: string;
    publishStatus: string;
    modelAlias: string;
    stickyOnTop: boolean;
    appliedCount: number;
    tableName: string;
    activeStatus: string;
    modelType: string;
    stepId: number;
    projectId: number;
    modelName: string;
    updatedBy: any;
  }

  export interface ITags {
    tagCode: string;
    tagAlias: string;
  }

  export interface IMasterTableInfo {
    modelId: number;
    modelName?: string;
    modelAlias?: string;
    tableName?: string;
    tableAlias?: string;
    fields: IMasterTableField[];

    /** 模型关联关系 */
    modelRelation: IModelRelation[];
  }

  export interface IModelRelation {
    fieldName: string;
    relatedModelId: number;
    relatedFieldName: string;
    relatedMethod: string;
  }

  export interface IMasterTableField {
    /**字段ID */
    fieldId: number;

    /** 字段名称 */
    fieldName: string;

    /** 字段别名 */
    fieldAlias: string;

    /** 字段位置 */
    fieldIndex: number;

    /** 数据类型 */
    fieldType: string;

    /** 字段类型 */
    fieldCategory: string;

    /** 主键 */
    isPrimaryKey: boolean;

    /** 是否可编辑 */
    editable: boolean;

    /** 是否可删除 */
    deletable: boolean;

    /** 字段描述 */
    description: string;

    /** 字段约束内容 */
    fieldConstraintContent: IFieldConstraintContent | null;

    /** 清洗规则 */
    fieldCleanContent: IFieldCleanContent | null;

    /** 来源模型id */
    sourceModelId: string | number | null;

    /** 来源字段 */
    sourceFieldName: string | null;

    /** 是否关联字段 */
    isJoinField: boolean;

    /** 是否扩展字段 */
    isExtendedField: boolean;

    /** 扩展字段对应的关联字段名称 */
    joinFieldName: string;

    /** 前端唯一ID */
    uid: string;

    /** 是否来自加载数据表 */
    isFromRt: boolean;

    /** 编辑/删除字段信息 */
    editableDeletableInfo: IEditableDeletableInfo;
  }

  export interface IEditableDeletableInfo {
    fields: any[];
    isAggregationField: boolean;
    isConditionField: boolean;
    isJoinField: boolean;
    isUsedByCalcAtom: boolean;
    isUsedByOtherFields: boolean;
    isExtendedFieldDeletableEditable: boolean;
  }

  export interface IFieldCleanContent {
    cleanOption: string;
    cleanContent: string;
  }

  /** 字段约束 */
  export interface IFieldConstraintContent {
    groups: IFieldConstraintContentGroups;
    op: string;
  }

  export interface IFieldConstraintContentGroups {
    op: string;
    items: IFieldConstraintContentItem[];
  }
  export interface IFieldConstraintContentItem {
    constraintId: string;
    constraintContent: string;
  }

  export interface IFieldContraintConfig {
    description: any;
    constraintId: string;
    editable: boolean;
    constraintType: string;
    constraintValue: string;
    constraintName: string;
    validator: IValidator;
    allowFieldType: string[];
  }

  export interface IValidator {
    content: any;
    type: string;
  }

  // 统计口径列表
  export interface ICalculationAtomsList {
    errors: any;
    message: string;
    code: string;
    result: boolean;
    data: IData;
  }

  export interface IData {
    results: Array<IResults>;
    stepId: number;
  }

  export interface IResults {
    modelId: number;
    projectId: number;
    calculationAtomName: string;
    calculationAtomAlias: string;
    calculationAtomType: string;
    description: string;
    fieldType: string;
    calculationContent: ICalculationContent;
    calculationFomula: string;
    indicatorCount: number;
    createdBy: string;
    createdAt: string;
    updatedBy: string;
    updatedAt: string;
    indicators: Array<IIndicators>;
  }

  export interface IIndicators {
    indicatorName: string;
    indicatorAlias: string;
    subIndicators: Array<ISubIndicators>;
  }

  export interface ISubIndicators {
    indicatorName: string;
    indicatorAlias: string;
  }

  export interface ICalculationContent {
    option: string;
    content: IContent;
  }

  export interface IContent {
    calculationField: string;
    calculationFunction: string;
  }

  export interface DiffResult {
    updateNum: number;
    deleteNum: number;
    createNum: number;
    hasDiff: Boolean;
  }

  export interface DiffObjectItem {
    diffType: String;
    objectId: number;
    objectType: number;
    diffKeys?: string[];
    diffObjects?: DiffObjectItem[];
  }
}
