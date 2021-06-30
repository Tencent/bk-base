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

import { generateId } from '@/common/js/util';
import { IDataModelManage, IMasterTableInfo } from '../Interface/index';
export namespace CDataModelManage {
  export class MasterTableInfo implements IDataModelManage.IMasterTableInfo {
    public modelId: number;
    public modelName?: string;
    public modelAlias?: string;
    public tableName?: string;
    public tableAlias?: string;
    public fields: IDataModelManage.IMasterTableField[];
    public modelRelation: IDataModelManage.IModelRelation[];

    constructor(info: IDataModelManage.IMasterTableInfo): IDataModelManage.IMasterTableInfo {
      const { modelId, modelName, modelAlias, tableName, tableAlias, fields = [], modelRelation = [] } = info;
      this.modelId = modelId;
      this.modelName = modelName;
      this.modelAlias = modelAlias;
      this.tableName = tableName;
      this.tableAlias = tableAlias;
      this.fields = fields;
      this.modelRelation = modelRelation;
    }
  }

  export class MstTbField implements IDataModelManage.IMasterTableField {
    /**字段ID */
    public fieldId: number | string;
    public fieldName: string;
    public fieldAlias: string;
    public fieldIndex: number;
    public fieldType: string;
    public fieldCategory: string;
    public description: string;
    public fieldConstraintContent: IDataModelManage.IFieldConstraintContent | null;
    public fieldCleanContent: IFieldCleanContent;
    public sourceModelId: string | number;
    public sourceFieldName: string;

    /** 是否为主键 */
    public isPrimaryKey: Boolean;

    /** 是否关联字段 */
    public isJoinField: Boolean;

    /** 是否扩展字段 */
    public isExtendedField: Boolean;

    /** 扩展字段对应的关联字段名称 */
    public joinFieldName: string;

    /** 前端唯一ID */
    public uid: string;

    /** 是否来自加载数据表 */
    public isFromRt: boolean;

    /** 编辑/删除字段信息 */
    public editableDeletableInfo: IEditableDeletableInfo;

    constructor(
      field: IDataModelManage.IMasterTableField = {},
      isDefaultId: boolean = true
    ): IDataModelManage.IMasterTableField {
      const defaultEditableDeletableInfo = {
        fields: [],
        isAggregationField: false,
        isConditionField: false,
        isJoinField: false,
        isUsedByCalcAtom: false,
        isUsedByOtherFields: false,
        isExtendedFieldDeletableEditable: true,
      };
      const {
        fieldId = '',
        fieldName = '',
        fieldAlias = '',
        fieldIndex = 0,
        fieldType = 'string',
        fieldCategory = '',
        description = '',
        fieldConstraintContent = null,
        fieldCleanContent = null,
        sourceModelId = null,
        sourceFieldName = null,
        isJoinField = false,
        isExtendedField = false,
        joinFieldName = '',
        isFromRt = false,
        isPrimaryKey = false,
        uid,
        editable = true,
        deletable = true,
        editableDeletableInfo = defaultEditableDeletableInfo,
      } = field;
      this.uid = uid ? uid : generateId('bkdatamodel_field_');
      Object.assign(this, {
        fieldId,
        fieldName,
        fieldAlias,
        fieldIndex,
        fieldType,
        fieldCategory,
        description,
        fieldConstraintContent,
        fieldCleanContent,
        sourceModelId,
        sourceFieldName,
        isJoinField,
        isExtendedField,
        joinFieldName,
        isFromRt,
        isPrimaryKey,
        editableDeletableInfo,
        editable,
        deletable,
      });
    }
  }
}
