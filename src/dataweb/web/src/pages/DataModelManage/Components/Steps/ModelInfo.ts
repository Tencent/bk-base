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

import { getRecommendsTags } from '@/api/common';
import { Component, PropSync, Ref, Watch } from 'vue-property-decorator';
import { createModel, getModelInfo, updateModel, validateModelName } from '../../Api/index';
import { DataModelStorage } from '../../Controller/DataModelStorage';
import { DataModelManage } from './IStepsManage';

@Component({
  name: 'ModelInfo',
  components: {},
})
export default class ModelInfo extends DataModelManage.IStepsManage {
  @Ref() public readonly modelInfoForm;
  @PropSync('isLoading', { default: false }) public syncIsLoading;

  private storageKey = 'modelFormInfo';

  public tags = {
    visible: {
      results: [],
    },
  };

  public defaultFormData = {
    model_name: '',
    model_alias: '',
    model_type: '',
    description: '',
    tags: [],
    project_id: 0,
  };

  public formData = {
    model_name: '',
    model_alias: '',
    model_type: '',
    description: '',
    tags: [],
    project_id: 0,
  };

  public rules = {
    model_name: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        min: 1,
        message(val) {
          return `${val}不能小于1个字符`;
        },
        trigger: 'blur',
      },
      {
        max: 50,
        message: '不能多于50个字符',
        trigger: 'blur',
      },
      {
        regex: /^[a-zA-Z][a-zA-Z0-9_]*$/,
        message: '只能是英文字母、下划线和数字组成，且字母开头',
        trigger: 'blur',
      },
      {
        validator: this.validateModelName,
        message: '数据模型名称已存在，请修改数据模型名称',
        trigger: 'blur',
      },
    ],
    model_alias: [
      {
        required: true,
        message: '必填项',
        trigger: 'blur',
      },
      {
        min: 1,
        message(val) {
          return `${val}不能小于1个字符`;
        },
        trigger: 'blur',
      },
      {
        max: 50,
        message: '不能多于50个字符',
        trigger: 'blur',
      },
    ],
    model_type: [
      {
        required: true,
        message: '请选择项目',
        trigger: 'blur',
      },
    ],
    tags: [
      {
        required: true,
        message: '请选择标签',
        trigger: 'blur',
      },
      {
        validator(val) {
          return val.length >= 1;
        },
        message: '请选择标签',
        trigger: 'blur',
      },
    ],
  };

  public timer = null;

  /** 是否为新增模型 */
  get isNewForm() {
    return !this.formData.created_at;
  }

  get tagList() {
    return this.tags.visible.results || [];
  }

  get tagCount() {
    return this.formData.tags.length;
  }

  @Watch('modelId', { immediate: true })
  public handleModelIdChanged() {
    this.loadModelInfo();
  }

  @Watch('updateEvents', { deep: true })
  public handleUpdateEventFired(val: IUpdateEvent) {
    /**
        /* updateModelName : 事件来自右侧顶部工具栏更新模型名称
         */
    if (val && val.name === 'updateModelName') {
      this.handleUpdateModel(val.params[0]);
    }
  }

  /**
    /* 切换 tab，如果 modelId 为 0，通过监听 activeModelTabItem 触发 loadModelInfo
     */
  @Watch('activeModelTabItem', { deep: true })
  public handleActiveModelTabItemChange() {
    this.activeModelTabItem && this.activeModelTabItem.modelId === 0 && this.loadModelInfo();
  }

  /**
    /* 暂存新建表填入的数据
     */
  @Watch('formData', { deep: true })
  public handleStorageFormData() {
    if (this.activeModelTabItem && this.modelId === 0) {
      if (this.timer) {
        clearTimeout(this.timer);
        this.timer = null;
      }
      this.timer = setTimeout(() => {
        DataModelStorage.update(this.storageKey + this.activeModelTabItem.id, this.formData);
      }, 400);
    }
  }

  public handleModelTypeSelected(type: string) {
    this.formData.model_type = type;
  }

  public handleTags(tagCodes: any[]) {
    const result: object[] = [];
    this.tagList.forEach((item: any) => {
      if (tagCodes.includes(item.code)) {
        result.push({
          tag_code: item.code,
          tag_alias: item.alias,
        });
        tagCodes.splice(tagCodes.indexOf(item.code), 1);
      }
    });
    if (tagCodes.length) {
      tagCodes.forEach((item: any) => {
        result.push({
          tag_code: '',
          tag_alias: item,
        });
      });
    }
    return result;
  }

  public submitForm() {
    return this.modelInfoForm
      .validate()
      .then(res => {
        const tagCodes = this.handleTags(JSON.parse(JSON.stringify(this.formData.tags)));
        if (this.isNewForm) {
          const { model_name, model_alias, model_type, description } = this.formData;
          return createModel(model_name, model_alias, model_type, this.projectId, description, tagCodes)
            .then(res => {
              return res.setData(this, 'formData').then(r => {
                const activeTabIndex = this.DataModelTabManage.getActiveItemIndex();
                if (activeTabIndex >= 0) {
                  const activeTab = this.DataModelTabManage.tabManage.items[activeTabIndex];
                  activeTab.name = this.formData.model_name;
                  activeTab.displayName = this.formData.model_alias;
                  activeTab.id = this.formData.model_id;
                  activeTab.modelId = this.formData.model_id;
                  activeTab.lastStep = this.formData.step_id;
                  activeTab.publishStatus = this.formData.publish_status;
                  activeTab.isNew = false;
                  this.DataModelTabManage.updateTabItem(activeTab, activeTabIndex);
                  this.DataModelTabManage.dispatchEvent('updateModel', [this.formData]);
                  // 保存后，新建表单缓存数据清空
                  DataModelStorage.update(this.storageKey + 0, null);
                  // this.appendRouter({ modelId: this.formData.model_id })
                  this.changeRouterWithParams(
                    'dataModelEdit',
                    { project_id: this.activeModelTabItem.projectId },
                    { modelId: this.formData.model_id }
                  );
                }
                this.showMessage(this.$t('创建成功'), 'success');
                this.backFillTags(res.data.tags);
                this.syncPreNextBtnManage.isPreNextBtnConfirm = false;
                return Promise.resolve(true);
              });
            });
        } else {
          const { model_id, model_alias, description } = this.formData;
          return updateModel(model_id, model_alias, description, tagCodes).then(res => {
            if (res.validateResult()) {
              const activeTabIndex = this.DataModelTabManage.getActiveItemIndex();
              if (activeTabIndex >= 0) {
                const activeTab = this.DataModelTabManage.tabManage.items[activeTabIndex];
                activeTab.displayName = this.formData.model_alias;
                activeTab.publishStatus = res.data.publish_status;
                this.formData.publish_status = res.data.publish_status;
                this.DataModelTabManage.updateTabItem(activeTab, activeTabIndex);
                this.DataModelTabManage.dispatchEvent('updateModel', [this.formData]);
              }
              this.showMessage(this.$t('更新成功'), 'success');
              this.backFillTags(res.data.tags);
              this.initPreNextManage(false);
              this.syncPreNextBtnManage.isPreNextBtnConfirm = false;
              return Promise.resolve(true);
            } else {
              return Promise.reject(res.message);
            }
          });
        }
      })
      ['catch'](err => console.log(err));
  }

  public backFillTags(tags: object[]) {
    const tagsMap = {};
    tags.forEach(item => {
      tagsMap[item.tag_code] = item.tag_alias;
    });
    const tagsArr = Object.keys(tagsMap);
    this.tags.visible.results.forEach(item => {
      if (tagsArr.includes(item.code)) {
        tagsArr.splice(tagsArr.indexOf(item.code), 1);
      }
    });
    tagsArr.forEach(item => {
      this.tags.visible.results.push({
        code: item,
        alias: tagsMap[item],
      });
    });
  }

  public handleFormItemChanged() {
    if (this.modelId > 0) {
      this.syncPreNextBtnManage.isPreNextBtnConfirm = true;
    }
  }

  public handleCreateTagValidator(value: string) {
    return !this.tagList.find(tag => tag.alias === value);
  }

  public handleTagFilterCallback(value: string, searchKey: string, list: any[]) {
    let filterData = [];
    if (Array.isArray(searchKey)) {
      // 数组，过滤多个关键字
      const filterDataList = searchKey.map(key => {
        return list.filter(item => item[key].toLowerCase().indexOf(value) !== -1);
      });
      filterData = Array.from(new Set(filterDataList.flat()));
    } else {
      filterData = list.filter(item => item[searchKey].toLowerCase().indexOf(value) !== -1);
    }
    // 完全匹配放在第一个
    let index = -1;
    for (let i = 0, len = filterData.length; i < len; i++) {
      const item = filterData[i];
      if (item.alias === value) {
        index = i;
        break;
      }
    }
    if (index > -1) {
      const firstItem = filterData[index];
      filterData.splice(index, 1);
      filterData.splice(0, 0, firstItem);
    }
    return filterData;
  }

  public handleCancelClick() {
    this.loadModelInfo();
  }

  public loadModelInfo() {
    /** 设置Cancel请求 */
    this.handleCancelRequest(this.cancelKey);

    /** 重置FormData */
    this.$set(this, 'formData', JSON.parse(JSON.stringify(this.defaultFormData)));

    this.$nextTick(() => {
      this.modelInfoForm && this.modelInfoForm.clearError();
    });

    if (this.modelId > 0) {
      this.initPreNextManage(false);
      this.syncIsLoading = true;
      getModelInfo(this.modelId, [], this.getCancelToken(this.cancelKey))
        .then(res => {
          res.setData(this, 'formData');
          this.formData.tags = this.formData.tags.map(item => item.tag_code);
        })
        ['finally'](() => {
          this.syncIsLoading = false;
        });
    }

    if (this.modelId === 0) {
      // 确保刷新的时候可以取到type
      this.$nextTick(() => {
        /** 新增模型时，保存之前下一步不可用 */
        this.initPreNextManage(false, false);
        const storageData = DataModelStorage.get(this.storageKey + this.activeModelTabItem.id);
        if (storageData) {
          Object.assign(this.formData, storageData);
        } else {
          const item = this.DataModelTabManage.getActiveItem()[0];
          this.formData.model_type = item.modelType;
        }
      });
    }
  }

  public handleUpdateModel(item: any) {
    this.formData.model_alias = item.model_alias;
  }

  public getTagList() {
    getRecommendsTags().then(res => {
      res.setData(this, 'tags');
    });
  }

  public async validateModelName() {
    if (!this.isNewForm) {
      return true;
    }
    const res = await validateModelName(this.formData.model_name);
    return !res.data;
  }

  public mounted() {
    this.initPreNextManage(false, !!this.modelId);
    this.getTagList();
  }
}
