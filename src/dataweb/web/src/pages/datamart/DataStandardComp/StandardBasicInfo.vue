

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
  <div class="basic-container">
    <BasicConstruct :title="$t('基本信息')"
      :tipsContent="$t('数据标准基本信息')">
      <div class="form-container pl15">
        <form class="bk-form flex5">
          <div class="bk-form-item">
            <label :title="$t('标准名称')"
              class="bk-label info-left">
              {{ $t('标准名称') }}
            </label>
            <div class="bk-form-content info-right">
              <div :title="standardDetail.basic_info.standard_name"
                class="text-overflow max-width394">
                {{ standardDetail.basic_info.standard_name }}
              </div>
            </div>
          </div>
          <div class="bk-form-item">
            <label :title="$t('标准描述')"
              class="bk-label info-left text-overflow">
              {{ $t('标准描述') }}
            </label>
            <div class="bk-form-content info-right">
              <div :title="standardDetail.basic_info.description"
                class="text-overflow max-width394">
                {{ standardDetail.basic_info.description }}
              </div>
            </div>
          </div>
          <div class="bk-form-item">
            <label :title="$t('标签')"
              class="bk-label info-left">
              {{ $t('标签') }}
            </label>
            <div class="bk-form-content info-right">
              <span v-for="(item, index) in standardDetail.basic_info.tag_list.slice(0, 7)"
                :key="index"
                :title="item.alias"
                class="tag text-overflow">
                {{ item.alias }}
              </span>
              <bkdata-popover placement="top">
                <span v-if="standardDetail.basic_info.tag_list.length > 7"
                  class="icon-ellipsis platform-item-ellipsis" />
                <div slot="content"
                  class="clearfix"
                  style="white-space: normal; max-width: 500px; max-height: 400px">
                  <span v-for="(item, index) in standardDetail.basic_info.tag_list.slice(7)"
                    :key="index"
                    class="data-platform-item">
                    {{ item.alias }}
                  </span>
                </div>
              </bkdata-popover>
            </div>
          </div>
        </form>
        <form class="bk-form flex2-5">
          <div class="bk-form-item">
            <label :title="$t('操作者')"
              class="bk-label info-left">
              {{ $t('操作者') }}
            </label>
            <div class="bk-form-content info-right">
              {{ standardDetail.basic_info.updated_by || '' }}
            </div>
          </div>
          <div class="bk-form-item">
            <label :title="$t('修改时间')"
              class="bk-label info-left">
              {{ $t('修改时间') }}
            </label>
            <div class="bk-form-content info-right">
              {{ standardDetail.basic_info.updated_at }}
            </div>
          </div>
          <div class="bk-form-item">
            <label :title="$t('当前版本')"
              class="bk-label info-left">
              {{ $t('当前版本') }}
            </label>
            <div class="bk-form-content info-right">
              {{ standardDetail.version_info.standard_version }}
            </div>
          </div>
        </form>
        <form class="bk-form flex2-5">
          <div class="bk-form-item">
            <label :title="$t('标准内容')"
              class="bk-label info-left">
              {{ $t('标准内容') }}
            </label>
            <div class="bk-form-content info-right">
              {{ `【${standardDetail.basic_info.detaildata_count}】/【${standardDetail.basic_info.indicator_count}】` }}
            </div>
          </div>
          <div class="bk-form-item">
            <label :title="$t('关联数据')"
              class="bk-label info-left">
              {{ $t('关联数据') }}
            </label>
            <div :class="[standardDetail.basic_info.linked_data_count > 0 ? 'click-style' : '']"
              class="bk-form-content info-right"
              @click="linkToDiction()">
              {{ standardDetail.basic_info.linked_data_count }}
            </div>
          </div>
          <div class="bk-form-item">
            <label :title="$t('版本描述')"
              class="bk-label info-left text-overflow">
              {{ $t('版本描述') }}
            </label>
            <div class="bk-form-content info-right">
              <div :title="standardDetail.version_info.description"
                class="text-overflow max-width264">
                {{ standardDetail.version_info.description }}
              </div>
            </div>
          </div>
        </form>
      </div>
    </BasicConstruct>
  </div>
</template>

<script>
import BasicConstruct from './BasicConstruct';
export default {
  components: {
    BasicConstruct,
  },
  props: {
    standardDetail: {
      type: Object,
      default: () => ({}),
    },
  },
  data() {
    return {};
  },
  methods: {
    linkToDiction() {
      if (!this.standardDetail.basic_info.linked_data_count) return;
      this.$router.push({
        name: 'DataSearchResult',
        params: {
          standard_version_id: this.standardDetail.version_info.id,
          standard_name: this.standardDetail.basic_info.standard_name,
        },
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.click-style {
  color: #3a84ff;
  cursor: pointer;
}
.tag {
  display: inline-block;
  max-width: 69px;
  min-width: 34px;
  margin-left: 10px;
  padding: 2px 4px;
  border: 1px solid #ddd;
  line-height: normal;
  &:first-child {
    margin-left: 0;
  }
}
.platform-item-ellipsis {
  display: inline-block;
  max-width: 69px;
  min-width: 34px;
  margin-left: 10px;
  padding: 2px 4px;
  border: 1px solid #ddd;
  font-size: 18px;
  font-weight: bolder;
}
.data-platform-item {
  white-space: nowrap;
  margin-left: 10px;
  padding: 2px 4px;
  border: 1px solid #ddd;
  float: left;
  &:first-child {
    margin-left: 0px;
  }
}
.basic-container {
  box-shadow: 2px 3px 5px 0 rgba(33, 34, 50, 0.15);
  border-radius: 2px;
  border: 1px solid rgba(195, 205, 215, 0.6);
  padding: 0 20px 20px 5px;
  margin-bottom: 20px;
}
.form-container {
  display: flex;
  flex-wrap: nowrap;
  .bk-form {
    flex: 1;
  }
  .flex5 {
    flex: 5;
  }
  .flex2-5 {
    flex: 2.5;
  }
}
form {
  border: 1px solid #d9dfe5;
  .bk-form-item {
    margin-top: 0;
    border-top: 1px solid #d9dfe5;

    &:first-child {
      border-top: none;
    }
  }
  .bk-form-item {
    background: #efefef;
    .bk-form-content {
      .max-width394 {
        max-width: 652px;
      }
      .max-width264 {
        max-width: 264px;
      }
    }
  }
  label.info-left {
    width: 124px;
    background: #efefef;
    white-space: nowrap;
  }

  .info-right {
    display: inherit;
    line-height: 34px;
    min-height: 34px;
    margin-left: 124px;
    background: #fff;
    padding-left: 15px;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  span.name {
    padding: 2px 5px;
    border-radius: 2px;
    background: #f7f7f7;
    border: 1px solid #dbe1e7;
    display: inline-block;
    margin-right: 3px;
    color: #737987;
    line-height: 25px;
  }
  .tags-item {
    font-size: 12px;
  }
  .single-tag,
  .multiple-tag {
    display: inline-block;
    height: 25px;
    line-height: 25px;
    background: #fdf6ec;
    color: #faad14;
    font-size: 12px;
    padding: 0 5px;
    border-radius: 3px;
    margin-right: 5px;
    margin-bottom: 3px;
    margin-top: 3px;
  }
  .multiple-tag {
    background: #e1ecff;
    color: #3a84ff;
  }
}
</style>
