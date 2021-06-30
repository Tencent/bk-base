

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
  <div class="tag-list-body">
    <div v-for="(row, index) in tagList"
      :key="index"
      :class="['tag-list-row', { 'row-expand': row.expand }]">
      <div v-if="!row.expand"
        class="tag-list-top">
        <div :class="['tag-list-title', { 'single-row': isSingleRow }]">
          <div class="title-text">
            {{ row[tagName] }}
          </div>
          <div class="icon-area" />
        </div>
        <div class="tag-list-tags">
          <div v-for="(tag, ind) in row[subTopList]"
            :key="ind"
            class="tag-wrapper">
            <span
              :class="[multiple ? 'tag' : 'single-tag', { 'is-active': tag.active }]"
              @click="itemClickHandle(tag, row, dataIndex)">
              {{ tag[tagName] }}
            </span>
          </div>
        </div>
        <div
          v-if="row[subList] && row[subList].length && row.is_more_show && !text"
          class="click-more"
          @click="showMoreHandle(row, item)">
          {{ $t('更多') }}
          <span class="icon-down-shape" />
        </div>
      </div>
      <div v-else
        class="tag-list-more">
        <div v-for="(subRow, ind) in row[subList]"
          :key="ind"
          class="more-list-row">
          <div v-if="index === 0"
            class="tag-list-title">
            <div class="title-text">
              {{ row[tagName] }}
            </div>
            <div class="icon-area" />
          </div>
          <div class="tag-list-sub-title">
            <div class="tag-wrapper">
              <span
                :class="[
                  multiple ? 'tag' : 'single-tag',
                  { 'is-active': subRow.active },
                  { disabled: isDisabledTag(subRow) },
                ]"
                @click="itemClickHandle(subRow, row, dataIndex)">
                {{ subRow[tagName] }}
              </span>
            </div>
          </div>
          <div class="tag-list-tags">
            <div v-for="(tag, inds) in subRow[subList]"
              :key="inds"
              class="tag-wrapper">
              <span
                :class="[
                  multiple ? 'tag' : 'single-tag',
                  { 'is-active': tag.active },
                  { disabled: isDisabledTag(tag) },
                ]"
                @click="itemClickHandle(tag, row, dataIndex)">
                {{ tag[tagName] }}
              </span>
            </div>
          </div>
          <div v-if="index === 0"
            class="click-more"
            @click="showMoreHandle(row, item)">
            {{ $t('收起') }}
            <span class="icon-up-shape" />
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
export default {
  model: {
    prop: 'selected',
    event: 'change',
  },
  props: {
    tagList: {
      type: Array,
      default: () => [],
    },
    multiple: {
      type: Boolean,
      default: true,
    },
    isSingleRow: {
      type: Boolean,
      default: false,
    },
    subTopList: {
      type: String,
      default: 'subTopList',
    },
    subList: {
      type: String,
      default: 'subList',
    },
    selected: {
      type: Array,
      default: () => [],
    },
  },
  data() {
    return {
      tagData: [],
      tagSelected: [],
    };
  },
  computed: {
    tags: {},
  },
  created() {
    this.tagData = this.tagList.map(tagRow => {
      const retTagRow = JSON.parse(JSON.stringify(tagRow));
      retTagRow.expand = false;
    });
  },
  methods: {
    showMoreHandle(row, item) {
      row.expand = !row.expand;
      // 不允许同时打开多行
      if (row.expand) {
        parent.data.forEach(rowItem => {
          if (rowItem !== row) {
            rowItem.expand = false;
          }
        });
      }
    },
    /** 根据所选/清除标签， 同步同一个(tagId相同)的标签
     *       - 因为同一个标签可能在top_list的同时，也在sub_list，因此该函数通过遍历保证标签状态同步
     *       - 不支持多选时，需要将其他标签的状态置为false
     */
    changeTagStatus(tag, index) {
      const { active } = tag;

      this.tags[index].forEach(tagItem => {
        if (tagItem[this.tagId] === tag[this.tagId]) {
          tagItem.active = active;
        } else if (!this.multiple) {
          tagItem.active = false;
        }
      });
    },
    /** 标签点击事件
     *  tag(Object): 所点击标签
     *  row(Object): 标签所在行
     *  index(Number): 标签所在data-tag-list的索引
     */
    itemClickHandle(tag, row, index) {
      if (this.isDisabledTag(tag)) return;
      tag.active = !tag.active;
      tag.active && this.addTag(tag, row, index);
      !tag.active && this.removeTag(tag, index);
    },
    addTag(tag, row, index) {
      if (this.multiple) {
        this.tagSelected.push(tag);
        const filterObj = {}; // 用于去重的临时对象

        /** 1. 将已选tag添加至顶栏
         *  2. 去重，防止顶栏已有的tag被重复追加
         *  3. 超过最大宽度，列表出栈
         */
        row[this.subTopList].push(tag);
        row[this.subTopList].forEach((item, index) => {
          if (filterObj.hasOwnProperty(item[this.tagId])) {
            row[this.subTopList].splice(index, 1);
          } else {
            filterObj[item[this.tagId]] = true;
          }
        });
        if (row[this.subTopList].length > 12) {
          row[this.subTopList].shift();
        }
      } else {
        this.tagSelected = new Array(tag);
      }
      this.$emit('change', this.collection); // 同步v-model的tags
      this.changeTagStatus(tag, index);
    },
  },
};
</script>
