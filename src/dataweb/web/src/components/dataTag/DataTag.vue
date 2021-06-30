

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
  <div style="width: 100%">
    <div v-if="focusMode"
      class="data-tag-wrapper">
      <div v-if="!isFullScreenShow"
        :class="['data-tag-inputs', 'no-margin', { 'display-input': inlineHeight }]">
        <div class="data-tag-collection">
          <span v-for="(tag, index) in collection"
            :key="index"
            :class="['tag']">
            {{ tag[tagName] }}
            <span class="icon-close"
              @click="removeTag(tag)" />
          </span>
          <input
            v-model="text"
            type="text"
            style="width: calc(100% - 5px)"
            :class="['tag-input', { 'tag-big-size': !collection.length }]"
            :placeholder="dynamicPlaceholder"
            @focus="inputFocusHandle">
        </div>
        <div class="data-tag-icon-group">
          <span class="icon-close-circle-shape"
            :title="$t('清空')"
            @click="clearAll" />
        </div>
      </div>
    </div>
    <transition name="fade">
      <div
        v-show="isFullScreenShow"
        ref="fullScreenWrapper"
        class="full-screen-wrapper"
        :style="{ top: fullWrapperTop }">
        <div class="data-tag-wrapper">
          <h2 class="data-tag-header">
            {{ $t(header) }}
          </h2>
          <!-- 上方输入框样式，展示所选tag -->
          <div class="data-tag-inputs">
            <div class="data-tag-collection">
              <span v-for="(tag, index) in collection"
                :key="index"
                :title="tag[tagName]"
                :class="['tag']">
                {{ tag[tagName] }}
                <span class="icon-close"
                  @click="removeTag(tag)" />
              </span>
              <input
                v-model="text"
                type="text"
                :class="['tag-input', { 'tag-big-size': !collection.length }]"
                :placeholder="dynamicPlaceholder"
                @keydown.delete="deleteItem">
            </div>
            <div class="data-tag-icon-group">
              <span class="icon-close-circle-shape"
                :title="$t('清空')"
                @click="clearAll" />
            </div>
          </div>

          <!-- 标签 List  tip + list-body -->
          <div v-for="(item, dataIndex) in data"
            :key="dataIndex"
            class="data-tag-list">
            <div v-if="item.label"
              class="tag-list-tip">
              <span :class="['icon-info', item.multiple ? 'icon-multiple' : 'icon-single']" />
              <span :title="item.label"
                class="tip-text">
                {{ item.label }}
              </span>
            </div>
            <div class="tag-list-body">
              <div
                v-for="(row, index) in item.data"
                :key="index"
                :class="['tag-list-row', { 'row-expand': row.expand }]">
                <div v-if="!row.expand"
                  class="tag-list-top">
                  <div :class="['tag-list-title', { 'single-row': singleRowStyle }]">
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
                        :title="tag[tagName]"
                        :class="[tag.multiple ? 'tag' : 'tag', { 'is-active': tag.active }]"
                        @click="itemClickHandle(tag, row, dataIndex)">
                        {{ tag[tagName] }}
                      </span>
                    </div>
                  </div>
                  <div
                    v-if="row[subList] && row[subList].length && row.is_more_show && !text"
                    class="click-more"
                    @click="switchMore(row, item)">
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
                          :title="subRow[tagName]"
                          :class="[
                            subRow.multiple ? 'tag' : 'single-tag',
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
                          :title="tag[tagName]"
                          :class="[
                            tag.multiple ? 'tag' : 'single-tag',
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
                      @click="switchMore(row, item)">
                      {{ $t('收起') }}
                      <span class="icon-up-shape" />
                    </div>
                  </div>
                </div>
              </div>
              <div
                v-if="loading"
                v-bkloading="{ isLoading: loading, title: $t('数据加载中') }"
                class="tag-empty-body" />
            </div>
          </div>

          <div class="operation-panel">
            <bkdata-button :theme="'primary'"
              :title="$t('确定')"
              class="mr10"
              @click="confirmHandle">
              {{ $t('确定') }}
            </bkdata-button>
            <bkdata-button :theme="'default'"
              type="submit"
              :title="$t('取消')"
              @click="cancelHandle">
              {{ $t('取消') }}
            </bkdata-button>
          </div>
        </div>
      </div>
    </transition>
  </div>
</template>

<script>
export default {
  model: {
    prop: 'selected',
    event: 'change',
  },
  props: {
    fullWrapperTop: {
      type: String,
      default: '97px',
    },
    focusMode: {
      // 外层input+focus全屏模式，默认打开
      type: Boolean,
      default: true,
    },
    inlineHeight: {
      // 有些input需要限制高度为32px，与其他位置等高
      type: Boolean,
      default: false,
    },
    singleRowStyle: {
      // 是否单行展示（单行展示时，左边距更小）
      type: Boolean,
      default: false,
    },
    header: {
      type: String,
      default: '数据标签',
    },
    inputFocusHandle: {
      type: Function,
      default: () => {
        this.isFullScreenShow = true;
      },
    },
    /** 标签关键字映射，以适配后台数据不同关键字类型
     *       subToplist: 一级展示列表
     *       subList: 子列表，点击更多展开列表
     *       tagName: tag的显示名称,
     *       tagId: tag的唯一标符
     */
    tagKeysMap: {
      type: Object,
      default: () => ({
        subList: 'sub_list',
        tagName: 'tag_alias',
        subTopList: 'sub_top_list',
        tagId: 'tag_code',
      }),
    },
    cancelHandle: {
      type: Function,
      default: () => {},
    },
    confirmHandle: {
      type: Function,
      default: () => {},
    },
    /** 数据结构
     *  data => dataBLock 两块内容可独立配置多选/单选
     *      dataBlock:{
     *          label: 描述文字
     *          multiple：是否多选
     *          data: 数据 Array
     *      }
     *      data必含字段:
     *          expand: 该行是否展开
     *          sub_top_list： 未展开的列表，字段名称可改，根据keymap
     *          sub_list: 展开后的列表，字段名称课改
     *          tag_code: 唯一标识，可改
     *          tag_alias： tag中文显示，可改
     *          backupTopList: 原始的toplist，用于搜索过滤功能
     */
    data: {
      type: Array,
      default: () => [],
    },
    placeholder: {
      type: String,
      default: '',
    },
    selected: {
      type: [Array, Object],
      default: () => [],
    },
  },
  data() {
    return {
      collection: [], // 所选标签,
      text: '',
      isFullScreenShow: false,
    };
  },
  computed: {
    loading() {
      return this.data[0].data.length === 0;
    },
    dynamicPlaceholder() {
      if (this.collection.length) {
        return this.$t('点击编辑标签');
      }
      return this.placeholder;
    },
    /** 根据关键字映射计算的标签名 */
    tagName() {
      return this.tagKeysMap.tagName;
    },
    subList() {
      return this.tagKeysMap.subList;
    },
    subTopList() {
      return this.tagKeysMap.subTopList;
    },
    tagId() {
      return this.tagKeysMap.tagId;
    },
    // 将所有tag 数据 flat成一个数组
    tags() {
      const tagGroup = [];
      this.data.forEach(group => {
        const tags = [];
        group.data.forEach(tagGroup => {
          tagGroup[this.subTopList].forEach(tag => {
            tags.push(tag);
          });
          tagGroup[this.subList]
            && tagGroup[this.subList].forEach(tagItem => {
              tags.push(tagItem);
              tagItem[this.subList]
                && tagItem[this.subList].forEach(item => {
                  tags.push(item);
                });
            });
        });
        tagGroup.push(tags);
      });
      return tagGroup;
    },
  },
  watch: {
    selected(value) {
      this.collection = [...value];
    },
    /** 当输入框有内容时，需要过滤top_list，实现搜索功能 */
    text(val) {
      this.data.forEach((itemBlock, blockIndex) => {
        itemBlock.data.forEach((tagRow, rowIndex) => {
          if (val) {
            /** 如果用户输入了内容，根据输入内容，替换sub_top_list*/
            const filterRow = [];
            tagRow[this.subList].forEach(tagGroup => {
              if (this.tagSearch(tagGroup[this.tagName], val)) {
                filterRow.push(tagGroup);
              }
              tagGroup[this.subList]
                && tagGroup[this.subList].forEach(tag => {
                  if (this.tagSearch(tag[this.tagName], val)) {
                    filterRow.push(tag);
                  }
                });
            });
            tagRow[this.subTopList] = filterRow;
          } else {
            /** 用户输入为空，还原sub_top_list，同时，检索当前选择的标签，变化状态 */
            tagRow[this.subTopList] = tagRow.backupTopList;
            const tagSelectedPool = this.collection.map(item => item[this.tagId]);
            tagRow[this.subTopList].forEach(tagGroup => {
              if (tagSelectedPool.includes(tagGroup[this.tagId])) {
                tagGroup.active = true;
              }
            });
          }
        });
      });
    },
  },
  mounted() {
    document.body.appendChild(this.$refs.fullScreenWrapper);
  },
  methods: {
    /** 标签模糊搜索，不区分大小写 */
    tagSearch(tagName, key) {
      if (tagName.toLocaleLowerCase().includes(key.toLocaleLowerCase())) {
        return true;
      }
      return false;
    },
    isDisabledTag(tag) {
      if (tag[this.tagName] === '其他' || tag[this.tagName] === '系统') {
        return true;
      }
      return false;
    },
    deleteItem(e) {
      if (this.text) return;
      else if (this.collection.length) {
        this.collection.pop();
      }
    },
    switchFullScreen(status) {
      this.isFullScreenShow = status;
    },
    tagClass(tag) {
      if (tag.multiple) {
        return 'tag';
      }
      return 'single-tag';
    },
    switchMore(row, parent) {
      row.expand = !row.expand;
      // 保证只有一行展开
      if (row.expand) {
        parent.data.forEach(rowItem => {
          if (rowItem !== row) {
            rowItem.expand = false;
          }
        });
      }
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
    /** 添加标签
     *   - 标签组支持多选时，将tag放入collection
     *   - 标签组不支持多选时，过滤掉collection的single标签，并将当前tag打上single标记，放入collection
     */
    addTag(tag, row, index) {
      if (this.data[index].multiple) {
        this.collection.push(tag);
        this.text = ''; // 清空输入框
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
        this.collection = this.collection.filter(tag => tag.multiple);
        this.collection.push(tag);
      }
      this.$emit('change', this.collection); // 同步v-model的tags
      this.changeTagStatus(tag, index);
    },
    removeTag(tag, index) {
      /** index 为空，说明是直接点击input的tag进行删除，需要改变一次状态 */
      if (index === undefined) {
        tag.active = !tag.active;
      }
      this.collection = this.collection.filter(item => item[this.tagId] !== tag[this.tagId]);
      this.$emit('change', this.collection);
      this.changeTagStatus(tag, index);
    },
    clearAll() {
      const topList = this.subTopList;
      const subList = this.subList;
      /** 清空input的内容
       *   清除所有标签的active状态
       *   清空textInput的内容
       */
      this.collection = [];
      this.data.forEach(tagGroup => {
        tagGroup.data.length
          && tagGroup.data.forEach(row => {
            row[topList]
              && row[topList].forEach(tag => {
                tag.active = false;
              });
            this.clearTagStatus(row);
          });
      });
      this.text = '';
      this.$emit('change', this.collection);
    },
    /** 递归清除所有标签的active状态 */
    clearTagStatus(tagGroup, clearAll = true) {
      const subList = this.subList;
      if (!tagGroup[subList] || tagGroup[subList].length === 0) return;
      tagGroup[subList].forEach(tag => {
        // 清空所有选择
        tag.active = false;
        return this.clearTagStatus(tag);
      });
    },
    /** 根据所选/清除标签， 同步同一个(tagId相同)的标签
     *       - 因为同一个标签可能在top_list的同时，也在sub_list，因此该函数通过遍历保证标签状态同步
     *       - 不支持多选时，需要将其他标签的状态置为false
     */
    changeTagStatus(tag, index) {
      const { active } = tag;
      if (index === undefined) {
        index = this.tags[0].includes(tag) ? 0 : 1;
      }

      this.tags[index].forEach(tagItem => {
        if (tagItem[this.tagId] === tag[this.tagId]) {
          tagItem.active = active;
        } else if (!tagItem.multiple) {
          tagItem.active = false;
        }
      });
    },
    /** 恢复初始状态 */
    resetStatus() {
      this.tags.forEach(tagGroup => {
        tagGroup.forEach(tag => {
          tag.active = false;
        });
      });
    },
  },
};
</script>

<style lang="scss">
@import './DataTag.scss';
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.1s ease-out;
}

.fade-enter,
.fade-leave-to {
  opacity: 0;
}
.full-screen-wrapper {
  overflow-y: scroll;
  width: 100%;
  display: flex;
  background: #fff;
  justify-content: center;
  position: fixed;
  left: 0px;
  bottom: 0;
  z-index: 2051;
  transition: opacity 1s;
  @media only screen and (max-width: 1800px) {
    /** broser smaller than 1800 */
    .data-tag-wrapper {
      width: 76%;
    }
  }
  @media only screen and (min-width: 1800px) {
    /** broser biger than 1800 */
    .data-tag-wrapper {
      width: 70%;
    }
  }
  .data-tag-wrapper {
    margin-top: 19px;
    z-index: 999;
    .operation-panel {
      margin-bottom: 20px;
    }
  }
}
</style>
