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

/* eslint-disable no-param-reassign */
/**
 *  数据标签
 */

/** 递归进行标签的状态清除动作 */
const clearStatus = function (tagList, key) {
  if (!tagList[key] || (tagList[key] && tagList[key].length === 0)) {
    return;
  }
  tagList[key].forEach((tag) => {
    tag.active = false;
  });
  return clearStatus(tagList[key]);
};

const state = {
  dataTagList: [],
  overallList: [],
  multipleTagsGroup: [],
  singleTagsGroup: [],
  dictType: false, // 是否是数据字典所用标签
  flatTags: [], // 所有数据标签扁平集合，flatTags[0]为多选标签集合， flatTags[1]为多选标签
};
const getters = {
  getTagList: state => state.dataTagList,
  getOverallTags: state => state.overallList,
  isDictTags: state => state.dictType,
  getMultipleTagsGroup: state => state.multipleTagsGroup,
  getSingleTagsGroup: state => state.singleTagsGroup,
  getAllFlatTags: state => state.flatTags,
};

const actions = {
  initTagData({ commit }, payload) {
    commit('resetTagData');
    commit('setDataTagList', payload.tagList);
    commit('setOverallTags', payload.overall);
    commit('setTagsType', payload.dataDict);
    commit('setFlatTags');
    commit('setTagGroups');
  },
};
const mutations = {
  /** 清空标签系统状态 */
  resetTagData(state) {
    state.dataTagList.forEach((group) => {
      group.data = [];
    });
    state.multipleTagsGroup = [];
    state.singleTagsGroup = [];
    state.flatTags = [];
  },
  /** 设置标签列表 */
  setDataTagList(state, list) {
    state.dataTagList = list.splice(0);
  },
  /** 设置热门标签 */
  setOverallTags(state, tags) {
    state.overallList = tags.splice(0);
  },
  /** 设置标签类型 */
  setTagsType(state, isDictTags) {
    state.dictType = isDictTags;
  },
  /** 根据标签的多/单选特性进行标签分组 */
  setTagGroups(state) {
    state.flatTags.forEach((tags) => {
      tags.forEach((tag) => {
        if (tag.multiple) {
          state.multipleTagsGroup.push(tag);
        } else if (Object.prototype.hasOwnProperty.call(tag, 'multiple') && !tag.multiple) {
          // 保证标签具有multiple属性，即正确存储的标签系统
          state.singleTagsGroup.push(tag);
        }
      });
    });
  },
  /** 拍平标签列表 */
  setFlatTags(state) {
    const tagGroup = [];
    state.dataTagList.forEach((group) => {
      const tags = [];
      group.data.forEach((tagGroup) => {
        tagGroup.sub_top_list.forEach((tag) => {
          tags.push(tag);
        });
        tagGroup.sub_list
                    && tagGroup.sub_list.forEach((tagItem) => {
                      tags.push(tagItem);
                      tagItem.sub_list
                            && tagItem.sub_list.forEach((item) => {
                              tags.push(item);
                            });
                    });
      });
      tagGroup.push(tags);
    });
    state.flatTags = tagGroup;
  },
  /** 设置标签的label */
  setDataTagLabel(state, label) {
    state.dataTagList.forEach((item, index) => {
      item.label = label[index];
    });
  },
  setTagStatus(state, labelInfo) {
    const { id, status } = labelInfo;
    state.flatTags.forEach((group) => {
      group.forEach((tag) => {
        if (tag.tag_code === id) {
          tag.active = status;
        }
      });
    });
  },
  /** 清除标签状态，所有标签的active置为false */
  clearActiveStatus(state) {
    state.dataTagList.forEach((group) => {
      group.data.forEach((tagGroup) => {
        ['sub_top_list', 'sub_list'].forEach((key) => {
          clearStatus(tagGroup, key);
        });
      });
    });
  },
};

export default {
  namespaced: true,
  state,
  getters,
  actions,
  mutations,
};
