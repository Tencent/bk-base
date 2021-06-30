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

/* eslint-disable array-callback-return */
/* eslint-disable no-param-reassign */
import { mapGetters } from 'vuex';
export default {
  computed: {
    ...mapGetters({
      isDictTags: 'dataTag/isDictTags',
    }),
  },
  methods: {
    /** 标签列表的code包含|, 在回填时需要进一步处理 */
    getTagCode(code) {
      if (code.includes('|')) {
        return code.split('|')[0];
      }
      return code;
    },
    /**
     * @param {*} overall 是否展示整体热门标签
     * @param {*} dataDict 是否在数据字典页面，与数据集成的标签内容不一样
     */
    getDataTagListFromServer(overall = false, dataDict = false) {
      const queryCongfig = {
        top: 12,
        is_latest_api: 1,
        overall_top: 10,
        is_overall_topn_shown: overall ? 1 : 0,
        used_in_data_dict: dataDict ? 1 : 0,
      };
      /** 当标签为空，或者类型改变时，重新拉去标签，否则清除标签的选中状态 */
      if (this.getSingleTagsGroup.length === 0 || this.isDictTags !== dataDict) {
        return this.bkRequest
          .httpRequest('dataAccess/getDataTagList', {
            query: queryCongfig,
          })
          .then((res) => {
            if (res.result) {
              res.result
                && this.$store.dispatch('dataTag/initTagData', {
                  tagList: res.data.tag_list,
                  overall: res.data.overall_top_tag_list,
                  dataDict,
                });
            } else {
              this.getMethodWarning(res.message, res.code);
            }
          });
      }
      this.$store.commit('dataTag/clearActiveStatus');
    },
    /** tag标签去重 */
    tagsDeduplicate(group, id) {
      const obj = {};
      return group.filter((tag) => {
        // eslint-disable-next-line no-prototype-builtins
        if (!obj.hasOwnProperty(tag[id])) {
          obj[tag[id]] = true;
          return true;
        }
        return false;
      });
    },
    handleTagData(obj, filterObj, isMultiple = true) {
      obj.forEach((item) => {
        filterObj.filter((opt) => {
          const code = this.getTagCode(opt.tag_code);
          if (code === item.code) {
            item.isMultiple = isMultiple;
          }
        });
      });
    },
  },
};
