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

class IDE {
  constructor(options) {
    this.rtids = options.params.rtids;
    this.fieldsData = {};
    this.NoCompareArr = options.params.noCompareArr;
  }

  formatTableFieldsAttribute(response, rtid) {
    response.data
            && response.data.forEach((item) => {
              Object.keys(item).forEach((attr) => {
                if (!this.NoCompareArr.includes(attr)) {
                  // 过滤掉不必要的字段
                  if (!this.fieldsData[item.field_name]) {
                    // 初始化
                    this.fieldsData[item.field_name] = {
                      baseNode: [],
                      attrs: {},
                    };
                    this.fieldsData[item.field_name].baseNode.push(rtid); // 添加数据源的id
                  } else {
                    if (!this.fieldsData[item.field_name].baseNode.includes(rtid)) {
                      this.fieldsData[item.field_name].baseNode.push(rtid);
                    }
                  }
                  if (!this.fieldsData[item.field_name].attrs[attr]) {
                    // 添加字段属性
                    this.fieldsData[item.field_name].attrs[attr] = {
                      [item[attr]]: {
                        nodes: [rtid],
                      },
                    };
                  } else {
                    if (this.fieldsData[item.field_name].attrs[attr][item[attr]]) {
                      this.fieldsData[item.field_name].attrs[attr][item[attr]].nodes.push(rtid);
                    } else {
                      this.fieldsData[item.field_name].attrs[attr][item[attr]] = {
                        nodes: [rtid],
                      };
                    }
                  }
                }
              });
            });
  }

  formatMultiTableSchemaResponse(responses, options) {
    const { rtids } = options.params;
    responses.forEach((item, index) => {
      if (item.result) {
        this.formatTableFieldsAttribute(item, rtids[index], options);
      }
    });

    const valiSomeFun = function (item) {
      return item && item.nodes.length !== rtids.length;
    };

    Object.keys(this.fieldsData).forEach((key) => {
      this.fieldsData[key].isAvailable = !Object.keys(this.fieldsData[key].attrs).some((attr) => {
        const attrObj = this.fieldsData[key].attrs[attr];
        return attrObj && Object.keys(attrObj).some(sub => valiSomeFun(attrObj[sub]));
      });
    });

    return this.fieldsData;
  }
}

export default IDE;
