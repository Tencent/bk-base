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

/** Model - 获取模型列表
 * @param (query) {
     "biz_id": 591,
     "submitted_by": "admin",
     "request_field": ["project_id", "biz_id", "model_id",
                    "model_version_id", "model_name", "dscription",
                    "version_description"],
     "project_id": 1,
     "project_id_list": []
     "is_foggy": true,
     "is_public": true,
     "is_last_avaliable": true # 输出最新的
     "model_status": "complete"
}
 */
const getAlgorithmModleList = {
  url: '/v3/algorithm/v2/model/',
};

/** 获取模型版本信息
 * @params modelId
 * @query version_id
 */
const getModeVersion = {
  url: 'algorithm_models/:modelId/get_model_version/',
};

/**
 * @query project_id
 */
const getModeListSample = {
  url: 'algorithm_models/list_sample/',
};

const staticViusalScene = {
  url: 'static/dist/js/visual-scene.json',
};
export { getAlgorithmModleList, getModeVersion, getModeListSample, staticViusalScene };
