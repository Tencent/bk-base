/*
 * Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
 *
 * Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
 *
 * BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
 *
 * License for BK-BASE 蓝鲸基础平台:
 * --------------------------------------------------------------------
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
 * documentation files (the "Software"), to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
 * and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
 * LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
 * NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.tencent.bk.base.dataflow.bksql.mlsql.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.meta.MLSqlInputConfig;
import com.tencent.bk.base.dataflow.bksql.mlsql.model.meta.MLSqlOutputConfig;

public class MLSqlModel {

    @JsonProperty("model_name")
    private String modelName;

    @JsonProperty("model_alias")
    private String modelAlias;
    private MLSqlModelFrameWork framework;
    private MLSqlModelType type;

    @JsonProperty("project_id")
    private Integer projectID;

    @JsonProperty("algorithm_name")
    private String algorithmName;

    @JsonProperty("model_storage")
    private MLSqlModelStorage modelStorage;

    @JsonProperty("train_mode")
    private MLSqlModelTrainMode trainMode;
    private String sensitivity;
    private Integer active;
    private MLSqlModelStatus status;

    @JsonProperty("input_config")
    private MLSqlInputConfig inputConfig;

    @JsonProperty("output_config")
    private MLSqlOutputConfig outputConfig;

    public MLSqlModel() {

    }

    public String getModelName() {
        return modelName;
    }

    public void setModelName(String name) {
        this.modelName = name;
        this.modelAlias = name;
    }

    public String getModelAlias() {
        return modelAlias;
    }

    public MLSqlModelFrameWork getFramework() {
        return framework;
    }

    public MLSqlModelType getType() {
        return type;
    }

    public Integer getProjectID() {
        return projectID;
    }

    public String getAlgorithmName() {
        return algorithmName;
    }

    public void setAlgorithmName(String algorithmName) {
        this.algorithmName = algorithmName;
    }

    public MLSqlModelStorage getModelStorageId() {
        return modelStorage;
    }

    public MLSqlModelTrainMode getTrainMode() {
        return trainMode;
    }

    public String getSensitivity() {
        return sensitivity;
    }

    public Integer getActive() {
        return active;
    }

    public MLSqlModelStatus getStatus() {
        return status;
    }

    public MLSqlInputConfig getInputConfig() {
        return inputConfig;
    }

    public MLSqlOutputConfig getOutputConfig() {
        return outputConfig;
    }

    public void setModelStorage(MLSqlModelStorage storage) {
        this.modelStorage = storage;
    }

}
