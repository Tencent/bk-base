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

package com.tencent.bk.base.datalab.queryengine.server.third;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class BatchJobConfig {

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("username")
    private String username;

    @JsonProperty("job_type")
    private String jobType = "one_time_sql";

    @JsonProperty("processing_logic")
    private String processingLogic;

    @JsonProperty("processing_type")
    private String processingType = "QuerySet";

    @JsonProperty("geog_area_code")
    private String geogAreaCode = "inland";

    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("inputs")
    private List<InputResultTable> inputs;

    @JsonProperty("outputs")
    private List<OutputResultTable> outputs;

    @JsonProperty("deploy_config")
    private Map<String, Object> deployConfig =
            ImmutableMap
                    .of("resource", ImmutableMap
                            .of("executor.memory", "8g",
                                    "executor.cores", 4,
                                    "executor.memoryOverhead", "4g"));

    @JsonProperty("queryset_params")
    private Map<String, Object> querysetParams;

    @Data
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)

    public static class InputResultTable {

        @JsonProperty("result_table_id")
        private String resultTableId;

        @JsonProperty("storage_type")
        private String storageType;

        @JsonProperty("partition")
        private Map<String, Object> partition;
    }

    @Data
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OutputResultTable {

        @JsonProperty("result_table_id")
        private String resultTableId;

        @JsonProperty("mode")
        private String mode;

        @JsonProperty("partition")
        private Map<String, Object> storage;
    }

}
