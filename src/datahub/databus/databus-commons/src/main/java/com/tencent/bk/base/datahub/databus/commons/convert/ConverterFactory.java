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

package com.tencent.bk.base.datahub.databus.commons.convert;

import com.tencent.bk.base.datahub.databus.commons.Consts;
import com.tencent.bk.base.datahub.databus.commons.TaskContext;

public class ConverterFactory {

    private ConverterFactory() {
    }

    private static class Holder {

        private static final ConverterFactory INSTANCE = new ConverterFactory();
    }

    public static final ConverterFactory getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * 根据task context中的源数据类型创建对应的converter
     *
     * @param ctx TaskContext
     * @return 对应的数据converter
     */
    public Converter createConverter(TaskContext ctx) {
        Converter converter = null;
        switch (ctx.getSourceMsgType().toLowerCase()) {
            case Consts.ETL:
                converter = new EtlConverter(ctx.getEtlConf(), ctx.getDataId());
                break;
            case Consts.AVRO:
                converter = new AvroConverter();
                break;
            case Consts.JSON:
                converter = new JsonConverter();
                break;
            case Consts.PARQUET:
                converter = new ParquetConverter();
                break;
            case Consts.DOCKER_LOG:
                converter = new DockerLogConverter();
                break;
            default:
                converter = new JsonConverter();
        }

        return converter;
    }
}
