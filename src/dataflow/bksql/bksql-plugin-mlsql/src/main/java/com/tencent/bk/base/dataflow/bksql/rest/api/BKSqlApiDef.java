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

package com.tencent.bk.base.dataflow.bksql.rest.api;

import com.tencent.bk.base.datalab.bksql.core.BKSqlService;
import com.tencent.bk.base.datalab.bksql.rest.wrapper.ParsingContext;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/bksql/api")
public class BKSqlApiDef {

    private static final Logger logger = LoggerFactory.getLogger(BKSqlApiDef.class);

    @Inject
    private BKSqlService bkSqlService;

    @POST
    @Path("/convert/{type}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Object convert(
            @PathParam("type") String type,
            ParsingContext parsingContext
    ) throws Exception {
        try {
            if (!bkSqlService.isRegistered(type)) {
                throw new IllegalArgumentException("invalid com.tencent.bk.base.dataflow.bksql.protocol type: " + type);
            }
            return bkSqlService.convert(type, parsingContext);
        } catch (Exception e) {
            //e.printStackTrace();
            logger.error(String.format("Error converting sql [%s] using type: %s", parsingContext.getSql(), type), e);
            throw e;
        }
    }

    /**
     * 定义访问接口
     */
    @POST
    @Path("/multiconvert/{types}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Object multiConvert(
            @PathParam("types") String types,
            ParsingContext parsingContext
    ) {
        return Arrays.stream(types.split(","))
                .collect(Collectors.toSet())
                .stream()
                .map(type -> {
                    if (!bkSqlService.isRegistered(type)) {
                        throw new IllegalArgumentException("invalid com.tencent.bk.base.dataflow.bksql.protocol type: "
                                + type);
                    }
                    try {
                        return new AbstractMap.SimpleEntry<>(type, bkSqlService.convert(type, parsingContext));
                    } catch (Exception e) {
                        logger.error(String.format("Error converting sql [%s] using type: %s", parsingContext.getSql(),
                                type), e);
                        throw new RuntimeException(e);
                    }
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @GET
    @Path("/protocols")
    @Produces(MediaType.APPLICATION_JSON)
    public Object protocols() throws Exception {
        return bkSqlService.protocols();
    }

    @GET
    @Path("/protocols/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    public Object protocol(@PathParam("name") String name) throws Exception {
        return bkSqlService.protocol(name);
    }
}
