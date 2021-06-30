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

import static org.hamcrest.core.Is.is;

import com.tencent.bk.base.datalab.meta.Field;
import com.tencent.bk.base.datalab.meta.Storages;
import com.tencent.bk.base.datalab.meta.StoragesProperty;
import com.tencent.bk.base.datalab.meta.TableMeta;
import java.util.Comparator;
import java.util.List;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
@SpringBootTest
@EnableAutoConfiguration
public class MetaApiServiceImplTest {

    @Autowired
    private MetaApiService metaApiService;

    @Test
    @Ignore
    public void fetchTableFields() {
        String rt = "tab";
        TableMeta tableMeta = metaApiService.fetchTableFields(rt);
        final List<Field> fields = tableMeta.getFields();
        fields.sort(Comparator.comparingInt(Field::getFieldIndex));
        Assert.assertTrue(fields.size() > 0);
    }

    @Test
    @Ignore
    public void fetchTableStorages() {
        String rt = "tab";
        String expectedResult = "test_schema.tab";
        TableMeta tableMeta = metaApiService.fetchTableStorages(rt);
        final Storages storages = tableMeta.getStorages();
        StoragesProperty mysqlProperties = storages.getAdditionalProperties()
                .get("mysql");
        Assert.assertThat(mysqlProperties.getPhysicalTableName(), is(expectedResult));
    }
}