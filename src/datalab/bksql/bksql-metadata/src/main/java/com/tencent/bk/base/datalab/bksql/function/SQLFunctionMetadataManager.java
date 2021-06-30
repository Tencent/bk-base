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

package com.tencent.bk.base.datalab.bksql.function;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.HandleCallback;
import org.jdbi.v3.core.HandleConsumer;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

public class SQLFunctionMetadataManager implements FunctionMetadataManager {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Jdbi jdbi;
    private final String tableName;

    public SQLFunctionMetadataManager(String url, final String tableName) throws Exception {
        jdbi = Jdbi.create(url);
        this.tableName = tableName;
        createFunctionTable();
    }

    private void createFunctionTable() throws Exception {
        jdbi.useHandle(new HandleConsumer<Exception>() {
            @Override
            public void useHandle(Handle handle) throws Exception {
                handle.createBatch()
                        .add(String.format("CREATE TABLE IF NOT EXISTS %s ("
                                + "id BIGINT(20) AUTO_INCREMENT NOT NULL,"
                                + "namespace VARCHAR(255) NOT NULL,"
                                + "function_name VARCHAR(255) NOT NULL,"
                                + "description VARCHAR(255) NOT NULL,"
                                + "parameter_list VARCHAR(255) NOT NULL,"
                                + "return_type VARCHAR(255) NOT NULL,"
                                + "payload BLOB NOT NULL,"
                                + "PRIMARY KEY (id)"
                                + ")", tableName))
                        .add(String.format("CREATE INDEX idx_1 ON %s(namespace, function_name)",
                                tableName))
                        .execute();
            }
        });
    }

    @Override
    public Writer writer() {
        return new WriterImpl(this);
    }

    @Override
    public Reader reader(List<String> namespaces) {
        return new ReaderImpl(this, namespaces);
    }

    public static class ReaderImpl implements Reader {

        private final SQLFunctionMetadataManager manager;
        private final List<String> namespaces;

        public ReaderImpl(SQLFunctionMetadataManager manager, List<String> namespaces) {
            this.manager = manager;
            this.namespaces = namespaces;
        }

        @Override
        public List<FunctionMetadata> fetchFunctionMetadata(final String functionName)
                throws Exception {
            return manager.jdbi.withHandle(new HandleCallback<List<FunctionMetadata>, Exception>() {
                @Override
                public List<FunctionMetadata> withHandle(Handle handle) {
                    return handle.createQuery(String.format(
                            "SELECT payload FROM %s WHERE namespace IN (<namespaces>) AND "
                                    + "function_name = :function_name ORDER BY id DESC",
                            manager.tableName))
                            .bindList("namespaces", namespaces)
                            .bind("function_name", functionName)
                            .map(new RowMapper<FunctionMetadata>() {
                                @Override
                                public FunctionMetadata map(ResultSet rs, StatementContext ctx)
                                        throws SQLException {
                                    try {
                                        return manager.objectMapper
                                                .readValue(rs.getBytes("payload"),
                                                        FunctionMetadata.class);
                                    } catch (IOException e) {
                                        throw new SQLException(e);
                                    }
                                }
                            })
                            .list();
                }
            });
        }

        @Override
        public List<FunctionMetadata> fetchAllFunctionMetadata() throws Exception {
            return manager.jdbi.withHandle(new HandleCallback<List<FunctionMetadata>, Exception>() {
                @Override
                public List<FunctionMetadata> withHandle(Handle handle) {
                    return handle.createQuery(String.format(
                            "SELECT payload FROM %s WHERE namespace IN (<namespaces>) ORDER BY id"
                                    + " DESC",
                            manager.tableName))
                            .bindList("namespaces", namespaces)
                            .map(new RowMapper<FunctionMetadata>() {
                                @Override
                                public FunctionMetadata map(ResultSet rs, StatementContext ctx)
                                        throws SQLException {
                                    try {
                                        return manager.objectMapper
                                                .readValue(rs.getBytes("payload"),
                                                        FunctionMetadata.class);
                                    } catch (IOException e) {
                                        throw new SQLException(e);
                                    }
                                }
                            })
                            .list();
                }
            });
        }

    }


    public static class WriterImpl implements Writer {

        private final SQLFunctionMetadataManager manager;

        public WriterImpl(SQLFunctionMetadataManager manager) {
            this.manager = manager;
        }

        @Override
        public Integer addFunctionMetadata(final FunctionMetadata functionMetadata)
                throws Exception {
            return manager.jdbi.withHandle(new HandleCallback<Integer, Exception>() {
                @Override
                public Integer withHandle(Handle handle) throws Exception {
                    return handle.createUpdate(String.format(
                            "INSERT INTO %s (namespace, function_name, description, "
                                    + "parameter_list, return_type, payload)"
                                    + " VALUES (:namespace, :function_name, :description, "
                                    + ":parameter_list, :return_type, :payload)",
                            manager.tableName))
                            .bind("namespace", functionMetadata.getNamespace())
                            .bind("function_name", functionMetadata.getFunctionName())
                            .bind("description", functionMetadata.getDescription())
                            .bind("parameter_list", manager.objectMapper
                                    .writeValueAsString(functionMetadata.getParameterList()))
                            .bind("return_type", functionMetadata.getReturnType())
                            .bind("payload",
                                    manager.objectMapper.writeValueAsBytes(functionMetadata))
                            .execute();
                }
            });
        }
    }
}
