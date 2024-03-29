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

package com.tencent.bk.base.datalab.queryengine.validator.constant;

import com.google.common.collect.ImmutableList;
import java.util.List;

public class PrestoTestConstants {

    public static final List<String> KEY_WORDS = ImmutableList.of("ABSOLUTE",
            "ACTION",
            "ADD",
            "ABS",
            "AFTER",
            "ALL",
            "ALLOCATE",
            "ALLOW",
            "ALTER",
            "AND",
            "ANY",
            "ARE",
            "ARRAY",
            "ARRAY_MAX_CARDINALITY",
            "AS",
            "ASC",
            "ASENSITIVE",
            "ASSERTION",
            "ASYMMETRIC",
            "AT",
            "ATOMIC",
            "AUTHORIZATION",
            "AVG",
            "BEFORE",
            "BEGIN",
            "BEGIN_FRAME",
            "BEGIN_PARTITION",
            "BETWEEN",
            "BIGINT",
            "BINARY",
            "BIT",
            "BLOB",
            "BOOLEAN",
            "BOTH",
            "BREADTH",
            "BY",
            "CALL",
            "CALLED",
            "CARDINALITY",
            "CASCADE",
            "CASCADED",
            "CASE",
            "CAST",
            "CATALOG",
            "CEIL",
            "CEILING",
            "CHAR",
            "CHARACTER",
            "CHARACTER_LENGTH",
            "CHAR_LENGTH",
            "CHECK",
            "CLASSIFIER",
            "CLOB",
            "CLOSE",
            "COALESCE",
            "COLLATE",
            "COLLATION",
            "COLLECT",
            "COLUMN",
            "COMMIT",
            "CONDITION",
            "CONNECT",
            "CONNECTION",
            "CONSTRAINT",
            "CONSTRAINTS",
            "CONSTRUCTOR",
            "CONTAINS",
            "CONTINUE",
            "CONVERT",
            "CORR",
            "CORRESPONDING",
            "COUNT",
            "COVAR_POP",
            "COVAR_SAMP",
            "CREATE",
            "CROSS",
            "CUBE",
            "CUME_DIST",
            "CURRENT",
            "CURRENT_CATALOG",
            "CURRENT_DATE",
            "CURRENT_DEFAULT_TRANSFORM_GROUP",
            "CURRENT_PATH",
            "CURRENT_ROLE",
            "CURRENT_ROW",
            "CURRENT_SCHEMA",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
            "CURRENT_USER",
            "CURSOR",
            "CYCLE",
            "DATA",
            "DATE",
            "DAY",
            "DEALLOCATE",
            "DEC",
            "DECIMAL",
            "DECLARE",
            "DEFAULT",
            "DEFERRABLE",
            "DEFERRED",
            "DEFINE",
            "DELETE",
            "DENSE_RANK",
            "DEPTH",
            "DEREF",
            "DESC",
            "DESCRIBE",
            "DESCRIPTOR",
            "DETERMINISTIC",
            "DIAGNOSTICS",
            "DISALLOW",
            "DISCONNECT",
            "DISTINCT",
            "DOMAIN",
            "DOUBLE",
            "DROP",
            "DYNAMIC",
            "EACH",
            "ELEMENT",
            "ELSE",
            "EMPTY",
            "END",
            "END_FRAME",
            "END_PARTITION",
            "EQUALS",
            "ESCAPE",
            "EVERY",
            "EXCEPT",
            "EXCEPTION",
            "EXEC",
            "EXECUTE",
            "EXISTS",
            "EXIT",
            "EXP",
            "EXPLAIN",
            "EXTEND",
            "EXTERNAL",
            "EXTRACT",
            "FALSE",
            "FETCH",
            "FILTER",
            "FIRST",
            "FIRST_VALUE",
            "FLOAT",
            "FLOOR",
            "FOR",
            "FOREIGN",
            "FOUND",
            "FRAME_ROW",
            "FREE",
            "FROM",
            "FULL",
            "FUNCTION",
            "FUSION",
            "GENERAL",
            "GET",
            "GLOBAL",
            "GO",
            "GOTO",
            "GRANT",
            "GROUP",
            "GROUPING",
            "GROUPS",
            "HAVING",
            "HOLD",
            "HOUR",
            "IDENTITY",
            "IMMEDIATE",
            "IMMEDIATELY",
            "IMPORT",
            "IN",
            "INDICATOR",
            "INITIAL",
            "INITIALLY",
            "INNER",
            "INOUT",
            "INPUT",
            "INSENSITIVE",
            "INSERT",
            "INT",
            "INTEGER",
            "INTERSECT",
            "INTERSECTION",
            "INTERVAL",
            "INTO",
            "IS",
            "ISOLATION",
            "JOIN",
            "JSON_ARRAY",
            "JSON_ARRAYAGG",
            "JSON_EXISTS",
            "JSON_OBJECT",
            "JSON_OBJECTAGG",
            "JSON_QUERY",
            "JSON_VALUE",
            "KEY",
            "LAG",
            "LANGUAGE",
            "LARGE",
            "LAST",
            "LAST_VALUE",
            "LATERAL",
            "LEAD",
            "LEADING",
            "LEFT",
            "LEVEL",
            "LIKE",
            "LIKE_REGEX",
            "LIMIT",
            "LN",
            "LOCAL",
            "LOCALTIME",
            "LOCALTIMESTAMP",
            "LOCATOR",
            "LOWER",
            "MAP",
            "MATCH",
            "MATCHES",
            "MATCH_NUMBER",
            "MATCH_RECOGNIZE",
            "MAX",
            "MEASURES",
            "MEMBER",
            "MERGE",
            "METHOD",
            "MIN",
            "MINUS",
            "MINUTE",
            "MOD",
            "MODIFIES",
            "MODULE",
            "MONTH",
            "MULTISET",
            "NAMES",
            "NATIONAL",
            "NATURAL",
            "NCHAR",
            "NCLOB",
            "NEW",
            "NEXT",
            "NO",
            "NONE",
            "NORMALIZE",
            "NOT",
            "NTH_VALUE",
            "NTILE",
            "NULL",
            "NULLIF",
            "NUMERIC",
            "OBJECT",
            "OCCURRENCES_REGEX",
            "OCTET_LENGTH",
            "OF",
            "OFFSET",
            "OLD",
            "OMIT",
            "ON",
            "ONE",
            "ONLY",
            "OPEN",
            "OPTION",
            "OR",
            "ORDER",
            "ORDINALITY",
            "OUT",
            "OUTER",
            "OUTPUT",
            "OVER",
            "OVERLAPS",
            "OVERLAY",
            "PAD",
            "PARAMETER",
            "PARTIAL",
            "PARTITION",
            "PATH",
            "PATTERN",
            "PER",
            "PERCENT",
            "PERCENTILE_CONT",
            "PERCENTILE_DISC",
            "PERCENT_RANK",
            "PERIOD",
            "PERMUTE",
            "PORTION",
            "POSITION",
            "POSITION_REGEX",
            "POWER",
            "PRECEDES",
            "PRECISION",
            "PREPARE",
            "PRESERVE",
            "PREV",
            "PRIMARY",
            "PRIOR",
            "PRIVILEGES",
            "PROCEDURE",
            "PUBLIC",
            "RANGE",
            "RANK",
            "READ",
            "READS",
            "REAL",
            "RECURSIVE",
            "REF",
            "REFERENCES",
            "REFERENCING",
            "REGR_AVGX",
            "REGR_AVGY",
            "REGR_COUNT",
            "REGR_INTERCEPT",
            "REGR_R2",
            "REGR_SLOPE",
            "REGR_SXX",
            "REGR_SXY",
            "REGR_SYY",
            "RELATIVE",
            "RELEASE",
            "RESET",
            "RESTRICT",
            "RESULT",
            "RETURN",
            "RETURNS",
            "REVOKE",
            "RIGHT",
            "ROLE",
            "ROLLBACK",
            "ROLLUP",
            "ROUTINE",
            "ROW",
            "ROWS",
            "ROW_NUMBER",
            "RUNNING",
            "SAVEPOINT",
            "SCHEMA",
            "SCOPE",
            "SCROLL",
            "SEARCH",
            "SECOND",
            "SECTION",
            "SEEK",
            "SELECT",
            "SENSITIVE",
            "SESSION",
            "SESSION_USER",
            "SET",
            "SETS",
            "SHOW",
            "SIMILAR",
            "SIZE",
            "SKIP",
            "SMALLINT",
            "SOME",
            "SPACE",
            "SPECIFIC",
            "SPECIFICTYPE",
            "SQL",
            "SQLEXCEPTION",
            "SQLSTATE",
            "SQLWARNING",
            "SQRT",
            "START",
            "STATE",
            "STATIC",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "STREAM",
            "SUBMULTISET",
            "SUBSET",
            "SUBSTRING",
            "SUBSTRING_REGEX",
            "SUCCEEDS",
            "SUM",
            "SYMMETRIC",
            "SYSTEM",
            "SYSTEM_TIME",
            "SYSTEM_USER",
            "TABLE",
            "TABLESAMPLE",
            "TEMPORARY",
            "THEN",
            "TIME",
            "TIMESTAMP",
            "TIMEZONE_HOUR",
            "TIMEZONE_MINUTE",
            "TINYINT",
            "TO",
            "TRAILING",
            "TRANSACTION",
            "TRANSLATE",
            "TRANSLATE_REGEX",
            "TRANSLATION",
            "TREAT",
            "TRIGGER",
            "TRIM",
            "TRIM_ARRAY",
            "TRUE",
            "TRUNCATE",
            "UESCAPE",
            "UNDER",
            "UNION",
            "UNIQUE",
            "UNKNOWN",
            "UNNEST",
            "UPDATE",
            "UPPER",
            "UPSERT",
            "USAGE",
            "USER",
            "USING",
            "VALUE",
            "VALUES",
            "VALUE_OF",
            "VARBINARY",
            "VARCHAR",
            "VARYING",
            "VAR_POP",
            "VAR_SAMP",
            "VERSION",
            "VERSIONING",
            "VIEW",
            "WHEN",
            "WHENEVER",
            "WHERE",
            "WIDTH_BUCKET",
            "WINDOW",
            "WITH",
            "WITHIN",
            "WITHOUT",
            "WORK",
            "WRITE",
            "YEAR",
            "ZONE");

}
