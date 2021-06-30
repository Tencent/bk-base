# -*- coding: utf-8 -*-
"""
Tencent is pleased to support the open source community by making BK-BASE 蓝鲸基础平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-BASE 蓝鲸基础平台 is licensed under the MIT License.
License for BK-BASE 蓝鲸基础平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import logging

import parse

from metadata.exc import MetaDataErrorCodes, MySQLBackendError
from metadata.util.common import snake_to_camel
from metadata.util.i18n import selfish as _

logging.getLogger('parse').setLevel(logging.INFO)

raw_errors_info = [
    ['1000', 'ER_HASHCHK', _("hashchk")],
    ['1001', 'ER_NISAMCHK', _("isamchk")],
    ['1002', 'ER_NO', _("NO")],
    ['1003', 'ER_YES', _("YES")],
    ['1004', 'ER_CANT_CREATE_FILE', _("Can't create file '{}' (errno: {})")],
    ['1005', 'ER_CANT_CREATE_TABLE', _("Can't create table '{}' (errno: {})")],
    ['1006', 'ER_CANT_CREATE_DB', _("Can't create database '{}' (errno: {}")],
    ['1007', 'ER_DB_CREATE_EXISTS', _("Can't create database '{}'; database exists")],
    ['1008', 'ER_DB_DROP_EXISTS', _("Can't drop database '{}'; database doesn't exist")],
    ['1009', 'ER_DB_DROP_DELETE', _("Error dropping database (can't delete '{}', errno: {})")],
    ['1010', 'ER_DB_DROP_RMDIR', _("Error dropping database (can't rmdir '{}', errno: {})")],
    ['1011', 'ER_CANT_DELETE_FILE', _("Error on delete of '{}' (errno: {})")],
    ['1012', 'ER_CANT_FIND_SYSTEM_REC', _("Can't read record in system table")],
    ['1013', 'ER_CANT_GET_STAT', _("Can't get status of '{}' (errno: {})")],
    ['1014', 'ER_CANT_GET_WD', _("Can't get working directory (errno: {})")],
    ['1015', 'ER_CANT_LOCK', _("Can't lock file (errno: {})")],
    ['1016', 'ER_CANT_OPEN_FILE', _("Can't open file: '{}' (errno: {})")],
    ['1017', 'ER_FILE_NOT_FOUND', _("Can't find file: '{}' (errno: {})")],
    ['1018', 'ER_CANT_READ_DIR', _("Can't read dir of '{}' (errno: {})")],
    ['1019', 'ER_CANT_SET_WD', _("Can't change dir to '{}' (errno: {})")],
    ['1020', 'ER_CHECKREAD', _("Record has changed since last read in table '{}'")],
    ['1021', 'ER_DISK_FULL', _("Disk full ({}); waiting for someone to free some space...")],
    ['1022', 'ER_DUP_KEY', _("Can't write; duplicate key in table '{}'")],
    ['1023', 'ER_ERROR_ON_CLOSE', _("Error on close of '{}' (errno: {})")],
    ['1024', 'ER_ERROR_ON_READ', _("Error reading file '{}' (errno: {})")],
    ['1025', 'ER_ERROR_ON_RENAME', _("Error on rename of '{}' to '{}' (errno: {})")],
    ['1026', 'ER_ERROR_ON_WRITE', _("Error writing file '{}' (errno: {})")],
    ['1027', 'ER_FILE_USED', _("'{}' is locked against change")],
    ['1028', 'ER_FILSORT_ABORT', _("Sort aborted")],
    ['1029', 'ER_FORM_NOT_FOUND', _("View '{}' doesn't exist for '{}'")],
    ['1030', 'ER_GET_ERRN', _("Got error {} from storage engine")],
    ['1031', 'ER_ILLEGAL_HA', _("Table storage engine for '{}' doesn't have this option")],
    ['1032', 'ER_KEY_NOT_FOUND', _("Can't find record in '{}'")],
    ['1033', 'ER_NOT_FORM_FILE', _("Incorrect information in file: '{}'")],
    ['1034', 'ER_NOT_KEYFILE', _("Incorrect key file for table '{}'; try to repair it")],
    ['1035', 'ER_OLD_KEYFILE', _("Old key file for table '{}'; repair it!")],
    ['1036', 'ER_OPEN_AS_READONLY', _("Table '{}' is read only")],
    ['1037', 'ER_OUTOFMEMORY', _("Out of memory; restart server and try again (needed {} bytes)")],
    ['1038', 'ER_OUT_OF_SORTMEMORY', _("Out of sort memory, consider increasing server sort buffer size")],
    ['1039', 'ER_UNEXPECTED_EOF', _("Unexpected EOF found when reading file '{}' (Errno: {})")],
    ['1040', 'ER_CON_COUNT_ERROR', _("Too many connections")],
    [
        '1041',
        'ER_OUT_OF_RESOURCES',
        _(
            "Out of memory; check if mysqld or some other process uses all available memory; "
            "if not, you may have to use 'ulimit' to allow mysqld to use more memory or you can add more swap space"
        ),
    ],
    ['1042', 'ER_BAD_HOST_ERROR', _("Can't get hostname for your address")],
    ['1043', 'ER_HANDSHAKE_ERROR', _("Bad handshake")],
    ['1044', 'ER_DBACCESS_DENIED_ERROR', _("Access denied for user '{}'@'{}' to database '{}'")],
    ['1045', 'ER_ACCESS_DENIED_ERROR', _("Access denied for user '{}'@'{}' (using password: {})")],
    ['1046', 'ER_NO_DB_ERROR', _("No database selected")],
    ['1047', 'ER_UNKNOWN_COM_ERROR', _("Unknown command")],
    ['1048', 'ER_BAD_NULL_ERROR', _("Column '{}' cannot be null")],
    ['1049', 'ER_BAD_DB_ERROR', _("Unknown database '{}'")],
    ['1050', 'ER_TABLE_EXISTS_ERROR', _("Table '{}' already exists")],
    ['1051', 'ER_BAD_TABLE_ERROR', _("Unknown table '{}'")],
    ['1052', 'ER_NON_UNIQ_ERROR', _("Column '{}' in {} is ambiguous")],
    ['1053', 'ER_SERVER_SHUTDOWN', _("Server shutdown in progress")],
    ['1054', 'ER_BAD_FIELD_ERROR', _("Unknown column '{}' in '{}'")],
    ['1055', 'ER_WRONG_FIELD_WITH_GROUP', _("'{}' isn't in GROUP BY")],
    ['1056', 'ER_WRONG_GROUP_FIELD', _("Can't group on '{}'")],
    ['1057', 'ER_WRONG_SUM_SELECT', _("Statement has sum functions and columns in same statement")],
    ['1058', 'ER_WRONG_VALUE_COUNT', _("Column count doesn't match value count")],
    ['1059', 'ER_TOO_LONG_IDENT', _("Identifier name '{}' is too long")],
    ['1060', 'ER_DUP_FIELDNAME', _("Duplicate column name '{}'")],
    ['1061', 'ER_DUP_KEYNAME', _("Duplicate key name '{}'")],
    ['1062', 'ER_DUP_ENTRY', _("Duplicate entry '{}' for key {}")],
    ['1063', 'ER_WRONG_FIELD_SPEC', _("Incorrect column specifier for column '{}'")],
    ['1064', 'ER_PARSE_ERROR', _("{} near '{}' at line {}")],
    ['1065', 'ER_EMPTY_QUERY', _("Query was empty")],
    ['1066', 'ER_NONUNIQ_TABLE', _("Not unique table/alias: '{}'")],
    ['1067', 'ER_INVALID_DEFAULT', _("Invalid default value for '{}'")],
    ['1068', 'ER_MULTIPLE_PRI_KEY', _("Multiple primary key defined")],
    ['1069', 'ER_TOO_MANY_KEYS', _("Too many keys specified; max {} keys allowed")],
    ['1070', 'ER_TOO_MANY_KEY_PARTS', _("Too many key parts specified; max {} parts allowed")],
    ['1071', 'ER_TOO_LONG_KEY', _("Specified key was too long; max key length is {} bytes")],
    ['1072', 'ER_KEY_COLUMN_DOES_NOT_EXITS', _("Key column '{}' doesn't exist in table")],
    ['1073', 'ER_BLOB_USED_AS_KEY', _("BLOB column '{}' can't be used in key specification with the used table type")],
    ['1074', 'ER_TOO_BIG_FIELDLENGTH', _("Column length too big for column '{}' (max = {}); use BLOB or TEXT instead")],
    [
        '1075',
        'ER_WRONG_AUTO_KEY',
        _("Incorrect table definition; there can be only one auto column and it must be defined as a key"),
    ],
    ['1076', 'ER_READY', _("{}: ready for connections. Version: '{}' socket: '{}' port: {}")],
    ['1077', 'ER_NORMAL_SHUTDOWN', _("{}: Normal shutdown")],
    ['1078', 'ER_GOT_SIGNAL', _("{}: Got signal {}. Aborting!")],
    ['1079', 'ER_SHUTDOWN_COMPLETE', _("{}: Shutdown complete")],
    ['1080', 'ER_FORCING_CLOSE', _("{}: Forcing close of thread {} user: '{}'")],
    ['1081', 'ER_IPSOCK_ERROR', _("Can't create IP socket")],
    ['1082', 'ER_NO_SUCH_INDEX', _("Table '{}' has no index like the one used in CREATE INDEX; recreate the table")],
    ['1083', 'ER_WRONG_FIELD_TERMINATORS', _("Field separator argument is not what is expected; check the manual")],
    [
        '1084',
        'ER_BLOBS_AND_NO_TERMINATED',
        _("You can't use fixed rowlength with BLOBs; please use 'fields terminated by'"),
    ],
    ['1085', 'ER_TEXTFILE_NOT_READABLE', _("The file '{}' must be in the database directory or be readable by all")],
    ['1086', 'ER_FILE_EXISTS_ERROR', _("File '{}' already exists")],
    ['1087', 'ER_LOAD_INF', _("Records: {} Deleted: {} Skipped: {} Warnings: {}")],
    ['1088', 'ER_ALTER_INF', _("Records: {} Duplicates: {}")],
    [
        '1089',
        'ER_WRONG_SUB_KEY',
        _(
            "Incorrect prefix key; the used key part isn't a string, the used length is longer than the key part, "
            "or the storage engine doesn't support unique prefix keys"
        ),
    ],
    ['1090', 'ER_CANT_REMOVE_ALL_FIELDS', _("You can't delete all columns with ALTER TABLE; use DROP TABLE instead")],
    ['1091', 'ER_CANT_DROP_FIELD_OR_KEY', _("Can't DROP '{}'; check that column/key exists")],
    ['1092', 'ER_INSERT_INF', _("Records: {} Duplicates: {} Warnings: {}")],
    ['1093', 'ER_UPDATE_TABLE_USED', _("You can't specify target table '{}' for update in FROM clause")],
    ['1094', 'ER_NO_SUCH_THREAD', _("Unknown thread id: {}")],
    ['1095', 'ER_KILL_DENIED_ERROR', _("You are not owner of thread {}")],
    ['1096', 'ER_NO_TABLES_USED', _("No tables used")],
    ['1097', 'ER_TOO_BIG_SET', _("Too many strings for column {} and SET")],
    ['1098', 'ER_NO_UNIQUE_LOGFILE', _("Can't generate a unique log-filename {}.(1-999)")],
    ['1099', 'ER_TABLE_NOT_LOCKED_FOR_WRITE', _("Table '{}' was locked with a READ lock and can't be updated")],
    ['1100', 'ER_TABLE_NOT_LOCKED', _("Table '{}' was not locked with LOCK TABLES")],
    ['1101', 'ER_BLOB_CANT_HAVE_DEFAULT', _("BLOB/TEXT column '{}' can't have a default value")],
    ['1102', 'ER_WRONG_DB_NAME', _("Incorrect database name '{}'")],
    ['1103', 'ER_WRONG_TABLE_NAME', _("Incorrect table name '{}'")],
    [
        '1104',
        'ER_TOO_BIG_SELECT',
        _(
            "The SELECT would examine more than MAX_JOIN_SIZE rows; check your WHERE and use SET SQL_BIG_SELECTS=1 "
            "or SET MAX_JOIN_SIZE=# if the SELECT is okay"
        ),
    ],
    ['1105', 'ER_UNKNOWN_ERROR', _("Unknown error")],
    ['1106', 'ER_UNKNOWN_PROCEDURE', _("Unknown procedure '{}'")],
    ['1107', 'ER_WRONG_PARAMCOUNT_TO_PROCEDURE', _("Incorrect parameter count to procedure '{}'")],
    ['1108', 'ER_WRONG_PARAMETERS_TO_PROCEDURE', _("Incorrect parameters to procedure '{}'")],
    ['1109', 'ER_UNKNOWN_TABLE', _("Unknown table '{}' in {}")],
    ['1110', 'ER_FIELD_SPECIFIED_TWICE', _("Column '{}' specified twice")],
    ['1111', 'ER_INVALID_GROUP_FUNC_USE', _("Invalid use of group function")],
    ['1112', 'ER_UNSUPPORTED_EXTENSION', _("Table '{}' uses an extension that doesn't exist in this MariaDB version")],
    ['1113', 'ER_TABLE_MUST_HAVE_COLUMNS', _("A table must have at least 1 column")],
    ['1114', 'ER_RECORD_FILE_FULL', _("The table '{}' is full")],
    ['1115', 'ER_UNKNOWN_CHARACTER_SET', _("Unknown character set: '{}'")],
    ['1116', 'ER_TOO_MANY_TABLES', _("Too many tables; MariaDB can only use {} tables in a join")],
    ['1117', 'ER_TOO_MANY_FIELDS', _("Too many columns")],
    [
        '1118',
        'ER_TOO_BIG_ROWSIZE',
        _(
            "Row size too large. The maximum row size for the used table type, not counting BLOBs, "
            "is {}. You have to change some columns to TEXT or BLOBs"
        ),
    ],
    [
        '1119',
        'ER_STACK_OVERRUN',
        _(
            "Thread stack overrun: Used: {} of a {} stack. Use 'mysqld --thread_stack=#' "
            "to specify a bigger stack if needed"
        ),
    ],
    ['1120', 'ER_WRONG_OUTER_JOIN', _("Cross dependency found in OUTER JOIN; examine your ON conditions")],
    [
        '1121',
        'ER_NULL_COLUMN_IN_INDEX',
        _(
            "Table handler doesn't support NULL in given index. Please change column '{}' to be NOT NULL "
            "or use another handler"
        ),
    ],
    ['1122', 'ER_CANT_FIND_UDF', _("Can't load function '{}'")],
    ['1123', 'ER_CANT_INITIALIZE_UDF', _("Can't initialize function '{}'; {}")],
    ['1124', 'ER_UDF_NO_PATHS', _("No paths allowed for shared library")],
    ['1125', 'ER_UDF_EXISTS', _("Function '{}' already exists")],
    ['1126', 'ER_CANT_OPEN_LIBRARY', _("Can't open shared library '{}' (Errno: {} {})")],
    ['1127', 'ER_CANT_FIND_DL_ENTRY', _("Can't find symbol '{}' in library")],
    ['1128', 'ER_FUNCTION_NOT_DEFINED', _("Function '{}' is not defined")],
    [
        '1129',
        'ER_HOST_IS_BLOCKED',
        _("Host '{}' is blocked because of many connection errors; unblock with 'mysqladmin flush-hosts'"),
    ],
    ['1130', 'ER_HOST_NOT_PRIVILEGED', _("Host '{}' is not allowed to connect to this MariaDB server")],
    [
        '1131',
        'ER_PASSWORD_ANONYMOUS_USER',
        _("You are using MariaDB as an anonymous user and anonymous users are not allowed to change passwords"),
    ],
    [
        '1132',
        'ER_PASSWORD_NOT_ALLOWED',
        _("You must have privileges to update tables in the mysql database to be able to change passwords for others"),
    ],
    ['1133', 'ER_PASSWORD_NO_MATCH', _("Can't find any matching row in the user table")],
    ['1134', 'ER_UPDATE_INF', _("Rows matched: {} Changed: {} Warnings: {}")],
    [
        '1135',
        'ER_CANT_CREATE_THREAD',
        _(
            "Can't create a new thread (Errno {}); if you are not out of available memory, "
            "you can consult the manual for a possible OS-dependent bug"
        ),
    ],
    ['1136', 'ER_WRONG_VALUE_COUNT_ON_ROW', _("Column count doesn't match value count at row {}")],
    ['1137', 'ER_CANT_REOPEN_TABLE', _("Can't reopen table: '{}'")],
    ['1138', 'ER_INVALID_USE_OF_NULL', _("Invalid use of NULL value")],
    ['1139', 'ER_REGEXP_ERROR', _("Got error '{}' from regexp")],
    [
        '1140',
        'ER_MIX_OF_GROUP_FUNC_AND_FIELDS',
        _(
            "Mixing of GROUP columns (MIN(),MAX(),COUNT(),...) with no GROUP columns is illegal "
            "if there is no GROUP BY clause"
        ),
    ],
    ['1141', 'ER_NONEXISTING_GRANT', _("There is no such grant defined for user '{}' on host '{}'")],
    ['1142', 'ER_TABLEACCESS_DENIED_ERROR', _("{} command denied to user '{}'@'{}' for table '{}'")],
    ['1143', 'ER_COLUMNACCESS_DENIED_ERROR', _("{} command denied to user '{}'@'{}' for column '{}' in table '{}'")],
    [
        '1144',
        'ER_ILLEGAL_GRANT_FOR_TABLE',
        _("Illegal GRANT/REVOKE command; please consult the manual to see which privileges can be used"),
    ],
    ['1145', 'ER_GRANT_WRONG_HOST_OR_USER', _("The host or user argument to GRANT is too long")],
    ['1146', 'ER_NO_SUCH_TABLE', _("Table '{}.{}' doesn't exist")],
    [
        '1147',
        'ER_NONEXISTING_TABLE_GRANT',
        _("There is no such grant defined for user '{}' on host '{}' on table '{}'"),
    ],
    ['1148', 'ER_NOT_ALLOWED_COMMAND', _("The used command is not allowed with this MariaDB version")],
    [
        '1149',
        'ER_SYNTAX_ERROR',
        _(
            "You have an error in your SQL syntax; "
            "check the manual that corresponds to your MariaDB server version for the right syntax to use"
        ),
    ],
    ['1150', 'ER_DELAYED_CANT_CHANGE_LOCK', _("Delayed insert thread couldn't get requested lock for table {}")],
    ['1151', 'ER_TOO_MANY_DELAYED_THREADS', _("Too many delayed threads in use")],
    ['1152', 'ER_ABORTING_CONNECTION', _("Aborted connection {} to db: '{}' user: '{}' ({})")],
    ['1153', 'ER_NET_PACKET_TOO_LARGE', _("Got a packet bigger than 'max_allowed_packet' bytes")],
    ['1154', 'ER_NET_READ_ERROR_FROM_PIPE', _("Got a read error from the connection pipe")],
    ['1155', 'ER_NET_FCNTL_ERROR', _("Got an error from fcntl()")],
    ['1156', 'ER_NET_PACKETS_OUT_OF_ORDER', _("Got packets out of order")],
    ['1157', 'ER_NET_UNCOMPRESS_ERROR', _("Couldn't uncompress communication packet")],
    ['1158', 'ER_NET_READ_ERROR', _("Got an error reading communication packets")],
    ['1159', 'ER_NET_READ_INTERRUPTED', _("Got timeout reading communication packets")],
    ['1160', 'ER_NET_ERROR_ON_WRITE', _("Got an error writing communication packets")],
    ['1161', 'ER_NET_WRITE_INTERRUPTED', _("Got timeout writing communication packets")],
    ['1162', 'ER_TOO_LONG_STRING', _("Result string is longer than 'max_allowed_packet' bytes")],
    ['1163', 'ER_TABLE_CANT_HANDLE_BLOB', _("The used table type doesn't support BLOB/TEXT columns")],
    ['1164', 'ER_TABLE_CANT_HANDLE_AUTO_INCREMENT', _("The used table type doesn't support AUTO_INCREMENT columns")],
    [
        '1165',
        'ER_DELAYED_INSERT_TABLE_LOCKED',
        _("INSERT DELAYED can't be used with table '{}' because it is locked with LOCK TABLES"),
    ],
    ['1166', 'ER_WRONG_COLUMN_NAME', _("Incorrect column name '{}'")],
    ['1167', 'ER_WRONG_KEY_COLUMN', _("The used storage engine can't index column '{}'")],
    [
        '1168',
        'ER_WRONG_MRG_TABLE',
        _("Unable to open underlying table which is differently defined or of non-MyISAM type or doesn't exist"),
    ],
    ['1169', 'ER_DUP_UNIQUE', _("Can't write, because of unique constraint, to table '{}'")],
    ['1170', 'ER_BLOB_KEY_WITHOUT_LENGTH', _("BLOB/TEXT column '{}' used in key specification without a key length")],
    [
        '1171',
        'ER_PRIMARY_CANT_HAVE_NULL',
        _("All parts of a PRIMARY KEY must be NOT NULL; if you need NULL in a key, use UNIQUE instead"),
    ],
    ['1172', 'ER_TOO_MANY_ROWS', _("Result consisted of more than one row")],
    ['1173', 'ER_REQUIRES_PRIMARY_KEY', _("This table type requires a primary key")],
    ['1174', 'ER_NO_RAID_COMPILED', _("This version of MariaDB is not compiled with RAID support")],
    [
        '1175',
        'ER_UPDATE_WITHOUT_KEY_IN_SAFE_MODE',
        _("You are using safe update mode and you tried to update a table without a WHERE that uses a KEY column"),
    ],
    ['1176', 'ER_KEY_DOES_NOT_EXITS', _("Key '{}' doesn't exist in table '{}'")],
    ['1177', 'ER_CHECK_NO_SUCH_TABLE', _("Can't open table")],
    ['1178', 'ER_CHECK_NOT_IMPLEMENTED', _("The storage engine for the table doesn't support {}")],
    [
        '1179',
        'ER_CANT_DO_THIS_DURING_AN_TRANSACTION',
        _("You are not allowed to execute this command in a transaction"),
    ],
    ['1180', 'ER_ERROR_DURING_COMMIT', _("Got error {} during COMMIT")],
    ['1181', 'ER_ERROR_DURING_ROLLBACK', _("Got error {} during ROLLBACK")],
    ['1182', 'ER_ERROR_DURING_FLUSH_LOGS', _("Got error {} during FLUSH_LOGS")],
    ['1183', 'ER_ERROR_DURING_CHECKPOINT', _("Got error {} during CHECKPOINT")],
    ['1184', 'ER_NEW_ABORTING_CONNECTION', _("Aborted connection {} to db: '{}' user: '{}' host: '{}' ({})")],
    ['1185', 'ER_DUMP_NOT_IMPLEMENTED', _("The storage engine for the table does not support binary table dump")],
    ['1186', 'ER_FLUSH_MASTER_BINLOG_CLOSED', _("Binlog closed, cannot RESET MASTER")],
    ['1187', 'ER_INDEX_REBUILD', _("Failed rebuilding the index of dumped table '{}'")],
    ['1188', 'ER_MASTER', _("Error from master: '{}'")],
    ['1189', 'ER_MASTER_NET_READ', _("Net error reading from master")],
    ['1190', 'ER_MASTER_NET_WRITE', _("Net error writing to master")],
    ['1191', 'ER_FT_MATCHING_KEY_NOT_FOUND', _("Can't find FULLTEXT index matching the column list")],
    [
        '1192',
        'ER_LOCK_OR_ACTIVE_TRANSACTION',
        _("Can't execute the given command because you have active locked tables or an active transaction"),
    ],
    ['1193', 'ER_UNKNOWN_SYSTEM_VARIABLE', _("Unknown system variable '{}'")],
    ['1194', 'ER_CRASHED_ON_USAGE', _("Table '{}' is marked as crashed and should be repaired")],
    ['1195', 'ER_CRASHED_ON_REPAIR', _("Table '{}' is marked as crashed and last (automatic?) repair failed")],
    ['1196', 'ER_WARNING_NOT_COMPLETE_ROLLBACK', _("Some non-transactional changed tables couldn't be rolled back")],
    [
        '1197',
        'ER_TRANS_CACHE_FULL',
        _(
            "Multi-statement transaction required more than 'max_binlog_cache_size' bytes of storage; "
            "increase this mysqld variable and try again"
        ),
    ],
    ['1198', 'ER_SLAVE_MUST_STOP', _("This operation cannot be performed with a running slave; run STOP SLAVE first")],
    ['1199', 'ER_SLAVE_NOT_RUNNING', _("This operation requires a running slave; configure slave and do START SLAVE")],
    ['1200', 'ER_BAD_SLAVE', _("The server is not configured as slave; fix in config file or with CHANGE MASTER TO")],
    [
        '1201',
        'ER_MASTER_INF',
        _("Could not initialize master info structure; more error messages can be found in the MariaDB error log"),
    ],
    ['1202', 'ER_SLAVE_THREAD', _("Could not create slave thread; check system resources")],
    [
        '1203',
        'ER_TOO_MANY_USER_CONNECTIONS',
        _("User {} already has more than 'max_user_connections' active connections"),
    ],
    ['1204', 'ER_SET_CONSTANTS_ONLY', _("You may only use constant expressions with SET")],
    ['1205', 'ER_LOCK_WAIT_TIMEOUT', _("Lock wait timeout exceeded; try restarting transaction")],
    ['1206', 'ER_LOCK_TABLE_FULL', _("The total number of locks exceeds the lock table size")],
    ['1207', 'ER_READ_ONLY_TRANSACTION', _("Update locks cannot be acquired during a READ UNCOMMITTED transaction")],
    ['1208', 'ER_DROP_DB_WITH_READ_LOCK', _("DROP DATABASE not allowed while thread is holding global read lock")],
    ['1209', 'ER_CREATE_DB_WITH_READ_LOCK', _("CREATE DATABASE not allowed while thread is holding global read lock")],
    ['1210', 'ER_WRONG_ARGUMENTS', _("Incorrect arguments to {}")],
    ['1211', 'ER_NO_PERMISSION_TO_CREATE_USER', _("'{}'@'{}' is not allowed to create new users")],
    [
        '1212',
        'ER_UNION_TABLES_IN_DIFFERENT_DIR',
        _("Incorrect table definition; all MERGE tables must be in the same database"),
    ],
    ['1213', 'ER_LOCK_DEADLOCK', _("Deadlock found when trying to get lock; try restarting transaction")],
    ['1214', 'ER_TABLE_CANT_HANDLE_FT', _("The used table type doesn't support FULLTEXT indexes")],
    ['1215', 'ER_CANNOT_ADD_FOREIGN', _("Cannot add foreign key constraint")],
    ['1216', 'ER_NO_REFERENCED_ROW', _("Cannot add or update a child row: a foreign key constraint fails")],
    ['1217', 'ER_ROW_IS_REFERENCED', _("Cannot delete or update a parent row: a foreign key constraint fails")],
    ['1218', 'ER_CONNECT_TO_MASTER', _("Error connecting to master: {}")],
    ['1219', 'ER_QUERY_ON_MASTER', _("Error running query on master: {}")],
    ['1220', 'ER_ERROR_WHEN_EXECUTING_COMMAND', _("Error when executing command {}: {}")],
    ['1221', 'ER_WRONG_USAGE', _("Incorrect usage of {} and {}")],
    [
        '1222',
        'ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT',
        _("The used SELECT statements have a different number of columns"),
    ],
    ['1223', 'ER_CANT_UPDATE_WITH_READLOCK', _("Can't execute the query because you have a conflicting read lock")],
    ['1224', 'ER_MIXING_NOT_ALLOWED', _("Mixing of transactional and non-transactional tables is disabled")],
    ['1225', 'ER_DUP_ARGUMENT', _("Option '{}' used twice in statement")],
    ['1226', 'ER_USER_LIMIT_REACHED', _("User '{}' has exceeded the '{}' resource (current value: {})")],
    [
        '1227',
        'ER_SPECIFIC_ACCESS_DENIED_ERROR',
        _("Access denied; you need (at least one of) the {} privilege(s) for this operation"),
    ],
    ['1228', 'ER_LOCAL_VARIABLE', _("Variable '{}' is a SESSION variable and can't be used with SET GLOBAL")],
    ['1229', 'ER_GLOBAL_VARIABLE', _("Variable '{}' is a GLOBAL variable and should be set with SET GLOBAL")],
    ['1230', 'ER_NO_DEFAULT', _("Variable '{}' doesn't have a default value")],
    ['1231', 'ER_WRONG_VALUE_FOR_VAR', _("Variable '{}' can't be set to the value of '{}'")],
    ['1232', 'ER_WRONG_TYPE_FOR_VAR', _("Incorrect argument type to variable '{}'")],
    ['1233', 'ER_VAR_CANT_BE_READ', _("Variable '{}' can only be set, not read")],
    ['1234', 'ER_CANT_USE_OPTION_HERE', _("Incorrect usage/placement of '{}'")],
    ['1235', 'ER_NOT_SUPPORTED_YET', _("This version of MariaDB doesn't yet support '{}'")],
    [
        '1236',
        'ER_MASTER_FATAL_ERROR_READING_BINLOG',
        _("Got fatal error {} from master when reading data from binary log: '{}'"),
    ],
    ['1237', 'ER_SLAVE_IGNORED_TABLE', _("Slave SQL thread ignored the query because of replicate-*-table rules")],
    ['1238', 'ER_INCORRECT_GLOBAL_LOCAL_VAR', _("Variable '{}' is a {} variable")],
    ['1239', 'ER_WRONG_FK_DEF', _("Incorrect foreign key definition for '{}': {}")],
    ['1240', 'ER_KEY_REF_DO_NOT_MATCH_TABLE_REF', _("Key reference and table reference don't match")],
    ['1241', 'ER_OPERAND_COLUMNS', _("Operand should contain {} column(s)")],
    ['1242', 'ER_SUBQUERY_NO_1_ROW', _("Subquery returns more than 1 row")],
    ['1243', 'ER_UNKNOWN_STMT_HANDLER', _("Unknown prepared statement handler ({}) given to {}")],
    ['1244', 'ER_CORRUPT_HELP_DB', _("Help database is corrupt or does not exist")],
    ['1245', 'ER_CYCLIC_REFERENCE', _("Cyclic reference on subqueries")],
    ['1246', 'ER_AUTO_CONVERT', _("Converting column '{}' from {} to {}")],
    ['1247', 'ER_ILLEGAL_REFERENCE', _("Reference '{}' not supported ({})")],
    ['1248', 'ER_DERIVED_MUST_HAVE_ALIAS', _("Every derived table must have its own alias")],
    ['1249', 'ER_SELECT_REDUCED', _("Select {} was reduced during optimization")],
    ['1250', 'ER_TABLENAME_NOT_ALLOWED_HERE', _("Table '{}' from one of the SELECTs cannot be used in {}")],
    [
        '1251',
        'ER_NOT_SUPPORTED_AUTH_MODE',
        _("Client does not support authentication protocol requested by server; consider upgrading MariaDB client"),
    ],
    ['1252', 'ER_SPATIAL_CANT_HAVE_NULL', _("All parts of a SPATIAL index must be NOT NULL")],
    ['1253', 'ER_COLLATION_CHARSET_MISMATCH', _("COLLATION '{}' is not valid for CHARACTER SET '{}'")],
    ['1254', 'ER_SLAVE_WAS_RUNNING', _("Slave is already running")],
    ['1255', 'ER_SLAVE_WAS_NOT_RUNNING', _("Slave already has been stopped")],
    [
        '1256',
        'ER_TOO_BIG_FOR_UNCOMPRESS',
        _(
            "Uncompressed data size too large; the maximum size is {} (probably, "
            "length of uncompressed data was corrupted)"
        ),
    ],
    ['1257', 'ER_ZLIB_Z_MEM_ERROR', _("ZLIB: Not enough memory")],
    [
        '1258',
        'ER_ZLIB_Z_BUF_ERROR',
        _("ZLIB: Not enough room in the output buffer (probably, length of uncompressed data was corrupted)"),
    ],
    ['1259', 'ER_ZLIB_Z_DATA_ERROR', _("ZLIB: Input data corrupted")],
    ['1260', 'ER_CUT_VALUE_GROUP_CONCAT', _("Row {} was cut by GROUP_CONCAT()")],
    ['1261', 'ER_WARN_TOO_FEW_RECORDS', _("Row {} doesn't contain data for all columns")],
    [
        '1262',
        'ER_WARN_TOO_MANY_RECORDS',
        _("Row {} was truncated; it contained more data than there were input columns"),
    ],
    [
        '1263',
        'ER_WARN_NULL_TO_NOTNULL',
        _("Column set to default value; NULL supplied to NOT NULL column '{}' at row {}"),
    ],
    ['1264', 'ER_WARN_DATA_OUT_OF_RANGE', _("Out of range value for column '{}' at row {}")],
    ['1265', 'WARN_DATA_TRUNCATED', _("Data truncated for column '{}' at row {}")],
    ['1266', 'ER_WARN_USING_OTHER_HANDLER', _("Using storage engine {} for table '{}'")],
    ['1267', 'ER_CANT_AGGREGATE_2COLLATIONS', _("Illegal mix of collations ({},{}) and ({},{}) for operation '{}'")],
    ['1268', 'ER_DROP_USER', _("Cannot drop one or more of the requested users")],
    ['1269', 'ER_REVOKE_GRANTS', _("Can't revoke all privileges for one or more of the requested users")],
    [
        '1270',
        'ER_CANT_AGGREGATE_3COLLATIONS',
        _("Illegal mix of collations ({},{}), ({},{}), ({},{}) for operation '{}'"),
    ],
    ['1271', 'ER_CANT_AGGREGATE_NCOLLATIONS', _("Illegal mix of collations for operation '{}'")],
    [
        '1272',
        'ER_VARIABLE_IS_NOT_STRUCT',
        _("Variable '{}' is not a variable component (can't be used as XXXX.variable_name)"),
    ],
    ['1273', 'ER_UNKNOWN_COLLATION', _("Unknown collation: '{}'")],
    [
        '1274',
        'ER_SLAVE_IGNORED_SSL_PARAMS',
        _(
            "SSL parameters in CHANGE MASTER are ignored because this MariaDB slave was compiled without SSL support; "
            "they can be used later if MariaDB slave with SSL is started"
        ),
    ],
    [
        '1275',
        'ER_SERVER_IS_IN_SECURE_AUTH_MODE',
        _(
            "Server is running in --secure-auth mode, but '{}'@'{}' has a password in the old format; "
            "please change the password to the new format"
        ),
    ],
    ['1276', 'ER_WARN_FIELD_RESOLVED', _("Field or reference '{}{}{}{}{}' of SELECT #{} was resolved in SELECT #{}")],
    ['1277', 'ER_BAD_SLAVE_UNTIL_COND', _("Incorrect parameter or combination of parameters for START SLAVE UNTIL")],
    [
        '1278',
        'ER_MISSING_SKIP_SLAVE',
        _(
            "It is recommended to use --skip-slave-start when doing step-by-step replication with START SLAVE UNTIL; "
            "otherwise, you will get problems if you get an unexpected slave's mysqld restart"
        ),
    ],
    ['1279', 'ER_UNTIL_COND_IGNORED', _("SQL thread is not to be started so UNTIL options are ignored")],
    ['1280', 'ER_WRONG_NAME_FOR_INDEX', _("Incorrect index name '{}'")],
    ['1281', 'ER_WRONG_NAME_FOR_CATALOG', _("Incorrect catalog name '{}'")],
    ['1282', 'ER_WARN_QC_RESIZE', _("Query cache failed to set size {}; new query cache size is {}")],
    ['1283', 'ER_BAD_FT_COLUMN', _("Column '{}' cannot be part of FULLTEXT index")],
    ['1284', 'ER_UNKNOWN_KEY_CACHE', _("Unknown key cache '{}'")],
    [
        '1285',
        'ER_WARN_HOSTNAME_WONT_WORK',
        _(
            "MariaDB is started in --skip-name-resolve mode; you must restart it "
            "without this switch for this grant to work"
        ),
    ],
    ['1286', 'ER_UNKNOWN_STORAGE_ENGINE', _("Unknown storage engine '{}'")],
    [
        '1287',
        'ER_WARN_DEPRECATED_SYNTAX',
        _("'{}' is deprecated and will be removed in a future release. Please use {} instead"),
    ],
    ['1288', 'ER_NON_UPDATABLE_TABLE', _("The target table {} of the {} is not updatable")],
    [
        '1289',
        'ER_FEATURE_DISABLED',
        _("The '{}' feature is disabled; you need MariaDB built with '{}' to have it working"),
    ],
    [
        '1290',
        'ER_OPTION_PREVENTS_STATEMENT',
        _("The MariaDB server is running with the {} option so it cannot execute this statement"),
    ],
    ['1291', 'ER_DUPLICATED_VALUE_IN_TYPE', _("Column '{}' has duplicated value '{}' in {}")],
    ['1292', 'ER_TRUNCATED_WRONG_VALUE', _("Truncated incorrect {} value: '{}'")],
    [
        '1293',
        'ER_TOO_MUCH_AUTO_TIMESTAMP_COLS',
        _(
            "Incorrect table definition; there can be only one TIMESTAMP column with CURRENT_TIMESTAMP in DEFAULT "
            "or ON UPDATE clause"
        ),
    ],
    ['1294', 'ER_INVALID_ON_UPDATE', _("Invalid ON UPDATE clause for '{}' column")],
    ['1295', 'ER_UNSUPPORTED_PS', _("This command is not supported in the prepared statement protocol yet")],
    ['1296', 'ER_GET_ERRMSG', _("Got error {} '{}' from {}")],
    ['1297', 'ER_GET_TEMPORARY_ERRMSG', _("Got temporary error {} '{}' from {}")],
    ['1298', 'ER_UNKNOWN_TIME_ZONE', _("Unknown or incorrect time zone: '{}'")],
    ['1299', 'ER_WARN_INVALID_TIMESTAMP', _("Invalid TIMESTAMP value in column '{}' at row {}")],
    ['1300', 'ER_INVALID_CHARACTER_STRING', _("Invalid {} character string: '{}'")],
    [
        '1301',
        'ER_WARN_ALLOWED_PACKET_OVERFLOWED',
        _("Result of {}() was larger than max_allowed_packet ({}) - truncated"),
    ],
    ['1302', 'ER_CONFLICTING_DECLARATIONS', _("Conflicting declarations: '{}{}' and '{}{}'")],
    ['1303', 'ER_SP_NO_RECURSIVE_CREATE', _("Can't create a {} from within another stored routine")],
    ['1304', 'ER_SP_ALREADY_EXISTS', _("{} {} already exists")],
    ['1305', 'ER_SP_DOES_NOT_EXIST', _("{} {} does not exist")],
    ['1306', 'ER_SP_DROP_FAILED', _("Failed to DROP {} {}")],
    ['1307', 'ER_SP_STORE_FAILED', _("Failed to CREATE {} {}")],
    ['1308', 'ER_SP_LILABEL_MISMATCH', _("{} with no matching label: {}")],
    ['1309', 'ER_SP_LABEL_REDEFINE', _("Redefining label {}")],
    ['1310', 'ER_SP_LABEL_MISMATCH', _("End-label {} without match")],
    ['1311', 'ER_SP_UNINIT_VAR', _("Referring to uninitialized variable {}")],
    ['1312', 'ER_SP_BADSELECT', _("PROCEDURE {} can't return a result set in the given context")],
    ['1313', 'ER_SP_BADRETURN', _("RETURN is only allowed in a FUNCTION")],
    ['1314', 'ER_SP_BADSTATEMENT', _("{} is not allowed in stored procedures")],
    [
        '1315',
        'ER_UPDATE_LOG_DEPRECATED_IGNORED',
        _(
            "The update log is deprecated and replaced by the binary log; "
            "SET SQL_LOG_UPDATE has been ignored. This option will be removed in MariaDB 5.6."
        ),
    ],
    [
        '1316',
        'ER_UPDATE_LOG_DEPRECATED_TRANSLATED',
        _(
            "The update log is deprecated and replaced by the binary log; "
            "SET SQL_LOG_UPDATE has been translated to SET SQL_LOG_BIN. This option will be removed in MariaDB 5.6."
        ),
    ],
    ['1317', 'ER_QUERY_INTERRUPTED', _("Query execution was interrupted")],
    ['1318', 'ER_SP_WRONG_NO_OF_ARGS', _("Incorrect number of arguments for {} {}; expected {}, got {}")],
    ['1319', 'ER_SP_COND_MISMATCH', _("Undefined CONDITION: {}")],
    ['1320', 'ER_SP_NORETURN', _("No RETURN found in FUNCTION {}")],
    ['1321', 'ER_SP_NORETURNEND', _("FUNCTION {} ended without RETURN")],
    ['1322', 'ER_SP_BAD_CURSOR_QUERY', _("Cursor statement must be a SELECT")],
    ['1323', 'ER_SP_BAD_CURSOR_SELECT', _("Cursor SELECT must not have INTO")],
    ['1324', 'ER_SP_CURSOR_MISMATCH', _("Undefined CURSOR: {}")],
    ['1325', 'ER_SP_CURSOR_ALREADY_OPEN', _("Cursor is already open")],
    ['1326', 'ER_SP_CURSOR_NOT_OPEN', _("Cursor is not open")],
    ['1327', 'ER_SP_UNDECLARED_VAR', _("Undeclared variable: {}")],
    ['1328', 'ER_SP_WRONG_NO_OF_FETCH_ARGS', _("Incorrect number of FETCH variables")],
    ['1329', 'ER_SP_FETCH_NO_DATA', _("No data - zero rows fetched, selected, or processed")],
    ['1330', 'ER_SP_DUP_PARAM', _("Duplicate parameter: {}")],
    ['1331', 'ER_SP_DUP_VAR', _("Duplicate variable: {}")],
    ['1332', 'ER_SP_DUP_COND', _("Duplicate condition: {}")],
    ['1333', 'ER_SP_DUP_CURS', _("Duplicate cursor: {}")],
    ['1334', 'ER_SP_CANT_ALTER', _("Failed to ALTER {} {}")],
    ['1335', 'ER_SP_SUBSELECT_NYI', _("Subquery value not supported")],
    ['1336', 'ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG', _("{} is not allowed in stored function or trigger")],
    [
        '1337',
        'ER_SP_VARCOND_AFTER_CURSHNDLR',
        _("Variable or condition declaration after cursor or handler declaration"),
    ],
    ['1338', 'ER_SP_CURSOR_AFTER_HANDLER', _("Cursor declaration after handler declaration")],
    ['1339', 'ER_SP_CASE_NOT_FOUND', _("Case not found for CASE statement")],
    ['1340', 'ER_FPARSER_TOO_BIG_FILE', _("Configuration file '{}' is too big")],
    ['1341', 'ER_FPARSER_BAD_HEADER', _("Malformed file type header in file '{}'")],
    ['1342', 'ER_FPARSER_EOF_IN_COMMENT', _("Unexpected end of file while parsing comment '{}'")],
    ['1343', 'ER_FPARSER_ERROR_IN_PARAMETER', _("Error while parsing parameter '{}' (line: '{}')")],
    ['1344', 'ER_FPARSER_EOF_IN_UNKNOWN_PARAMETER', _("Unexpected end of file while skipping unknown parameter '{}'")],
    ['1345', 'ER_VIEW_NO_EXPLAIN', _("EXPLAIN/SHOW can not be issued; lacking privileges for underlying table")],
    ['1346', 'ER_FRM_UNKNOWN_TYPE', _("File '{}' has unknown type '{}' in its header")],
    ['1347', 'ER_WRONG_OBJECT', _("'{}.{}' is not {}")],
    ['1348', 'ER_NONUPDATEABLE_COLUMN', _("Column '{}' is not updatable")],
    ['1349', 'ER_VIEW_SELECT_DERIVED', _("View's SELECT contains a subquery in the FROM clause")],
    ['1350', 'ER_VIEW_SELECT_CLAUSE', _("View's SELECT contains a '{}' clause")],
    ['1351', 'ER_VIEW_SELECT_VARIABLE', _("View's SELECT contains a variable or parameter")],
    ['1352', 'ER_VIEW_SELECT_TMPTABLE', _("View's SELECT refers to a temporary table '{}'")],
    ['1353', 'ER_VIEW_WRONG_LIST', _("View's SELECT and view's field list have different column counts")],
    ['1354', 'ER_WARN_VIEW_MERGE', _("View merge algorithm can't be used here for now (assumed undefined algorithm)")],
    ['1355', 'ER_WARN_VIEW_WITHOUT_KEY', _("View being updated does not have complete key of underlying table in it")],
    [
        '1356',
        'ER_VIEW_INVALID',
        _(
            "View '{}.{}' references invalid table(s) or column(s) or function(s) or definer/invoker of view "
            "lack rights to use them"
        ),
    ],
    ['1357', 'ER_SP_NO_DROP_SP', _("Can't drop or alter a {} from within another stored routine")],
    ['1358', 'ER_SP_GOTO_IN_HNDLR', _("GOTO is not allowed in a stored procedure handler")],
    ['1359', 'ER_TRG_ALREADY_EXISTS', _("Trigger already exists")],
    ['1360', 'ER_TRG_DOES_NOT_EXIST', _("Trigger does not exist")],
    ['1361', 'ER_TRG_ON_VIEW_OR_TEMP_TABLE', _("Trigger's '{}' is view or temporary table")],
    ['1362', 'ER_TRG_CANT_CHANGE_ROW', _("Updating of {} row is not allowed in {}trigger")],
    ['1363', 'ER_TRG_NO_SUCH_ROW_IN_TRG', _("There is no {} row in {} trigger")],
    ['1364', 'ER_NO_DEFAULT_FOR_FIELD', _("Field '{}' doesn't have a default value")],
    ['1365', 'ER_DIVISION_BY_ZER', _("Division by 0")],
    ['1366', 'ER_TRUNCATED_WRONG_VALUE_FOR_FIELD', _("Incorrect {} value: '{}' for column '{}' at row {}")],
    ['1367', 'ER_ILLEGAL_VALUE_FOR_TYPE', _("Illegal {} '{}' value found during parsing")],
    ['1368', 'ER_VIEW_NONUPD_CHECK', _("CHECK OPTION on non-updatable view '{}.{}'")],
    ['1369', 'ER_VIEW_CHECK_FAILED', _("CHECK OPTION failed '{}.{}'")],
    ['1370', 'ER_PROCACCESS_DENIED_ERROR', _("{} command denied to user '{}'@'{}' for routine '{}'")],
    ['1371', 'ER_RELAY_LOG_FAIL', _("Failed purging old relay logs: {}")],
    ['1372', 'ER_PASSWD_LENGTH', _("Password hash should be a {}-digit hexadecimal number")],
    ['1373', 'ER_UNKNOWN_TARGET_BINLOG', _("Target log not found in binlog index")],
    ['1374', 'ER_IO_ERR_LOG_INDEX_READ', _("I/O error reading log index file")],
    ['1375', 'ER_BINLOG_PURGE_PROHIBITED', _("Server configuration does not permit binlog purge")],
    ['1376', 'ER_FSEEK_FAIL', _("Failed on fseek()")],
    ['1377', 'ER_BINLOG_PURGE_FATAL_ERR', _("Fatal error during log purge")],
    ['1378', 'ER_LOG_IN_USE', _("A purgeable log is in use, will not purge")],
    ['1379', 'ER_LOG_PURGE_UNKNOWN_ERR', _("Unknown error during log purge")],
    ['1380', 'ER_RELAY_LOG_INIT', _("Failed initializing relay log position: {}")],
    ['1381', 'ER_NO_BINARY_LOGGING', _("You are not using binary logging")],
    ['1382', 'ER_RESERVED_SYNTAX', _("The '{}' syntax is reserved for purposes internal to the MariaDB server")],
    ['1383', 'ER_WSAS_FAILED', _("WSAStartup Failed")],
    ['1384', 'ER_DIFF_GROUPS_PROC', _("Can't handle procedures with different groups yet")],
    ['1385', 'ER_NO_GROUP_FOR_PROC', _("Select must have a group with this procedure")],
    ['1386', 'ER_ORDER_WITH_PROC', _("Can't use ORDER clause with this procedure")],
    [
        '1387',
        'ER_LOGGING_PROHIBIT_CHANGING_OF',
        _("Binary logging and replication forbid changing the global server {}"),
    ],
    ['1388', 'ER_NO_FILE_MAPPING', _("Can't map file: {}, errno: {}")],
    ['1389', 'ER_WRONG_MAGIC', _("Wrong magic in {}")],
    ['1390', 'ER_PS_MANY_PARAM', _("Prepared statement contains too many placeholders")],
    ['1391', 'ER_KEY_PART_0', _("Key part '{}' length cannot be 0")],
    ['1392', 'ER_VIEW_CHECKSUM', _("View text checksum failed")],
    ['1393', 'ER_VIEW_MULTIUPDATE', _("Can not modify more than one base table through a join view '{}.{}'")],
    ['1394', 'ER_VIEW_NO_INSERT_FIELD_LIST', _("Can not insert into join view '{}.{}' without fields list")],
    ['1395', 'ER_VIEW_DELETE_MERGE_VIEW', _("Can not delete from join view '{}.{}'")],
    ['1396', 'ER_CANNOT_USER', _("Operation {} failed for {}")],
    ['1397', 'ER_XAER_NOTA', _("XAER_NOTA: Unknown XID")],
    ['1398', 'ER_XAER_INVAL', _("XAER_INVAL: Invalid arguments (or unsupported command)")],
    [
        '1399',
        'ER_XAER_RMFAIL',
        _("XAER_RMFAIL: The command cannot be executed when global transaction is in the {} state"),
    ],
    ['1400', 'ER_XAER_OUTSIDE', _("XAER_OUTSIDE: Some work is done outside global transaction")],
    [
        '1401',
        'ER_XAER_RMERR',
        _("XAER_RMERR: Fatal error occurred in the transaction branch - check your data for consistency"),
    ],
    ['1402', 'ER_XA_RBROLLBACK', _("XA_RBROLLBACK: Transaction branch was rolled back")],
    [
        '1403',
        'ER_NONEXISTING_PROC_GRANT',
        _("There is no such grant defined for user '{}' on host '{}' on routine '{}'"),
    ],
    ['1404', 'ER_PROC_AUTO_GRANT_FAIL', _("Failed to grant EXECUTE and ALTER ROUTINE privileges")],
    ['1405', 'ER_PROC_AUTO_REVOKE_FAIL', _("Failed to revoke all privileges to dropped routine")],
    ['1406', 'ER_DATA_TOO_LONG', _("Data too long for column '{}' at row {}")],
    ['1407', 'ER_SP_BAD_SQLSTATE', _("Bad SQLSTATE: '{}'")],
    ['1408', 'ER_STARTUP', _("{}: ready for connections. Version: '{}' socket: '{}' port: {} {}")],
    ['1409', 'ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR', _("Can't load value from file with fixed size rows to variable")],
    ['1410', 'ER_CANT_CREATE_USER_WITH_GRANT', _("You are not allowed to create a user with GRANT")],
    ['1411', 'ER_WRONG_VALUE_FOR_TYPE', _("Incorrect {} value: '{}' for function {}")],
    ['1412', 'ER_TABLE_DEF_CHANGED', _("Table definition has changed, please retry transaction")],
    ['1413', 'ER_SP_DUP_HANDLER', _("Duplicate handler declared in the same block")],
    [
        '1414',
        'ER_SP_NOT_VAR_ARG',
        _("OUT or INOUT argument {} for routine {} is not a variable or NEW pseudo-variable in BEFORE trigger"),
    ],
    ['1415', 'ER_SP_NO_RETSET', _("Not allowed to return a result set from a {}")],
    [
        '1416',
        'ER_CANT_CREATE_GEOMETRY_OBJECT',
        _("Cannot get geometry object from data you send to the GEOMETRY field"),
    ],
    [
        '1417',
        'ER_FAILED_ROUTINE_BREAK_BINLOG',
        _(
            "A routine failed and has neither NO SQL nor READS SQL DATA in its declaration "
            "and binary logging is enabled; "
            "if non-transactional tables were updated, the binary log will miss their changes"
        ),
    ],
    [
        '1418',
        'ER_BINLOG_UNSAFE_ROUTINE',
        _(
            "This function has none of DETERMINISTIC, NO SQL, or READS SQL DATA in its declaration "
            "and binary logging is enabled "
            "(you *might* want to use the less safe log_bin_trust_function_creators variable)"
        ),
    ],
    [
        '1419',
        'ER_BINLOG_CREATE_ROUTINE_NEED_SUPER',
        _(
            "You do not have the SUPER privilege and binary logging is enabled "
            "(you *might* want to use the less safe log_bin_trust_function_creators variable)"
        ),
    ],
    [
        '1420',
        'ER_EXEC_STMT_WITH_OPEN_CURSOR',
        _(
            "You can't execute a prepared statement which has an open cursor associated with it. "
            "Reset the statement to re-execute it."
        ),
    ],
    ['1421', 'ER_STMT_HAS_NO_OPEN_CURSOR', _("The statement ({}) has no open cursor.")],
    [
        '1422',
        'ER_COMMIT_NOT_ALLOWED_IN_SF_OR_TRG',
        _("Explicit or implicit commit is not allowed in stored function or trigger."),
    ],
    ['1423', 'ER_NO_DEFAULT_FOR_VIEW_FIELD', _("Field of view '{}.{}' underlying table doesn't have a default value")],
    ['1424', 'ER_SP_NO_RECURSION', _("Recursive stored functions and triggers are not allowed.")],
    ['1425', 'ER_TOO_BIG_SCALE', _("Too big scale {} specified for column '{}'. Maximum is {}.")],
    ['1426', 'ER_TOO_BIG_PRECISION', _("Too big precision {} specified for column '{}'. Maximum is {}.")],
    ['1427', 'ER_M_BIGGER_THAN_D', _("For float(M,D, double(M,D or decimal(M,D, M must be >= D (column '{}').")],
    [
        '1428',
        'ER_WRONG_LOCK_OF_SYSTEM_TABLE',
        _("You can't combine write-locking of system tables with other tables or lock types"),
    ],
    ['1429', 'ER_CONNECT_TO_FOREIGN_DATA_SOURCE', _("Unable to connect to foreign data source: {}")],
    [
        '1430',
        'ER_QUERY_ON_FOREIGN_DATA_SOURCE',
        _("There was a problem processing the query on the foreign data source. Data source error: {}"),
    ],
    [
        '1431',
        'ER_FOREIGN_DATA_SOURCE_DOESNT_EXIST',
        _("The foreign data source you are trying to reference does not exist. Data source error: {}"),
    ],
    [
        '1432',
        'ER_FOREIGN_DATA_STRING_INVALID_CANT_CREATE',
        _("Can't create federated table. The data source connection string '{}' is not in the correct format"),
    ],
    [
        '1433',
        'ER_FOREIGN_DATA_STRING_INVALID',
        _("The data source connection string '{}' is not in the correct format"),
    ],
    ['1434', 'ER_CANT_CREATE_FEDERATED_TABLE', _("Can't create federated table. Foreign data src error: {}")],
    ['1435', 'ER_TRG_IN_WRONG_SCHEMA', _("Trigger in wrong schema")],
    [
        '1436',
        'ER_STACK_OVERRUN_NEED_MORE',
        _(
            "Thread stack overrun: {} bytes used of a {} byte stack, and {} bytes needed. "
            "Use 'mysqld --thread_stack=#' to specify a bigger stack."
        ),
    ],
    ['1437', 'ER_TOO_LONG_BODY', _("Routine body for '{}' is too long")],
    ['1438', 'ER_WARN_CANT_DROP_DEFAULT_KEYCACHE', _("Cannot drop default keycache")],
    ['1439', 'ER_TOO_BIG_DISPLAYWIDTH', _("Display width out of range for column '{}' (max = {})")],
    ['1440', 'ER_XAER_DUPID', _("XAER_DUPID: The XID already exists")],
    ['1441', 'ER_DATETIME_FUNCTION_OVERFLOW', _("Datetime function: {} field overflow")],
    [
        '1442',
        'ER_CANT_UPDATE_USED_TABLE_IN_SF_OR_TRG',
        _(
            "Can't update table '{}' in stored function/trigger because it is already used by statement "
            "which invoked this stored function/trigger."
        ),
    ],
    ['1443', 'ER_VIEW_PREVENT_UPDATE', _("The definition of table '{}' prevents operation {} on table '{}'.")],
    [
        '1444',
        'ER_PS_NO_RECURSION',
        _(
            "The prepared statement contains a stored routine call that refers to that same statement. "
            "It's not allowed to execute a prepared statement in such a recursive manner"
        ),
    ],
    ['1445', 'ER_SP_CANT_SET_AUTOCOMMIT', _("Not allowed to set autocommit from a stored function or trigger")],
    ['1446', 'ER_MALFORMED_DEFINER', _("Definer is not fully qualified")],
    [
        '1447',
        'ER_VIEW_FRM_NO_USER',
        _(
            "View '{}'.'{}' has no definer information (old table format). "
            "Current user is used as definer. Please recreate the view!"
        ),
    ],
    ['1448', 'ER_VIEW_OTHER_USER', _("You need the SUPER privilege for creation view with '{}'@'{}' definer")],
    ['1449', 'ER_NO_SUCH_USER', _("The user specified as a definer ('{}'@'{}') does not exist")],
    ['1450', 'ER_FORBID_SCHEMA_CHANGE', _("Changing schema from '{}' to '{}' is not allowed.")],
    ['1451', 'ER_ROW_IS_REFERENCED_2', _("Cannot delete or update a parent row: a foreign key constraint fails ({})")],
    ['1452', 'ER_NO_REFERENCED_ROW_2', _("Cannot add or update a child row: a foreign key constraint fails ({})")],
    ['1453', 'ER_SP_BAD_VAR_SHADOW', _("Variable '{}' must be quoted with `...`, or renamed")],
    [
        '1454',
        'ER_TRG_NO_DEFINER',
        _(
            "No definer attribute for trigger '{}'.'{}'. "
            "The trigger will be activated under the authorization of the invoker, "
            "which may have insufficient privileges. Please recreate the trigger."
        ),
    ],
    ['1455', 'ER_OLD_FILE_FORMAT', _("'{}' has an old format, you should re-create the '{}' object(s)")],
    [
        '1456',
        'ER_SP_RECURSION_LIMIT',
        _("Recursive limit {} (as set by the max_sp_recursion_depth variable) was exceeded for routine {}"),
    ],
    [
        '1457',
        'ER_SP_PROC_TABLE_CORRUPT',
        _(
            "Failed to load routine {}. The table mysql.proc is missing, corrupt, "
            "or contains bad data (internal code {})"
        ),
    ],
    ['1458', 'ER_SP_WRONG_NAME', _("Incorrect routine name '{}'")],
    [
        '1459',
        'ER_TABLE_NEEDS_UPGRADE',
        _("Table upgrade required. Please do 'REPAIR TABLE `{}`' or dump/reload to fix it!"),
    ],
    ['1460', 'ER_SP_NO_AGGREGATE', _("AGGREGATE is not supported for stored functions")],
    [
        '1461',
        'ER_MAX_PREPARED_STMT_COUNT_REACHED',
        _("Can't create more than max_prepared_stmt_count statements (current value: {})"),
    ],
    ['1462', 'ER_VIEW_RECURSIVE', _("`{}`.`{}` contains view recursion")],
    ['1463', 'ER_NON_GROUPING_FIELD_USED', _("Non-grouping field '{}' is used in {} clause")],
    ['1464', 'ER_TABLE_CANT_HANDLE_SPKEYS', _("The used table type doesn't support SPATIAL indexes")],
    ['1465', 'ER_NO_TRIGGERS_ON_SYSTEM_SCHEMA', _("Triggers can not be created on system tables")],
    ['1466', 'ER_REMOVED_SPACES', _("Leading spaces are removed from name '{}'")],
    ['1467', 'ER_AUTOINC_READ_FAILED', _("Failed to read auto-increment value from storage engine")],
    ['1468', 'ER_USERNAME', _("user name")],
    ['1469', 'ER_HOSTNAME', _("host name")],
    ['1470', 'ER_WRONG_STRING_LENGTH', _("String '{}' is too long for {} (should be no longer than {})")],
    ['1471', 'ER_NON_INSERTABLE_TABLE', _("The target table {} of the {} is not insertable-into")],
    ['1472', 'ER_ADMIN_WRONG_MRG_TABLE', _("Table '{}' is differently defined or of non-MyISAM type or doesn't exist")],
    ['1473', 'ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT', _("Too high level of nesting for select")],
    ['1474', 'ER_NAME_BECOMES_EMPTY', _("Name '{}' has become ''")],
    [
        '1475',
        'ER_AMBIGUOUS_FIELD_TERM',
        _(
            "First character of the FIELDS TERMINATED string is ambiguous; please use non-optional "
            "and non-empty FIELDS ENCLOSED BY"
        ),
    ],
    ['1476', 'ER_FOREIGN_SERVER_EXISTS', _("The foreign server, {}, you are trying to create already exists.")],
    [
        '1477',
        'ER_FOREIGN_SERVER_DOESNT_EXIST',
        _("The foreign server name you are trying to reference does not exist. Data source error: {}"),
    ],
    ['1478', 'ER_ILLEGAL_HA_CREATE_OPTION', _("Table storage engine '{}' does not support the create option '{}'")],
    [
        '1479',
        'ER_PARTITION_REQUIRES_VALUES_ERROR',
        _("Syntax error: {} PARTITIONING requires definition of VALUES {} for each partition"),
    ],
    ['1480', 'ER_PARTITION_WRONG_VALUES_ERROR', _("Only {} PARTITIONING can use VALUES {} in partition definition")],
    ['1481', 'ER_PARTITION_MAXVALUE_ERROR', _("MAXVALUE can only be used in last partition definition")],
    ['1482', 'ER_PARTITION_SUBPARTITION_ERROR', _("Subpartitions can only be hash partitions and by key")],
    ['1483', 'ER_PARTITION_SUBPART_MIX_ERROR', _("Must define subpartitions on all partitions if on one partition")],
    [
        '1484',
        'ER_PARTITION_WRONG_NO_PART_ERROR',
        _("Wrong number of partitions defined, mismatch with previous setting"),
    ],
    [
        '1485',
        'ER_PARTITION_WRONG_NO_SUBPART_ERROR',
        _("Wrong number of subpartitions defined, mismatch with previous setting"),
    ],
    [
        '1486',
        'ER_CONST_EXPR_IN_PARTITION_FUNC_ERROR',
        _("Constant/Random expression in (sub)partitioning function is not allowed"),
    ],
    [
        '1486',
        'ER_WRONG_EXPR_IN_PARTITION_FUNC_ERROR',
        _("Constant, random or timezone-dependent expressions in (sub)partitioning function are not allowed"),
    ],
    ['1487', 'ER_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR', _("Expression in RANGE/LIST VALUES must be constant")],
    ['1488', 'ER_FIELD_NOT_FOUND_PART_ERROR', _("Field in list of fields for partition function not found in table")],
    ['1489', 'ER_LIST_OF_FIELDS_ONLY_IN_HASH_ERROR', _("List of fields is only allowed in KEY partitions")],
    [
        '1490',
        'ER_INCONSISTENT_PARTITION_INFO_ERROR',
        _("The partition info in the frm file is not consistent with what can be written into the frm file"),
    ],
    ['1491', 'ER_PARTITION_FUNC_NOT_ALLOWED_ERROR', _("The {} function returns the wrong type")],
    ['1492', 'ER_PARTITIONS_MUST_BE_DEFINED_ERROR', _("For {} partitions each partition must be defined")],
    [
        '1493',
        'ER_RANGE_NOT_INCREASING_ERROR',
        _("VALUES LESS THAN value must be strictly increasing for each partition"),
    ],
    ['1494', 'ER_INCONSISTENT_TYPE_OF_FUNCTIONS_ERROR', _("VALUES value must be of same type as partition function")],
    [
        '1495',
        'ER_MULTIPLE_DEF_CONST_IN_LIST_PART_ERROR',
        _("Multiple definition of same constant in list partitioning"),
    ],
    ['1496', 'ER_PARTITION_ENTRY_ERROR', _("Partitioning can not be used stand-alone in query")],
    [
        '1497',
        'ER_MIX_HANDLER_ERROR',
        _("The mix of handlers in the partitions is not allowed in this version of MariaDB"),
    ],
    ['1498', 'ER_PARTITION_NOT_DEFINED_ERROR', _("For the partitioned engine it is necessary to define all {}")],
    ['1499', 'ER_TOO_MANY_PARTITIONS_ERROR', _("Too many partitions (including subpartitions) were defined")],
    [
        '1500',
        'ER_SUBPARTITION_ERROR',
        _("It is only possible to mix RANGE/LIST partitioning with HASH/KEY partitioning for subpartitioning"),
    ],
    ['1501', 'ER_CANT_CREATE_HANDLER_FILE', _("Failed to create specific handler file")],
    ['1502', 'ER_BLOB_FIELD_IN_PART_FUNC_ERROR', _("A BLOB field is not allowed in partition function")],
    [
        '1503',
        'ER_UNIQUE_KEY_NEED_ALL_FIELDS_IN_PF',
        _("A {} must include all columns in the table's partitioning function"),
    ],
    ['1504', 'ER_NO_PARTS_ERROR', _("Number of {} = 0 is not an allowed value")],
    [
        '1505',
        'ER_PARTITION_MGMT_ON_NONPARTITIONED',
        _("Partition management on a not partitioned table is not possible"),
    ],
    [
        '1506',
        'ER_FOREIGN_KEY_ON_PARTITIONED',
        _("Foreign key clause is not yet supported in conjunction with partitioning"),
    ],
    ['1507', 'ER_DROP_PARTITION_NON_EXISTENT', _("Error in list of partitions to {}")],
    ['1508', 'ER_DROP_LAST_PARTITION', _("Cannot remove all partitions, use DROP TABLE instead")],
    ['1509', 'ER_COALESCE_ONLY_ON_HASH_PARTITION', _("COALESCE PARTITION can only be used on HASH/KEY partitions")],
    [
        '1510',
        'ER_REORG_HASH_ONLY_ON_SAME_N',
        _("REORGANIZE PARTITION can only be used to reorganize partitions not to change their numbers"),
    ],
    [
        '1511',
        'ER_REORG_NO_PARAM_ERROR',
        _("REORGANIZE PARTITION without parameters can only be used on auto-partitioned tables using HASH PARTITIONs"),
    ],
    ['1512', 'ER_ONLY_ON_RANGE_LIST_PARTITION', _("{} PARTITION can only be used on RANGE/LIST partitions")],
    ['1513', 'ER_ADD_PARTITION_SUBPART_ERROR', _("Trying to Add partition(s) with wrong number of subpartitions")],
    ['1514', 'ER_ADD_PARTITION_NO_NEW_PARTITION', _("At least one partition must be added")],
    ['1515', 'ER_COALESCE_PARTITION_NO_PARTITION', _("At least one partition must be coalesced")],
    ['1516', 'ER_REORG_PARTITION_NOT_EXIST', _("More partitions to reorganize than there are partitions")],
    ['1517', 'ER_SAME_NAME_PARTITION', _("Duplicate partition name {}")],
    ['1518', 'ER_NO_BINLOG_ERROR', _("It is not allowed to shut off binlog on this command")],
    [
        '1519',
        'ER_CONSECUTIVE_REORG_PARTITIONS',
        _("When reorganizing a set of partitions they must be in consecutive order"),
    ],
    [
        '1520',
        'ER_REORG_OUTSIDE_RANGE',
        _(
            "Reorganize of range partitions cannot change total ranges except for last partition "
            "where it can extend the range"
        ),
    ],
    ['1521', 'ER_PARTITION_FUNCTION_FAILURE', _("Partition function not supported in this version for this handler")],
    ['1522', 'ER_PART_STATE_ERROR', _("Partition state cannot be defined from CREATE/ALTER TABLE")],
    ['1523', 'ER_LIMITED_PART_RANGE', _("The {} handler only supports 32 bit integers in VALUES")],
    ['1524', 'ER_PLUGIN_IS_NOT_LOADED', _("Plugin '{}' is not loaded")],
    ['1525', 'ER_WRONG_VALUE', _("Incorrect {} value: '{}'")],
    ['1526', 'ER_NO_PARTITION_FOR_GIVEN_VALUE', _("Table has no partition for value {}")],
    ['1527', 'ER_FILEGROUP_OPTION_ONLY_ONCE', _("It is not allowed to specify {} more than once")],
    ['1528', 'ER_CREATE_FILEGROUP_FAILED', _("Failed to create {}")],
    ['1529', 'ER_DROP_FILEGROUP_FAILED', _("Failed to drop {}")],
    ['1530', 'ER_TABLESPACE_AUTO_EXTEND_ERROR', _("The handler doesn't support autoextend of tablespaces")],
    ['1531', 'ER_WRONG_SIZE_NUMBER', _("A size parameter was incorrectly specified, either number or on the form 10M")],
    [
        '1532',
        'ER_SIZE_OVERFLOW_ERROR',
        _("The size number was correct but we don't allow the digit part to be more than 2 billion"),
    ],
    ['1533', 'ER_ALTER_FILEGROUP_FAILED', _("Failed to alter: {}")],
    ['1534', 'ER_BINLOG_ROW_LOGGING_FAILED', _("Writing one row to the row-based binary log failed")],
    ['1535', 'ER_BINLOG_ROW_WRONG_TABLE_DEF', _("Table definition on master and slave does not match: {}")],
    [
        '1536',
        'ER_BINLOG_ROW_RBR_TO_SBR',
        _(
            "Slave running with --log-slave-updates must use row-based binary logging to be able to replicate "
            "row-based binary log events"
        ),
    ],
    ['1537', 'ER_EVENT_ALREADY_EXISTS', _("Event '{}' already exists")],
    ['1538', 'ER_EVENT_STORE_FAILED', _("Failed to store event {}. Error code {} from storage engine.")],
    ['1539', 'ER_EVENT_DOES_NOT_EXIST', _("Unknown event '{}'")],
    ['1540', 'ER_EVENT_CANT_ALTER', _("Failed to alter event '{}'")],
    ['1541', 'ER_EVENT_DROP_FAILED', _("Failed to drop {}")],
    ['1542', 'ER_EVENT_INTERVAL_NOT_POSITIVE_OR_TOO_BIG', _("INTERVAL is either not positive or too big")],
    ['1543', 'ER_EVENT_ENDS_BEFORE_STARTS', _("ENDS is either invalid or before STARTS")],
    ['1544', 'ER_EVENT_EXEC_TIME_IN_THE_PAST', _("Event execution time is in the past. Event has been disabled")],
    ['1545', 'ER_EVENT_OPEN_TABLE_FAILED', _("Failed to open mysql.event")],
    ['1546', 'ER_EVENT_NEITHER_M_EXPR_NOR_M_AT', _("No datetime expression provided")],
    [
        '1547',
        'ER_COL_COUNT_DOESNT_MATCH_CORRUPTED',
        _("Column count of mysql.{} is wrong. Expected {}, found {}. The table is probably corrupted"),
    ],
    ['1548', 'ER_CANNOT_LOAD_FROM_TABLE', _("Cannot load from mysql.{}. The table is probably corrupted")],
    ['1549', 'ER_EVENT_CANNOT_DELETE', _("Failed to delete the event from mysql.event")],
    ['1550', 'ER_EVENT_COMPILE_ERROR', _("Error during compilation of event's body")],
    ['1551', 'ER_EVENT_SAME_NAME', _("Same old and new event name")],
    ['1552', 'ER_EVENT_DATA_TOO_LONG', _("Data for column '{}' too long")],
    ['1553', 'ER_DROP_INDEX_FK', _("Cannot drop index '{}': needed in a foreign key constraint")],
    [
        '1554',
        'ER_WARN_DEPRECATED_SYNTAX_WITH_VER',
        _("The syntax '{}' is deprecated and will be removed in MariaDB {}. Please use {} instead"),
    ],
    ['1555', 'ER_CANT_WRITE_LOCK_LOG_TABLE', _("You can't write-lock a log table. Only read access is possible")],
    ['1556', 'ER_CANT_LOCK_LOG_TABLE', _("You can't use locks with log tables.")],
    [
        '1557',
        'ER_FOREIGN_DUPLICATE_KEY',
        _("Upholding foreign key constraints for table '{}', entry '{}', key {} would lead to a duplicate entry"),
    ],
    [
        '1558',
        'ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE',
        _(
            "Column count of mysql.{} is wrong. Expected {}, found {}. Created with MariaDB {}, now running {}. "
            "Please use mysql_upgrade to fix this error."
        ),
    ],
    [
        '1559',
        'ER_TEMP_TABLE_PREVENTS_SWITCH_OUT_OF_RBR',
        _("Cannot switch out of the row-based binary log format when the session has open temporary tables"),
    ],
    [
        '1560',
        'ER_STORED_FUNCTION_ PREVENTS_SWITCH_BINLOG_FORMAT',
        _("Cannot change the binary logging format inside a stored function or trigger"),
    ],
    [
        '1561',
        'ER_NDB_CANT_SWITCH_BINLOG_FORMAT',
        _("The NDB cluster engine does not support changing the binlog format on the fly yet"),
    ],
    ['1562', 'ER_PARTITION_NO_TEMPORARY', _("Cannot create temporary table with partitions")],
    ['1563', 'ER_PARTITION_CONST_DOMAIN_ERROR', _("Partition constant is out of partition function domain")],
    ['1564', 'ER_PARTITION_FUNCTION_IS_NOT_ALLOWED', _("This partition function is not allowed")],
    ['1565', 'ER_DDL_LOG_ERROR', _("Error in DDL log")],
    ['1566', 'ER_NULL_IN_VALUES_LESS_THAN', _("Not allowed to use NULL value in VALUES LESS THAN")],
    ['1567', 'ER_WRONG_PARTITION_NAME', _("Incorrect partition name")],
    [
        '1568',
        'ER_CANT_CHANGE_TX_ISOLATION',
        _("Transaction isolation level can't be changed while a transaction is in progress"),
    ],
    [
        '1569',
        'ER_DUP_ENTRY_AUTOINCREMENT_CASE',
        _("ALTER TABLE causes auto_increment resequencing, resulting in duplicate entry '{}' for key '{}'"),
    ],
    ['1570', 'ER_EVENT_MODIFY_QUEUE_ERROR', _("Internal scheduler error {}")],
    ['1571', 'ER_EVENT_SET_VAR_ERROR', _("Error during starting/stopping of the scheduler. Error code {}")],
    ['1572', 'ER_PARTITION_MERGE_ERROR', _("Engine cannot be used in partitioned tables")],
    ['1573', 'ER_CANT_ACTIVATE_LOG', _("Cannot activate '{}' log")],
    ['1574', 'ER_RBR_NOT_AVAILABLE', _("The server was not built with row-based replication")],
    ['1575', 'ER_BASE64_DECODE_ERROR', _("Decoding of base64 string failed")],
    ['1576', 'ER_EVENT_RECURSION_FORBIDDEN', _("Recursion of EVENT DDL statements is forbidden when body is present")],
    [
        '1577',
        'ER_EVENTS_DB_ERROR',
        _("Cannot proceed because system tables used by Event Scheduler were found damaged at server start"),
    ],
    ['1578', 'ER_ONLY_INTEGERS_ALLOWED', _("Only integers allowed as number here")],
    ['1579', 'ER_UNSUPORTED_LOG_ENGINE', _("This storage engine cannot be used for log tables")],
    ['1580', 'ER_BAD_LOG_STATEMENT', _("You cannot '{}' a log table if logging is enabled")],
    [
        '1581',
        'ER_CANT_RENAME_LOG_TABLE',
        _(
            "Cannot rename '{}'. When logging enabled, rename to/from log table must rename two tables: "
            "the log table to an archive table and another table back to '{}'"
        ),
    ],
    ['1582', 'ER_WRONG_PARAMCOUNT_TO_NATIVE_FCT', _("Incorrect parameter count in the call to native function '{}'")],
    ['1583', 'ER_WRONG_PARAMETERS_TO_NATIVE_FCT', _("Incorrect parameters in the call to native function '{}'")],
    ['1584', 'ER_WRONG_PARAMETERS_TO_STORED_FCT', _("Incorrect parameters in the call to stored function '{}'")],
    ['1585', 'ER_NATIVE_FCT_NAME_COLLISION', _("This function '{}' has the same name as a native function")],
    ['1586', 'ER_DUP_ENTRY_WITH_KEY_NAME', _("Duplicate entry '{}' for key '{}'")],
    ['1587', 'ER_BINLOG_PURGE_EMFILE', _("Too many files opened, please execute the command again")],
    [
        '1588',
        'ER_EVENT_CANNOT_CREATE_IN_THE_PAST',
        _(
            "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. "
            "The event was dropped immediately after creation."
        ),
    ],
    [
        '1589',
        'ER_EVENT_CANNOT_ALTER_IN_THE_PAST',
        _(
            "Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. "
            "The event was dropped immediately after creation."
        ),
    ],
    ['1590', 'ER_SLAVE_INCIDENT', _("The incident {} occured on the master. Message: {}")],
    ['1591', 'ER_NO_PARTITION_FOR_GIVEN_VALUE_SILENT', _("Table has no partition for some existing values")],
    [
        '1592',
        'ER_BINLOG_UNSAFE_STATEMENT',
        _("Unsafe statement written to the binary log using statement format since BINLOG_FORMAT = STATEMENT. {}"),
    ],
    ['1593', 'ER_SLAVE_FATAL_ERROR', _("Fatal error: {}")],
    ['1594', 'ER_SLAVE_RELAY_LOG_READ_FAILURE', _("Relay log read failure: {}")],
    ['1595', 'ER_SLAVE_RELAY_LOG_WRITE_FAILURE', _("Relay log write failure: {}")],
    ['1596', 'ER_SLAVE_CREATE_EVENT_FAILURE', _("Failed to create {}")],
    ['1597', 'ER_SLAVE_MASTER_COM_FAILURE', _("Master command {} failed: {}")],
    ['1598', 'ER_BINLOG_LOGGING_IMPOSSIBLE', _("Binary logging not possible. Message: {}")],
    ['1599', 'ER_VIEW_NO_CREATION_CTX', _("View `{}`.`{}` has no creation context")],
    ['1600', 'ER_VIEW_INVALID_CREATION_CTX', _("Creation context of view `{}`.`{}' is invalid")],
    ['1601', 'ER_SR_INVALID_CREATION_CTX', _("Creation context of stored routine `{}`.`{}` is invalid")],
    ['1602', 'ER_TRG_CORRUPTED_FILE', _("Corrupted TRG file for table `{}`.`{}`")],
    ['1603', 'ER_TRG_NO_CREATION_CTX', _("Triggers for table `{}`.`{}` have no creation context")],
    ['1604', 'ER_TRG_INVALID_CREATION_CTX', _("Trigger creation context of table `{}`.`{}` is invalid")],
    ['1605', 'ER_EVENT_INVALID_CREATION_CTX', _("Creation context of event `{}`.`{}` is invalid")],
    ['1606', 'ER_TRG_CANT_OPEN_TABLE', _("Cannot open table for trigger `{}`.`{}`")],
    ['1607', 'ER_CANT_CREATE_SROUTINE', _("Cannot create stored routine `{}`. Check warnings")],
    [
        '1609',
        'ER_NO_FORMAT_DESCRIPTION_EVENT _BEFORE_BINLOG_STATEMENT',
        _("The BINLOG statement of type `{}` was not preceded by a format description BINLOG statement."),
    ],
    ['1610', 'ER_SLAVE_CORRUPT_EVENT', _("Corrupted replication event was detected")],
    ['1611', 'ER_LOAD_DATA_INVALID_COLUMN', _("Invalid column reference ({}) in LOAD DATA")],
    ['1612', 'ER_LOG_PURGE_NO_FILE', _("Being purged log {} was not found")],
    ['1613', 'ER_XA_RBTIMEOUT', _("XA_RBTIMEOUT: Transaction branch was rolled back: took too long")],
    ['1614', 'ER_XA_RBDEADLOCK', _("XA_RBDEADLOCK: Transaction branch was rolled back: deadlock was detected")],
    ['1615', 'ER_NEED_REPREPARE', _("Prepared statement needs to be re-prepared")],
    ['1616', 'ER_DELAYED_NOT_SUPPORTED', _("DELAYED option not supported for table '{}'")],
    ['1617', 'WARN_NO_MASTER_INF', _("The master info structure does not exist")],
    ['1618', 'WARN_OPTION_IGNORED', _("<{}> option ignored")],
    ['1619', 'WARN_PLUGIN_DELETE_BUILTIN', _("Built-in plugins cannot be deleted")],
    ['1620', 'WARN_PLUGIN_BUSY', _("Plugin is busy and will be uninstalled on shutdown")],
    ['1621', 'ER_VARIABLE_IS_READONLY', _("{} variable '{}' is read-only. Use SET {} to assign the value")],
    [
        '1622',
        'ER_WARN_ENGINE_TRANSACTION_ROLLBACK',
        _(
            "Storage engine {} does not support rollback for this statement. "
            "Transaction rolled back and must be restarted"
        ),
    ],
    ['1623', 'ER_SLAVE_HEARTBEAT_FAILURE', _("Unexpected master's heartbeat data: {}")],
    [
        '1624',
        'ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE',
        _(
            "The requested value for the heartbeat period is either negative "
            "or exceeds the maximum allowed ({} seconds)."
        ),
    ],
    ['1625', 'ER_NDB_REPLICATION_SCHEMA_ERROR', _("Bad schema for mysql.ndb_replication table. Message: {}")],
    ['1626', 'ER_CONFLICT_FN_PARSE_ERROR', _("Error in parsing conflict function. Message: {}")],
    ['1627', 'ER_EXCEPTIONS_WRITE_ERROR', _("Write to exceptions table failed. Message: {}")],
    ['1628', 'ER_TOO_LONG_TABLE_COMMENT', _("Comment for table '{}' is too long (max = {})")],
    ['1629', 'ER_TOO_LONG_FIELD_COMMENT', _("Comment for field '{}' is too long (max = {})")],
    [
        '1630',
        'ER_FUNC_INEXISTENT_NAME_COLLISION',
        _(
            "FUNCTION {} does not exist. "
            "Check the 'Function Name Parsing and Resolution' section in the Reference Manual"
        ),
    ],
    ['1631', 'ER_DATABASE_NAME', _("Database")],
    ['1632', 'ER_TABLE_NAME', _("Table")],
    ['1633', 'ER_PARTITION_NAME', _("Partition")],
    ['1634', 'ER_SUBPARTITION_NAME', _("Subpartition")],
    ['1635', 'ER_TEMPORARY_NAME', _("Temporary")],
    ['1636', 'ER_RENAMED_NAME', _("Renamed")],
    ['1637', 'ER_TOO_MANY_CONCURRENT_TRXS', _("Too many active concurrent transactions")],
    ['1638', 'WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED', _("Non-ASCII separator arguments are not fully supported")],
    ['1639', 'ER_DEBUG_SYNC_TIMEOUT', _("debug sync point wait timed out")],
    ['1640', 'ER_DEBUG_SYNC_HIT_LIMIT', _("debug sync point hit limit reached")],
    ['1641', 'ER_DUP_SIGNAL_SET', _("Duplicate condition information item '{}'")],
    ['1642', 'ER_SIGNAL_WARN', _("Unhandled user-defined warning condition")],
    ['1643', 'ER_SIGNAL_NOT_FOUND', _("Unhandled user-defined not found condition")],
    ['1644', 'ER_SIGNAL_EXCEPTION', _("Unhandled user-defined exception condition")],
    ['1645', 'ER_RESIGNAL_WITHOUT_ACTIVE_HANDLER', _("RESIGNAL when handler not active")],
    ['1646', 'ER_SIGNAL_BAD_CONDITION_TYPE', _("SIGNAL/RESIGNAL can only use a CONDITION defined with SQLSTATE")],
    ['1647', 'WARN_COND_ITEM_TRUNCATED', _("Data truncated for condition item '{}'")],
    ['1648', 'ER_COND_ITEM_TOO_LONG', _("Data too long for condition item '{}'")],
    ['1649', 'ER_UNKNOWN_LOCALE', _("Unknown locale: '{}'")],
    [
        '1650',
        'ER_SLAVE_IGNORE_SERVER_IDS',
        _("The requested server id {} clashes with the slave startup option --replicate-same-server-id"),
    ],
    [
        '1651',
        'ER_QUERY_CACHE_DISABLED',
        _("Query cache is disabled; restart the server with query_cache_type=1 to enable it"),
    ],
    ['1652', 'ER_SAME_NAME_PARTITION_FIELD', _("Duplicate partition field name '{}'")],
    ['1653', 'ER_PARTITION_COLUMN_LIST_ERROR', _("Inconsistency in usage of column lists for partitioning")],
    ['1654', 'ER_WRONG_TYPE_COLUMN_VALUE_ERROR', _("Partition column values of incorrect type")],
    ['1655', 'ER_TOO_MANY_PARTITION_FUNC_FIELDS_ERROR', _("Too many fields in '{}'")],
    ['1656', 'ER_MAXVALUE_IN_VALUES_IN', _("Cannot use MAXVALUE as value in VALUES IN")],
    ['1657', 'ER_TOO_MANY_VALUES_ERROR', _("Cannot have more than one value for this type of {} partitioning")],
    [
        '1658',
        'ER_ROW_SINGLE_PARTITION_FIELD_ERROR',
        _("Row expressions in VALUES IN only allowed for multi-field column partitioning"),
    ],
    [
        '1659',
        'ER_FIELD_TYPE_NOT_ALLOWED_AS_PARTITION_FIELD',
        _("Field '{}' is of a not allowed type for this type of partitioning"),
    ],
    ['1660', 'ER_PARTITION_FIELDS_TOO_LONG', _("The total length of the partitioning fields is too large")],
    [
        '1661',
        'ER_BINLOG_ROW_ENGINE_AND_STMT_ENGINE',
        _(
            "Cannot execute statement: impossible to write to binary log since both row-incapable engines "
            "and statement-incapable engines are involved."
        ),
    ],
    [
        '1662',
        'ER_BINLOG_ROW_MODE_AND_STMT_ENGINE',
        _(
            "Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = ROW "
            "and at least one table uses a storage engine limited to statement-based logging."
        ),
    ],
    [
        '1663',
        'ER_BINLOG_UNSAFE_AND_STMT_ENGINE',
        _(
            "Cannot execute statement: impossible to write to binary log since statement is unsafe, "
            "storage engine is limited to statement-based logging, and BINLOG_FORMAT = MIXED. {}"
        ),
    ],
    [
        '1664',
        'ER_BINLOG_ROW_INJECTION_AND_STMT_ENGINE',
        _(
            "Cannot execute statement: impossible to write to binary log since statement is in row format "
            "and at least one table uses a storage engine limited to statement-based logging."
        ),
    ],
    [
        '1665',
        'ER_BINLOG_STMT_MODE_AND_ROW_ENGINE',
        _(
            "Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = STATEMENT "
            "and at least one table uses a storage engine limited to row-based logging.{}"
        ),
    ],
    [
        '1666',
        'ER_BINLOG_ROW_INJECTION_AND_STMT_MODE',
        _(
            "Cannot execute statement: impossible to write to binary log since statement is in row format and "
            "BINLOG_FORMAT = STATEMENT."
        ),
    ],
    [
        '1667',
        'ER_BINLOG_MULTIPLE_ENGINES _AND_SELF_LOGGING_ENGINE',
        _(
            "Cannot execute statement: impossible to write to binary log since more than one engine is involved "
            "and at least one engine is self-logging."
        ),
    ],
    [
        '1668',
        'ER_BINLOG_UNSAFE_LIMIT',
        _(
            "The statement is unsafe because it uses a LIMIT clause. "
            "This is unsafe because the set of rows included cannot be predicted."
        ),
    ],
    [
        '1669',
        'ER_BINLOG_UNSAFE_INSERT_DELAYED',
        _(
            "The statement is unsafe because it uses INSERT DELAYED. "
            "This is unsafe because the times when rows are inserted cannot be predicted."
        ),
    ],
    [
        '1670',
        'ER_BINLOG_UNSAFE_SYSTEM_TABLE',
        _(
            "The statement is unsafe because it uses the general log, slow query log, or performance_schema table(s). "
            "This is unsafe because system tables may differ on slaves."
        ),
    ],
    [
        '1671',
        'ER_BINLOG_UNSAFE_AUTOINC_COLUMNS',
        _(
            "Statement is unsafe because it invokes a trigger or a stored function "
            "that inserts into an AUTO_INCREMENT column. Inserted values cannot be logged correctly."
        ),
    ],
    [
        '1672',
        'ER_BINLOG_UNSAFE_UDF',
        _("Statement is unsafe because it uses a UDF which may not return the same value on the slave."),
    ],
    [
        '1673',
        'ER_BINLOG_UNSAFE_SYSTEM_VARIABLE',
        _("Statement is unsafe because it uses a system variable that may have a different value on the slave."),
    ],
    [
        '1674',
        'ER_BINLOG_UNSAFE_SYSTEM_FUNCTION',
        _("Statement is unsafe because it uses a system function that may return a different value on the slave."),
    ],
    [
        '1675',
        'ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS',
        _(
            "Statement is unsafe because it accesses a non-transactional table after accessing a transactional table "
            "within the same transaction."
        ),
    ],
    ['1676', 'ER_MESSAGE_AND_STATEMENT', _("{} Statement: {}")],
    [
        '1677',
        'ER_SLAVE_CONVERSION_FAILED',
        _("Column {} of table '{}.{}' cannot be converted from type '{}' to type '{}'"),
    ],
    ['1678', 'ER_SLAVE_CANT_CREATE_CONVERSION', _("Can't create conversion table for table '{}.{}'")],
    [
        '1679',
        'ER_INSIDE_TRANSACTION _PREVENTS_SWITCH_BINLOG_FORMAT',
        _("Cannot modify @@session.binlog_format inside a transaction"),
    ],
    ['1680', 'ER_PATH_LENGTH', _("The path specified for {} is too long.")],
    [
        '1681',
        'ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT',
        _("'{}' is deprecated and will be removed in a future release."),
    ],
    ['1682', 'ER_WRONG_NATIVE_TABLE_STRUCTURE', _("Native table '{}'.'{}' has the wrong structure")],
    ['1683', 'ER_WRONG_PERFSCHEMA_USAGE', _("Invalid performance_schema usage.")],
    [
        '1684',
        'ER_WARN_I_S_SKIPPED_TABLE',
        _("Table '{}'.'{}' was skipped since its definition is being modified by concurrent DDL statement"),
    ],
    [
        '1685',
        'ER_INSIDE_TRANSACTION _PREVENTS_SWITCH_BINLOG_DIRECT',
        _("Cannot modify @@session.binlog_direct_non_transactional_updates inside a transaction"),
    ],
    [
        '1686',
        'ER_STORED_FUNCTION_PREVENTS _SWITCH_BINLOG_DIRECT',
        _("Cannot change the binlog direct flag inside a stored function or trigger"),
    ],
    ['1687', 'ER_SPATIAL_MUST_HAVE_GEOM_COL', _("A SPATIAL index may only contain a geometrical type column")],
    ['1688', 'ER_TOO_LONG_INDEX_COMMENT', _("Comment for index '{}' is too long (max = {})")],
    ['1689', 'ER_LOCK_ABORTED', _("Wait on a lock was aborted due to a pending exclusive lock")],
    ['1690', 'ER_DATA_OUT_OF_RANGE', _("{} value is out of range in '{}'")],
    ['1691', 'ER_WRONG_SPVAR_TYPE_IN_LIMIT', _("A variable of a non-integer based type in LIMIT clause")],
    [
        '1692',
        'ER_BINLOG_UNSAFE_MULTIPLE_ENGINES _AND_SELF_LOGGING_ENGINE',
        _("Mixing self-logging and non-self-logging engines in a statement is unsafe."),
    ],
    [
        '1693',
        'ER_BINLOG_UNSAFE_MIXED_STATEMENT',
        _(
            "Statement accesses nontransactional table as well as transactional or temporary table, "
            "and writes to any of them."
        ),
    ],
    [
        '1694',
        'ER_INSIDE_TRANSACTION_ PREVENTS_SWITCH_SQL_LOG_BIN',
        _("Cannot modify @@session.sql_log_bin inside a transaction"),
    ],
    [
        '1695',
        'ER_STORED_FUNCTION_ PREVENTS_SWITCH_SQL_LOG_BIN',
        _("Cannot change the sql_log_bin inside a stored function or trigger"),
    ],
    ['1696', 'ER_FAILED_READ_FROM_PAR_FILE', _("Failed to read from the .par file")],
    ['1697', 'ER_VALUES_IS_NOT_INT_TYPE_ERROR', _("VALUES value for partition '{}' must have type INT")],
    ['1698', 'ER_ACCESS_DENIED_NO_PASSWORD_ERROR', _("Access denied for user '{}'@'{}'")],
    ['1699', 'ER_SET_PASSWORD_AUTH_PLUGIN', _("SET PASSWORD has no significance for users authenticating via plugins")],
    [
        '1700',
        'ER_GRANT_PLUGIN_USER_EXISTS',
        _("GRANT with IDENTIFIED WITH is illegal because the user {} already exists"),
    ],
    ['1701', 'ER_TRUNCATE_ILLEGAL_FK', _("Cannot truncate a table referenced in a foreign key constraint ({})")],
    ['1702', 'ER_PLUGIN_IS_PERMANENT', _("Plugin '{}' is force_plus_permanent and can not be unloaded")],
    [
        '1703',
        'ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MIN',
        _(
            "The requested value for the heartbeat period is less than 1 millisecond. The value is reset to 0, "
            "meaning that heartbeating will effectively be disabled."
        ),
    ],
    [
        '1704',
        'ER_SLAVE_HEARTBEAT_VALUE_OUT_OF_RANGE_MAX',
        _(
            "The requested value for the heartbeat period exceeds the value of slave_net_timeout seconds. "
            "A sensible value for the period should be less than the timeout."
        ),
    ],
    [
        '1705',
        'ER_STMT_CACHE_FULL',
        _(
            "Multi-row statements required more than 'max_binlog_stmt_cache_size' bytes of storage; "
            "increase this mysqld variable and try again"
        ),
    ],
    [
        '1706',
        'ER_MULTI_UPDATE_KEY_CONFLICT',
        _("Primary key/partition key update is not allowed since the table is updated both as '{}' and '{}'."),
    ],
    [
        '1707',
        'ER_TABLE_NEEDS_REBUILD',
        _("Table rebuild required. Please do 'ALTER TABLE `{}` FORCE' or dump/reload to fix it!"),
    ],
    ['1708', 'WARN_OPTION_BELOW_LIMIT', _("The value of '{}' should be no less than the value of '{}'")],
    ['1709', 'ER_INDEX_COLUMN_TOO_LONG', _("Index column size too large. The maximum column size is {} bytes.")],
    ['1710', 'ER_ERROR_IN_TRIGGER_BODY', _("Trigger '{}' has an error in its body: '{}'")],
    ['1711', 'ER_ERROR_IN_UNKNOWN_TRIGGER_BODY', _("Unknown trigger has an error in its body: '{}'")],
    ['1712', 'ER_INDEX_CORRUPT', _("Index {} is corrupted")],
    ['1713', 'ER_UNDO_RECORD_TOO_BIG', _("Undo log record is too big.")],
    [
        '1714',
        'ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT',
        _(
            "INSERT IGNORE... SELECT is unsafe because the order in which rows are retrieved by the SELECT determines "
            "which (if any) rows are ignored. This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1715',
        'ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE',
        _(
            "INSERT... SELECT... ON DUPLICATE KEY UPDATE is unsafe because the order "
            "in which rows are retrieved by the SELECT determines which (if any) rows are updated. "
            "This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1716',
        'ER_BINLOG_UNSAFE_REPLACE_SELECT',
        _(
            "REPLACE... SELECT is unsafe because the order in which rows are retrieved by the SELECT determines "
            "which (if any) rows are replaced. This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1717',
        'ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT',
        _(
            "CREATE... IGNORE SELECT is unsafe because the order in which rows are retrieved by the SELECT determines "
            "which (if any) rows are ignored. This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1718',
        'ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT',
        _(
            "CREATE... REPLACE SELECT is unsafe because the order in which rows are retrieved by the SELECT determines "
            "which (if any) rows are replaced. This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1719',
        'ER_BINLOG_UNSAFE_UPDATE_IGNORE',
        _(
            "UPDATE IGNORE is unsafe because the order in which rows are updated determines "
            "which (if any) rows are ignored. This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1720',
        'ER_PLUGIN_NO_UNINSTALL',
        _("Plugin '{}' is marked as not dynamically uninstallable. You have to stop the server to uninstall it."),
    ],
    [
        '1721',
        'ER_PLUGIN_NO_INSTALL',
        _("Plugin '{}' is marked as not dynamically installable. You have to stop the server to install it."),
    ],
    [
        '1722',
        'ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT',
        _(
            "Statements writing to a table with an auto-increment column after selecting from another table are unsafe "
            "because the order in which rows are retrieved determines what (if any) rows will be written. "
            "This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1723',
        'ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC',
        _(
            "CREATE TABLE... SELECT... on a table with an auto-increment column is unsafe because the order "
            "in which rows are retrieved by the SELECT determines which (if any) rows are inserted. "
            "This order cannot be predicted and may differ on master and the slave."
        ),
    ],
    [
        '1724',
        'ER_BINLOG_UNSAFE_INSERT_TWO_KEYS',
        _("INSERT... ON DUPLICATE KEY UPDATE on a table with more than one UNIQUE KEY is unsafe"),
    ],
    ['1725', 'ER_TABLE_IN_FK_CHECK', _("Table is being used in foreign key check.")],
    ['1726', 'ER_UNSUPPORTED_ENGINE', _("Storage engine '{}' does not support system tables. [{}.{}]")],
    [
        '1727',
        'ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST',
        _("INSERT into autoincrement field which is not the first part in the composed primary key is unsafe."),
    ],
    ['1728', 'ER_CANNOT_LOAD_FROM_TABLE_V2', _("Cannot load from {}.{}. The table is probably corrupted")],
    [
        '1729',
        'ER_MASTER_DELAY_VALUE_OUT_OF_RANGE',
        _("The requested value {} for the master delay exceeds the maximum {}"),
    ],
    [
        '1730',
        'ER_ONLY_FD_AND_RBR_EVENTS_ALLOWED_IN_BINLOG_STATEMENT',
        _("Only Format_description_log_event and row events are allowed in BINLOG statements (but {} was provided"),
    ],
    ['1731', 'ER_PARTITION_EXCHANGE_DIFFERENT_OPTION', _("Non matching attribute '{}' between partition and table")],
    ['1732', 'ER_PARTITION_EXCHANGE_PART_TABLE', _("Table to exchange with partition is partitioned: '{}'")],
    ['1733', 'ER_PARTITION_EXCHANGE_TEMP_TABLE', _("Table to exchange with partition is temporary: '{}'")],
    ['1734', 'ER_PARTITION_INSTEAD_OF_SUBPARTITION', _("Subpartitioned table, use subpartition instead of partition")],
    ['1735', 'ER_UNKNOWN_PARTITION', _("Unknown partition '{}' in table '{}'")],
    ['1736', 'ER_TABLES_DIFFERENT_METADATA', _("Tables have different definitions")],
    ['1737', 'ER_ROW_DOES_NOT_MATCH_PARTITION', _("Found a row that does not match the partition")],
    [
        '1738',
        'ER_BINLOG_CACHE_SIZE_GREATER_THAN_MAX',
        _(
            "Option binlog_cache_size ({}) is greater than max_binlog_cache_size ({}); "
            "setting binlog_cache_size equal to max_binlog_cache_size."
        ),
    ],
    [
        '1739',
        'ER_WARN_INDEX_NOT_APPLICABLE',
        _("Cannot use {} access on index '{}' due to type or collation conversion on field '{}'"),
    ],
    [
        '1740',
        'ER_PARTITION_EXCHANGE_FOREIGN_KEY',
        _("Table to exchange with partition has foreign key references: '{}'"),
    ],
    ['1741', 'ER_NO_SUCH_KEY_VALUE', _("Key value '{}' was not found in table '{}.{}'")],
    ['1742', 'ER_RPL_INFO_DATA_TOO_LONG', _("Data for column '{}' too long")],
    [
        '1743',
        'ER_NETWORK_READ_EVENT_CHECKSUM_FAILURE',
        _("Replication event checksum verification failed while reading from network."),
    ],
    [
        '1744',
        'ER_BINLOG_READ_EVENT_CHECKSUM_FAILURE',
        _("Replication event checksum verification failed while reading from a log file."),
    ],
    [
        '1745',
        'ER_BINLOG_STMT_CACHE_SIZE_GREATER_THAN_MAX',
        _(
            "Option binlog_stmt_cache_size ({}) is greater than max_binlog_stmt_cache_size ({}); "
            "setting binlog_stmt_cache_size equal to max_binlog_stmt_cache_size."
        ),
    ],
    ['1746', 'ER_CANT_UPDATE_TABLE_IN_CREATE_TABLE_SELECT', _("Can't update table '{}' while '{}' is being created.")],
    ['1747', 'ER_PARTITION_CLAUSE_ON_NONPARTITIONED', _("PARTITION () clause on non partitioned table")],
    ['1748', 'ER_ROW_DOES_NOT_MATCH_GIVEN_PARTITION_SET', _("Found a row not matching the given partition set")],
    ['1749', 'ER_NO_SUCH_PARTITION_UNUSED', _("partition '{}' doesn't exist")],
    [
        '1750',
        'ER_CHANGE_RPL_INFO_REPOSITORY_FAILURE',
        _("Failure while changing the type of replication repository: {}."),
    ],
    [
        '1751',
        'ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_CREATED_TEMP_TABLE',
        _("The creation of some temporary tables could not be rolled back."),
    ],
    [
        '1752',
        'ER_WARNING_NOT_COMPLETE_ROLLBACK_WITH_DROPPED_TEMP_TABLE',
        _("Some temporary tables were dropped, but these operations could not be rolled back."),
    ],
    ['1753', 'ER_MTS_FEATURE_IS_NOT_SUPPORTED', _("{} is not supported in multi-threaded slave mode. {}")],
    [
        '1754',
        'ER_MTS_UPDATED_DBS_GREATER_MAX',
        _(
            "The number of modified databases exceeds the maximum {}; "
            "the database names will not be included in the replication event metadata."
        ),
    ],
    [
        '1755',
        'ER_MTS_CANT_PARALLEL',
        _(
            "Cannot execute the current event group in the parallel mode. Encountered event {}, relay-log name {}, "
            "position {} which prevents execution of this event group in parallel mode. Reason: {}."
        ),
    ],
    ['1756', 'ER_MTS_INCONSISTENT_DATA', _("{}")],
    [
        '1757',
        'ER_FULLTEXT_NOT_SUPPORTED_WITH_PARTITIONING',
        _("FULLTEXT index is not supported for partitioned tables."),
    ],
    ['1758', 'ER_DA_INVALID_CONDITION_NUMBER', _("Invalid condition number")],
    ['1759', 'ER_INSECURE_PLAIN_TEXT', _("Sending passwords in plain text without SSL/TLS is extremely insecure.")],
    [
        '1760',
        'ER_INSECURE_CHANGE_MASTER',
        _(
            "Storing MySQL user name or password information in the master info repository is not secure and "
            "is therefore not recommended. "
            "Please consider using the USER and PASSWORD connection options for START SLAVE; "
            "see the 'START SLAVE Syntax' in the MySQL Manual for more information."
        ),
    ],
    [
        '1761',
        'ER_FOREIGN_DUPLICATE_KEY_WITH_CHILD_INFO',
        _("Foreign key constraint for table '{}', record '{}' would lead to a duplicate entry in table '{}', key '{}'"),
    ],
    [
        '1762',
        'ER_FOREIGN_DUPLICATE_KEY_WITHOUT_CHILD_INFO',
        _("Foreign key constraint for table '{}', record '{}' would lead to a duplicate entry in a child table"),
    ],
    [
        '1763',
        'ER_SQLTHREAD_WITH_SECURE_SLAVE',
        _("Setting authentication options is not possible when only the Slave SQL Thread is being started."),
    ],
    ['1764', 'ER_TABLE_HAS_NO_FT', _("The table does not have FULLTEXT index to support this query")],
    [
        '1765',
        'ER_VARIABLE_NOT_SETTABLE_IN_SF_OR_TRIGGER',
        _("The system variable {} cannot be set in stored functions or triggers."),
    ],
    [
        '1766',
        'ER_VARIABLE_NOT_SETTABLE_IN_TRANSACTION',
        _("The system variable {} cannot be set when there is an ongoing transaction."),
    ],
    [
        '1767',
        'ER_GTID_NEXT_IS_NOT_IN_GTID_NEXT_LIST',
        _("The system variable @@SESSION.GTID_NEXT has the value {}, which is not listed in @@SESSION.GTID_NEXT_LIST."),
    ],
    [
        '1768',
        'ER_CANT_CHANGE_GTID_NEXT_IN_TRANSACTION_WHEN_GTID_NEXT_LIST_IS_NULL',
        _("The system variable @@SESSION.GTID_NEXT cannot change inside a transaction."),
    ],
    ['1769', 'ER_SET_STATEMENT_CANNOT_INVOKE_FUNCTION', _("The statement 'SET {}' cannot invoke a stored function.")],
    [
        '1770',
        'ER_GTID_NEXT_CANT_BE_AUTOMATIC_IF_GTID_NEXT_LIST_IS_NON_NULL',
        _("The system variable @@SESSION.GTID_NEXT cannot be 'AUTOMATIC' when @@SESSION.GTID_NEXT_LIST is non-NULL."),
    ],
    [
        '1771',
        'ER_SKIPPING_LOGGED_TRANSACTION',
        _("Skipping transaction {} because it has already been executed and logged."),
    ],
    ['1772', 'ER_MALFORMED_GTID_SET_SPECIFICATION', _("Malformed GTID set specification '{}'.")],
    ['1773', 'ER_MALFORMED_GTID_SET_ENCODING', _("Malformed GTID set encoding.")],
    ['1774', 'ER_MALFORMED_GTID_SPECIFICATION', _("Malformed GTID specification '{}'.")],
    [
        '1775',
        'ER_GNO_EXHAUSTED',
        _(
            "Impossible to generate Global Transaction Identifier: the integer component reached the maximal value. "
            "Restart the server with a new server_uuid."
        ),
    ],
    [
        '1776',
        'ER_BAD_SLAVE_AUTO_POSITION',
        _(
            "Parameters MASTER_LOG_FILE, MASTER_LOG_POS, "
            "RELAY_LOG_FILE and RELAY_LOG_POS cannot be set when MASTER_AUTO_POSITION is active."
        ),
    ],
    [
        '1777',
        'ER_AUTO_POSITION_REQUIRES_GTID_MODE_ON',
        _("CHANGE MASTER TO MASTER_AUTO_POSITION = 1 can only be executed when @@GLOBAL.GTID_MODE = ON."),
    ],
    [
        '1778',
        'ER_CANT_DO_IMPLICIT_COMMIT_IN_TRX_WHEN_GTID_NEXT_IS_SET',
        _("Cannot execute statements with implicit commit inside a transaction when @@SESSION.GTID_NEXT != AUTOMATIC."),
    ],
    [
        '1779',
        'ER_GTID_MODE_2_OR_3_REQUIRES_DISABLE_GTID_UNSAFE_STATEMENTS_ON',
        _("GTID_MODE = ON or GTID_MODE = UPGRADE_STEP_2 requires DISABLE_GTID_UNSAFE_STATEMENTS = 1."),
    ],
    [
        '1779',
        'ER_GTID_MODE_2_OR_3_REQUIRES_ENFORCE_GTID_CONSISTENCY_ON',
        _("@@GLOBAL.GTID_MODE = ON or UPGRADE_STEP_2 requires @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1."),
    ],
    [
        '1780',
        'ER_GTID_MODE_REQUIRES_BINLOG',
        _("@@GLOBAL.GTID_MODE = ON or UPGRADE_STEP_1 or UPGRADE_STEP_2 requires --log-bin and --log-slave-updates."),
    ],
    [
        '1781',
        'ER_CANT_SET_GTID_NEXT_TO_GTID_WHEN_GTID_MODE_IS_OFF',
        _("@@SESSION.GTID_NEXT cannot be set to UUID:NUMBER when @@GLOBAL.GTID_MODE = OFF."),
    ],
    [
        '1782',
        'ER_CANT_SET_GTID_NEXT_TO_ANONYMOUS_WHEN_GTID_MODE_IS_ON',
        _("@@SESSION.GTID_NEXT cannot be set to ANONYMOUS when @@GLOBAL.GTID_MODE = ON."),
    ],
    [
        '1783',
        'ER_CANT_SET_GTID_NEXT_LIST_TO_NON_NULL_WHEN_GTID_MODE_IS_OFF',
        _("@@SESSION.GTID_NEXT_LIST cannot be set to a non-NULL value when @@GLOBAL.GTID_MODE = OFF."),
    ],
    [
        '1784',
        'ER_FOUND_GTID_EVENT_WHEN_GTID_MODE_IS_OFF',
        _("Found a Gtid_log_event or Previous_gtids_log_event when @@GLOBAL.GTID_MODE = OFF."),
    ],
    [
        '1785',
        'ER_GTID_UNSAFE_NON_TRANSACTIONAL_TABLE',
        _(
            "When @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1, "
            "updates to non-transactional tables can only be done in either autocommitted statements or "
            "single-statement transactions, and never in the same statement as updates to transactional tables."
        ),
    ],
    [
        '1786',
        'ER_GTID_UNSAFE_CREATE_SELECT',
        _("CREATE TABLE ... SELECT is forbidden when @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1."),
    ],
    [
        '1787',
        'ER_GTID_UNSAFE_CREATE_DROP_TEMPORARY_TABLE_IN_TRANSACTION',
        _(
            "When @@GLOBAL.ENFORCE_GTID_CONSISTENCY = 1, "
            "the statements CREATE TEMPORARY TABLE and "
            "DROP TEMPORARY TABLE can be executed in a non-transactional context only, and require that AUTOCOMMIT = 1."
        ),
    ],
    [
        '1788',
        'ER_GTID_MODE_CAN_ONLY_CHANGE_ONE_STEP_AT_A_TIME',
        _(
            "The value of @@GLOBAL."
            "GTID_MODE can only change one step at a time: OFF <-> UPGRADE_STEP_1 <-> UPGRADE_STEP_2 <-> ON. "
            "Also note that this value must be stepped up or down simultaneously on all servers; "
            "see the Manual for instructions."
        ),
    ],
    [
        '1789',
        'ER_MASTER_HAS_PURGED_REQUIRED_GTIDS',
        _(
            "The slave is connecting using CHANGE MASTER TO MASTER_AUTO_POSITION = 1, "
            "but the master has purged binary logs containing GTIDs that the slave requires."
        ),
    ],
    [
        '1790',
        'ER_CANT_SET_GTID_NEXT_WHEN_OWNING_GTID',
        _(
            "@@SESSION.GTID_NEXT cannot be changed by a client that owns a GTID. The client owns {}. "
            "Ownership is released on COMMIT or ROLLBACK."
        ),
    ],
    ['1791', 'ER_UNKNOWN_EXPLAIN_FORMAT', _("Unknown EXPLAIN format name: '{}'")],
    ['1792', 'ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION', _("Cannot execute statement in a READ ONLY transaction.")],
    ['1793', 'ER_TOO_LONG_TABLE_PARTITION_COMMENT', _("Comment for table partition '{}' is too long (max = {}")],
    [
        '1794',
        'ER_SLAVE_CONFIGURATION',
        _(
            "Slave is not configured or failed to initialize properly. "
            "You must at least set --server-id to enable either a master or a slave. "
            "Additional error messages can be found in the MySQL error log."
        ),
    ],
    ['1795', 'ER_INNODB_FT_LIMIT', _("InnoDB presently supports one FULLTEXT index creation at a time")],
    ['1796', 'ER_INNODB_NO_FT_TEMP_TABLE', _("Cannot create FULLTEXT index on temporary InnoDB table")],
    ['1797', 'ER_INNODB_FT_WRONG_DOCID_COLUMN', _("Column '{}' is of wrong type for an InnoDB FULLTEXT index")],
    ['1798', 'ER_INNODB_FT_WRONG_DOCID_INDEX', _("Index '{}' is of wrong type for an InnoDB FULLTEXT index")],
    [
        '1799',
        'ER_INNODB_ONLINE_LOG_TOO_BIG',
        _(
            "Creating index '{}' required more than 'innodb_online_alter_log_max_size' bytes of modification log. "
            "Please try again."
        ),
    ],
    ['1800', 'ER_UNKNOWN_ALTER_ALGORITHM', _("Unknown ALGORITHM '{}'")],
    ['1801', 'ER_UNKNOWN_ALTER_LOCK', _("Unknown LOCK type '{}'")],
    [
        '1802',
        'ER_MTS_CHANGE_MASTER_CANT_RUN_WITH_GAPS',
        _(
            "CHANGE MASTER cannot be executed when the slave was stopped with an error or killed in MTS mode. "
            "Consider using RESET SLAVE or START SLAVE UNTIL."
        ),
    ],
    [
        '1803',
        'ER_MTS_RECOVERY_FAILURE',
        _(
            "Cannot recover after SLAVE errored out in parallel execution mode. "
            "Additional error messages can be found in the MySQL error log."
        ),
    ],
    [
        '1804',
        'ER_MTS_RESET_WORKERS',
        _("Cannot clean up worker info tables. Additional error messages can be found in the MySQL error log."),
    ],
    [
        '1805',
        'ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2',
        _("Column count of {}.{} is wrong. Expected {}, found {}. The table is probably corrupted"),
    ],
    ['1806', 'ER_SLAVE_SILENT_RETRY_TRANSACTION', _("Slave must silently retry current transaction")],
    [
        '1807',
        'ER_DISCARD_FK_CHECKS_RUNNING',
        _("There is a foreign key check running on table '{}'. Cannot discard the table."),
    ],
    ['1808', 'ER_TABLE_SCHEMA_MISMATCH', _("Schema mismatch ({}")],
    ['1809', 'ER_TABLE_IN_SYSTEM_TABLESPACE', _("Table '{}' in system tablespace")],
    ['1810', 'ER_IO_READ_ERROR', _("IO Read error: ({}, {}) {}")],
    ['1811', 'ER_IO_WRITE_ERROR', _("IO Write error: ({}, {}) {}")],
    ['1812', 'ER_TABLESPACE_MISSING', _("Tablespace is missing for table '{}'")],
    [
        '1813',
        'ER_TABLESPACE_EXISTS',
        _("Tablespace for table '{}' exists. Please DISCARD the tablespace before IMPORT."),
    ],
    ['1814', 'ER_TABLESPACE_DISCARDED', _("Tablespace has been discarded for table '{}'")],
    ['1815', 'ER_INTERNAL_ERROR', _("Internal error: {}")],
    ['1816', 'ER_INNODB_IMPORT_ERROR', _("ALTER TABLE '{}' IMPORT TABLESPACE failed with error {} : '{}'")],
    ['1817', 'ER_INNODB_INDEX_CORRUPT', _("Index corrupt: {}")],
    [
        '1818',
        'ER_INVALID_YEAR_COLUMN_LENGTH',
        _("YEAR({}) column type is deprecated. Creating YEAR(4) column instead."),
    ],
    ['1819', 'ER_NOT_VALID_PASSWORD', _("Your password does not satisfy the current policy requirements")],
    ['1820', 'ER_MUST_CHANGE_PASSWORD', _("You must SET PASSWORD before executing this statement")],
    [
        '1821',
        'ER_FK_NO_INDEX_CHILD',
        _("Failed to add the foreign key constaint. Missing index for constraint '{}' in the foreign table '{}'"),
    ],
    [
        '1822',
        'ER_FK_NO_INDEX_PARENT',
        _("Failed to add the foreign key constaint. Missing index for constraint '{}' in the referenced table '{}'"),
    ],
    ['1823', 'ER_FK_FAIL_ADD_SYSTEM', _("Failed to add the foreign key constraint '{}' to system tables")],
    ['1824', 'ER_FK_CANNOT_OPEN_PARENT', _("Failed to open the referenced table '{}'")],
    [
        '1825',
        'ER_FK_INCORRECT_OPTION',
        _("Failed to add the foreign key constraint on table '{}'. Incorrect options in FOREIGN KEY constraint '{}'"),
    ],
    ['1826', 'ER_FK_DUP_NAME', _("Duplicate foreign key constraint name '{}'")],
    [
        '1827',
        'ER_PASSWORD_FORMAT',
        _(
            "The password hash doesn't have the expected format. "
            "Check if the correct password algorithm is being used with the PASSWORD() function."
        ),
    ],
    ['1828', 'ER_FK_COLUMN_CANNOT_DROP', _("Cannot drop column '{}': needed in a foreign key constraint '{}'")],
    [
        '1829',
        'ER_FK_COLUMN_CANNOT_DROP_CHILD',
        _("Cannot drop column '{}': needed in a foreign key constraint '{}' of table '{}'"),
    ],
    [
        '1830',
        'ER_FK_COLUMN_NOT_NULL',
        _("Column '{}' cannot be NOT NULL: needed in a foreign key constraint '{}' SET NULL"),
    ],
    [
        '1831',
        'ER_DUP_INDEX',
        _(
            "Duplicate index '{}' defined on the table '{}.{}'. "
            "This is deprecated and will be disallowed in a future release."
        ),
    ],
    ['1832', 'ER_FK_COLUMN_CANNOT_CHANGE', _("Cannot change column '{}': used in a foreign key constraint '{}'")],
    [
        '1833',
        'ER_FK_COLUMN_CANNOT_CHANGE_CHILD',
        _("Cannot change column '{}': used in a foreign key constraint '{}' of table '{}'"),
    ],
    [
        '1834',
        'ER_FK_CANNOT_DELETE_PARENT',
        _("Cannot delete rows from table which is parent in a foreign key constraint '{}' of table '{}'"),
    ],
    ['1835', 'ER_MALFORMED_PACKET', _("Malformed communication packet.")],
    ['1836', 'ER_READ_ONLY_MODE', _("Running in read-only mode")],
    [
        '1837',
        'ER_GTID_NEXT_TYPE_UNDEFINED_GROUP',
        _(
            "When @@SESSION.GTID_NEXT is set to a GTID, "
            "you must explicitly set it to a different value after a COMMIT or ROLLBACK. "
            "Please check GTID_NEXT variable manual page for detailed explanation. Current @@SESSION.GTID_NEXT is '{}'."
        ),
    ],
    ['1838', 'ER_VARIABLE_NOT_SETTABLE_IN_SP', _("The system variable {} cannot be set in stored procedures.")],
    [
        '1839',
        'ER_CANT_SET_GTID_PURGED_WHEN_GTID_MODE_IS_OFF',
        _("@@GLOBAL.GTID_PURGED can only be set when @@GLOBAL.GTID_MODE = ON."),
    ],
    [
        '1840',
        'ER_CANT_SET_GTID_PURGED_WHEN_GTID_EXECUTED_IS_NOT_EMPTY',
        _("@@GLOBAL.GTID_PURGED can only be set when @@GLOBAL.GTID_EXECUTED is empty."),
    ],
    [
        '1841',
        'ER_CANT_SET_GTID_PURGED_WHEN_OWNED_GTIDS_IS_NOT_EMPTY',
        _("@@GLOBAL.GTID_PURGED can only be set when there are no ongoing transactions (not even in other clients)."),
    ],
    ['1842', 'ER_GTID_PURGED_WAS_CHANGED', _("@@GLOBAL.GTID_PURGED was changed from '{}' to '{}'.")],
    ['1843', 'ER_GTID_EXECUTED_WAS_CHANGED', _("@@GLOBAL.GTID_EXECUTED was changed from '{}' to '{}'.")],
    [
        '1844',
        'ER_BINLOG_STMT_MODE_AND_NO_REPL_TABLES',
        _(
            "Cannot execute statement: impossible to write to binary log since BINLOG_FORMAT = STATEMENT, "
            "and both replicated and non replicated tables are written to."
        ),
    ],
    ['1845', 'ER_ALTER_OPERATION_NOT_SUPPORTED', _("{} is not supported for this operation. Try {}.")],
    ['1846', 'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON', _("{} is not supported. Reason: {}. Try {}.")],
    ['1847', 'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COPY', _("COPY algorithm requires a lock")],
    [
        '1848',
        'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_PARTITION',
        _("Partition specific operations do not yet support LOCK/ALGORITHM"),
    ],
    [
        '1849',
        'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_RENAME',
        _("Columns participating in a foreign key are renamed"),
    ],
    ['1850', 'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE', _("Cannot change column type INPLACE")],
    ['1851', 'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FK_CHECK', _("Adding foreign keys needs foreign_key_checks=OFF")],
    [
        '1852',
        'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_IGNORE',
        _("Creating unique indexes with IGNORE requires COPY algorithm to remove duplicate rows"),
    ],
    [
        '1853',
        'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOPK',
        _("Dropping a primary key is not allowed without also adding a new primary key"),
    ],
    ['1854', 'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_AUTOINC', _("Adding an auto-increment column requires a lock")],
    [
        '1855',
        'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_HIDDEN_FTS',
        _("Cannot replace hidden FTS_DOC_ID with a user-visible one"),
    ],
    ['1856', 'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_CHANGE_FTS', _("Cannot drop or rename FTS_DOC_ID")],
    ['1857', 'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_FTS', _("Fulltext index creation requires a lock")],
    [
        '1858',
        'ER_SQL_SLAVE_SKIP_COUNTER_NOT_SETTABLE_IN_GTID_MODE',
        _(
            "sql_slave_skip_counter can not be set when the server is running with @@GLOBAL.GTID_MODE = ON. Instead, "
            "for each transaction that you want to skip, "
            "generate an empty transaction with the same GTID as the transaction"
        ),
    ],
    ['1859', 'ER_DUP_UNKNOWN_IN_INDEX', _("Duplicate entry for key '{}'")],
    [
        '1860',
        'ER_IDENT_CAUSES_TOO_LONG_PATH',
        _("Long database name and identifier for object resulted in path length exceeding {} characters. Path: '{}'."),
    ],
    [
        '1861',
        'ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL',
        _("cannot silently convert NULL values, as required in this SQL_MODE"),
    ],
    [
        '1862',
        'ER_MUST_CHANGE_PASSWORD_LOGIN',
        _("Your password has expired. To log in you must change it using a client that supports expired passwords."),
    ],
    ['1863', 'ER_ROW_IN_WRONG_PARTITION', _("Found a row in wrong partition {}")],
    [
        '1864',
        'ER_MTS_EVENT_BIGGER_PENDING_JOBS_SIZE_MAX',
        _(
            "Cannot schedule event {}, relay-log name {}, "
            "position {} to Worker thread because its size {} exceeds {} of slave_pending_jobs_size_max."
        ),
    ],
    ['1865', 'ER_INNODB_NO_FT_USES_PARSER', _("Cannot CREATE FULLTEXT INDEX WITH PARSER on InnoDB table")],
    ['1866', 'ER_BINLOG_LOGICAL_CORRUPTION', _("The binary log file '{}' is logically corrupted: {}")],
    [
        '1867',
        'ER_WARN_PURGE_LOG_IN_USE',
        _("file {} was not purged because it was being read by {} thread(s), purged only {} out of {} files."),
    ],
    ['1868', 'ER_WARN_PURGE_LOG_IS_ACTIVE', _("file {} was not purged because it is the active log file.")],
    [
        '1869',
        'ER_AUTO_INCREMENT_CONFLICT',
        _("Auto-increment value in UPDATE conflicts with internally generated values"),
    ],
    [
        '1870',
        'WARN_ON_BLOCKHOLE_IN_RBR',
        _("Row events are not logged for {} statements that modify BLACKHOLE tables in row format. Table(s): '{}'"),
    ],
    ['1871', 'ER_SLAVE_MI_INIT_REPOSITORY', _("Slave failed to initialize master info structure from the repository")],
    [
        '1872',
        'ER_SLAVE_RLI_INIT_REPOSITORY',
        _("Slave failed to initialize relay log info structure from the repository"),
    ],
    [
        '1873',
        'ER_ACCESS_DENIED_CHANGE_USER_ERROR',
        _("Access denied trying to change to user '{}'@'{}' (using password: {}). Disconnecting."),
    ],
    ['1874', 'ER_INNODB_READ_ONLY', _("InnoDB is in read only mode.")],
    [
        '1875',
        'ER_STOP_SLAVE_SQL_THREAD_TIMEOUT',
        _(
            "STOP SLAVE command execution is incomplete: Slave SQL thread got the stop signal, thread is busy, "
            "SQL thread will stop once the current task is complete."
        ),
    ],
    [
        '1876',
        'ER_STOP_SLAVE_IO_THREAD_TIMEOUT',
        _(
            "STOP SLAVE command execution is incomplete: Slave IO thread got the stop signal, thread is busy, "
            "IO thread will stop once the current task is complete."
        ),
    ],
    [
        '1877',
        'ER_TABLE_CORRUPT',
        _("Operation cannot be performed. The table '{}.{}' is missing, corrupt or contains bad data."),
    ],
    ['1878', 'ER_TEMP_FILE_WRITE_FAILURE', _("Temporary file write failure.")],
    [
        '1879',
        'ER_INNODB_FT_AUX_NOT_HEX_ID',
        _("Upgrade index name failed, please use create index(alter table) algorithm copy to rebuild index."),
    ],
    [
        '1880',
        'ER_OLD_TEMPORALS_UPGRADED',
        _("TIME/TIMESTAMP/DATETIME columns of old format have been upgraded to the new format."),
    ],
    ['1881', 'ER_INNODB_FORCED_RECOVERY', _("Operation not allowed when innodb_forced_recovery > 0.")],
    [
        '1882',
        'ER_AES_INVALID_IV',
        _("The initialization vector supplied to {} is too short. Must be at least {} bytes long"),
    ],
    ['1883', 'ER_PLUGIN_CANNOT_BE_UNINSTALLED', _("Plugin '{}' cannot be uninstalled now. {}")],
    [
        '1884',
        'ER_GTID_UNSAFE_BINLOG_SPLITTABLE_STATEMENT_AND_GTID_GROUP',
        _(
            "Cannot execute statement because it needs to be written to the binary log as multiple statements,"
            " and this is not allowed when @@SESSION.GTID_NEXT == 'UUID:NUMBER'."
        ),
    ],
    [
        '1885',
        'ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER',
        _(
            "Slave has more GTIDs than the master has, using the master's SERVER_UUID. "
            "This may indicate that the end of the binary log was truncated or that the last binary log file was lost, "
            "e.g., after a power or disk failure when sync_binlog != 1. "
            "The master may or may not have rolled back transactions that were already replicated to the slave. "
            "Suggest to replicate any transactions that master has rolled back from slave to master, "
            "and/or commit empty transactions on master to account for transactions that have been committed on master"
            " but are not included in GTID_EXECUTED."
        ),
    ],
]


class BaseMySQLRawError(MySQLBackendError):
    raw_code = None
    raw_name = None
    raw_msg_template = None
    raw_msg_pattern = None

    def __init__(self, *args, **kwargs):
        self.msg_template = self.raw_msg_template
        super(BaseMySQLRawError, self).__init__(*args, **kwargs)


mysql_raw_errors = {}
metadata_error_codes = MetaDataErrorCodes()
start_code = int(metadata_error_codes.RAW_MYSQL)
for error_code, err_name, raw_msg_template in raw_errors_info:
    error_code = int(error_code)
    full_err_name = 'MySQL{}Error'.format(snake_to_camel(err_name.lower(), apply_to_first=True))
    cls = type(
        str(full_err_name),
        (BaseMySQLRawError,),
        {
            str('raw_code'): error_code,
            str('raw_name'): err_name,
            str('raw_msg_template'): raw_msg_template,
            str('raw_msg_pattern'): parse.compile(raw_msg_template),
            str('code'): start_code,
        },
    )
    start_code += 1
    globals()[str(full_err_name)] = cls
    mysql_raw_errors[error_code] = cls
