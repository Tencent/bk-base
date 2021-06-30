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
import re
import sys
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

USE_TWOPHASE = False

sys.path.insert(0, '../../')
from metadata.db_models.meta_service.log_models import Base as LogBase
from metadata.db_models.meta_service.meta_models import Base as MetaBase
from metadata.db_models.meta_service.replica_models import ReplicaBase

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)
logger = logging.getLogger('alembic.env')

# gather section names referring to different
# databases.  These are named "engine1", "engine2"
# in the sample .ini file.
db_names = config.get_main_option('databases')

# add your model's MetaData objects here
# for 'autogenerate' support.  These must be set
# up to hold just those tables targeting a
# particular database. table.tometadata() may be
# helpful here in case a "copy" of
# a MetaData is needed.
# from myapp import mymodel
# target_metadata = {
#       'engine1':mymodel.metadata1,
#       'engine2':mymodel.metadata2
# }
target_metadata = {
    'log_engine': LogBase.metadata,
    'meta_engine': MetaBase.metadata,
    'replica_engine': ReplicaBase.metadata,
}


# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    # for the --sql use case, run migrations for each URL into
    # individual files.

    engines = {}
    for name in re.split(r',\s*', db_names):
        engines[name] = rec = {}
        rec['url'] = context.config.get_section_option(name, "sqlalchemy.url")

    for name, rec in list(engines.items()):
        logger.info("Migrating database %s" % name)
        file_ = "%s.sql" % name
        logger.info("Writing output to %s" % file_)
        with open(file_, 'w') as buffer:
            context.configure(
                url=rec['url'],
                output_buffer=buffer,
                target_metadata=target_metadata.get(name),
                literal_binds=True,
                compare_type=True,
            )
            with context.begin_transaction():
                context.run_migrations(engine_name=name)


def meta_include_symbol(tablename, schema):
    if tablename.startswith("metadata") or tablename.startswith("dm"):
        return True
    else:
        return False


def replica_include_symbol(tablename, schema):
    if not (tablename.startswith("metadata") or tablename.startswith("dm")):
        return True
    else:
        return False


def both_include_symbol(tablename, schema):
    return True


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """

    # for the direct-to-DB use case, start a transaction on all
    # engines, then run all migrations, then commit all transactions.

    engines = {}
    for name in re.split(r',\s*', db_names):
        engines[name] = rec = {}
        rec['engine'] = engine_from_config(
            context.config.get_section(name), prefix='sqlalchemy.', poolclass=pool.NullPool
        )
    try:
        for name, rec in list(engines.items()):
            engine = rec['engine']
            rec['connection'] = conn = engine.connect()

            if USE_TWOPHASE:
                rec['transaction'] = conn.begin_twophase()
            else:
                rec['transaction'] = conn.begin()

            if name == 'meta_engine':
                include_symbol = meta_include_symbol
            elif name == 'replica_engine':
                include_symbol = replica_include_symbol
            else:
                include_symbol = both_include_symbol
            logger.info("Migrating database %s" % name)
            context.configure(
                connection=rec['connection'],
                upgrade_token="%s_upgrades" % name,
                downgrade_token="%s_downgrades" % name,
                target_metadata=target_metadata.get(name),
                include_symbol=include_symbol,
                compare_type=True,
            )
            context.run_migrations(engine_name=name)

            if USE_TWOPHASE:
                rec['transaction'].prepare()

            rec['transaction'].commit()
    except Exception:
        for rec in list(engines.values()):
            t = rec.get('transaction')
            if t:
                t.rollback()
        raise
    finally:
        for rec in list(engines.values()):
            conn = rec.get('connection')
            if conn:
                conn.close()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
