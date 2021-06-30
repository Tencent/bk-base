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
import time
from codecs import open

import click
from jsonext import dumps
from metadata.backend.dgraph.backend import DgraphBackend
from metadata.backend.dgraph.client import DgraphClient
from metadata.db_models.meta_service import replica_models_collection
from metadata.runtime import rt_context
from metadata.state.state import State, StateMode
from metadata.support import join_service_party
from metadata.type_system.expression import (
    generate_def_expressions_doc,
    generate_expression,
)
from metadata.util.i18n import lazy_selfish as _
from metadata_biz.db_models.bkdata import managed_models_collection
from metadata_biz.types import default_registry as dgraph_models_collection

module_logger = logging.getLogger(__name__)


@click.command("show-defs-diff")
@click.option(
    "-b",
    "--layer_backend_type",
    default="dgraph",
    type=click.Choice(("dgraph", "dgraph_backup")),
    help=_("Layer backend type in backend ha mode."),
)
def show_defs_diff(layer_backend_type):
    """
    展示local和dgraph的defs差异
    :return:
    """
    if rt_context.config_collection.normal_config.SERVICE_HA and rt_context.config_collection.normal_config.BACKEND_HA:
        backend_var = "dgraph_backend" if layer_backend_type == "dgraph" else "dgraph_backup_backend"
        join_service_party(True)
        time.sleep(5)
        backend = getattr(rt_context, backend_var)
    else:
        backend = DgraphBackend(rt_context.m_resource.dgraph_session)
    diffs = backend.gen_types_diff()
    if diffs:
        click.echo("add defs:\n {}\n".format(diffs.get("add", set())))
        click.echo("delete defs:\n {}\n".format(diffs.get("delete", set())))
    else:
        click.echo("local defs is None, tha is weird ...")


@click.command("declare-defs")
@click.option(
    "-b",
    "--layer_backend_type",
    default="dgraph",
    type=click.Choice(("dgraph", "dgraph_backup")),
    help=_("Layer backend type in backend ha mode."),
)
def declare_defs(layer_backend_type):
    """
    Todo:整理下backend_type和相关使用；整理下State。
    :param layer_backend_type:
    :return:
    """
    if rt_context.config_collection.normal_config.SERVICE_HA and rt_context.config_collection.normal_config.BACKEND_HA:
        backend_var = "dgraph_backend" if layer_backend_type == "dgraph" else "dgraph_backup_backend"
        join_service_party(True)
        time.sleep(5)
        db = getattr(rt_context, backend_var)
    else:
        join_service_party(True)
        time.sleep(5)
        db = DgraphBackend(rt_context.m_resource.dgraph_session)
    db.gen_types(list(rt_context.md_types_registry.values()))
    db.declare_types()


@click.command()
def export_def_expressions():
    full_expressions = {}
    for md_name, md_type in rt_context.md_types_registry.items():
        full_expressions[md_name] = generate_expression(md_type)
    click.echo(
        dumps(full_expressions, ensure_ascii=False, indent=10),
    )
    return full_expressions


@click.command()
def export_def_expressions_doc():
    full_docs = generate_def_expressions_doc()
    with open("docs/metadata_types.rst", "w", encoding="utf8") as fw:
        fw.write("metadata types\n===============")
        for doc in full_docs:
            fw.write(doc + "\n")


@click.command()
def aspire():
    click.echo("Don't play mediocrity, but play inspired.")


@click.command()
def set_init_data():
    # 动态获取dgraph集群
    system_state = State("/metadata", StateMode.ONLINE)
    system_state.syncing()
    dgraph_conf_mapping = system_state["backend_configs"].get("dgraph", {})
    click.echo("mapping: {}\n".format(dgraph_conf_mapping))

    # tag blank数据
    init_set_dict = {
        "{getTagRoot(func: eq(Tag.id, 0)) {uid Tag.id Tag.code}}": [
            '_:Tag.blank <Tag.code> "blank" .',
            '_:Tag.blank <Tag.id> "0" .',
        ]
    }
    for env_id, dgraph_conf in list(dgraph_conf_mapping.items()):
        if dgraph_conf["ha_state"] != "master" and not rt_context.config_collection.normal_config.BACKEND_HA:
            continue
        click.echo("init_env: {}: {}\n".format(env_id, dgraph_conf["config"]["SERVERS"]))
        for query_statement, init_set_lst in list(init_set_dict.items()):
            click.echo("now init_data: {}\n".format(init_set_lst))
            dgraph_client = DgraphClient(servers=dgraph_conf["config"]["SERVERS"])
            qr = dgraph_client.query(query_statement)
            if len(qr["data"]["getTagRoot"]) == 0:
                r = dgraph_client.mutate(init_set_lst, action="set", data_format="rdf", commit_now=True)
                click.echo("init_res: {}\n".format(r))
            else:
                click.echo("init_data has been registered\n")


@click.command()
@click.option("-m", "--mode", help=_("show mode"), default="all")
def show_managed_models(mode):
    managed_models = dict(same=list(), diff=dict(replica=list(), dgraph=list()))
    biz_md_names = {md_name: None for md_name in managed_models_collection.keys()}
    replica_md_names = {md_name: None for md_name in replica_models_collection.keys()}
    dgraph_md_names = {md_name: None for md_name in dgraph_models_collection.keys()}
    for md_name in biz_md_names:
        if md_name in replica_md_names and md_name in dgraph_md_names:
            managed_models["same"].append(md_name)
            del replica_md_names[md_name]
            del dgraph_md_names[md_name]
            continue
        if md_name not in replica_md_names:
            managed_models["diff"]["replica"].append("{} {}".format("-", md_name))
        else:
            del replica_md_names[md_name]
        if md_name not in dgraph_md_names:
            managed_models["diff"]["dgraph"].append("{} {}".format("-", md_name))
        else:
            del dgraph_md_names[md_name]
    if replica_md_names:
        for md_name in replica_md_names:
            managed_models["diff"]["replica"].append("{} {}".format("+", md_name))
    if dgraph_md_names:
        for md_name in dgraph_md_names:
            managed_models["diff"]["dgraph"].append("{} {}".format("+", md_name))
    if mode != "all":
        click.echo("show models {}: {}\n".format(mode, dumps(managed_models.get(mode, {}))))
    else:
        click.echo("show models: {}\n".format(dumps(managed_models)))


@click.group()
def cli():
    pass


cli.add_command(show_defs_diff)
cli.add_command(declare_defs)
cli.add_command(aspire)
cli.add_command(export_def_expressions)
cli.add_command(export_def_expressions_doc)
cli.add_command(set_init_data)
cli.add_command(show_managed_models)
