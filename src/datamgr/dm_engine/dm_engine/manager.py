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

import os
from importlib import import_module

import click


def import_command(name):
    module = import_module("dm_engine.commands")
    return getattr(module, name)


@click.command()
@click.option("-d", "--show-detail", "show_detail", required=False)
@click.option("-c", "--task-code", "task_code", required=False)
def list_tasks(**kwargs):
    command = import_command("list_tasks")
    command(**kwargs)


@click.command()
def list_master(**kwargs):
    command = import_command("list_master")
    command(**kwargs)


@click.command()
def list_agent_worker(**kwargs):
    command = import_command("list_agent")
    command(**kwargs)


@click.command()
def list_worker(**kwargs):
    command = import_command("list_worker")
    command(**kwargs)


@click.command()
def read_queue_status(**kwargs):
    command = import_command("read_queue_status")
    command(**kwargs)


@click.command()
def list_plan_tasks(**kwargs):
    command = import_command("list_plan_tasks")
    command(**kwargs)


@click.command()
@click.argument("task_code")
def run_task_local(**kwargs):
    command = import_command("run_task_local")
    command(**kwargs)


@click.command()
@click.argument("task_code")
def push_task(**kwargs):
    command = import_command("push_task")
    command(**kwargs)


@click.command()
def clear_status(**kwargs):
    command = import_command("clear_status")
    command(**kwargs)


@click.command()
@click.argument("task_code")
def generate_task_sql(**kwargs):
    command = import_command("generate_task_sql")
    command(**kwargs)


@click.command()
def run_master():
    from dm_engine.master.svr import MasterSvr

    svr = MasterSvr()
    svr.start()


@click.command()
def run_agent_worker():
    from dm_engine.agent.svr import AgentSvr

    svr = AgentSvr()
    svr.start()


@click.command()
@click.option("-p", "--port", "port", required=True, type=int)
@click.option("-a", "--addr", "addr", required=False)
def run_metrics_server(port, addr):
    from dm_engine.monitor.app import start_wsgi_server

    params = {"port": port}
    if addr:
        params["addr"] = addr

    start_wsgi_server(**params)


@click.group()
@click.option("-s", "--settings")
@click.option("-t", "--tasksdir")
def cli(settings, tasksdir):
    from dm_engine.runtime import g_context

    # INIT necessary environment variables
    if os.environ.get("PROMETHEUS_MULTIPROC_DIR") is None:
        os.environ["PROMETHEUS_MULTIPROC_DIR"] = "multiproc-metrics"

    if settings is not None:
        g_context.update("settings", settings)
    if tasksdir is not None:
        g_context.update("tasksdir", tasksdir)

    # Check whether the directory of metrics is exist...
    metrics_dir = "./multiproc-metrics"
    os.makedirs(metrics_dir, exist_ok=True)


cli.add_command(run_master)
cli.add_command(run_agent_worker)
cli.add_command(list_tasks)
cli.add_command(list_master)
cli.add_command(list_agent_worker)
cli.add_command(list_worker)
cli.add_command(read_queue_status)
cli.add_command(list_plan_tasks)
cli.add_command(run_task_local)
cli.add_command(push_task)
cli.add_command(clear_status)
cli.add_command(generate_task_sql)
cli.add_command(run_metrics_server)


if __name__ == "__main__":
    cli()
