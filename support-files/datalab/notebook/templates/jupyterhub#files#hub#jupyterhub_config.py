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
import re
import sys

from tornado.httpclient import AsyncHTTPClient
from kubernetes import client
from jupyterhub.utils import url_path_join

configuration_directory = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, configuration_directory)

from z2jh import get_config, set_config_if_not_none

# Configure JupyterHub to use the curl backend for making HTTP requests,
# rather than the pure-python implementations. The default one starts
# being too slow to make a large number of requests to the proxy API
# at the rate required.
AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient")

c.ConfigurableHTTPProxy.api_url = 'http://{}:{}'.format(os.environ['PROXY_API_SERVICE_HOST'],
                                                        int(os.environ['PROXY_API_SERVICE_PORT']))
c.ConfigurableHTTPProxy.should_start = False

c.JupyterHub.cleanup_servers = False
c.JupyterHub.last_activity_interval = 60
c.JupyterHub.spawner_class = 'kubespawner.KubeSpawner'
c.JupyterHub.tornado_settings = {
    'headers': {
        'Content-Security-Policy': "frame-ancestors self http://localhost:8380 http://127.0.0.1:8380; report-uri "
                                   "/api/security/csp-report "
    }
}
c.JupyterHub.allow_named_servers = True

notebook_dir = '/home/datalab/notebooks/{username}'
c.KubeSpawner.notebook_dir = notebook_dir
c.KubeSpawner.volumes = [
    {'name': 'notebook', 'hostPath': {'path': '/data/datalab/user_notebook/{username}', 'type': "DirectoryOrCreate"}}
]
c.KubeSpawner.volume_mounts = [{'name': 'notebook', 'mountPath': '/home/datalab/notebooks/{username}'}]
c.KubeSpawner.args = [
    '--allow-root',
    '--NotebookApp.tornado_settings={"headers":{"Content-Security-Policy": "frame-ancestors * self '
    'http://localhost:8380 http://127.0.0.1:8380; report-uri /api/security/csp-report"}}',
]

c.Authenticator.auto_login = False


def camelCaseify(s):
    """convert snake_case to camelCase

    For the common case where some_value is set from someValue
    so we don't have to specify the name twice.
    """
    return re.sub(r"_([a-z])", lambda m: m.group(1).upper(), s)


# configure the hub db connection
db_type = get_config('hub.db.type')
if db_type == 'sqlite-pvc':
    c.JupyterHub.db_url = "sqlite:///jupyterhub.sqlite"
elif db_type == "sqlite-memory":
    c.JupyterHub.db_url = "sqlite://"
elif db_type == "mysql": 
    c.JupyterHub.db_url = db_url
else:
    set_config_if_not_none(c.JupyterHub, "db_url", "hub.db.url")


for trait, cfg_key in (
    # Max number of servers that can be spawning at any one time
    ('concurrent_spawn_limit', None),
    # Max number of servers to be running at one time
    ('active_server_limit', None),
    # base url prefix
    ('base_url', None),
    ('allow_named_servers', None),
    ('named_server_limit_per_user', None),
    ('authenticate_prometheus', None),
    ('redirect_to_server', None),
    ('shutdown_on_logout', None),
    ('template_paths', None),
    ('template_vars', None),
):
    if cfg_key is None:
        cfg_key = camelCaseify(trait)
    set_config_if_not_none(c.JupyterHub, trait, 'hub.' + cfg_key)

c.JupyterHub.ip = os.environ['PROXY_PUBLIC_SERVICE_HOST']
c.JupyterHub.port = int(os.environ['PROXY_PUBLIC_SERVICE_PORT'])

# the hub should listen on all interfaces, so the proxy can access it
c.JupyterHub.hub_ip = '0.0.0.0'

# implement common labels
# this duplicates the jupyterhub.commonLabels helper
common_labels = c.KubeSpawner.common_labels = {}
common_labels['app'] = get_config(
    "nameOverride",
    default=get_config("Chart.Name", "jupyterhub"),
)
common_labels['heritage'] = "jupyterhub"
chart_name = get_config('Chart.Name')
chart_version = get_config('Chart.Version')
if chart_name and chart_version:
    common_labels['chart'] = "{}-{}".format(
        chart_name, chart_version.replace('+', '_'),
    )
release = get_config('Release.Name')
if release:
    common_labels['release'] = release

c.KubeSpawner.namespace = os.environ.get('POD_NAMESPACE', 'default')

# Max number of consecutive failures before the Hub restarts itself
# requires jupyterhub 0.9.2
set_config_if_not_none(
    c.Spawner,
    'consecutive_failure_limit',
    'hub.consecutiveFailureLimit',
)

for trait, cfg_key in (
    ('start_timeout', None),
    ('image_pull_policy', 'image.pullPolicy'),
    ('events_enabled', 'events'),
    ('extra_labels', None),
    ('extra_annotations', None),
    ('uid', None),
    ('fs_gid', None),
    ('service_account', 'serviceAccountName'),
    ('storage_extra_labels', 'storage.extraLabels'),
    ('tolerations', 'extraTolerations'),
    ('node_selector', None),
    ('node_affinity_required', 'extraNodeAffinity.required'),
    ('node_affinity_preferred', 'extraNodeAffinity.preferred'),
    ('pod_affinity_required', 'extraPodAffinity.required'),
    ('pod_affinity_preferred', 'extraPodAffinity.preferred'),
    ('pod_anti_affinity_required', 'extraPodAntiAffinity.required'),
    ('pod_anti_affinity_preferred', 'extraPodAntiAffinity.preferred'),
    ('lifecycle_hooks', None),
    ('init_containers', None),
    ('extra_containers', None),
    ('mem_limit', 'memory.limit'),
    ('mem_guarantee', 'memory.guarantee'),
    ('cpu_limit', 'cpu.limit'),
    ('cpu_guarantee', 'cpu.guarantee'),
    ('extra_resource_limits', 'extraResource.limits'),
    ('extra_resource_guarantees', 'extraResource.guarantees'),
    ('environment', 'extraEnv'),
    ('profile_list', None),
    ('extra_pod_config', None),
):
    if cfg_key is None:
        cfg_key = camelCaseify(trait)
    set_config_if_not_none(c.KubeSpawner, trait, 'singleuser.' + cfg_key)

image = get_config("singleuser.image.name")
if image:
    tag = get_config("singleuser.image.tag")
    if tag:
        image = "{}:{}".format(image, tag)

    c.KubeSpawner.image = image

if get_config('singleuser.imagePullSecret.enabled'):
    c.KubeSpawner.image_pull_secrets = 'singleuser-image-credentials'

# scheduling:
if get_config('scheduling.userScheduler.enabled'):
    c.KubeSpawner.scheduler_name = os.environ['HELM_RELEASE_NAME'] + "-user-scheduler"
if get_config('scheduling.podPriority.enabled'):
    c.KubeSpawner.priority_class_name = os.environ['HELM_RELEASE_NAME'] + "-default-priority"

# add node-purpose affinity
match_node_purpose = get_config('scheduling.userPods.nodeAffinity.matchNodePurpose')
if match_node_purpose:
    node_selector = dict(
        matchExpressions=[
            dict(
                key="hub.jupyter.org/node-purpose",
                operator="In",
                values=["user"],
            )
        ],
    )
    if match_node_purpose == 'prefer':
        c.KubeSpawner.node_affinity_preferred.append(
            dict(
                weight=100,
                preference=node_selector,
            ),
        )
    elif match_node_purpose == 'require':
        c.KubeSpawner.node_affinity_required.append(node_selector)
    elif match_node_purpose == 'ignore':
        pass
    else:
        raise ValueError("Unrecognized value for matchNodePurpose: %r" % match_node_purpose)

# add dedicated-node toleration
for key in (
    'hub.jupyter.org/dedicated',
    # workaround GKE not supporting / in initial node taints
    'hub.jupyter.org_dedicated',
):
    c.KubeSpawner.tolerations.append(
        dict(
            key=key,
            operator='Equal',
            value='user',
            effect='NoSchedule',
        )
    )


# Gives spawned containers access to the API of the hub
c.JupyterHub.hub_connect_ip = os.environ['HUB_SERVICE_HOST']
c.JupyterHub.hub_connect_port = int(os.environ['HUB_SERVICE_PORT'])

# Allow switching authenticators easily
auth_type = get_config('auth.type')
email_domain = 'local'

common_oauth_traits = (
        ('client_id', None),
        ('client_secret', None),
        ('oauth_callback_url', 'callbackUrl'),
)

c.JupyterHub.authenticator_class = 'jupyterhub.auth.DummyAuthenticator'
set_config_if_not_none(c.DummyAuthenticator, 'password', 'auth.dummy.password')

set_config_if_not_none(c.OAuthenticator, 'scope', 'auth.scopes')

set_config_if_not_none(c.Authenticator, 'enable_auth_state', 'auth.state.enabled')

# Enable admins to access user servers
set_config_if_not_none(c.JupyterHub, 'admin_access', 'auth.admin.access')
set_config_if_not_none(c.Authenticator, 'admin_users', 'auth.admin.users')
set_config_if_not_none(c.Authenticator, 'whitelist', 'auth.whitelist.users')

c.JupyterHub.services = []

if get_config('cull.enabled', False):
    cull_cmd = [
        'python3',
        '/etc/jupyterhub/cull_idle_servers.py',
    ]
    base_url = c.JupyterHub.get('base_url', '/')
    cull_cmd.append(
        '--url=http://127.0.0.1:8081' + url_path_join(base_url, 'hub/api')
    )

    cull_timeout = get_config('cull.timeout')
    if cull_timeout:
        cull_cmd.append('--timeout=%s' % cull_timeout)

    cull_every = get_config('cull.every')
    if cull_every:
        cull_cmd.append('--cull-every=%s' % cull_every)

    cull_concurrency = get_config('cull.concurrency')
    if cull_concurrency:
        cull_cmd.append('--concurrency=%s' % cull_concurrency)

    if get_config('cull.users'):
        cull_cmd.append('--cull-users')

    cull_max_age = get_config('cull.maxAge')
    if cull_max_age:
        cull_cmd.append('--max-age=%s' % cull_max_age)

    c.JupyterHub.services.append({
        'name': 'cull-idle',
        'admin': True,
        'command': cull_cmd,
    })

for name, service in get_config('hub.services', {}).items():
    # jupyterhub.services is a list of dicts, but
    # in the helm chart it is a dict of dicts for easier merged-config
    service.setdefault('name', name)
    # handle camelCase->snake_case of api_token
    api_token = service.pop('apiToken', None)
    if api_token:
        service['api_token'] = api_token
    c.JupyterHub.services.append(service)


set_config_if_not_none(c.Spawner, 'cmd', 'singleuser.cmd')
set_config_if_not_none(c.Spawner, 'default_url', 'singleuser.defaultUrl')

cloud_metadata = get_config('singleuser.cloudMetadata', {})

if not cloud_metadata.get('enabled', False):
    # Use iptables to block access to cloud metadata by default
    network_tools_image_name = get_config('singleuser.networkTools.image.name')
    network_tools_image_tag = get_config('singleuser.networkTools.image.tag')
    ip_block_container = client.V1Container(
        name="block-cloud-metadata",
        image=f"{network_tools_image_name}:{network_tools_image_tag}",
        command=[
            'iptables',
            '-A', 'OUTPUT',
            '-d', cloud_metadata.get('ip', '169.254.169.254'),
            '-j', 'DROP'
        ],
        security_context=client.V1SecurityContext(
            privileged=True,
            run_as_user=0,
            capabilities=client.V1Capabilities(add=['NET_ADMIN'])
        )
    )

    c.KubeSpawner.init_containers.append(ip_block_container)


if get_config('debug.enabled', False):
    c.JupyterHub.log_level = 'DEBUG'
    c.Spawner.debug = True


extra_config = get_config('hub.extraConfig', {})
if isinstance(extra_config, str):
    from textwrap import indent, dedent
    msg = dedent(
    """
    hub.extraConfig should be a dict of strings,
    but found a single string instead.

    extraConfig as a single string is deprecated
    as of the jupyterhub chart version 0.6.

    The keys can be anything identifying the
    block of extra configuration.

    Try this instead:

        hub:
          extraConfig:
            myConfig: |
              {}

    This configuration will still be loaded,
    but you are encouraged to adopt the nested form
    which enables easier merging of multiple extra configurations.
    """
    )
    print(
        msg.format(
            indent(extra_config, ' ' * 10).lstrip()
        ),
        file=sys.stderr
    )
    extra_config = {'deprecated string': extra_config}

for key, config_py in sorted(extra_config.items()):
    print("Loading extra config: %s" % key)
    exec(config_py)
