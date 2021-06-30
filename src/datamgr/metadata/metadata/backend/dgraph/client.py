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
"""
Dgraph Http Client with transaction support.

Multiply alpha server is supported.
Due to this client based on http protocol, any concurrent lib like Gevent or Eventlet is well supported.
"""


import json
import logging
import random

import requests
from requests.adapters import HTTPAdapter

from .exceptions import (
    DgraphServerError,
    TnxCommittedError,
    TnxReadOnlyError,
    TnxUnInitError,
)


class DgraphClient(object):
    """
    Dgraph Http Client with transaction support.
    """

    mutate_templates = {
        'rdf': {
            'set': """
                {{
                    set {{
                        {mutation_block}
                    }}
                }}
            """,
            'delete': """
                {{
                    delete {{
                        {mutation_block}
                    }}
                }}
            """,
            'upsert': """
                upsert {{
                    query {query_block}
                    mutation {if_conds} {{ {mutation_type} {{
                        {mutation_block}
                    }} }}
                }}
            """,
        },
        'json': {
            'set': '{"set": []}',
            'delete': '{"delete": []}',
            'upsert': '{"query": "", "cond": "", "set": [], "delete": []}',
        },
    }

    def __init__(self, servers=None, polled=False, pool_size=20, pool_maxsize=20 * 2):
        self.servers = ['http://localhost:8080'] if not servers else servers
        if not polled:
            self.connect_session = requests
        else:
            self.connect_session = requests.session()
            self.connect_session.mount('https://', HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_maxsize))
            self.connect_session.mount('http://', HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_maxsize))
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def server(self):
        return random.choice(self.servers)

    @property
    def alter_url(self):
        return self.server + '/alter'

    @property
    def query_url(self):
        return self.server + '/query'

    @property
    def mutate_url(self):
        return self.server + '/mutate'

    @property
    def commit_url(self):
        return self.server + '/commit'

    @property
    def abort_url(self):
        return self.server + '/abort'

    @staticmethod
    def _process_response(r, raise_err=True):
        r.raise_for_status()
        r_info = r.json()
        if 'errors' not in r_info:
            if raise_err:
                return r_info
            else:
                return True, r_info
        else:
            if raise_err:
                raise DgraphServerError(r_info)
            else:
                return False, r_info

    @staticmethod
    def _change_rdf_to_json(rdf_contents):
        """
        change the rdf mutation query into json format
        :param rdf_contents:
        :return: list json formatted query contents list
        """
        json_query_dict = {}
        link_target_dict = {}
        for line in rdf_contents:
            subject, predicate, object_, period = line.strip().split(' ')
            predicate = predicate.strip('<').strip('>') if predicate != '*' else None
            object_ = object_.strip('"') if object_ != '*' else None
            if subject not in json_query_dict:
                json_query_dict[subject] = dict(uid=subject)
                link_target_dict[subject] = dict()
            if predicate:
                if object_ and object_.startswith('uid('):
                    link_target_dict[subject][predicate] = object_
                    object_ = dict(uid=object_)
                json_query_dict[subject][predicate] = object_
        for subject, link_relation in list(link_target_dict.items()):
            for predicate, object_ in list(link_relation.items()):
                if object_ in json_query_dict:
                    json_query_dict[subject][predicate] = json_query_dict[object_]
                    del json_query_dict[object_]
        return list(json_query_dict.values())

    def tnx(self, read_only=False, best_effort=False):
        """
        Start an transaction and make some operation to DB.

        :param read_only: whether the transaction is only for query
        :param best_effort: let the transaction run in 'best effort' mode (see official reference)
        :return: ret
        """
        return DgraphTnx(self, read_only, best_effort)

    def alter(self, statement):
        """
        Execute the alter statement.

        :param statement: alter statement
        :return: ret
        """
        r = self.connect_session.post(self.alter_url, data=statement)
        return self._process_response(r)

    def query(self, statement, read_only=False, best_effort=False, start_ts=None, server=None):
        """
        The raw query implement which is not suggested to use. Use the query method in Tnx.

        :param statement: query statement
        :param read_only: whether the transaction is only for query
        :param best_effort: let the transaction run in 'best effort' mode (see official reference)
        :param start_ts: transaction id
        :param server: dgraph alpha server to use.
        :return: ret
        """

        statement = str(statement).encode('utf-8')
        headers = {'content-type': 'application/graphql+-'}
        params = {}
        # ToCheck: ro / be if still enable
        if read_only:
            params['ro'] = str(True)
            if best_effort:
                params['be'] = str(True)
        # compatible: after version >=1.1.0,startTs is a query param for query/mutate/commit request
        if start_ts:
            params['startTs'] = start_ts
        query_url = self.query_url if not server else server + '/query'
        # Todo: after upgraded, remove the start_ts after url
        url = query_url + '/{}'.format(start_ts) if start_ts else query_url
        r = self.connect_session.post(url, data=statement, params=params, headers=headers)
        return self._process_response(r)

    def mutate(
        self,
        modify_contents,
        action,
        data_format='rdf',
        commit_now=False,
        start_ts=None,
        server=None,
        statement='',
        if_cond='',
    ):
        """
        The raw mutate implement which is not suggested to use. Use the mutate method in Tnx.

        :param modify_contents: modify content
        :param action: action type
        :param data_format: json or rdf
        :param commit_now: whether to commit now
        :param start_ts:  transaction id
        :param server: dgraph alpha server to use
        :param statement: query block
        :param if_cond: the run-mutate-conditions of query result in upsert block
        :return: ret
        """

        modify_contents = [str(item) for item in modify_contents]
        if action not in ('set', 'delete'):
            raise ValueError('Invalid mutate action.')
        if data_format not in ('rdf', 'json'):
            raise ValueError('Invalid mutate format.')
        headers = {}
        params = {}
        full_content = {}
        statement = str(statement)
        template_type = str('upsert') if statement else action
        template = self.mutate_templates[data_format][template_type]
        if data_format == 'rdf':
            headers['content-type'] = 'application/rdf'
            full_content = template.format(
                query_block=statement,
                if_conds=if_cond,
                mutation_type=action,
                mutation_block=str('\n').join(modify_contents),
            )
        elif data_format == 'json':
            headers['content-type'] = 'application/json'
            # Todo: after upgraded, remove header: X-Dgraph-MutationType
            headers["X-Dgraph-MutationType"] = 'json'
            template_obj = json.loads(template)
            template_obj['query'] = statement.replace('"', '\\"')
            template_obj['cond'] = if_cond
            template_obj[action] = self._change_rdf_to_json(modify_contents)
            full_content_obj = (
                {
                    'query': template_obj['query'],
                    'mutations': [
                        {key: block for key, block in template_obj.items if block and key in ('cond', 'set', 'delete')}
                    ],
                }
                if template_type == 'upsert'
                else {key: block for key, block in template_obj.items if block}
            )
            full_content = json.dumps(full_content_obj)
        if commit_now:
            # Todo: after upgraded, remove header: X-Dgraph-CommitNow
            headers["X-Dgraph-CommitNow"] = str(True)
            params['commitNow'] = 'true'
        # compatible: after version >=1.1.0,startTs is a query param for query/mutate/commit request
        if start_ts:
            params['startTs'] = start_ts
        mutate_url = self.mutate_url if not server else server + '/mutate'
        # Todo: after upgraded, remove start_ts after url
        url = mutate_url + '/{}'.format(start_ts) if start_ts else mutate_url
        r = self.connect_session.post(url, data=full_content, params=params, headers=headers)
        return self._process_response(r)

    def commit(self, commit_token=None, start_ts=None, abort=False, server=None, start_ts_on_url=False):
        """
        Execute the commit directive. Use the commit method in Tnx.

        :param commit_token: keys and preds of mutations in Tnx
        :param start_ts:  transaction id
        :param abort: abort the Tnx and rollback all mutations
        :param server: dgraph alpha server to use
        :return: ret or (bool, ret) ret info
        """

        params = {}
        if start_ts:
            params['startTs'] = start_ts
        if abort:
            params['abort'] = 'true'
        # Todo: after upgrade, remove the start_ts after url
        commit_url = self.commit_url if not server else server + '/commit'
        url = commit_url + '/{}'.format(start_ts) if start_ts and start_ts_on_url else commit_url
        r = self.connect_session.post(url, json=commit_token, params=params)
        if abort:
            return self._process_response(r, raise_err=False)
        return self._process_response(r)

    def discard(self, start_ts, server=None):
        """
        compatible the abort command to cancel the Tnx before 1.1.0 (not included), this method will be scrapped soon...
        """
        abort_url = self.abort_url if not server else server + '/abort'
        url = abort_url + '/{}'.format(start_ts)
        r = self.connect_session.post(url)
        return self._process_response(r, raise_err=False)


class DgraphTnx(object):
    """
    Dgraph transaction.
    """

    empty_query = "{node(func: has(_empty_))}"

    def __init__(self, client, read_only=False, best_effort=False):
        """
        Create a dgraph transaction object, but the transaction is not started until invoke the `post_init` method.

        :param client: underlay client used of transaction.
        :param read_only:  whether the transaction is only for query
        :param best_effort: let the transaction run in 'best effort' mode (see official reference)
        """
        self.client = client  # type:DgraphClient
        self.read_only = read_only
        self.best_effort = best_effort
        self.start_ts = None
        self.server = self.client.server
        self.state_keys = set()
        self.preds = set()
        self.committed = False

    def __enter__(self):
        """
        Create and start a transaction use context manager.

        :return:
        """
        self.post_init()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb, start_ts_on_url=True):
        self.discard(start_ts_on_url)

    def post_init(self):
        """
        Start the transaction actually.

        :return:
        """
        if not self.read_only:
            r_info = self.query(self.empty_query)
            start_ts = r_info['extensions']['txn']['start_ts']
        else:
            start_ts = 0
        self.start_ts = start_ts

    def query(self, statement):
        """
        Query db in this transaction.
        """
        return self.client.query(
            statement,
            read_only=self.read_only,
            best_effort=self.best_effort,
            start_ts=self.start_ts,
            server=self.server,
        )

    def mutate(self, modify_contents, action='set', data_format='rdf', commit_now=False, statement='', if_cond=''):
        """
        Mutate the db in this transaction.

        :param modify_contents: modify content
        :param action: set or delete
        :param data_format: json or rdf
        :param commit_now: whether to commit now
        :param statement: query of upsert block
        :param if_cond: the run-mutate-conditions of query result in upsert block
        :return: ret
        """
        if self.read_only:
            raise TnxReadOnlyError("Can't mutate in read only tnx.")
        if not self.start_ts:
            raise TnxUnInitError("Tnx is not inited for transaction.")
        processed_r = self.client.mutate(
            modify_contents,
            action,
            data_format,
            commit_now=commit_now,
            start_ts=self.start_ts,
            server=self.server,
            statement=statement,
            if_cond=if_cond,
        )
        if not commit_now:
            self.state_keys.update(processed_r['extensions']['txn'].get('keys', []))
            self.preds.update(processed_r['extensions']['txn'].get('preds', []))
        else:
            self.committed = True
        return processed_r

    def commit(self, start_ts_on_url=False):
        """
        Commit the mutations in this transaction.
        """
        if self.read_only:
            raise TnxReadOnlyError("Can't commit in read only tnx.")
        if not self.start_ts:
            raise TnxUnInitError("Tnx is not inited for transaction.")
        if self.committed:
            raise TnxCommittedError("It's better to use a new tnx after this one is committed.")
        state_keys = list(self.state_keys)
        preds = list(self.preds)
        commit_json = state_keys if start_ts_on_url else {'keys': state_keys, 'preds': preds}
        processed_r = self.client.commit(
            commit_token=commit_json, start_ts=self.start_ts, server=self.server, start_ts_on_url=start_ts_on_url
        )
        self.committed = True
        self.state_keys.clear()
        self.preds.clear()
        return processed_r

    def discard(self, start_ts_on_url=False):
        """
        Cancel this transaction.
        """
        if self.start_ts is not None and not self.committed:
            if start_ts_on_url:
                status, ret_info = self.client.discard(start_ts=self.start_ts, server=self.server)
            else:
                status, ret_info = self.client.commit(
                    start_ts=self.start_ts, abort=True, server=self.server, start_ts_on_url=start_ts_on_url
                )
            if not status:
                if 'Transaction has been aborted. Please retry.' not in json.dumps(ret_info):
                    raise DgraphServerError(ret_info)
