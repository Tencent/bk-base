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
import random
import sys
from socket import AF_INET, SO_REUSEADDR, SOL_SOCKET
from socket import error as socket_error
from socket import socket
from uuid import uuid4

import gevent
import tblib
from about_time import about_time
from gevent.pywsgi import WSGIHandler, WSGIServer
from pymysql import MySQLError
from sqlalchemy.exc import DBAPIError, IntegrityError
from tinyrpc import MethodNotFoundError, ServerError
from tinyrpc.dispatch import RPCDispatcher
from tinyrpc.protocols.jsonrpc import JSONRPCErrorResponse, JSONRPCSuccessResponse
from tinyrpc.server.gevent import RPCServerGreenlets
from tinyrpc.transports.wsgi import WsgiServerTransport

from metadata.backend.interface import BackendType
from metadata.backend.mysql.errors import mysql_raw_errors
from metadata.exc import DispatchBackendError, UnhealthyFilterRequest
from metadata.runtime import rt_g, rt_local, rt_local_manager
from metadata.service.access.token import TokenManager
from metadata.util.i18n import selfish as _
from metadata.util.wsgi import WSGIHttpBasicAuth

module_logger = logging.getLogger(__name__)


class WsgiHandlerWithTimeout(WSGIHandler):
    def read_requestline(self):
        with gevent.Timeout(120):
            super(WsgiHandlerWithTimeout, self).read_requestline()


def tcp_listener(address, backlog=1024, reuse_addr=None, reuse_port=None, family=AF_INET):
    """A shortcut to create a TCP socket, bind it and put it into listening state."""
    sock = socket(family=family)
    if reuse_addr is not None:
        sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, int(reuse_addr))
    if reuse_port is not None:
        # WARNING!!: SO_REUSEPORT/reuse_port feature should only be enabled on system versions higher than Linux 3.9
        int_reuse_port_flag = 15
        sock.setsockopt(SOL_SOCKET, int_reuse_port_flag, int(reuse_port))
    try:
        sock.bind(address)
    except socket_error as ex:
        strerror = getattr(ex, 'strerror', None)
        if strerror is not None:
            ex.strerror = strerror + ': ' + repr(address)
        raise
    sock.listen(backlog)
    sock.setblocking(False)
    return sock


class WSGIServerForRPC(WSGIServer):
    pass


class EnhancedWsgiServerTransport(WsgiServerTransport):
    """
    添加了Http Basic Auth验证的Server协议。
    Todo: RPC客户端添加Auth后打开。
    """

    def __init__(self, *args, **kwargs):
        auth_info = kwargs.pop(str('auth_info'), [])
        super(EnhancedWsgiServerTransport, self).__init__(*args, **kwargs)
        self.auth_middleware = WSGIHttpBasicAuth(auth_info)

    def handle(self, environ, start_response):
        authed_handler = super(EnhancedWsgiServerTransport, self).handle
        return authed_handler(environ, start_response)


class EnhancedRPCServer(RPCServerGreenlets):
    def __init__(self, transport, protocol, dispatcher, spawn_func=None):
        self.spawn_func = gevent.spawn if not spawn_func else spawn_func
        super(EnhancedRPCServer, self).__init__(transport, protocol, dispatcher)

    def _spawn(self, func, *args, **kwargs):
        if callable(self.spawn_func):
            self.spawn_func(func, *args, **kwargs)
        else:
            super(EnhancedRPCServer, self)._spawn(func, *args, **kwargs)


class EnhancedRPCDispatcher(RPCDispatcher):
    """
    添加了local自动注入的dispatcher。
    """

    def __init__(self, *args, **kwargs):
        self.local_manager = kwargs.pop(str('local_manager'), rt_local_manager)
        self.local = kwargs.pop(str('local'), rt_local)
        super(EnhancedRPCDispatcher, self).__init__()
        self.config_collection = rt_g.config_collection
        self.db_conf = self.config_collection.db_config
        self.normal_conf = self.config_collection.normal_config
        self.service_switches = None

    @staticmethod
    def set_request_state(request, state):
        """
        设置请求状态机状态：
               -> dispatched -> success / fail (final)
        access -> shunted (final)
               -> filtered (final)
        unknown (异常状态)

        :param request: 请求实体
        :param state: 当前设置状态
        :return: None
        """
        final_state_set = {'shunted', 'filtered', 'success', 'fail'}
        now_state = getattr(request, str('r_state'), 'unknown')
        if now_state not in final_state_set:
            setattr(request, str('r_state'), state)

    def _dispatch(self, request, caller):
        system_state = getattr(rt_g, str('system_state'), {})
        try:
            self.service_switches = system_state.cache['service_switches']
        except KeyError:
            module_logger.warning('service_switches is not existed in zk cache')
        finally:
            self.service_switches = (
                self.normal_conf.SERVICE_SWITCHES if self.service_switches is None else self.service_switches
            )
        self.local.service_switches = self.service_switches
        self.local.dispatch_id = str(uuid4())

        module_logger.info(
            'Executing RPC Call {} with arg {}, kwargs {}. The id is {}'.format(
                request.method, request.args, request.kwargs, self.local.dispatch_id
            )
        )
        try:
            self.set_request_state(request, 'access')
            with about_time() as t:
                language = request.kwargs.pop(str('rpc_extra'), {}).pop(str('language'), 'zh_CN')
                self.local.language = language if language != 'zh-hans' else 'zh_Hans'
                ret = self._enhanced_dispatch(request, caller)
            during_time = t.duration
            self.set_request_state(request, 'success')
            module_logger.info(
                _('Executed RPC Call {} in {} with r_state {}, arg {}, kwargs {}. The id is {}.').format(
                    request.method, during_time, request.r_state, request.args, request.kwargs, self.local.dispatch_id
                )
            )
            module_logger.debug(
                _('Executed RPC Call {} in {} with r_state {}, arg {}, kwargs {}. The result is {}, id is {}.').format(
                    request.method,
                    during_time,
                    request.r_state,
                    request.args,
                    request.kwargs,
                    ret.serialize(),
                    self.local.dispatch_id,
                )
            )
            if isinstance(ret, JSONRPCSuccessResponse):
                module_logger.info(
                    {
                        'rpc_call': request.method,
                        'call_args': request.args,
                        'call_kwargs': request.kwargs,
                        'elapsed_time': during_time,
                        'worker_tag': rt_g.worker_tag,
                        'metric_type': 'rpc_call',
                        'result_status': 'success',
                        'dispatch_id': self.local.dispatch_id,
                    },
                    extra={'metric': True},
                )
            elif isinstance(ret, JSONRPCErrorResponse):
                module_logger.info(
                    {
                        'rpc_call': request.method,
                        'call_args': request.args,
                        'call_kwargs': request.kwargs,
                        'elapsed_time': during_time,
                        'worker_tag': rt_g.worker_tag,
                        'metric_type': 'rpc_call',
                        'result_status': 'fail',
                        'dispatch_id': self.local.dispatch_id,
                    },
                    extra={'metric': True},
                )
            return ret
        except Exception:
            self.set_request_state(request, 'fail')
            module_logger.exception(
                _('Error in internal server. The rpc id is {}').format(self.local.dispatch_id),
                {'interrupt_error': True},
            )
            e = ServerError()
            e = self.error_enhance(e)
            return request.error_respond(e)
        finally:
            session_dct = getattr(self.local, 'session_dct', {})
            for session in session_dct.values():
                if hasattr(session, 'close'):
                    session.close()
            self.local_manager.cleanup()

    def _enhanced_dispatch(self, request, caller):
        try:
            # 分流策略
            shunt_switch = self.service_switches.get(str('shunt'), {})
            if shunt_switch.get('master', False):
                self.dispatch_backend(request)
            # 过滤策略
            filter_switch = self.service_switches.get(str('filter'), {})
            if filter_switch.get('master', False):
                self.filter_if_not_healthy(request)
            # 清理请求不需要的参数
            self.clean_request_params(request)
            # 兼容逻辑, 下次发布去掉
            if 'affect_backup_only' in request.kwargs:
                del request.kwargs['affect_backup_only']

            method = self.get_method(request.method)
            self.set_request_state(request, 'dispatched')
            if caller is not None:
                result = caller(method, request.args, request.kwargs)
            else:
                result = method(*request.args, **request.kwargs)
        except UnhealthyFilterRequest as ue:
            # 服务不稳定,过滤部分请求
            self.set_request_state(request, 'filtered')
            self.error_enhance(ue)
            module_logger.exception(_('service is unhealthy. The rpc id is {}').format(self.local.dispatch_id))
            return request.error_respond(ue)
        except MethodNotFoundError as me:
            self.set_request_state(request, 'fail')
            self.error_enhance(me)
            return request.error_respond(me)
        except Exception as e:
            self.set_request_state(request, 'fail')
            extra = {'interrupt_error': not self.err_report_filter(e)}
            e = self.error_enhance(e)
            module_logger.exception(
                _('Error in call method. The rpc id is {}').format(self.local.dispatch_id), extra=extra
            )
            return request.error_respond(e)

        return request.respond(result)

    def filter_if_not_healthy(self, request):
        # 非dgraph流量暂时不做屏蔽
        backend_type = request.kwargs.get(str('backend_type'), BackendType.DGRAPH.value)
        if backend_type != BackendType.DGRAPH.value:
            return
        filter_rate_set = set()
        filter_switch = self.service_switches.get(str('filter'), {})

        # dgraph 后端检查
        if filter_switch.get('backend_health', False):
            system_state = getattr(rt_g, str('system_state'), {})
            try:
                online_warning_config = system_state.cache['backend_warning']
            except KeyError:
                online_warning_config = {}
                module_logger.warning('backend_warning is not existed in zk cache')

            if online_warning_config and isinstance(online_warning_config, dict):
                off_srv_cnt = len(online_warning_config.get('OFF_SERVERS', []))
                on_srv_cnt = len(online_warning_config.get('SERVERS', []))
                # 只挂一台暂时不处理
                if off_srv_cnt > 1:
                    r = round((100 * off_srv_cnt) / (2 * (off_srv_cnt + on_srv_cnt)), 2)
                    filter_rate_set.add(r)

        # service负载检查
        if filter_switch.get('service_load', False):
            system_state = getattr(rt_g, str('system_state'), {})
            try:
                service_load_info = system_state.cache['service_load_info']
            except KeyError:
                service_load_info = {}
                module_logger.warning('service_load_info is not existed in zk cache')

            if service_load_info.get('rules', {}):
                rules_dict = service_load_info['rules']
                # 针对读写分别做过滤控制
                for query_type, rules_item in list(rules_dict.items()):
                    if request.method in self.normal_conf.SUMMARIZE_METHOD[query_type]:
                        hit_item = max(list(rules_item.values()), key=lambda item: item.get('rate', 0))
                        filter_rate_set.add(hit_item['rate'])

        # TODO: 可添加针对读写和优先级的过滤控制
        # 控制比例进行过滤[0-100]
        if filter_rate_set and random.uniform(0, 100) < min(max(filter_rate_set), 50):
            module_logger.warning(
                'service_load is heavy, {} is filtered with max of {} but limited by 50%'.format(
                    request.method, filter_rate_set
                )
            )
            raise UnhealthyFilterRequest()

    def dispatch_backend(self, request):
        """
        检查token和读写类型，进行分流处理

        :param request: dict 请求参数
        :return: None
        """
        try:
            # 非dgraph流量暂时不做分流
            backend_type = request.kwargs.get(str('backend_type'), BackendType.DGRAPH.value)
            if backend_type != BackendType.DGRAPH.value:
                return

            t_key = request.kwargs.get(str('token_pkey'), request.method)
            t_key = t_key.strip(self.normal_conf.TOKEN_FILLING_CHR)
            t_val = request.kwargs.get(str('token_msg'), None)
            # t_level = request.kwargs.get(str('token_level'), self.normal_conf.TOKEN_LEVEL_NONE)
            shunt = False
            # 限定主集群流量
            if t_key in self.normal_conf.SHUNT_TOKEN_CONFIG.get(str('master'), []):
                return
            # 限定备集群流量
            if t_key in self.normal_conf.SHUNT_TOKEN_CONFIG.get(str('slave'), []):
                shunt = True
            # 灰度使用
            elif t_key in self.normal_conf.SHUNT_TOKEN_CONFIG.get(str('check'), []):
                tm = TokenManager(t_key)
                shunt = tm.check_shunt(t_val)
            # 分流备集群
            if shunt:
                request.kwargs.update(backend_type=BackendType.DGRAPH_BACKUP.value)
                self.set_request_state(request, 'shunted')
        except Exception as de:
            module_logger.exception(_('occur an error when dispatch backend. msg is {}.').format(de))
            raise DispatchBackendError()

    def error_enhance(self, e):
        try:
            if isinstance(e, DBAPIError) and isinstance(getattr(e, str('orig'), None), MySQLError):
                error_code = e.orig.args[0]
                error_msg = e.orig.args[1]
                error_type = mysql_raw_errors.get(error_code, None)
                if error_type:
                    r = error_type.raw_msg_pattern.parse(error_msg)
                    if r:
                        e = error_type(*r.fixed)
        except Exception:
            pass
        data = getattr(e, str('data'), {})
        data['dispatch_id'] = self.local.dispatch_id
        tb_type, tb_value, tb_now = sys.exc_info()
        if tb_now:
            data['tb'] = tblib.Traceback(tb_now).to_dict()
        if tb_value:
            data['exe_msg'] = tb_value
        e.data = data
        return e

    @staticmethod
    def clean_request_params(request):
        del_list = ['token_pkey', 'token_msg', 'token_level']
        for key in del_list:
            if key in request.kwargs:
                del request.kwargs[key]

    @staticmethod
    def err_report_filter(e):
        if isinstance(e, IntegrityError):
            return True
        else:
            return False
