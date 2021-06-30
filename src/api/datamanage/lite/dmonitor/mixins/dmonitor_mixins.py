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


import time
from django.utils.translation import ugettext as _

from common.log import logger
from common.bklanguage import BkLanguage, bktranslates

from datamanage import pizza_settings as settings
from datamanage import exceptions as dm_errors
from datamanage.exceptions import DatamanageErrorCode
from datamanage.utils.api import MetaApi, JobnaviApi, DataflowApi, ModelApi
from datamanage.utils.dbtools.influx_util import influx_query
from datamanage.utils.dbtools.kafka_util import send_kafka_messages
from datamanage.utils.dmonitor.metricmanager import MetricManager
from datamanage.utils.num_tools import safe_int
from datamanage.utils.time_tools import timetostr


def get_dmonitor_ret(
    self, input_metric, output_metric, drop_metric, input_sql, output_sql, drop_sql, rt_id, nowtime_str
):
    input_data = input_metric.query(sql=input_sql)
    output_data = output_metric.query(sql=output_sql)
    drop_data = drop_metric.query(sql=drop_sql)

    try:
        input_cnt = 0
        for point in input_data.get_points():
            input_cnt += safe_int(point.get('cnt', 0), 0)
    except Exception:
        input_cnt = 0

    try:
        output_cnt = 0
        for point in output_data.get_points():
            output_cnt += safe_int(point.get('cnt', 0), 0)
    except Exception:
        output_cnt = 0

    try:
        drop_cnt = 0
        for point in drop_data.get_points():
            drop_cnt += safe_int(point.get('cnt', 0), 0)
    except Exception:
        drop_cnt = 0
    result = {
        'alerts': self.query_rt_alerts(rt_id),
        'status': {
            'input_cnt_10min': input_cnt,
            'input_cnt': safe_int(input_cnt // 10, 0),
            'output_cnt': safe_int(output_cnt // 10, 0),
            'drop_cnt': safe_int(drop_cnt // 10, 0),
            'stat_time': nowtime_str,
        },
    }
    return result


class DmonitorMixin(object):
    def query_rt_status(self, rt_ids):
        rt_infos = {}
        rt_results = {}

        # 查询TRT表的定义， 判断是哪种类型的RT
        response = MetaApi.result_tables.list(
            {
                'result_table_ids': rt_ids,
            }
        )
        if not response.is_success():
            raise dm_errors.QueryResultTableError(message_kv={'message': response.message})
        else:
            result_tables = response.data

        rt_infos = {item['result_table_id']: item for item in result_tables}
        # 把RT拆分成不同分类的
        for rt_id in rt_ids:
            if rt_id not in rt_infos:
                rt_results[rt_id] = "unknown rt %s" % rt_id
                continue
            processing_type = rt_infos[rt_id].get('processing_type')

            if processing_type == 'clean':
                rt_results[rt_id] = self.query_clean_rt_status(rt_infos[rt_id])
            elif processing_type == 'batch':
                rt_results[rt_id] = self.query_offline_rt_status(rt_infos[rt_id])
            elif processing_type == 'stream':
                rt_results[rt_id] = self.query_realtime_rt_status(rt_infos[rt_id])
            else:
                rt_results[rt_id] = "unknown rt type %s" % processing_type

        return rt_results

    def query_clean_rt_status(self, rt_info):
        result = False
        nowtime = time.time()
        nowtime_str = timetostr(nowtime)
        rt_id = rt_info.get('result_table_id', '')
        try:
            # 按rt查询metric
            manager = MetricManager()
            input_metric = manager.dmonitor_input_metric()
            output_metric = manager.dmonitor_output_metric()
            drop_metric = manager.dmonitor_drop_metric()

            output_sql = (
                '''
                select sum(data_inc) as cnt from data_loss_output_total
                where logical_tag = '%s' and time >= now() - 15m and time < now() - 5m
                and (component = 'clean') group by module, component;
            '''
                % rt_id
            )
            input_sql = (
                '''
                select sum(data_inc) as cnt from data_loss_input_total
                where logical_tag = '%s' and time >= now() - 15m and time < now() - 5m
                and (component = 'clean') group by module, component;
            '''
                % rt_id
            )
            drop_sql = (
                '''
                select sum(data_cnt) as cnt from data_loss_drop
                where logical_tag = '%s' and time >= now() - 15m and time < now() - 5m
                and (component = 'clean') group by module, component;
            '''
                % rt_id
            )

            result = get_dmonitor_ret(
                self, input_metric, output_metric, drop_metric, input_sql, output_sql, drop_sql, rt_id, nowtime_str
            )
        except Exception as e:
            logger.error("failed to query rt %s status %s" % (rt_id, e), DatamanageErrorCode.LOGIC_QUERY_USE_RESULT_ERR)
        return result

    def query_offline_rt_status(self, rt_info, offline_query_periods=None):
        result_table_id = rt_info.get('result_table_id')

        final_result = {
            'status': self.get_batch_executions(result_table_id, offline_query_periods),
            'alerts': self.query_rt_alerts(result_table_id),
        }
        return final_result

    def get_batch_executions(self, processing_id=None, result_table_id=None, period=None):
        processing_type = None
        geog_areas = []
        if processing_id is None:
            try:
                res = MetaApi.result_tables.retrieve(
                    {'result_table_id': result_table_id, 'related': ['data_processing']}
                )
                processing_id = res.data.get('data_processing', {}).get('processing_id')
                processing_type = res.data.get('processing_type')
                geog_areas = res.data.get('tags', {}).get('manage', {}).get('geog_area', [])
            except Exception:
                raise dm_errors.QueryScheduleInfoError(
                    _('无法获取结果表({result_table_id})所属任务的调度信息'.format(result_table_id=result_table_id))
                )
        else:
            try:
                res = MetaApi.data_processings.retrieve({'processing_id': processing_id})
                processing_type = res.data.get('processing_type')
                geog_areas = res.data.get('tags', {}).get('manage', {}).get('geog_area', [])
            except Exception:
                raise dm_errors.QueryScheduleInfoError(
                    _('无法获取数据处理任务({processing_id})的调度信息'.format(processing_id=processing_id))
                )

        if processing_type == 'batch':
            try:
                res = DataflowApi.get_batch_job_param({'job_id': processing_id}, raise_exception=True)
                job_exec_config = res.data
            except Exception as e:
                raise dm_errors.QueryScheduleInfoError(_('获取离线任务调度周期失败, 原因: {error}'.format(error=e)))
        elif processing_type == 'model':
            try:
                res = ModelApi.model_schedule_info({'result_table_id': result_table_id}, raise_exception=True)
                job_exec_config = res.data
            except Exception as e:
                raise dm_errors.QueryScheduleInfoError(_('获取模型任务调度周期失败, 原因: {error}'.format(error=e)))
        else:
            raise dm_errors.QueryScheduleInfoError(
                _('暂不支持获取{processing_type}类型任务的调度信息'.format(processing_type=processing_type))
            )

        interval = job_exec_config.get('schedule_period', 'hour')
        count_freq = job_exec_config.get('count_freq', 1)
        if interval == 'hour':
            periods = safe_int(period, 0) if safe_int(period, 0) > 0 else 48
            interval_sec = 3600 * count_freq
        elif interval == 'day':
            periods = safe_int(period, 0) if safe_int(period, 0) > 0 else 7
            interval_sec = 86400 * count_freq
        else:
            periods = safe_int(period, 0) if safe_int(period, 0) > 0 else 48
            interval_sec = 3600 * count_freq

        result = {"interval": interval_sec, "execute_history": []}

        try:
            params = {'schedule_id': processing_id, 'limit': periods}
            geog_area_codes = [x.get('code') for x in geog_areas]
            if geog_area_codes:
                params['tags'] = geog_area_codes
            params['cluster_id'] = self.get_jobnavi_config(geog_area_codes) or 'default'

            execute_records = JobnaviApi.get_cluster_execute_info(params).data
            for execute_record in execute_records:
                job_status_code, job_status_str, err_code, err_msg = self.get_batch_detail(execute_record)

                start_time = timetostr((execute_record.get('created_at') or 0) // 1000, '%Y-%m-%d %H:%M:%S')
                period_start_timestamp = (execute_record.get('schedule_time') or 0) // 1000
                period_end_timestamp = (execute_record.get('schedule_time') or 0) // 1000 + interval_sec

                period_start_time = self.get_schedule_time_display(period_start_timestamp)
                period_end_time = self.get_schedule_time_display(period_end_timestamp)
                try:
                    end_time = timetostr((execute_record.get('updated_at') or 0) // 1000, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    end_time = period_end_time
                exe_history = {
                    'status': job_status_code,
                    'status_str': job_status_str,
                    'execute_id': execute_record.get('execute_info', {}).get('id'),
                    'src_status': execute_record.get('status'),
                    'end_time': end_time,
                    'start_time': start_time,
                    'err_msg': err_msg,
                    'err_code': err_code,
                    'period_start_time': period_start_time,
                    'period_end_time': period_end_time,
                }
                result['execute_history'].append(exe_history)
        except Exception as e:
            logger.error('failed to query job svr records %s ' % e, DatamanageErrorCode.LOGIC_QUERY_USE_RESULT_ERR)

        return result

    def get_jobnavi_config(self, geog_areas):
        try:
            res = JobnaviApi.get_jobnavi_config({'tags': geog_areas}, raise_exception=True)
            if len(res.data) > 0:
                return res.data[0].get('cluster_name')
        except Exception as e:
            logger.error(_('获取Jobnavi集群信息失败: {error}').format(error=e))
        return None

    def get_batch_detail(self, execute_record):
        status = execute_record.get('status', '').lower()
        err_code, err_msg = '', ''
        if status in ['failed', 'killed', 'skipped', 'disabled', 'decommissioned']:
            job_status_code = 'fail'
            job_status_str = _('失败')
            if status in ['killed', 'disabled']:
                err_code = '-1'
                err_msg = _('前序节点执行失败')
            elif status == 'skipped':
                err_code = '-3'
                err_msg = _('前序节点未执行，请等待下个周期')
            elif status == 'decommissioned':
                err_code = '-2'
                err_msg = _('任务已退役')
        elif status == 'failed_succeeded':
            job_status_code = 'warning'
            job_status_str = _('警告')
        elif status == 'running':
            job_status_str = _('运行中')
            job_status_code = 'running'
        elif status in ['preparing', 'none']:
            job_status_str = _('等待中')
            job_status_code = 'waiting'
        else:
            job_status_str = _('成功')
            job_status_code = 'finished'

        try:
            if execute_record['info']:
                err_code, err_msg = execute_record['info'].split(' -> ')
        except Exception:
            err_code, err_msg = '', execute_record['info']
        err_msg = bktranslates(err_msg)

        return job_status_code, job_status_str, err_code, err_msg

    def get_schedule_time_display(self, schedule_timestamp):
        if BkLanguage.current_language() == BkLanguage.CN:
            schedule_time = timetostr(schedule_timestamp, '%Y年%m月%d日%H时'.encode('utf-8')).decode('utf-8')
        else:
            try:
                schedule_time = timetostr(schedule_timestamp, '%Y.%m.%d %-I %p')
            except Exception:
                schedule_time = timetostr(schedule_timestamp, '%Y.%m.%d %I %p')
        return schedule_time

    def query_realtime_rt_status(self, rt_info):
        result = False
        nowtime = time.time()
        nowtime_str = timetostr(nowtime)
        rt_id = rt_info.get('result_table_id', '')
        try:
            # 按rt查询metric
            manager = MetricManager()
            input_metric = manager.dmonitor_input_metric()
            output_metric = manager.dmonitor_output_metric()
            drop_metric = manager.dmonitor_drop_metric()

            output_sql = (
                "select sum(data_inc) as cnt from data_loss_output_total "
                "where logical_tag = '%s' and time >= now() - 15m and time < now() - 5m "
                "and (module = 'stream');" % rt_id
            )
            input_sql = (
                "select sum(data_inc) as cnt from data_loss_input_total "
                "where logical_tag = '%s' and time >= now() - 15m and time < now() - 5m "
                "and (module = 'stream');" % rt_id
            )
            drop_sql = (
                "select sum(data_cnt) as cnt from data_loss_drop "
                "where logical_tag = '%s' and time >= now() - 15m and time < now() - 5m "
                "and (module = 'stream');" % rt_id
            )

            result = get_dmonitor_ret(
                self, input_metric, output_metric, drop_metric, input_sql, output_sql, drop_sql, rt_id, nowtime_str
            )
        except Exception as e:
            logger.error("failed to query rt %s status %s" % (rt_id, e), DatamanageErrorCode.LOGIC_QUERY_USE_RESULT_ERR)
        return result

    def query_rt_alerts(self, rt_id):
        alerts = []
        try:
            sql = (
                """
                SELECT * FROM dmonitor_alerts
                WHERE result_table_id = '%s' and time > now() - 1h
                GROUP BY alert_code ORDER BY time desc limit 5
            """
                % rt_id
            )
            database = "monitor_data_metrics"
            alert_list = influx_query(sql, db=database, is_dict=True)
            message_key = 'message_zh-cn' if BkLanguage.current_language() == BkLanguage.CN else 'message_en'
            for alert in alert_list:
                alerts.append(
                    {
                        "start_time": timetostr(alert.get('time', time.time())),
                        "msg": alert.get(message_key, alert.get('message', '')),
                    }
                )
        except Exception:
            return alerts

        return alerts

    def send_metrics_to_kafka(
        self,
        message,
        kafka_cluster=settings.DMONITOR_KAFKA_CLUSTER,
        kafka_topic=settings.DMONITOR_RAW_METRICS_KAFKA_TOPIC,
        geog_area=None,
    ):
        try:
            send_kafka_messages(kafka_cluster, kafka_topic, [message], geog_area)
        except dm_errors.DatamanageError as e:
            raise e
        except Exception as e:
            logger.error(e)
            raise dm_errors.KafkaSendMessageError()
        return True
