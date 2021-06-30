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

summary:
    This library is provided to allow standard python
    logging to output log data as JSON formatted strings
    ready to be shipped out to logstash.
"""

import datetime
import json
import logging
import socket
import traceback as tb


def _default_json_default(obj):
    """
    Coerce everything to strings.
    All objects representing time get output as ISO8601.
    """
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    else:
        return str(obj)


class LogstashFormatter(logging.Formatter):
    """
    A custom formatter to prepare logs to be
    shipped out to logstash.
    """

    def __init__(self, fmt=None, datefmt=None, style="%", json_cls=None, json_default=_default_json_default):
        """
        :param fmt: Config as a JSON string, allowed fields;
               extra: provide extra fields always present in logs
               source_host: override source host name
        :param datefmt: Date format to use (required by logging.Formatter
            interface but not used)
        :param json_cls: JSON encoder to forward to json.dumps
        :param json_default: Default JSON representation for unknown types,
                             by default coerce everything to a string
        """

        if fmt is not None:
            self._fmt = json.loads(fmt)
        else:
            self._fmt = {}
        self.json_default = json_default
        self.json_cls = json_cls
        if "extra" not in self._fmt:
            self.defaults = {}
        else:
            self.defaults = self._fmt["extra"]
        if "source_host" in self._fmt:
            self.source_host = self._fmt["source_host"]
        else:
            try:
                self.source_host = socket.gethostname()
            except Exception:
                self.source_host = ""

    def format(self, record):
        """
        Format a log record to JSON, if the message is a dict
        assume an empty message and use the dict as additional
        fields.
        """

        fields = record.__dict__.copy()

        if isinstance(record.msg, dict):
            fields.update(record.msg)
            fields.pop("msg")
            msg = ""
        else:
            msg = record.getMessage()

        try:
            msg = msg.format(**fields)
        except (KeyError, IndexError, ValueError):
            pass
        except Exception:
            # in case we can not format the msg properly we log it as is instead of crashing
            msg = msg

        if "msg" in fields:
            fields.pop("msg")

        if "exc_info" in fields:
            if fields["exc_info"]:
                formatted = tb.format_exception(*fields["exc_info"])
                fields["exception"] = formatted
            fields.pop("exc_info")

        if "exc_text" in fields and not fields["exc_text"]:
            fields.pop("exc_text")

        logr = self.defaults.copy()

        logr.update(
            {
                "@message": msg,
                "@timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "@source_host": self.source_host,
                "@fields": self._build_fields(logr, fields),
            }
        )

        return json.dumps(logr, default=self.json_default, cls=self.json_cls)

    def _build_fields(self, defaults, fields):
        """Return provided fields including any in defaults

        >>> f = LogstashFormatter()
        # Verify that ``fields`` is used
        >>> f._build_fields({}, {'foo': 'one'}) == \
                {'foo': 'one'}
        True
        # Verify that ``@fields`` in ``defaults`` is used
        >>> f._build_fields({'@fields': {'bar': 'two'}}, {'foo': 'one'}) == \
                {'foo': 'one', 'bar': 'two'}
        True
        # Verify that ``fields`` takes precedence
        >>> f._build_fields({'@fields': {'foo': 'two'}}, {'foo': 'one'}) == \
                {'foo': 'one'}
        True
        """
        return dict(list(defaults.get("@fields", {}).items()) + list(fields.items()))


class LogstashFormatterV1(LogstashFormatter):
    """
    A custom formatter to prepare logs to be
    shipped out to logstash V1 format.
    """

    def format(self, record):
        """
        Format a log record to JSON, if the message is a dict
        assume an empty message and use the dict as additional
        fields.
        """

        fields = record.__dict__.copy()

        if "msg" in fields and isinstance(fields["msg"], dict):
            msg = fields.pop("msg")
            fields.update(msg)

        elif "msg" in fields and "message" not in fields:
            msg = record.getMessage()
            # fields.pop('msg')

            try:
                msg = msg.format(**fields)
            except (KeyError, IndexError):
                pass
            except Exception:
                # in case we can not format the msg properly we log it as is instead of crashing
                msg = msg
            fields["message"] = msg

        if "exc_info" in fields:
            if fields["exc_info"]:
                formatted = tb.format_exception(*fields["exc_info"])
                fields["exception"] = formatted
            fields.pop("exc_info")

        if "exc_text" in fields and not fields["exc_text"]:
            fields.pop("exc_text")

        now = datetime.datetime.utcnow()
        base_log = {
            "@timestamp": now.strftime("%Y-%m-%dT%H:%M:%S") + ".%03d" % (now.microsecond / 1000) + "Z",
            "@version": 1,
            "source_host": self.source_host,
        }
        base_log.update(fields)

        logr = self.defaults.copy()
        logr.update(base_log)

        return json.dumps(logr, default=self.json_default, cls=self.json_cls)
